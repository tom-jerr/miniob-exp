# UPDATE + 标准事务 + MVCC/OCC 实现计划

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** 让 `UPDATE` 在三类表引擎上工作，并把 heap 的 MVCC 补成可回滚、可恢复的追加写多版本事务，另加 Snapshot Isolation 的 OCC TrxKit。

**Architecture:** 采用 PostgreSQL 风格的追加写元组版本：`UPDATE = 老版本 end_xid=-T + 新版本 begin_xid=-T`；用 `TxnStatusTable` 做一次性的原子提交发布，逐行回填 `commit_seq` 可延后；OCC 复用同一套版本存储，额外维护 read set 并在提交时校验。

**Tech Stack:** C++17、CMake、gtest、MiniOB BufferPool/RecordManager/B+tree/Clog、obclient。

**Spec:** `docs/docs/design/miniob-update-mvcc-occ.md`

## Global Constraints

- 构建统一用 `bash build.sh debug --make -j8`（本机 32 核，限制并行度）。
- 不新增第三方依赖；沿用现有 gtest、clog、buffer pool。
- `deps/3rd/jsoncpp`、`deps/3rd/libevent` 是用户既有改动，不暂存、不修改。
- 现有 vacuous 行为必须不变：heap/btree/lsm 既有测试与 `basic` SQL 套件保持绿。
- 已知基线失败不作为门槛：`bplus_tree_log_test` concurrency 的 latch 断言、
  `vectorized-basic`、`vectorized-aggregation-and-group-by`、`dblab-sort`
  缺 result 文件。
- SQL case 的 `.result` 用 harness 写出的 `*.result.tmp` 生成，不手改。
- 每个任务只暂存本任务路径；完成时报告命令与输出。

## Review Focus

- 提交发生在 `COMMIT` 日志落盘之后、状态发布之前崩溃：恢复后必须全提交或全回滚。
- 读事务在提交回填进行中扫描：必须一次看到事务的全部行，不能看到一半。
- `UPDATE` 改变主键/索引列：旧索引项不能误指向新版本，失败时旧行与旧索引必须恢复。
- 旧 MVCC 表（4 字节 xid 字段）被新二进制打开：必须明确报错，不能按 8 字节误读。
- OCC 读到后又提交修改的行：提交校验必须中止，而不是静默覆盖。

---

### Task 1: 8 字节事务字段与兼容性守卫

**Review:** per-task — persisted format

**Files:**
- Modify: `src/observer/storage/field/field.h`, `src/observer/storage/field/field.cpp`
- Modify: `src/observer/storage/trx/mvcc_trx.h`, `src/observer/storage/trx/mvcc_trx.cpp`
- Modify: `src/observer/storage/table/table.cpp`
- Create: `unittest/observer/mvcc_xid_test.cpp`

**Interfaces:**
- Produces: `void Field::set_int64(Record &record, int64_t value); int64_t Field::get_int64(const Record &record);`
- Produces: `int64_t MvccTrxKit::next_trx_id(); int64_t MvccTrxKit::max_trx_id() const;`
- Produces: `RC MvccTrxKit::validate_trx_fields(const TableMeta &meta) const;`

- [ ] **Step 1: Write the failing test** in `unittest/observer/mvcc_xid_test.cpp`

Create a Db with `trx_kit_name="mvcc"`, create a table, then assert:
`db->trx_kit().trx_fields()->at(0).len() == 8`, `max_trx_id() == INT64_MAX`；
insert + sync + reopen，扫描可见；构造一个 len=4 的 `TableMeta`（复制真实的 meta 后把
隐藏字段 len 改为 4），断言 `validate_trx_fields(meta) == RC::INVALID_ARGUMENT`。

- [ ] **Step 2: Run it to verify it fails**

Run: `bash build.sh debug --make -j8 && ./build_debug/unittest/mvcc_xid_test`
Expected: 编译失败（`set_int64`/`validate_trx_fields` 不存在）。

- [ ] **Step 3: Implement**

`Field` 用 `memcpy` 读写 8 字节；`MvccTrxKit::init` 的两个字段 len 改 8；
计数器/`max_trx_id` 改 `int64_t`；`validate_trx_fields` 检查两个隐藏字段
`len()==8`，否则日志含表名并返回 `INVALID_ARGUMENT`。`Table::open` 之后、
trx 使用前调用该守卫。

- [ ] **Step 4: Run tests**

Run: `./build_debug/unittest/mvcc_xid_test && ./build_debug/unittest/mvcc_trx_log_test`
Expected: PASS。

- [ ] **Step 5: Commit**

```bash
git add src/observer/storage/field/field.h src/observer/storage/field/field.cpp \
  src/observer/storage/trx/mvcc_trx.h src/observer/storage/trx/mvcc_trx.cpp \
  src/observer/storage/table/table.cpp unittest/observer/mvcc_xid_test.cpp
git commit -m "feat(trx): 64-bit tuple xid fields with compatibility guard"
```

---

### Task 2: TxnStatusTable、原子提交与可见性解析

**Review:** per-task — concurrency, durability

**Files:**
- Create: `src/observer/storage/trx/trx_status.h`, `src/observer/storage/trx/trx_status.cpp`
- Modify: `src/observer/storage/trx/mvcc_trx.h/.cpp`, `mvcc_trx_log.h/.cpp`
- Create: `unittest/observer/mvcc_commit_atomic_test.cpp`

**Interfaces:**
- Produces: `class TxnStatusTable` with `RC register_trx(int64_t)`,
  `RC commit_trx(int64_t trx_id, int64_t commit_seq)`, `RC abort_trx(int64_t)`,
  `enum class State { ACTIVE, COMMITTED, ABORTED }`, `State get(int64_t, int64_t *commit_seq)`.
- Produces: `int64_t MvccTrxKit::next_commit_seq();`
- Produces: `int64_t MvccTrx::snapshot_seq() const;`（供 GC 计算水位）
- Consumes: Task 1 的 `get_int64/set_int64` 与 8 字节字段。

- [ ] **Step 1: Write the failing test**

T1 插入 3 行不提交；T2 扫描为 0；T1 `commit()`；T2（新快照）扫描为 3。
再用 `#define private public` 或 record updater 把 3 行的 `begin_xid` 改回 `-T1`
模拟“提交后尚未回填”；新的 T3 扫描仍必须看到 3 行（状态表解析）。
T1 回滚场景：插入 2 行后 rollback，T2 扫描为 0。

- [ ] **Step 2: Run it to verify it fails**

Run: `bash build.sh debug --make -j8 && ./build_debug/unittest/mvcc_commit_atomic_test`
Expected: FAIL（看不到全部行 / 状态表不存在）。

- [ ] **Step 3: Implement**

`MvccTrxKit` 持有 `TxnStatusTable` 与 `atomic<int64_t> commit_seq_`；
`MvccTrx::visit_record` 先用 Resolve 逻辑（负 xid 查状态表）再走现有可见性；
`commit()` 顺序改为：分配 commit_seq → `log_handler_.commit(trx_id, commit_seq)`
等待落盘 → `status_.commit_trx` → 逐行回填 → 清空；
`rollback()` 先 `status_.abort_trx` 再逆序撤销。
日志头中的 `trx_id`/`commit_trx_id` 改 64 位；`MvccTrxLogHandler::commit` 的
`commit_trx_id > trx_id` 断言改为只校验正数。

- [ ] **Step 4: Run tests**

Run: `./build_debug/unittest/mvcc_commit_atomic_test && ./build_debug/unittest/mvcc_xid_test && ./build_debug/unittest/mvcc_trx_log_test`
Expected: PASS。

- [ ] **Step 5: Commit**

```bash
git add src/observer/storage/trx/trx_status.h src/observer/storage/trx/trx_status.cpp \
  src/observer/storage/trx/mvcc_trx.h src/observer/storage/trx/mvcc_trx.cpp \
  src/observer/storage/trx/mvcc_trx_log.h src/observer/storage/trx/mvcc_trx_log.cpp \
  unittest/observer/mvcc_commit_atomic_test.cpp
git commit -m "feat(trx): atomic commit point via transaction status table"
```

---

### Task 3: 标准事务语义（失败事务、幂等结束语句、嵌套 BEGIN）

**Files:**
- Modify: `src/observer/session/session.h/.cpp`, `src/observer/sql/executor/sql_result.cpp`,
  `src/observer/sql/executor/trx_begin_executor.h`, `src/observer/sql/executor/trx_end_executor.h`,
  `src/common/sys/rc.h`（新增 `TRX_ABORTED`）
- Create: `test/case/test/trx_semantics.test` + generated `test/case/result/trx_semantics.result`

**Interfaces:**
- Produces: `bool Session::trx_failed() const; void Session::mark_trx_failed();`
- Produces: 多语句模式下语句失败后，除 ROLLBACK 外的语句返回新增的 `RC::TRX_ABORTED`。

- [ ] **Step 1: Write the failing SQL case**

覆盖：`begin; select * from missing_table; select * from t; commit;`
（第二条 select 必须在 failed 状态下返回 TRX_ABORTED，commit 必须回滚并报错）；
`begin; begin;`（嵌套报错）；未 begin 时 `commit;`/`rollback;` 的约定行为；
自动提交：单条 `insert` 在下一个连接/新事务中可见。

- [ ] **Step 2: Run it to verify it fails**

Run: `cd build_debug && python3 ../test/case/miniob_test.py --test-cases=trx_semantics`
Expected: FAIL（当前失败语句不会标记事务）。

- [ ] **Step 3: Implement**

`SqlResult::close` 在显式事务内语句失败时调用 `mark_trx_failed()`；
`TrxEndExecutor` 遇 failed 事务的 COMMIT 返回错误并执行 rollback，ROLLBACK 正常；
`TrxBeginExecutor` 在已开启事务时返回错误；`Session::destroy_trx` 清 failed 标志。

- [ ] **Step 4: Generate and verify result**

从 `/tmp/miniob/result_output/trx_semantics.result.tmp` 复制为
`test/case/result/trx_semantics.result`，重跑该 case 与 `basic`。
Expected: PASS。

- [ ] **Step 5: Commit**

```bash
git add src/observer/session/session.h src/observer/session/session.cpp \
  src/observer/sql/executor/sql_result.cpp src/observer/sql/executor/trx_begin_executor.h \
  src/observer/sql/executor/trx_end_executor.h test/case/test/trx_semantics.test \
  test/case/result/trx_semantics.result
git commit -m "feat(trx): standard transaction error semantics"
```

---

### Task 4: UPDATE 语句、RBO 计划与 heap 原地更新（vacuous）

**Files:**
- Modify: `src/observer/sql/stmt/update_stmt.h/.cpp`, `src/observer/sql/stmt/stmt.cpp`
- Modify: `src/observer/sql/optimizer/logical_plan_generator.h/.cpp`,
  `src/observer/sql/optimizer/physical_plan_generator.h/.cpp`（cascade 模式下 UPDATE
  返回 `UNSUPPORTED`，v1 只做默认 RBO 计划，见设计 §9.6）
- Modify: `src/observer/sql/operator/logical_operator.h`, `physical_operator.h`
- Create: `src/observer/sql/operator/update_logical_operator.h`
- Create: `src/observer/sql/operator/update_physical_operator.h/.cpp`
- Modify: `src/observer/storage/table/heap_table_engine.h/.cpp`, `src/observer/storage/table/table.h`
- Create: `unittest/observer/update_heap_test.cpp`
- Create: `test/case/test/update_heap.test` + generated result

**Interfaces:**
- Produces: `UpdateStmt` getters `Table *table()`, `const FieldMeta *field()`,
  `const Value &value()`, `FilterStmt *filter_stmt()`; `RC UpdateStmt::create(Db*, const UpdateSqlNode&, Stmt*&)`.
- Produces: `UpdatePhysicalOperator(Table *table, const FieldMeta *field, const Value &value)`
  与 Delete 相同的 `open/next/close/current_tuple` 形状；构造时收集行，再逐行更新。
- Produces: `RC Table::set_value_to_record(char *record_data, const Value &value, const FieldMeta *field)`（从 private 移到 public）。
- Consumes: `RC TableEngine::update_record_with_trx(const Record&, const Record&, Trx*)`（已存在）。

- [ ] **Step 1: Write the failing tests**

`update_heap_test.cpp`：建 heap 表（id, v 带索引），插入 3 行；
调用 `table->update_record_with_trx(old,new,&VacuousTrx)` 更新索引列，扫描与索引点查正确；
更新失败时（构造非法值长度）旧行仍在。
`update_heap.test`：`UPDATE t SET v=... WHERE ...`、无 WHERE、未知列、未知表、
类型错误。

- [ ] **Step 2: Run to verify failures**

Run: `bash build.sh debug --make -j8 && ./build_debug/unittest/update_heap_test`
Expected: FAIL（update_record_with_trx 返回 UNSUPPORTED / SQL 语法解析后无计划）。

- [ ] **Step 3: Implement**

`UpdateStmt::create`：解析表/字段/过滤条件，字段不存在或类型不可转换返回 INVALID_ARGUMENT；
`Stmt::create_stmt` 加 `SCF_UPDATE`；`LogicalPlanGenerator` 生成
`TableGet(READ_WRITE) → Filter? → UpdateLogicalOperator`；
`PhysicalPlanGenerator` 生成 `UpdatePhysicalOperator`；
算子 open 时先像 Delete 一样收集 record，再逐行复制、`set_value_to_record`、
调用 `trx->update_record`。
`HeapTableEngine::update_record_with_trx`：保存旧数据 → 原地 `update_record` →
对每个索引 delete 旧项 / insert 新项；任一步失败则恢复记录与索引，返回错误。

- [ ] **Step 4: Generate SQL result and run all**

从 harness tmp 生成 `update_heap.result`；运行
`./build_debug/unittest/update_heap_test`、`update_heap`、`basic`。
Expected: PASS。

- [ ] **Step 5: Commit**

```bash
git add src/observer/sql/stmt/update_stmt.h src/observer/sql/stmt/update_stmt.cpp \
  src/observer/sql/stmt/stmt.cpp src/observer/sql/optimizer/logical_plan_generator.* \
  src/observer/sql/optimizer/physical_plan_generator.* src/observer/sql/operator/logical_operator.h \
  src/observer/sql/operator/physical_operator.h src/observer/sql/operator/update_* \
  src/observer/storage/table/heap_table_engine.* src/observer/storage/table/table.h \
  unittest/observer/update_heap_test.cpp test/case/test/update_heap.test \
  test/case/result/update_heap.result
git commit -m "feat(sql): UPDATE on heap tables with index maintenance"
```

---

### Task 5: UPDATE 在 btree 聚簇引擎（vacuous）

**Files:**
- Modify: `src/observer/storage/table/btree_table_engine.h/.cpp`
- Modify: `unittest/observer/btree_table_engine_test.cpp`

**Interfaces:**
- Consumes: `BtreeTableEngine::build_key_from_record`（现有 private）；
- Produces: 覆盖 `RC update_record_with_trx(const Record &old, const Record &new, Trx*)`。

- [ ] **Step 1: Write failing test cases**

在 `btree_table_engine_test.cpp` 加：单列/复合主键表更新非主键列后
`get_record_by_key` 返回新值、次级索引查询正确；更新主键后旧 key 取不到、新 key 可取；
更新到已存在主键返回 `RECORD_DUPLICATE_KEY` 且旧行仍在；删除后再更新返回错误。

- [ ] **Step 2: Run and verify failure**

Run: `bash build.sh debug --make -j8 && ./build_debug/unittest/btree_table_engine_test`
Expected: FAIL（UNSUPPORTED）。

- [ ] **Step 3: Implement**

主键不变：`tree.update_entry(key, new_data, record_size)`（新增方法，内部在叶子页
覆盖 value 并写同样的 mtr 日志；或 delete+insert，若采用后者需在同一 mtr 内完成）。
主键变化：先插新 key（冲突则失败返回），成功后删旧 key；任一失败回滚新 key。
两种路径都要 delete 旧二级索引项、insert 新索引项；失败时恢复旧树项与索引。
只有 vacuous trx 可用；mvcc/occ 调用返回 `UNSUPPORTED`。

- [ ] **Step 4: Run tests**

Run: `./build_debug/unittest/btree_table_engine_test && ./build_debug/unittest/bplus_tree_modes_test`
Expected: PASS。

- [ ] **Step 5: Commit**

```bash
git add src/observer/storage/table/btree_table_engine.h src/observer/storage/table/btree_table_engine.cpp \
  src/observer/storage/index/bplus_tree.h src/observer/storage/index/bplus_tree.cpp \
  unittest/observer/btree_table_engine_test.cpp
git commit -m "feat(btree): update clustered rows and secondary indexes"
```

---

### Task 6: UPDATE 在 LSM 引擎（vacuous）

**Files:**
- Modify: `src/observer/storage/table/lsm_table_engine.h/.cpp`
- Modify: `unittest/observer/lsm_table_engine_test.cpp`

- [ ] **Step 1: Write failing test**

LSM 表插入 3 行，创建二级索引；更新非索引列与索引列后扫描/索引查询正确；
更新时索引插入失败（可用重复主键编码或注入失败）恢复旧 value；重启后新值仍在。

- [ ] **Step 2: Run to verify failure**

Run: `bash build.sh debug --make -j8 && ./build_debug/unittest/lsm_table_engine_test`
Expected: FAIL（UNIMPLEMENTED）。

- [ ] **Step 3: Implement**

`update_record_with_trx`：从 `old_record.key()` 取 LSM key；先读旧 value 备份；
`lsm_->put(key, new_record.data(), len)`；删除旧索引项、插入新项；
失败时 `put` 旧 value 并恢复索引。`record.key()` 保持不变；
`get_record_scanner` 走现有 WAL/memtable。若 memtable 触发冻结，返回既有
`UNIMPLEMENTED` 并记录为已知限制（ObLsm SSTable 实验未完成）。

- [ ] **Step 4: Run tests**

Run: `./build_debug/unittest/lsm_table_engine_test && ./build_debug/unittest/oblsm_recovery_test`
Expected: PASS。

- [ ] **Step 5: Commit**

```bash
git add src/observer/storage/table/lsm_table_engine.h src/observer/storage/table/lsm_table_engine.cpp \
  unittest/observer/lsm_table_engine_test.cpp
git commit -m "feat(lsm): update rows and secondary indexes"
```

---

### Task 7: MVCC UPDATE（删除标记 + 新版本）

**Review:** per-task — concurrency

**Files:**
- Modify: `src/observer/storage/trx/mvcc_trx.h/.cpp`
- Create: `unittest/observer/mvcc_update_test.cpp`

**Interfaces:**
- Consumes: `Table::visit_record`、`Table::insert_record`、`Table::delete_record`、索引维护。
- Produces: `RC MvccTrx::update_record(Table*, Record &old_record, Record &new_record)`。

- [ ] **Step 1: Write failing test**

T1 `begin`；读一行构造新 record；`update_record`；T1 内扫描看到新值、旧值不可见；
T2 快照仍看到旧值；T1 commit 后新事务看到新值、旧 RID 不可见；索引查询同样。
T1 rollback 后新事务仍看到旧值；T2 对同一行的并发 update 返回
`LOCKED_CONCURRENCY_CONFLICT`。

- [ ] **Step 2: Run verify fail**

Run: `bash build.sh debug --make -j8 && ./build_debug/unittest/mvcc_update_test`
Expected: FAIL（UNIMPLEMENTED）。

- [ ] **Step 3: Implement**

`update_record`：对旧行执行与 delete 相同的可见性/冲突检查，`end_xid=-trx_id_`，
记 `DELETE(old)`；把 `new_record` 的字段值复制为一行新 record（保留新数据、
RID 清空），设 `begin_xid=-trx_id_`、`end_xid=INF`，调用 `table->insert_record`，
记 `INSERT(new_rid)`；旧索引项保留，新行插入新索引项。
提交/回滚沿用 Task 2 的协议与逆序撤销；`MvccTrxLogHandler` 记录 DELETE+INSERT
两条逻辑日志（不新增 UPDATE 日志类型）。
`HeapTableEngine::update_record_with_trx` 对 MVCC 不使用；若被调用返回 `UNSUPPORTED`。

- [ ] **Step 4: Run tests**

Run: `./build_debug/unittest/mvcc_update_test && ./build_debug/unittest/mvcc_commit_atomic_test && ./build_debug/unittest/mvcc_trx_log_test`
Expected: PASS。

- [ ] **Step 5: Commit**

```bash
git add src/observer/storage/trx/mvcc_trx.h src/observer/storage/trx/mvcc_trx.cpp \
  unittest/observer/mvcc_update_test.cpp
git commit -m "feat(mvcc): update as delete plus new version"
```

---

### Task 8: 崩溃恢复与原子提交验证

**Review:** per-task — durability, recovery

**Files:**
- Modify: `src/observer/storage/trx/mvcc_trx_log.h/.cpp`（COMMIT 重放调用状态发布/回填）
- Modify: `unittest/observer/mvcc_trx_log_test.cpp`

- [ ] **Step 1: Write failing crash tests**

复用现有“复制未落盘文件后重开”的模式加两个场景：
(a) 3 行插入 + commit 日志已落盘、状态表尚未发布（进程重启后状态表天然丢失）、
数据页未 sync → 重开后 3 行全可见；
(b) 3 行插入 + 无 commit 日志 → 重开后 0 行可见。
再加一个 UPDATE + commit 的场景：新版本可见、旧版本不可见。

- [ ] **Step 2: Run verify fail**

Run: `bash build.sh debug --make -j8 && ./build_debug/unittest/mvcc_trx_log_test`
Expected: 新场景 FAIL。

- [ ] **Step 3: Implement**

`MvccTrxLogReplayer::replay` 收到 COMMIT：从日志取 commit_seq，调用
`TxnStatusTable.commit_trx`，执行回填（`recovering_` 路径），销毁对象；
`on_done` 对剩余事务 rollback（已有）。确保回填可重复执行（幂等）：
已是 commit_seq 的行跳过，负 xid 才改写。

- [ ] **Step 4: Run tests**

Run: `./build_debug/unittest/mvcc_trx_log_test && ./build_debug/unittest/mvcc_commit_atomic_test`
Expected: PASS。

- [ ] **Step 5: Commit**

```bash
git add src/observer/storage/trx/mvcc_trx_log.h src/observer/storage/trx/mvcc_trx_log.cpp \
  unittest/observer/mvcc_trx_log_test.cpp
git commit -m "fix(mvcc): atomic commit and recovery replay"
```

---

### Task 9: GC（死版本与索引清理）

**Review:** per-task — concurrency, data deletion

**Files:**
- Create: `src/observer/storage/trx/mvcc_gc.h/.cpp`
- Modify: `src/observer/storage/trx/mvcc_trx.cpp`（提交后触发/提供入口）
- Create: `unittest/observer/mvcc_gc_test.cpp`

**Interfaces:**
- Produces: `class MvccGc { RC collect(Db &db); }` 与 `void MvccTrxKit::set_gc(MvccGc*)`（或等价持有方式）。
- Consumes: `MvccTrxKit::all_trxes`、`MvccTrx::snapshot_seq()`、`TxnStatusTable`。
- 回收完成后可清理不再被任何 `-T` 引用的状态表项（按低水位）。

- [ ] **Step 1: Write failing test**

制造：已提交的 UPDATE 旧版本、已回滚的插入、已提交插入；关闭/推进活跃事务后
调用 `collect`；断言死版本被物理删除、索引项不再指向死 RID、可见行与索引查询不变；
仍有老快照事务时不得回收。

- [ ] **Step 2: Run verify fail**

Run: `bash build.sh debug --make -j8 && ./build_debug/unittest/mvcc_gc_test`
Expected: FAIL。

- [ ] **Step 3: Implement**

水位 = 活跃事务 snapshot_seq 最小值（无活跃事务取 commit_seq）。
遍历 heap 表记录：`begin==0` 或 `end>0 && end<watermark` 为死版本；
用记录数据删除其索引项，再 `table->delete_record` 释放槽位；
失败记录日志并跳过，不中断其它回收。提交后按阈值触发一次
（例如每 N 次提交或显式 `sync` 时），不做后台线程。

- [ ] **Step 4: Run tests**

Run: `./build_debug/unittest/mvcc_gc_test && ./build_debug/unittest/mvcc_update_test && ./build_debug/unittest/mvcc_commit_atomic_test`
Expected: PASS。

- [ ] **Step 5: Commit**

```bash
git add src/observer/storage/trx/mvcc_gc.h src/observer/storage/trx/mvcc_gc.cpp \
  src/observer/storage/trx/mvcc_trx.h src/observer/storage/trx/mvcc_trx.cpp \
  unittest/observer/mvcc_gc_test.cpp
git commit -m "feat(mvcc): garbage collect dead versions below watermark"
```

---

### Task 10: OCC TrxKit（Snapshot Isolation）

**Review:** per-task — concurrency

**Files:**
- Create: `src/observer/storage/trx/occ_trx.h/.cpp`
- Modify: `src/observer/storage/trx/trx.h/.cpp`（`Type::OCC`，`create("occ")`）
- Modify: `src/observer/storage/trx/mvcc_trx.h`（把子类需要的成员/方法改为 protected）
- Create: `unittest/observer/occ_trx_test.cpp`

**Interfaces:**
- Produces: `class OccTrx : public MvccTrx` 覆盖 `visit_record` 与 `commit`；
- Produces: `TrxKit *TrxKit::create(const char *name, Db*)` 支持 `"occ"`。
- Consumes: Task 2 的状态表/提交协议、Task 7 的 update。

- [ ] **Step 1: Write failing tests**

T1 begin，读到行 R（记录 read set）；T2 更新 R 并提交；T1 commit 必须返回
`LOCKED_CONCURRENCY_CONFLICT` 且不生效；T1 回滚。
无冲突的 T1/T2 各自提交成功；写写冲突在执行期返回冲突；
未提交行的脏读不可见；OCC 与 MVCC 对同一表混合使用时版本可见性一致。

- [ ] **Step 2: Run verify fail**

Run: `bash build.sh debug --make -j8 && ./build_debug/unittest/occ_trx_test`
Expected: FAIL（unknown trx kit / 无 read-set 校验）。

- [ ] **Step 3: Implement**

`OccTrx` 在 `visit_record` 只读可见时记录 `{table,rid,begin,end}`；
`commit` 先校验 write set（旧行仍为 `-T`），再逐条重读 read set，
若 `end_xid` 从 `INF` 变为已提交 csn 或行消失则冲突；通过后调用
`MvccTrx::commit`。`start_if_need` 继承；`rollback` 继承。
`OccTrxKit` 返回 `OccTrx`；`-t occ` 启动可用。审计 `TrxKit::Type` 的所有 switch
（当前 `lsm_record_scanner.cpp` 按 VACUOUS/LSM 分支），OCC 分支走 heap 扫描器；
btree/LSM 表用 occ 时沿用 MvccTrx 的 `UNSUPPORTED` 守卫。

- [ ] **Step 4: Run tests**

Run: `./build_debug/unittest/occ_trx_test && ./build_debug/unittest/mvcc_update_test`
Expected: PASS。

- [ ] **Step 5: Commit**

```bash
git add src/observer/storage/trx/occ_trx.h src/observer/storage/trx/occ_trx.cpp \
  src/observer/storage/trx/trx.h src/observer/storage/trx/trx.cpp \
  src/observer/storage/trx/mvcc_trx.h unittest/observer/occ_trx_test.cpp
git commit -m "feat(trx): OCC kit with snapshot read-set validation"
```

---

### Task 11: 隔离性测试、SQL 用例与文档

**Files:**
- Create: `test/case/test/update_index.test`, `test/case/test/mvcc_update.test` + results
- Create: `unittest/observer/trx_isolation_test.cpp`
- Modify: `docs/docs/design/miniob-transaction.md`（现状与扩展）
- Create: `docs/docs/design/miniob-update-mvcc-occ.md`（本设计，已存在则更新验证结果）

- [ ] **Step 1: Write failing/empty tests**

`trx_isolation_test.cpp`：用两个 MvccTrx/OccTrx 手工驱动，覆盖
脏读、不可重复读、丢失更新、写偏斜（SI 下允许，断言两个最终值）、
OCC 读集冲突；每个场景独立表与事务。

- [ ] **Step 2: Generate SQL results**

`update_index.test`：更新索引列、更新主键、无 WHERE、失败回滚；
`mvcc_update.test`：`begin; update; select; rollback/commit;`。
用 harness 的 `*.result.tmp` 生成 result，重跑两个 case 与 `basic`。

- [ ] **Step 3: Update docs**

在 `miniob-transaction.md` 补：8 字节 xid、状态表、原子提交、UPDATE 版本语义、
GC、OCC 及 SI 限制；注明不实现 MySQL undo log / ARIES CLR 的原因。

- [ ] **Step 4: Full verification**

Run:
`bash build.sh debug --make -j8`
`./build_debug/unittest/mvcc_xid_test ...`（列出全部新增/相关测试）
`./build_debug/unittest/mvcc_trx_log_test`
`cd build_debug && python3 ../test/case/miniob_test.py --test-cases=update_heap,update_index,mvcc_update,trx_semantics,basic`
手工：两个 obclient 会话 `begin/update/commit`、冲突、重启后可见性。
Expected: 全部 PASS；记录已知限制与基线失败。

- [ ] **Step 5: Commit**

```bash
git add test/case/test/update_index.test test/case/result/update_index.result \
  test/case/test/mvcc_update.test test/case/result/mvcc_update.result \
  unittest/observer/trx_isolation_test.cpp docs/docs/design/miniob-transaction.md \
  docs/docs/design/miniob-update-mvcc-occ.md
git commit -m "test/docs: UPDATE, MVCC and OCC coverage"
```

---

## Verification Summary

完成时逐项报告：构建、每个新单测、`mvcc_trx_log_test`、SQL case、手工双会话、
重启恢复；并列出未覆盖边界（SI 非可串行化、无 UNIQUE 约束、btree/LSM 无 MVCC、
旧 4 字节 xid 表不兼容、LSM 冻结仍受 ObLsm 实验限制）。不以“编译通过”代替并发/
恢复正确性。
