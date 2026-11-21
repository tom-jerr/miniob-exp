# 逻辑 CLog + 状态表 + 幂等回填恢复计划

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** 让 heap + MVCC 在崩溃后仅靠 `DiskLogHandler` 的事务逻辑日志重建提交状态，幂等回填已提交行的 `commit_seq`、把未提交行的插入标记为死亡，并恢复 `next_trx_id` / `commit_seq` 水位。

**Architecture:** 事务日志只记录 `(op, table_id, rid)` 与 `COMMIT(trx_id, commit_seq)`，元组字节继续由 `RecordLogReplayer` 按精确 RID + 页 LSN 重放。恢复分两段：`replay()` 只重建内存操作表；`on_done()`（此时 log handler 已 `start()`）先回填已提交事务，再逆序撤销未提交事务，最后推进计数器并清空状态表。所有 xid 改写都按“期望值 → 新值”做条件写，重复恢复不改变结果。

**Tech Stack:** C++17、CMake、gtest、MiniOB `DiskLogHandler` / `IntegratedLogReplayer` / heap record manager / B+tree redo。

**Spec:** `docs/docs/design/miniob-update-mvcc-occ.md` §5、§6.3、§6.4、§9.1。本计划取代 `docs/plans/2026-11-20-update-mvcc-occ.md` 的 Task 8；Task 2 的状态表/可见性接口以本计划为准。

## 前置状态

- 工作树已有未提交的 64 位 xid 半成品：`field.*`、`table.cpp`、`mvcc_trx.*`、`trx.h`，以及未跟踪的 `unittest/observer/mvcc_xid_test.cpp` 和设计/计划文档。执行前先由维护者提交或丢弃这批改动；本计划 Task 1 在其上收尾，不叠加新分支。
- `deps/3rd/jsoncpp`、`deps/3rd/libevent` 是维护者既有改动，不暂存、不修改。
- 本计划不依赖 `UPDATE` 实现：用同一事务内 `delete_record(old) + insert_record(new)` 模拟 UPDATE 日志。

## 与所给方案的偏差（需批准）

1. **TRANSACTION 日志不携带 `tuple_bytes`。** `RecordLogReplayer::replay_insert/update` 已经按日志中的精确 `RID` 重放元组字节，并用 `frame->lsn() >= entry.lsn()` 跳过已落盘页（`record_log.cpp:208-240`）。再放一份字节等于第二份 WAL，且两边可能不一致。恢复器因此只重放 xid 语义，不调用 `insert_record`，也不再需要“按 new_rid 幂等插入堆表 / 忽略索引重复键”。
2. **索引不由事务恢复器补插。** B+tree 自己的 redo 日志带页 LSN 幂等跳过（`bplus_tree_log.cpp:193-217`）。恢复测试只验证“重启后索引查询正确”，不新增 `(K,RID)` 重复键分支。
3. **撤销 INSERT 用 `begin_xid=0`，不删行。** 删除行要先删索引再删槽位，若崩溃在两步之间，重复恢复会撞 `HeapTableEngine::delete_record` 的索引断言。置 0 是单字段条件写，天然幂等；代价是死元组和索引项留给 GC，且 `visit_record` 必须显式把 `begin==0` 判为不可见（现有实现会误判为可见）。
4. **所有数据写入放在 `on_done()`。** `Db::recover()` 在 `replay()` 之后才 `log_handler_->start()`，而 `DiskLogHandler::_append` 未启动时直接 `ASSERT`。在 `replay()` 阶段写堆表会崩。`on_done()` 之后写还能顺带获得 record UPDATE 日志。
5. **没有专属 `OccTrxLogReplayer`。** 恢复逻辑与 OCC 的 read set 无关；`OccTrxKit` 继承 `MvccTrxKit::create_log_replayer()` 即可。当前工作树还没有 `OccTrxKit`，专属类会是空壳；OCC 恢复用例随 master plan Task 10 加入。
6. **恢复从 LSN 0 开始重放。** `commit_seq` 只存在于日志，若从 checkpoint 开始会丢掉旧计数器；所有物理 redo 都有页 LSN 幂等跳过，全量重放安全。日志继续不截断。`check_point_lsn_` 保留但不再决定回放起点。
7. **先写 DELETE 日志、再改 `end_xid`。** 现有顺序是“改页 → 写日志”，崩溃窗口会留下永久 `-T` 删除标记且日志中没有对应操作，行对读者可见、对写者永久冲突。INSERT 受 RID 分配限制仍保持“先插行、后写事务日志”，该窗口只留下一行永久不可见的 `begin<0`，列入已知限制。
8. **恢复不重建状态表的 COMMITTED 项。** 恢复期单线程、无读者；`on_done()` 一次性结算后清空状态表。运行期才用状态表为“已发布提交、尚未回填”的 `-T` 行提供可见性。

## Global Constraints

- 构建：`bash build.sh debug --make -j8`。测试二进制在 `build_debug/unittest/`。
- 仓库默认 `CONCURRENCY=OFF`，`common::Mutex` / `common::SharedMutex` 是空实现；本计划沿用现有锁封装，不额外承诺默认构建的多线程互斥。需要真实并发语义时用 `-DCONCURRENCY` 构建。
- 不新增第三方依赖。
- 旧 4 字节 xid 表和旧 32 位事务日志不兼容；测试目录每次重建，不提供升级路径。
- `replay()` 阶段只允许读，不得调用会落盘的 `Table::insert_record/delete_record/visit_record`。
- 保持 vacuous / LSM / btree 既有行为；本计划的恢复语义只覆盖 heap + MVCC。
- 已知基线失败不作为门槛：`bplus_tree_log_test` 的 latch 断言、`vectorized-basic`、`vectorized-aggregation-and-group-by`、`dblab-sort`。
- 每个任务只暂存本任务路径；完成后报告命令与输出。

## Review Focus

- COMMIT 日志已落盘、数据页未落盘：重启后已提交事务必须全部可见，不能停留在 `-T`。
- 没有 COMMIT 日志：重启后事务的所有行必须不可见；重复恢复不能把已撤销的行复活。
- 崩溃发生在回填一半：再次恢复必须把剩余行补齐，已回填的行跳过且不报错。
- 提交已发布但尚未回填时，旧快照不能看到新行；新快照必须一次看到事务的全部行。
- 清理 checkpoint 后重启：`commit_seq` 不能倒退，否则旧行会突然不可见或新行提前可见。

---

### Task 1: 64 位事务日志 ABI 与计数接口

**Review:** per-task — persisted format

**Files:**
- Modify: `src/observer/storage/trx/mvcc_trx_log.h`, `src/observer/storage/trx/mvcc_trx_log.cpp`
- Modify: `src/observer/storage/trx/trx.h`, `src/observer/storage/trx/vacuous_trx.h/.cpp`, `src/observer/storage/trx/lsm_mvcc_trx.h/.cpp`
- Modify: `src/observer/storage/trx/mvcc_trx.h/.cpp`（去掉 `static_cast<int32_t>`）
- Test: `unittest/observer/mvcc_log_abi_test.cpp`

**Interfaces:**
- Produces: `struct MvccTrxLogHeader { int32_t operation_type; int64_t trx_id; };`
- Produces: `struct MvccTrxCommitLogEntry { MvccTrxLogHeader header; int64_t commit_seq; };`
- Produces: `RC MvccTrxLogHandler::commit(int64_t trx_id, int64_t commit_seq);`
- Produces: `virtual int64_t Trx::id() const = 0;`
- Produces: `virtual Trx *TrxKit::create_trx(LogHandler &, int64_t trx_id) = 0;`

- [ ] **Step 1: Write the failing test** in `unittest/observer/mvcc_log_abi_test.cpp`

用 `DiskLogHandler` 直接 append 一条 `MvccTrxRecordLogEntry`（`trx_id = (1LL << 40) + 7`）和一条
`MvccTrxCommitLogEntry`（`commit_seq = (1LL << 40) + 9`），`wait_lsn` 后 `iterate`，解析并断言：

```cpp
ASSERT_EQ(parsed.insert.header.trx_id, (1LL << 40) + 7);
ASSERT_EQ(parsed.commit.commit_seq, (1LL << 40) + 9);
```

- [ ] **Step 2: Run it to verify it fails**

Run: `bash build.sh debug --make -j8 && ./build_debug/unittest/mvcc_log_abi_test`
Expected: 编译失败（`commit_seq` 不存在 / 结构体仍是 `int32_t`）。

- [ ] **Step 3: Implement**

- `MvccTrxLogHeader::trx_id`、`MvccTrxCommitLogEntry` 的提交号字段改 64 位；字段名用 `commit_seq`。
- `MvccTrxLogHandler::commit` 参数改 `int64_t`；`rollback` 的日志结构体零初始化，避免未初始化字节。
- `Trx::id()`、`TrxKit::create_trx(..., trx_id)` 及三个 kit 的签名改 `int64_t`；删除调用点的 `static_cast<int32_t>`。
- `MvccTrxLogReplayer` 解析日志时用 `memcpy` 到局部结构体，不再 `reinterpret_cast<const T *>(entry.data())`；`entry.data()` 不保证 8 字节对齐。
- 日志 payload 仍用 POD 原始字节，测试目录重建，不做旧日志兼容。

- [ ] **Step 4: Run tests**

Run: `./build_debug/unittest/mvcc_log_abi_test && ./build_debug/unittest/mvcc_trx_log_test`
Expected: PASS。

- [ ] **Step 5: Commit**

只暂存本任务涉及的 trx 头/源文件与测试，提交信息：`feat(trx): 64-bit transaction log ABI`。

---

### Task 2: TxnStatusTable 与快照可见性

**Review:** per-task — concurrency

**Files:**
- Create: `src/observer/storage/trx/trx_status.h`, `src/observer/storage/trx/trx_status.cpp`
- Modify: `src/observer/storage/trx/mvcc_trx.h/.cpp`
- Test: `unittest/observer/txn_status_test.cpp`

**Interfaces:**
- Produces:
  ```cpp
  class TxnStatusTable {
  public:
    enum class State { ACTIVE, COMMITTED, ABORTED };
    void register_trx(int64_t trx_id);
    void commit_trx(int64_t trx_id, int64_t commit_seq);
    void abort_trx(int64_t trx_id);
    State get(int64_t trx_id, int64_t *commit_seq) const;  // 缺失条目返回 ABORTED
    void clear();
    size_t size() const;
  };
  ```
- Produces: `int64_t MvccTrxKit::next_commit_seq();`（只分配，不发布）
- Produces: `int64_t MvccTrxKit::current_commit_seq() const;`（已发布水位，快照读它）
- Produces: `void MvccTrxKit::publish_commit_seq(int64_t commit_seq);`
- Produces: `TxnStatusTable &MvccTrxKit::status();`
- Produces: `int64_t MvccTrx::snapshot_seq() const;`
- Consumes: Task 1 的 `int64_t` id。

- [ ] **Step 1: Write the failing tests**

`TxnStatusTable` 状态机：未注册/ABORTED → `get` 返回 `ABORTED`；`register` 后 `ACTIVE`；
`commit_trx(T, 7)` 后 `COMMITTED` 且 `commit_seq == 7`；`abort_trx` 后 `ABORTED`；`clear` 后 `size()==0`。

未回填提交的可见性：

```cpp
// T1 插入 3 行，begin 仍为 -T1
// T2 start_if_need()：snapshot=0
int64_t seq = kit.next_commit_seq();
kit.status().commit_trx(t1->id(), seq);          // 只发布，不回填
kit.publish_commit_seq(seq);                     // 推进已发布水位
// T2 扫描 3 行 -> 仍全部 RECORD_INVISIBLE（snapshot 0 < seq）
// T3 = create_trx()：snapshot=seq，扫描 -> 3 行可见
// 直接读元组头，断言 begin 仍为 -T1（证明没有靠回填）
```

另外把一行 `begin_xid` 直接写成 0，断言新事务扫描不可见；把 `end_xid` 写成 0，断言仍可见；
把 `end_xid` 写成已提交 `commit_seq <= snapshot`，断言不可见。

快照后提交的删除：

```cpp
// T1 插入并提交（seq=1）；T2 start_if_need() 时 snapshot=1
// T3 删除该行并提交（seq=2）
// T2 READ_ONLY 扫描：仍可见
// T2 READ_WRITE 访问该行：LOCKED_CONCURRENCY_CONFLICT，不能覆盖已提交的删除
```

- [ ] **Step 2: Run it to verify it fails**

Run: `bash build.sh debug --make -j8 && ./build_debug/unittest/txn_status_test`
Expected: FAIL（`TxnStatusTable` 不存在，`begin==0` 被误判为可见）。

- [ ] **Step 3: Implement**

- `TxnStatusTable` 用 `mutable common::SharedMutex` + `unordered_map<int64_t, Status>`；`get` 取共享锁，写操作取独占锁。
- `MvccTrxKit` 持有 `status_`、`atomic<int64_t> next_seq_{0}`、`atomic<int64_t> published_seq_{0}`；`next_commit_seq = ++next_seq_`（只分配），`current_commit_seq = published_seq_.load()`，`publish_commit_seq` 由调用方在提交锁内推进 `published_seq_`。这样提交前取的快照不会看到正在提交的事务。
- `MvccTrx::start_if_need()` 分配 `trx_id_` 后 `status_.register_trx(trx_id_)`，并记录 `snapshot_seq_ = trx_kit_.current_commit_seq()`。
- 重写 `visit_record` 的判定，顺序固定为：
  1. `begin == 0` → `RECORD_INVISIBLE`（已中止的插入）；
  2. `begin < 0`：`-begin == self` 可见；否则仅当 `status == COMMITTED(csn) && csn <= snapshot_seq_` 可见，否则不可见；
  3. `begin > 0 && begin > snapshot_seq_` → 不可见；
  4. 把 `end == 0` 与 `end == max_trx_id()` 归一化为 `INF`；若 `end == INF`，行在删除维度上存活，直接可见；
  5. `end < 0`：`READ_ONLY` 下，`-end == self` 不可见，否则仅当已提交且 `csn <= snapshot_seq_` 不可见；`READ_WRITE` 下 `-end == self` 返回 `RECORD_INVISIBLE`，否则返回 `LOCKED_CONCURRENCY_CONFLICT`；
  6. `end > 0 && end <= snapshot_seq_` → 不可见；
  7. `end > 0 && end > snapshot_seq_`：`READ_ONLY` 可见；`READ_WRITE` 返回 `LOCKED_CONCURRENCY_CONFLICT`（该行在快照之后已被其它事务删除/覆盖，first-committer-wins）。

- [ ] **Step 4: Run tests**

Run: `./build_debug/unittest/txn_status_test && ./build_debug/unittest/mvcc_trx_log_test`
Expected: PASS。

- [ ] **Step 5: Commit**

提交信息：`feat(trx): status table and snapshot-based visibility`。

---

### Task 3: 运行期提交协议：先日志、再发布、后回填

**Review:** per-task — durability, concurrency

**Files:**
- Modify: `src/observer/storage/trx/mvcc_trx.h/.cpp`
- Modify: `src/observer/storage/trx/mvcc_trx_log.h/.cpp`
- Modify: `src/observer/storage/db/db.cpp`（`sync()` 的 checkpoint 守卫）
- Modify: `src/observer/storage/trx/trx.h`（`before_checkpoint()`）
- Test: `unittest/observer/mvcc_commit_protocol_test.cpp`

**Interfaces:**
- Produces: `RC MvccTrx::backfill(int64_t commit_seq);`
- Produces: `RC backfill_operations(const vector<Operation> &ops, int64_t trx_id, int64_t commit_seq);`
- Produces: `RC undo_operations(const vector<Operation> &ops, int64_t trx_id);`
- Produces: `RC rewrite_trx_xid(Table *table, const RID &rid, bool begin_field, int64_t expected, int64_t replacement, bool *already_done);`
- Produces: `virtual RC TrxKit::before_checkpoint() { return RC::SUCCESS; }`
- Produces: `RC MvccTrxKit::before_checkpoint() override;`
- Produces: `common::Mutex &MvccTrxKit::commit_lock();`
- Produces: `void MvccTrxKit::pending_backfill_add(int delta);`
- Produces: `size_t MvccTrxKit::pending_backfills() const;`
- Consumes: Task 2 的 `TxnStatusTable` / `snapshot_seq_`。

- [ ] **Step 1: Write the failing tests**

- `commit` 后：插入行的 `begin_xid == commit_seq`，`status.get(T)` 为 `COMMITTED`，`pending_backfills()==0`。
- `delete_record` 后 `end_xid == -T`；`commit` 后 `end_xid == commit_seq`。
- `insert + rollback` 后：行仍在表中，`begin_xid == 0`，`status.get(T)` 为 `ABORTED`；新事务扫描不可见。
- 回填失败不回滚提交：T1 插入一行后，测试直接 `table->delete_record(record)` 把槽位删掉，再 `T1.commit()`；断言 `commit()` 返回 `SUCCESS`、`pending_backfills()==1`、`db->sync()` 返回 `RC::INTERNAL`。
- 已回填行重复回填：再次调用 `T1.backfill(seq)`（测试可直接调用），断言返回 `SUCCESS` 且 xid 不变。

- [ ] **Step 2: Run it to verify it fails**

Run: `bash build.sh debug --make -j8 && ./build_debug/unittest/mvcc_commit_protocol_test`
Expected: FAIL（当前提交先改行、后写日志；`begin=0` 撤销语义不存在）。

- [ ] **Step 3: Implement**

`MvccTrx::commit()` 固定顺序（`started_ == false` 时直接返回 `SUCCESS`；状态发布后置 `started_=false`）：

```cpp
std::unique_lock<common::Mutex> commit_guard(trx_kit_.commit_lock());
int64_t seq = trx_kit_.next_commit_seq();
rc = log_handler_.commit(trx_id_, seq);          // append + wait_lsn
if (OB_FAIL(rc)) return rc;                      // 提交未成立
trx_kit_.pending_backfill_add(1);                // 先占位，防止 checkpoint 越过
trx_kit_.status().commit_trx(trx_id_, seq);      // 原子提交点
trx_kit_.publish_commit_seq(seq);                // 推进快照水位
started_ = false;
commit_guard.unlock();                           // 回填在锁外
RC backfill_rc = backfill(seq);                  // 失败只记录，不回滚提交
if (OB_SUCC(backfill_rc)) trx_kit_.pending_backfill_add(-1);
else LOG_ERROR(...);
operations_.clear();
return RC::SUCCESS;
```

- `backfill_operations(ops, trx_id, seq)` 对每条：
  - INSERT：`rewrite_trx_xid(table, rid, true, -trx_id, seq, &done)`；`RECORD_NOT_EXIST` 或 `done=false` → `LOG_ENTRY_INVALID`。
  - DELETE：`rewrite_trx_xid(table, rid, false, -trx_id, seq, &done)`；同上。
- `rollback()`：`started_ == false` 时直接返回 `SUCCESS`；否则先 `status_.abort_trx(trx_id_)`，再调用 `undo_operations(operations_, trx_id_)`（内部逆序）：INSERT 写 `begin: -T → 0`（缺失视为已撤销，跳过），DELETE 写 `end: -T → INF`（缺失为错误）。不再调用 `table->delete_record`。
- `delete_record()`：先 `log_handler_.delete_record(...)`，再 `visit_record` 写 `end_xid=-T`；写失败返回错误，事务后续由 rollback 结算。
- `rewrite_trx_xid` 放在 `mvcc_trx_log.cpp`：用 `Table::visit_record` 在页锁内读当前 xid；等于 `expected` 则写 `replacement` 并置 `*already_done=true`；等于 `replacement` 也置 true 但不写；其它值保持原样、置 `*already_done=false` 并返回 `RC::SUCCESS`，由调用方判定为日志矛盾。
- `MvccTrxKit::before_checkpoint()` 在 `pending_backfills_ != 0` 时记录错误并返回 `RC::INTERNAL`；`Db::sync()` 在刷页前调用它。
- 提交锁：`MvccTrxKit` 持有 `common::Mutex commit_lock_`，`MvccTrx::commit()` 在 `next_commit_seq() → COMMIT 落盘 → 状态发布` 全程持锁，保证 `commit_seq` 顺序与发布顺序一致；`current_commit_seq()` 只返回已发布的序号。回填在锁外执行。默认构建锁为空实现，与仓库现状一致。
- 删除 `commit_with_trx_id` 的旧“逐行改写后写日志”路径，`commit()` 直接走新协议。

- [ ] **Step 4: Run tests**

Run: `./build_debug/unittest/mvcc_commit_protocol_test && ./build_debug/unittest/mvcc_trx_log_test && ./build_debug/unittest/txn_status_test`
Expected: PASS。

- [ ] **Step 5: Commit**

提交信息：`feat(trx): log-first commit with idempotent xid backfill`。

---

### Task 4: 两段式恢复器与全量重放

**Review:** per-task — durability, recovery

**Files:**
- Modify: `src/observer/storage/trx/mvcc_trx_log.h/.cpp`
- Modify: `src/observer/storage/trx/mvcc_trx.h/.cpp`（删除 `recovering_` 与 redo 路径）
- Modify: `src/observer/storage/trx/trx.h`、`vacuous_trx.*`、`lsm_mvcc_trx.*`（删除 `redo` 与 `create_trx(..., trx_id)`）
- Modify: `src/observer/storage/db/db.cpp`（`recover()` 从 LSN 0 重放）
- Test: `unittest/observer/clog_recovery_test.cpp`

**Interfaces:**
- Produces: `RC MvccTrxKit::finish_recovery(int64_t max_trx_id, int64_t max_commit_seq);`
- Consumes: Task 3 的 `rewrite_trx_xid`、Task 2 的 `status()` / `published_seq_`。

- [ ] **Step 1: Write the failing tests** in `unittest/observer/clog_recovery_test.cpp`

公共辅助：建目录、`Db::init("mvcc")`、`wait_lsn(current_lsn)`、`filesystem::copy`、重开、扫描/索引点查/裸读 xid。

1. `committed_insert_survives_crash_before_page_flush`：T1 插入 3 行并 `commit`；只等日志、不 `sync`，复制目录后重开；可见 3 行；裸读 `begin_xid > 0`；`status().size()==0`。
2. `no_commit_log_means_aborted`：T1 插入 3 行，不 commit/rollback；等日志、复制、重开；可见 0 行；裸读所有 `begin_xid` 为 0。
3. `committed_update_survives_crash`：T1 插入并提交；T2 `delete_record(old) + insert_record(new)` 后提交；等日志、复制、重开；只看到新值；旧行 `end_xid == seq2`；新索引键命中 1 行，旧索引键 0 行。
4. `rolled_back_update_restores_old`：同上但 T2 `rollback`；重开后旧值可见、`end_xid == INF`；新行 `begin_xid == 0`；新索引键无可见行。
5. `aborted_delete_keeps_row`：T1 插入并提交；T2 `delete_record` 后 `rollback`；重开后行仍可见，`end_xid == INF`。
6. `double_recovery_is_idempotent`：对场景 1 的副本重开得到 db2，未 sync 再复制 db2 到 db3 重开；仍可见 3 行且 `Db::init` 成功。
7. `recovery_advances_counters`：场景 1 重开后，新事务 `id()` 大于日志中最大 trx id；`next_commit_seq()` 大于日志中最大 `commit_seq`。
8. `clean_checkpoint_then_recovery`：插入并提交后 `db->sync()`（checkpoint 前移），关闭重开；数据可见，新提交号仍大于旧提交号（证明从 LSN 0 重建计数器）。

- [ ] **Step 2: Run it to verify it fails**

Run: `bash build.sh debug --make -j8 && ./build_debug/unittest/clog_recovery_test`
Expected: 场景 1 FAIL（当前重放忽略 COMMIT 内容，行停在 `-T`，可见 0 行）。

- [ ] **Step 3: Implement**

- `MvccTrxLogReplayer` 改为：
  ```cpp
  unordered_map<int64_t, vector<Operation>> ops_;       // INSERT / DELETE
  unordered_map<int64_t, int64_t> committed_;           // T -> commit_seq
  unordered_set<int64_t> rolled_back_;
  int64_t max_trx_id_ = 0, max_commit_seq_ = 0;
  ```
- `replay(entry)`：按 `operation_type` 用 `memcpy` 解析；长度不足 → `LOG_ENTRY_INVALID`；表找不到 → `SCHEMA_TABLE_NOT_EXIST`；已 COMMIT/ROLLBACK 的事务再出现操作 → `LOG_ENTRY_INVALID`；COMMIT 记录 `max(max_commit_seq, seq)`，ROLLBACK 记入 `rolled_back_`；同一事务既 COMMIT 又 ROLLBACK → `LOG_ENTRY_INVALID`。**本阶段不写任何页。**
- `on_done()`：
  1. 遍历 `committed_`，调用 `backfill_operations(ops_[T], T, seq)`。
  2. 遍历 `ops_` 中未提交的事务，调用 `undo_operations(ops_[T], T)`（函数内部逆序）。
  3. `trx_kit_.finish_recovery(max_trx_id_, max_commit_seq_)`：`current_trx_id_ = max(current, max_trx_id_)`，`next_seq_ = max(next_seq_, max_commit_seq_)`，`published_seq_ = max(published_seq_, max_commit_seq_)`，`pending_backfills_ = 0`。
  4. `trx_kit_.status().clear()`。
  5. 任一错误立即返回，不继续掩盖；已写入的部分由下一次恢复按同样规则重做。
- `Db::recover()`：`log_handler_->replay(log_replayer, 0)`；保持 `replay → start → on_done` 的顺序。
- 删除死代码：`Trx::redo`、`MvccTrx::redo`、`MvccTrx::recovering_` 及其分支、`MvccTrx(kit, handler, trx_id)` 构造、`TrxKit::create_trx(handler, trx_id)` 及三个 kit 的实现；`LsmMvccTrxLogReplayer` 的空 `replay` 保留。
- `MvccTrxLogReplayer` 仍由 `MvccTrxKit::create_log_replayer` 返回；`OccTrxKit` 若继承 `MvccTrxKit` 则无需覆盖。

- [ ] **Step 4: Run tests**

Run:
`./build_debug/unittest/clog_recovery_test`
`./build_debug/unittest/mvcc_trx_log_test`
`./build_debug/unittest/mvcc_commit_protocol_test`
`./build_debug/unittest/txn_status_test`
Expected: PASS。

- [ ] **Step 5: Commit**

提交信息：`fix(mvcc): two-phase idempotent recovery from logical clog`。

---

### Task 5: 文档、OCC 接入说明与回归

**Files:**
- Modify: `docs/docs/design/miniob-update-mvcc-occ.md` §6.4、§9.1、§10
- Modify: `docs/docs/design/miniob-transaction.md`（恢复小节）
- Modify: `docs/plans/2026-11-20-update-mvcc-occ.md`（Task 8 标注由本计划取代）
- Test: 无新增；复用 Task 4 的 `clog_recovery_test`

- [ ] **Step 1: Update docs**

写明：逻辑日志只带 xid 语义；物理元组由 record/bplus redo 重放；提交顺序为
`COMMIT 落盘 → 状态发布 → 回填`；回填失败不回滚提交、checkpoint 被 `before_checkpoint()` 拒绝；
撤销 INSERT 用 `begin=0`；恢复从 LSN 0 重放且不截断日志；`TxnStatusTable` 恢复后清空；
`OccTrxKit` 复用同一 replayer。补上已知限制：INSERT 的“插行→写事务日志”窗口会留下永久不可见的 `begin<0` 行；死元组/索引项依赖 GC；无 ARIES CLR / 物理 undo / `page_lsn` 强制校验。

- [ ] **Step 2: OCC 接入**

若 `OccTrxKit` 已存在：确认其 `create_log_replayer` 未被覆盖，或覆盖为 `return new MvccTrxLogReplayer(db, *this, log_handler);`；用 `-t occ` 复制场景 1 的崩溃用例。
若尚未存在：在 master plan Task 10 的验收项中加一行“OCC 复用 `MvccTrxLogReplayer`，崩溃用例同 `clog_recovery_test`”，本任务不改代码。

- [ ] **Step 3: Full verification**

Run:
`bash build.sh debug --make -j8`
`./build_debug/unittest/mvcc_log_abi_test`
`./build_debug/unittest/txn_status_test`
`./build_debug/unittest/mvcc_commit_protocol_test`
`./build_debug/unittest/clog_recovery_test`
`./build_debug/unittest/mvcc_trx_log_test`
`./build_debug/unittest/mvcc_xid_test`（若已提交）
`cd build_debug && python3 ../test/case/miniob_test.py --test-cases=basic`
Expected: PASS；记录已知基线失败。

- [ ] **Step 4: Commit**

提交信息：`docs: document logical clog recovery semantics`。

---

## Verification Summary

完成时逐项报告：构建结果；`mvcc_log_abi_test`、`txn_status_test`、`mvcc_commit_protocol_test`、`clog_recovery_test`、`mvcc_trx_log_test` 的输出；`basic` SQL case；以及未覆盖边界：

- 不实现 CLR / 物理 undo / 全链路 `page_lsn` 校验；
- 不实现 checkpoint 日志截断，恢复全量重放；
- INSERT 的“插行→事务日志”窗口会遗留永久不可见行；
- 死元组与索引项留给 GC；
- OCC 只在 `OccTrxKit` 落地后跑同一崩溃矩阵；
- 索引恢复依赖 B+tree redo，不由事务恢复器补插。
