---
title: MiniOB UPDATE、MVCC 与 OCC 方案设计
---

# MiniOB UPDATE、MVCC 与 OCC 方案设计

状态：**已实现并验证**（2026-11-21；计划 `docs/plans/2026-11-20-update-mvcc-occ.md`）

## 1. 意图与场景

在 MiniOB 学习仓库中补齐三件事：

1. `UPDATE` 语句及其索引维护（heap / btree / lsm 三种表引擎）。
2. 标准事务语义：`BEGIN/COMMIT/ROLLBACK`、自动提交、原子提交、失败回滚。
3. MVCC 与 OCC 两种并发控制：heap 表上完整的多版本 `UPDATE`、崩溃恢复、GC；
   新增独立 `occ` TrxKit（Snapshot Isolation）。

目标场景是单机教学数据库、少量并发会话、无生产数据。正确性目标是让事务的可见性、
回滚、崩溃恢复可以用测试区分，而不是追求高并发吞吐。

## 2. 现状（代码事实）

- 语法已有单列 `UPDATE ID SET ID EQ value where`（`yacc_sql.y:488`），
  `UpdateSqlNode` 也只有单字段；但 `UpdateStmt::create` 是 TODO
  （`sql/stmt/update_stmt.cpp:21`），`Stmt::create_stmt` 没有 `SCF_UPDATE` 分支，
  逻辑/物理计划器和执行算子都没有 update。
- 三种引擎的 `update_record_with_trx` 分别返回 `UNSUPPORTED`/`UNIMPLEMENTED`
  （`heap_table_engine.h:33`、`btree_table_engine.h:41`、`lsm_table_engine.h:36`）。
  heap 的行页支持原地更新（`RecordPageHandler::update_record`），并有 redo 日志。
- 现有 MVCC 已经是 PostgreSQL 形状：每行两个隐藏字段
  `__trx_xid_begin/__trx_xid_end`（`MvccTrxKit::init`，当前是 4 字节 `INTS`）；
  插入/删除写负事务号表示未提交，提交时逐行改写为正数。
- 删除不删索引项：MVCC 删除只改 `end_xid`，索引扫描回表后由 `visit_record`
  过滤不可见版本（`mvcc_trx.cpp:175`）。这正好是“索引保留死版本”的方案。
- `MvccTrx::update_record` 是 `UNIMPLEMENTED`；提交逐行改写、没有原子提交点
  （文档 `miniob-transaction.md` 已承认）；没有 GC、没有多版本链。
- 事务日志是逻辑日志：`MvccTrxLogHandler` 记录 INSERT/DELETE/COMMIT/ROLLBACK
  （`mvcc_trx_log.cpp`），提交日志 `wait_lsn` 等落盘；恢复时
  `MvccTrxLogReplayer` 按日志重建事务操作，`on_done` 回滚没有 COMMIT 的事务。
  记录页和 B+树另有自己的 redo 日志。**没有物理 undo/CLR 机制。**
- `SqlResult::close` 在非多语句模式下自动 commit/rollback；
  `BEGIN` 打开多语句模式，`COMMIT/ROLLBACK` 由 `TrxEndExecutor` 处理。
- 没有 UNIQUE 索引：`IndexMeta` 只有 name/field，`CREATE INDEX` 语法没有
  UNIQUE；heap 表的主键只存元数据，不校验唯一性。btree 聚簇表通过树 key
  检查重复主键。
- 两个计划器：默认 RBO `physical_plan_generator`；会话打开 `use_cascade()`
  时走 cascade，`rules.h` 里已有 `UPDATE_TO_PHYSICAL` 枚举但没有实现。
- LSM 事务（`LsmMvccTrx`）仍依赖未实现的 `ObLsmTransaction`；btree 聚簇引擎
  目前明确拒绝 mvcc。

## 3. 目标与非目标

**目标**

- UPDATE 在 heap/btree/lsm 上可用（vacuous 事务），包括更新索引列、更新主键、
  条件更新、无 WHERE 更新、失败不留下半更新状态。
- heap 上的 MVCC：`UPDATE` 产生新版本；提交原子可见；回滚恢复旧版本；
  崩溃恢复后已提交事务全有或全无；死版本可回收。
- 新增 `occ` TrxKit（Snapshot Isolation）：快照读、读写集、提交校验、
  写写冲突中止。
- 标准事务：自动提交、显式事务、COMMIT/ROLLBACK 的原子性与错误语义。

**非目标（v1 明确不做）**

- MySQL/InnoDB 式 undo log + 原地更新 + read view 重建（第 6 节说明原因）。
- ARIES 式物理 undo/CLR：不新增物理 undo 子系统，复用现有逻辑事务日志。
- 可串行化 OCC / SSI / 谓词锁：只承诺 Snapshot Isolation。
- UNIQUE 索引语法与逻辑唯一约束（第 7 节说明；版本化索引键已为其留好空间）。
- btree 聚簇表与 LSM 表上的 MVCC/OCC；多列 SET；savepoint。
- 高并发性能优化、GC 后台线程调优。

## 4. 选定方案

采用 **PostgreSQL 风格追加写元组版本 + 独立 OCC Trx Kit（SI）**。

- 存储层只负责版本元组与 xid 标记：UPDATE = 旧版本标记 `end_xid=-T` +
  插入新版本 `begin_xid=-T`；旧索引项保留给老快照，新索引项指向新 RID。
- 事务层通过 `snapshot_seq`、`TxnStatusTable`、读写集与提交校验控制并发。
- 可见性解析把 `-T` 通过 `TxnStatusTable` 解释为 COMMITTED(csn)/ACTIVE/ABORTED，
  因此提交只需一次原子状态发布，逐行回填 `commit_seq` 可以延后。
- OCC 复用同一套版本存储；区别是额外维护 read set 并在提交时校验读行版本。

## 5. 数据结构与编码

### 5.1 元组头

复用现有隐藏字段，宽度改为 8 字节：

```text
begin_xid int64   // 创建者：-T 未提交，commit_seq 已提交，0 已中止
end_xid   int64   // 删除/覆盖者：INF=INT64_MAX 表示存活，其余同上
```

`MvccTrxKit::init()` 产出 len=8 的 `FieldMeta`；`Field` 增加
`set_int64/get_int64`（仍用 `AttrType::INTS` + len=8，避免新增 SQL 类型）。
`max_trx_id()` 改为 `INT64_MAX`，事务号/提交号计数器改为 `int64_t`。

**兼容性**：已存在的 mvcc 表隐藏字段是 4 字节，新二进制打开时必须在
`Table`/trx 初始化处检测并返回明确错误（不静默按 8 字节解释）。事务日志头也从
32 位事务号改为 64 位，旧 clog 文件同样不兼容；v1 不提供升级路径，mvcc/disk-log
的旧库需要删除重建（测试就是这样做的）。vacuous 表没有隐藏字段，不受影响。

### 5.2 TxnStatusTable

新增 `TxnStatusTable`（`storage/trx/trx_status.{h,cpp}`）：

```text
Status { ACTIVE, COMMITTED(commit_seq), ABORTED }
```

- `register(trx_id)`、`commit(trx_id, commit_seq)`、`abort(trx_id)`、
  `get(trx_id)`；内部 `shared_mutex` + `unordered_map<int64_t, Status>`。
- 每个 `MvccTrxKit` 一个实例，OCC kit 共享同一实现。
- 状态项在 GC 确认没有 `-T` 元组引用后清理；进程重启后由事务日志重建。

### 5.3 提交序号与快照

- `MvccTrxKit` 维护 `atomic<int64_t> next_trx_id_` 与
  `atomic<int64_t> commit_seq_`（或统一计数器 + 状态表）。
- `BEGIN` 时 `snapshot_seq = commit_seq_.load(acquire)`。
- 提交时 `commit_seq = commit_seq_.fetch_add(1) + 1`。

### 5.4 事务上下文

```text
MvccTrx/OccTrx:
  trx_id, snapshot_seq, started
  operations_ / write_set: {INSERT|UPDATE|DELETE, table, rid(s), old_rid, new_rid}
  read_set (OCC): {table, rid, observed_begin, observed_end}
```

## 6. 可见性、写路径与提交

### 6.1 可见性

```text
Resolve(xid, self):
  INF → INF
  0   → ABORTED
  xid > 0 → xid
  xid < 0:
    T = -xid
    T == self → SELF
    status(T) == COMMITTED(csn) → csn
    else → ACTIVE_OR_ABORTED
```

元组可见当且仅当：

1. `begin == SELF` 或 `begin` 是有效 csn 且 `begin <= snapshot_seq`；
2. `end == INF` 或 `end == SELF` 之外的非有效版本，或 `end` 是 csn 且
   `end > snapshot_seq`。

读写模式下的写冲突（first-updater-wins）保持现有语义：目标行的
`begin/end` 已是其它未提交事务的负 xid → `LOCKED_CONCURRENCY_CONFLICT`，立即中止。

### 6.2 UPDATE

- **vacuous**：`UpdatePhysicalOperator` 先收集目标行，再对每行构造新 record，
  调用 `trx->update_record`，最终到引擎实现：
  - heap：原地 `update_record` + 索引 delete/insert；失败恢复旧行与索引。
  - btree：主键不变时原地更新叶子 value；主键变化时先插新 key（冲突则失败），
    再删旧 key，并维护二级索引。
  - lsm：同一个 rid key 重新 put + 索引 delete/insert。
- **mvcc/occ**：`MvccTrx::update_record` = 对旧行做删除标记（`end_xid=-T`）+
  插入一份新行（`begin_xid=-T`，字段已由执行器改好）；旧索引项保留，
  新行插入新索引项；`operations_` 记录 DELETE(old) + INSERT(new)。
  回滚逆序恢复；提交按第 6.3 节处理。RID 会变化，SQL 层不暴露 RID。
- 无 WHERE 的 UPDATE 与 DELETE 一样先收集全部目标行再逐行更新；v1 接受
  该内存上限，后续可改为流式/分批。
- 候选行扫描使用 `READ_ONLY`：扫描只判断可见性，真正的 first-updater-wins
  在 `update_record/delete_record` 的原地 `READ_WRITE` 访问中检查。否则一个
  与 WHERE 无关、刚好被其它事务提交删除的行会让整条语句误报冲突。
- `CREATE INDEX`：执行器先 `start_if_need()` 再扫描。MVCC 会话的
  `current_trx()` 可能尚未启动，snapshot=0 会导致建索引时看不到已提交行，
  得到空索引；随后 UPDATE 只写入新版本索引项，GC 删除旧版本时便找不到旧项。
- 索引项删除是幂等的：GC/回滚删除旧版本时若索引项已不存在，记录警告并继续
  释放槽位，不再触发断言崩溃。
- 并发 rollback 只恢复仍属于本事务的删除标记；RID 被 GC/槽位复用后跳过错配，
  不再断言崩溃。该场景由并发压测发现（rollback 的旧 RID 已不属于本事务）。
- 执行算子必须把扫描的终止错误（如 `LOCKED_CONCURRENCY_CONFLICT`）向上
  返回，不能把“扫描失败、收集到 0 行”当成成功的 0 行更新。

### 6.3 提交协议（原子提交点）

1. `commit_seq = fetch_add(1)+1`。
2. 追加并等待 `COMMIT(trx_id, commit_seq)` 日志落盘（复用
   `MvccTrxLogHandler::commit` 的 `wait_lsn`）。
3. **提交点**：`TxnStatusTable.commit(trx_id, commit_seq)` 一次原子发布，
   随后才发布 `published_commit_seq`。分配序号、写 COMMIT 日志、状态发布、
   序号发布在同一把提交锁内串行化；否则新快照可能拿到“已分配但尚未提交”的
   C，先看到旧版本、提交后又看到新版本，造成同一行两个版本同时可见。
4. 逐行回填：把 `-T` 改写为 `commit_seq`（可同步或惰性；正确性不依赖回填）。
   回填失败不回滚已发布的提交：记录错误、保留状态表项，后续重试或重启时
   `recover` 再次回填；提交点在步骤 3 已经确立。
5. 清理操作列表；GC 可回收更老的版本。

回滚：先把状态置 `ABORTED`，再逆序执行 operations（插入行标记 0/删除索引并释放，
删除行恢复 `end_xid=INF`），最后清空。

### 6.4 恢复

- 复用现有逻辑日志：`INSERT/DELETE/COMMIT/ROLLBACK`；恢复时
  `MvccTrxLogReplayer` 重建未完成事务。
- 收到 `COMMIT` 日志：标记 `TxnStatusTable` 为 COMMITTED(commit_seq)，推进
  `commit_seq`，销毁事务对象。**日志回放期间不执行逐行回填**（避免在恢复中
  追加新日志）；回放结束、log handler 启动后，由启动 GC 把已提交 `-T`
  改写成提交序号。
- `sys.db` 元数据在 `check_point_lsn` 后追加持久化的 MVCC `commit_seq`；
  重启时恢复。旧格式（只有 LSN）仍可打开，GC 会从数据中的正 xid 自愈
  下界。这样 clean checkpoint 之后即使不再回放旧 commit 日志，已提交行
  仍对新快照可见。
- 恢复结束后仍无 COMMIT/ROLLBACK 的事务：按 ABORTED 回滚（现有 `on_done`）。
- 记录页/B+树的 redo 日志保持现状；不使用 CLR。若崩溃发生在 COMMIT 日志
  落盘之前，事务视为中止；之后视为提交——满足“无 COMMIT 记录即中止”。
- 恢复期间单线程、无其它事务，因此不需要原子可见性；原子可见性只在运行期
  由状态表保证。

### 6.5 GC

- 低水位 `watermark = min(已启动事务的 snapshot_seq)`；没有活跃事务时为当前
  `commit_seq`。未启动的会话事务不参与水位；`started_` 是 acquire/release
  原子量，保证 GC 读到 `started=true` 时 `snapshot_seq` 已初始化。
- 可回收版本：`begin == 0`、`begin < 0` 且状态 ABORTED、`end` 为已提交序号且
  `end <= watermark`、或 `end < 0` 且状态 ABORTED。使用 `<=`：快照等于
  `end` 的事务已经看不到该旧版本。
- 回收顺序：先扫描并收集死行 RID；同时把“已提交但仍是 `-T`”的
  `begin/end` 改写成状态表中的提交序号（checkpoint 安全），把 ABORTED
  的 `end` 恢复为 INF。然后逐行按 RID 重读、复制为自持内存后调用
  `Table::delete_record`；该路径同时删除索引项并释放槽位。
- 扫描到的正 begin/end 会推进 `commit_seq` 下界，兼容缺少持久化序号的旧
  元数据。
- 没有后台线程；SQL `sync;` 现在直接调用 `Db::sync`，执行一次
  `MvccGc::collect`（此前 `SYNC` 只有语法、没有执行器）。`Db::sync` 内有
  mutex，多个会话并发 sync 时不会同时跑 GC。
- 并发正确性：水位只保证逻辑可见性；页面级安全依赖 heap scanner 的 page latch
  与“先复制再删除”。真实 4 写 + 4 读 + 并发 `sync` 的压测在 ASan 与 TSan
  下运行（`test/case/concurrency/mvcc_concurrency_test.py`）；TSan 报告的是
  运行时调度暴露的行为，不是形式化证明。TSan 还报告了 clog/log buffer 等
  既有数据竞争，本设计未声称修复那些基础组件。

## 7. OCC Trx Kit

- `TrxKit::Type` 增加 `OCC`；`TrxKit::create("occ")`。
- `OccTrx` 复用 `MvccTrx` 的存储/版本逻辑，覆盖：
  - `visit_record`：可见性判断通过后，把 `{table, rid, begin_raw, end_raw}`
    记入 read set（只读和读写访问都记录；同一 RID 去重）。
  - `commit`：逐条按 RID 重读记录，行消失或 `begin_raw` 改变即冲突；
    `end_xid` 从原值变为快照之后提交的序号即冲突。本事务自己写入的
    `end=-T`、其它 ACTIVE/ABORTED 的 `-T` 不算冲突。校验失败先 `rollback()`
    撤销本事务写集，再返回 `LOCKED_CONCURRENCY_CONFLICT`。
  - 写集无需单独校验：first-updater-wins 在 `update/delete` 执行时已经用
    `-T` 抢占，其它事务不能覆盖该行。
- 写写冲突仍在执行期 first-updater-wins；OCC 额外拦截“读到后又被别人提交
  修改”的读-写冲突。
- 隔离级别：Snapshot Isolation；写偏斜（write skew）在 MVCC 下允许，OCC
  的 read-set 校验会中止其中一个写偏斜事务。两者都不承诺可串行化：OCC 不
  做谓词/范围校验，phantom 仍可能发生。

## 8. 标准事务语义

现有 `BEGIN/COMMIT/ROLLBACK` 与自动提交保留，补齐：

- 多语句模式下语句失败：标记事务为 failed；后续除 `ROLLBACK` 外拒绝执行，
  `COMMIT` 返回错误并回滚（避免“部分语句成功却提交”）。
- `COMMIT/ROLLBACK` 幂等：对未开启的事务返回明确错误或成功，二者择一并测试。
- 明确 `BEGIN` 嵌套行为：v1 直接报错（不支持 savepoint）。
- 错误码沿用 `LOCKED_CONCURRENCY_CONFLICT` / `RECORD_INVISIBLE`，客户端可见。

## 9. 与初始用户方案的差异（已执行）

1. **恢复**：不实现 ARIES 物理 undo/CLR，映射到 MiniOB 逻辑 clog + 状态表 +
   幂等回填。理由：现有记录/B+树日志是 redo 逻辑恢复，新增 CLR 会重写整个
   recovery 层；状态表已能满足原子可见与“无 COMMIT 即中止”。
2. **xid 宽度**：int64 通过隐藏字段 len=8 + `Field` 64 位访问实现，不新增
   `BIGINT` SQL 类型；旧的 4 字节 mvcc 表不兼容，打开时报错。
3. **唯一索引**：v1 不新增 UNIQUE 语法/约束。版本化索引键 `(K, RID)` 已满足
   后续实现条件；逻辑唯一校验涉及 B+tree 叶子 latch 内回表，单独作为后续项目。
4. **OCC 语义**：保留执行期 first-updater-wins，提交时校验读写集；SI，不是
   可串行化。
5. **引擎范围**：UPDATE 三种引擎的 vacuous 模式；MVCC/OCC 只在 heap。
6. **计划器**：v1 只实现默认 RBO 的 update 计划；cascade 模式下 UPDATE 返回
   `UNSUPPORTED`，cascade 规则留作后续任务。

## 10. 验证

- 单元：xid 宽度/兼容性检查；`TxnStatusTable` 状态机；提交原子可见性
  （未回填时另一事务也能看到全部行）；回滚；UPDATE 前后扫描与索引查询；
  GC 水位；OCC 读-写冲突。
- 崩溃：仿照 `mvcc_trx_log_test` 复制未落盘文件后重开，验证 COMMIT 前/后
  两种结果全有或全无。
- SQL：`primary-update.test` 生成结果；新增 `update_index.test`（更新索引列、
  更新主键、无 WHERE、失败回滚）与 `mvcc_update.test`（显式事务）。
- 并发：两个会话（或线程）验证快照读、丢失更新、写写冲突、OCC 读集冲突；
  Hermitage 风格的最小异常用例。
- 已入库用例：`mvcc_xid_test`、`mvcc_commit_atomic_test`、`mvcc_update_test`、
  `mvcc_recovery_atomic_test`、`mvcc_gc_test`、`occ_trx_test`、
  `trx_isolation_test`；SQL `update_heap/update_btree/update_lsm/update_index/
  trx_semantics/mvcc_update/basic`。
- 回归：现有 `mvcc_trx_log_test`、btree/lsm 引擎测试、`basic` SQL 套件。
  已知基线失败（dblab/vectorized）不作为回归门槛。

## 11. 计划

执行计划见 `docs/plans/2026-11-20-update-mvcc-occ.md`。设计批准与计划一起进行。


## 12. 实现与验证结果（2026-11-21）

已实现（11 个计划任务全部完成）：

- `UPDATE`：默认 RBO + cascade 物理计划、三种引擎 vacuous 更新、索引维护、
  SQL `update_index` 用例。
- MVCC：8 字节隐藏 xid、`TxnStatusTable` 原子提交、标准事务错误语义、
  heap `UPDATE`（delete-mark + 新版本）、崩溃恢复、显式 `sync` GC。
- OCC：`-t occ`、快照读、提交时 read-set 校验、写写冲突执行期中止。
- 修复了隔离测试暴露的真实缺陷：旧快照事务在另一事务提交删除后继续写同一行
  曾错误成功；现在 `visit_record(READ_WRITE)` 对“快照可见但已被提交删除”的
  版本返回 `LOCKED_CONCURRENCY_CONFLICT`，同时把 `INT64_MAX` 识别为存活哨兵。
- 修复 clean checkpoint 后 `commit_seq` 归零导致已提交行不可见：提交序号持久化
  到 `sys.db`，启动 GC 修复未回填 `-T`；重启验证返回 `1 | 20`。
- 修复 UPDATE/DELETE 扫描错误被吞：扫描返回冲突时必须让语句失败，而不是
  以 0 行更新返回 SUCCESS。

验证（本机 `build_debug`，ASan）：

- `mvcc_xid_test`、`mvcc_commit_atomic_test`、`mvcc_update_test`、
  `mvcc_recovery_atomic_test`（3 个崩溃场景）、`mvcc_gc_test`、
  `occ_trx_test`（4 个场景）、`trx_isolation_test`（脏读、不可重复读、丢失更新、
  写偏斜、OCC 读冲突）通过。
- `mvcc_trx_log_test` 全量运行（见 `task-11` 报告；耗时长）。
- SQL diff：`update_heap/update_btree/update_lsm/update_index/trx_semantics/
  basic` 通过；`mvcc_update` 以 `--server-args="-t mvcc"` 通过。
- 手工单会话 `-t mvcc` 验证 update/rollback/commit；`-t occ` 验证 update/commit。
- 手工 `-t mvcc -d disk` 提交后 kill + 重启：`select` 返回 `1 | 20`。
- SQL 双会话：未提交写对另一会话不可见；无关行上的未提交写不阻塞本行更新；
  同一行写冲突返回 `FAILURE`。
- `test/case/concurrency/mvcc_concurrency_test.py`：真实多连接 SQL 场景，
  每步中间结果断言；覆盖 MVCC 脏读/不可重复读/phantom/丢失更新/写偏斜、
  OCC 读集冲突与不相交提交、`sync` 前后索引可见性；最后 4 写 + 4 读 +
  持续 `sync` 的 ASan 压测，检查中途不崩、行数/值/索引最终一致。
- 压测还暴露并修复了：SQL `CREATE INDEX` 在 MVCC 下用未启动事务扫描导致
  空索引；`SYNC` 只有语法没有执行器；提交序号已分配但状态未发布时新快照
  可见性撕裂（提交锁 + `published_commit_seq`）；aborted delete mark 被 GC
  误删；rollback 对已 GC/复用的 RID 断言；GC 读取 `started_` 的 data race。
- 并发验证结果：ASan 全场景（MVCC/OCC + 4 写/4 读/sync）3×60 轮和 1×100 轮
  通过；TSan 单行 3×100 轮、全场景 2×20 轮行为通过。TSan 仍报告 clog
  log buffer、buffer pool `Frame::access` 等**既有基础组件**的数据竞争；
  这些不是本设计修改的 GC 语义，未在本任务中修复。
- 干净单实例 `mvcc_trx_log_test` 5/5 通过（约 600s）。

已知限制（与设计一致）：

- SI 不是可串行化；MVCC 允许写偏斜，OCC 不做谓词校验，phantom 仍可能。
- MVCC/OCC 仅 heap 表；btree/LSM 的 MVCC 返回 `UNSUPPORTED`。
- 无 UNIQUE 逻辑约束；无 savepoint；无后台 GC 线程；GC 与并发扫描的页面级
  互斥由调用方保证。
- 崩溃恢复不逐行回填 `-T`；行可见性依赖重启后由 COMMIT 日志重建的状态表。
- 旧的 4 字节 xid MVCC 表和旧 clog 不兼容，需删除重建；vacuous 表不受影响。
- 无 undo log / ARIES CLR：复用 MiniOB 逻辑事务日志 + 幂等状态表，不实现
  原地更新的物理 undo。
