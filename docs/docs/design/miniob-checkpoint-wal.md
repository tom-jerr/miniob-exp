---
title: MiniOB 检查点与 WAL 截断
---

# MiniOB 检查点与 WAL 截断

状态：**部分实现**。`Db::sync` 现在执行安全的“记录恢复起点”检查点；
**WAL 截断未实现**，所有 clog 文件保留。

## 1. 完整流程与当前实现

标准的 checkpoint 流程是：

1. 冻结/记录水位：暂停新写入（或加检查点锁），记录目标 `checkpoint_lsn`。
2. MVCC 回填与清理：`MvccGc::collect` 把已提交的 `-xid` 改写成
   `commit_seq`，清理所有活跃快照都不可见的死版本。
3. 全量刷脏页：把所有 buffer pool 脏页写入数据/索引文件。
4. 持久化屏障：对数据文件、double write buffer、元数据文件执行 `fsync`。
5. 原子更新元数据：把新 `checkpoint_lsn` 写入控制文件，`fsync` 控制文件
   及其目录。
6. 截断日志：确认元数据已持久化后，删除 `LSN < checkpoint_lsn` 的旧 WAL。

当前 `Db::sync()` 实现了 1 的一半、2、3、4 和 5；**第 6 步没有实现**。
它并不假装做了 WAL 截断：`LogFileManager` 没有 `remove/truncate` 接口，
所有日志文件都会保留。

### 1.1 冻结/水位

`MvccTrxKit` 有一把 `std::shared_mutex checkpoint_gate_`：

- `MvccTrx::start_if_need()` 持 shared lock；
- `Db::sync()` 持 unique lock，因此检查点期间不会启动新事务。

`Db::sync()` 在 unique lock 内检查 `all_trxes()`，只要还有 `started()` 的
MVCC 事务，就**刷页但不推进 checkpoint**。这样不需要无限等待长事务，也不会
把未提交事务修改过的页当作稳定检查点。非 MVCC 引擎没有这个 gate，仍依赖
调用方保证 sync 期间没有并发写。

目标 LSN 在刷页**之前**采样：

```text
target_lsn = log_handler->current_lsn()   // before flushing
flush tables -> dblwr -> fsync
wait_lsn(target_lsn)
meta.checkpoint_lsn = target_lsn
```

旧实现先刷页、再取 `current_lsn`，会把刷页期间提交的事务包含进 checkpoint，
但这些事务的页可能尚未刷出，恢复时又因 LSN 小于 checkpoint 被跳过。
先采样后刷页消除了这个窗口；target 之后提交的事务 LSN 必然更大，会在下次
恢复时重放。

### 1.2 回填与 GC

`Db::sync()` 先执行 `MvccGc::collect`：

- 已提交但仍是 `-T` 的 `begin/end` 改写成 `commit_seq`；
- `end` 为 ABORTED 的删除标记恢复为 INF（旧版本仍存活）；
- 删除所有已启动快照都不可见的版本及其索引项。

GC 与普通读写仍可并发；GC 的可见性由最老活跃快照水位保证，页面访问依赖
buffer pool 的 page latch。`Db::sync()` 有 mutex，避免多个会话同时跑 GC。

### 1.3 刷脏页与 fsync

`Table::sync()` 把数据页和索引页 flush 到全局 double write buffer；
`DiskDoubleWriteBuffer::flush_page()` 先 `fsync` dblwr，再把页写入数据文件；
随后 `BufferPoolManager::fsync_all()` 对所有数据/索引 buffer pool 文件
`fsync`；再刷新一次 dblwr 的 invalid 标记并 `fsync`。

这一步是 checkpoint 元数据之前的持久化屏障。当前仍未覆盖：
LSM 自己的 `.sst/.wal` 持久化语义、表元数据 `.table` 文件的 fsync。

### 1.4 控制文件

`Db::flush_meta()` 写 `sys.db.tmp`，`fsync` 文件，rename 为 `sys.db`，再
`fsync` 数据库目录。`sys.db` 当前是文本：

```text
<checkpoint_lsn> <persisted_commit_seq>
```

旧格式只有第一个数字，仍可读取。控制文件原子替换依赖 rename 的同目录原子性；
没有 double-buffer/控制页校验。

## 2. WAL 截断：为什么没有实现

当前 API 只提供 `list_files/last_file/next_file`，不提供删除。恢复从
`check_point_lsn` 开始重放；保留全部 WAL 能保证即使 checkpoint 元数据被误写，
仍有日志可回退重放。反过来，一旦提前删除日志，任何 checkpoint/meta 问题都会
变成不可恢复的数据丢失。

因此：

- `Db::sync()` 的注释明确写着不删除 WAL；
- `LogFileManager` 注释明确没有 remove/truncate；
- 检查点只改变恢复起点，不减少日志占用。

正确实现截断至少需要：

1. 检查点元数据已经 fsync（含目录）；
2. 数据页/索引页已经 fsync；
3. 没有活动事务的新写入可能跨越 checkpoint；
4. 只删除“最后一个 LSN < checkpoint_lsn”的完整日志文件；
5. 保留包含 `checkpoint_lsn` 的文件，绝不删除正在写的 last file；
6. 删除后执行目录 `fsync`；
7. 在“删除日志后重启恢复”的测试通过之前保持默认关闭。

建议的 `LogFileManager::remove_old_files(checkpoint_lsn)` 判据：

```text
for file in sorted(log_files):
    if file is last/active file: break
    if next_file.first_lsn <= checkpoint_lsn:
        remove(file)          # next file still covers checkpoint
```

删除前必须有进程级 checkpoint 锁，且不能在日志 writer 持文件句柄时移除当前
文件。当前没有这个接口，也没有对应故障注入测试。

## 3. 恢复保证与失败矩阵

在“所有 WAL 保留”的前提下：

| 崩溃点 | 结果 |
| --- | --- |
| 刷页前/刷页中 | meta 仍是旧 checkpoint，从旧位置重放；未提交事务由 on_done 回滚 |
| dblwr 写入后、数据 fsync 前 | 页可能旧/新混合；旧 checkpoint 或 dblwr 恢复路径重做 |
| 数据 fsync 后、meta rename 前 | meta 旧；重放 target 之后的日志，已刷页重做幂等 |
| meta rename 后、目录 fsync 前 | 可能旧 meta 或新 meta；两者都有完整 WAL，可重放 |
| meta 持久化后 | 数据页已 fsync，恢复从 target 开始；target 之后日志仍保留并重放 |

若以后加入 WAL 截断，上表必须改成“日志少于 target 时仍可恢复”的证明，并以
故障注入测试为准；不能只靠代码审阅。

## 4. 验证

- `unittest/observer/mvcc_recovery_atomic_test.cpp`
  - `checkpoint_survives_without_wal_files`：提交 → `db->sync()` →
    拷贝数据库 → 删除全部 clog 文件 → 重开仍读到已提交行；
  - 同时断言 `sync` 前后 WAL 文件数量不变（当前不截断）；
  - `active_transaction_blocks_checkpoint_advance`：有活跃 MVCC 事务时
    `sync` 成功但我刷页不推进 `checkpoint_lsn`（读控制文件验证）。
- `mvcc_xid_test / mvcc_commit_atomic_test / mvcc_update_test /
  mvcc_gc_test / occ_trx_test / trx_isolation_test`。
- SQL `update_index/update_heap/update_btree/update_lsm/trx_semantics/
  basic/mvcc_update`。
- 并发压力：`test/case/concurrency/mvcc_concurrency_test.py`。
- 全量 `mvcc_trx_log_test` 通过。

## 5. 已知边界

- 没有 WAL 截断；磁盘日志会持续增长。
- 没有全局 freeze；有活跃 MVCC 事务时跳过 checkpoint，而不是等待。
- 非 MVCC 引擎没有 checkpoint gate，仍依赖调用方 quiesce。
- 表元数据与 LSM 文件没有纳入统一 fsync 屏障。
- `sys.db` 没有控制页 checksum/双副本；rename 原子性依赖文件系统。
- WAL 删除 API 与“删除后恢复”故障注入测试均未实现。

## 6. 参考与技能检索

按用户要求检索了 `~/.codex/skills`：

- `.system/` 下是 skill-creator、imagegen、openai-docs、review-agent 等通用工具；
- `tom-jerr-blog-writing` 限定 `E:/blog` 的 MkDocs/Zensical 正式博客；
- `drawing-toolkit` 用于 SVG/图表。

没有 checkpoint/WAL/数据库存储相关 skill。本文按本仓库
`docs/DESIGN_RULES.md` 与现有设计文档的事实/证据格式编写，没有套用博客技能。
