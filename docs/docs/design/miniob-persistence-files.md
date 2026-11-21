---
title: 持久化文件与恢复路径
---

# 持久化文件与恢复路径

本文回答三个问题：

1. MiniOB 的一个数据库目录里，`sys.db`、`dblwr.db`、`*.data`、`*.index`、`clog` 分别保存什么？
2. 一次 INSERT / UPDATE / COMMIT 怎样经过 WAL、buffer pool、double write buffer 变成磁盘状态？
3. 进程崩溃重启后，恢复为什么先做 dblwr、再做 WAL 重放，`page.lsn` 又解决了什么重复问题？

结论先放在前面：

- `.data` / `.index` 是**页式物理数据**，`page.lsn` 和 checksum 让重做可以跳过已落盘页；
- `clog` 是**逻辑 + 物理混合 WAL**，负责页重做和事务状态重建；
- `dblwr.db` 只保证**单页写的原子性**，不保证事务原子性；
- `sys.db` 只是控制文件，保存 `checkpoint_lsn` 和持久化 `commit_seq`；
- 当前 **WAL 没有 fsync，也没有截断**，所以“进程崩溃恢复”和“掉电持久化”不能混为一谈。

---

## 1. 数据目录

一个数据库（默认 `sys`）大致包含：

```text
miniob/db/sys/
├── sys.db                  # DB 控制文件
├── dblwr.db                # double write buffer 共享文件
├── t.table                 # 表 schema/元数据
├── t.data                  # heap 表数据页
├── i_v.index               # B+tree 索引页
├── clog/
│   ├── clog_0.log          # 文件名是段内第一条日志的 LSN
│   └── clog_1000.log
└── lsm/
    ├── CURRENT
    ├── 0.mf                # manifest
    ├── 0.wal
    └── *.sst
```

命名规则来自 `src/observer/storage/common/meta_util.h`：

```cpp
DB_META_SUFFIX    = ".db";
TABLE_META_SUFFIX = ".table";
TABLE_DATA_SUFFIX = ".data";
TABLE_INDEX_SUFFIX = ".index";
```

`sys.db` 不是“系统表数据”，而是数据库控制文件；真正的表数据在 `*.data`。

---

## 2. 文件逐一说明

### 2.1 `sys.db`：控制文件

代码：`Db::init_meta()`、`Db::flush_meta()`。

内容当前是文本：

```text
<checkpoint_lsn> <persisted_commit_seq>
```

例子：

```text
800 120
```

- `checkpoint_lsn`：恢复日志的起点；
- `persisted_commit_seq`：上次持久化的 MVCC 提交序号；
- 旧格式只有第一个数字，读取时兼容。

写入方式：

```text
写 sys.db.tmp
fsync(sys.db.tmp)
rename(sys.db.tmp, sys.db)
fsync(数据库目录)
```

这个顺序解决的是“控制文件要么是旧版本，要么是新版本，不能是半个文件”。
它没有控制页 checksum，也没有双副本；依赖文件系统 rename 的原子性。

### 2.2 `*.table`：表 schema

代码：`TableMeta::serialize()` / `Table::create()` / `Db::open_all_tables()`。

内容为 JSON，主要包括：

```json
{
  "table_id": 1,
  "table_name": "t",
  "storage_format": 0,
  "storage_engine": 0,
  "fields": [...],
  "indexes": [...],
  "primary_keys": [...]
}
```

`fields` 不只包含用户列，还包含 MVCC 隐藏列：

```text
__trx_xid_begin
__trx_xid_end
```

字段元数据（offset、len、type）是解析 `t.data` 中二进制行的唯一依据。
`table_id` 还会被 WAL 用来定位表和 buffer pool。

当前 `.table` 是直接 `fstream` 写出，**没有 tmp + fsync + rename**；
DDL 后的 `Db::sync` 也不会 fsync `.table`。因此表结构本身的掉电持久化
弱于 `sys.db`。

### 2.3 `*.data`：heap 表数据页

文件按固定页组织，`BP_PAGE_SIZE = 8192` 字节：

```cpp
struct Page {
  LSN      lsn;       // 页面最后一条 WAL 的 LSN
  CheckSum check_sum; // 页校验和
  char     data[BP_PAGE_DATA_SIZE];
};
```

- 页 0 是 `BPFileHeader`：页总数、已分配页数、页面分配 bitmap；
- 其它页是记录页。

记录页布局：

```text
+-------------+-------------------+----------------------+
| PageHeader  | slot 分配 bitmap  | 定长 record 数据区    |
+-------------+-------------------+----------------------+
```

`PageHeader` 记录 `record_num`、`record_size`、`record_capacity`、
`data_offset` 等。行用 `RID(page_num, slot_num)` 定位，slot 是否有效看 bitmap。

MVCC 的多版本都在记录数据里：

```text
旧 RID：begin=1, end=C     // 被提交的 UPDATE/DELETE 标记为死版本
新 RID：begin=C, end=INF   // 活版本
```

UPDATE 不做原地覆盖，而是写新行；旧行直到 GC 才释放。

### 2.4 `*.index`：B+tree 索引页

文件同样是 8KB 页，页 0 是 `IndexFileHeader`：

```cpp
struct IndexFileHeader {
  PageNum  root_page;
  int32_t  internal_max_size;
  int32_t  leaf_max_size;
  int32_t  attr_length;
  int32_t  key_length;
  AttrType attr_type;
  int32_t  format_version;
  int32_t  value_size;
  int32_t  key_mode;
  int64_t  next_rowid;
};
```

heap 表的二级索引保存：

```text
key   = 索引列值
value = RID
```

例如：

```text
(10 -> 1:0)
(99 -> 1:1)
```

索引本身**不保存 MVCC 可见性**。扫描索引得到 RID 后，仍然要回表并调用
`trx->visit_record()`；不可见版本会被过滤。

所以 UPDATE 的索引策略是：

- 旧版本的索引项先保留，给老快照回表；
- 新版本插入新索引项；
- GC 删除旧版本时，删除旧索引项；
- 删除索引项因此必须容忍“已经不存在”。

### 2.5 `dblwr.db`：double write buffer

代码：`DiskDoubleWriteBuffer`。

解决的问题不是事务原子性，而是**单页写的 torn page**：

> 8KB 页可能只写了一部分就掉电，checksum 失败后不能直接拿它做 WAL 重做。

文件布局：

```text
+----------------------------+
| DoubleWriteBufferHeader    |  page_cnt
+----------------------------+
| DoubleWritePage 0          |
|   key {buffer_pool_id,page_num}
|   page_index
|   valid
|   Page {lsn,checksum,data} |
+----------------------------+
| DoubleWritePage 1          |
...
```

它是**所有数据/索引 buffer pool 共享**的，内存默认最多缓存 16 页。

刷脏页流程：

```text
DiskBufferPool::flush_page_internal(frame)
  -> 等待 page.lsn 对应 WAL 至少写入
  -> dblwr.add_page() 把整页写入 dblwr.db
  -> frame 清除 dirty

DiskDoubleWriteBuffer::flush_page()
  -> fsync(dblwr.db)
  -> 把 dblwr 页写到真实 t.data / *.index
  -> 标记 dblwr 页 valid=false
```

恢复时：

```text
dblwr.load_pages()
  -> 读 header.page_cnt
  -> 每个 DoubleWritePage 校验 Page checksum
  -> 只保留有效页

dblwr.recover() == flush_page()
  -> 在 WAL 重放之前，把有效页写回数据/索引文件
```

顺序是：

```text
dblwr 修复 torn page
  -> WAL 从 checkpoint 重放
  -> page.lsn 跳过已经落盘的日志
```

`dblwr` 不解决“事务提交了一半”这种问题；那属于 WAL + 事务状态表。

### 2.6 `clog/clog_<first_lsn>.log`：WAL

代码：`DiskLogHandler`、`LogFileManager`、`LogFileWriter`。

- 文件名是该文件第一条日志的 LSN，例如 `clog_1000.log`；
- 一个文件默认最多 1000 条日志；
- 每条日志：

```cpp
struct LogHeader {
  LSN     lsn;
  int32_t size;       // payload 大小
  int32_t module_id;
};
+ payload
```

模块：

| module | 作用 |
|---|---|
| `BUFFER_POOL` | 页分配/释放 |
| `RECORD_MANAGER` | 页面/记录的 INSERT、DELETE、UPDATE、INIT_PAGE |
| `BPLUS_TREE` | B+tree 页分裂、合并、root 更新等 |
| `TRANSACTION` | MVCC 的 INSERT/DELETE/COMMIT/ROLLBACK |

几个细节决定了恢复行为：

- `RECORD_MANAGER` 的 INSERT/UPDATE payload 包含完整 record；
- DELETE 只包含 RID；
- 页上保存最后一条 WAL 的 LSN，用于 `page.lsn >= log.lsn` 跳过；
- 当前**没有删除日志文件的接口**，所有 `clog_*.log` 保留。

> [!WARNING]
> `LogFileWriter::write()` 当前只调用 `write(2)`，没有 `fsync/fdatasync`。
> `DiskLogHandler::wait_lsn()` 等待的是写入完成，不是持久化完成。
> 因此“进程崩溃恢复”成立，但“掉电后 COMMIT 仍然持久”没有保证。
> 这也是 WAL 截断不能贸然实现的前置原因。

### 2.7 `lsm/`

`lsm/` 是 ObLsm 的文件：

```text
CURRENT
0.mf
0.wal
*.sst
```

heap/MVCC 行数据不走这些文件；但 `Db::init()` 总会打开 ObLsm，所以每个 DB
目录都有 `lsm/`。LSM 表引擎使用独立的一套 manifest/WAL/SST 持久化路径，
不在 heap 的 `.data + dblwr + clog` 恢复链里。

---

## 3. 写入路径

### 3.1 建表

```text
CREATE TABLE t(...)
  -> Table::create()
     -> 写 t.table（JSON schema）
     -> 创建 t.data，页 0 是 BPFileHeader
     -> 创建 HeapTableEngine
  -> DDL 后 CommandExecutor 自动执行 Db::sync()
```

### 3.2 INSERT

```text
MvccTrx::insert_record()
  1. record 写入 begin_xid=-T, end_xid=INF
  2. table->insert_record()
     -> RecordPageHandler 找一个空闲 slot
     -> RecordLogHandler::insert_record()
          WAL: module=RECORD_MANAGER
               page_num, slot_num, 整条 record
          frame->set_lsn(lsn)
     -> 修改 bitmap 和 record 数据
     -> frame->mark_dirty()
     -> 插入索引项，B+tree 写自己的 WAL
  3. MvccTrxLogHandler 写 TRANSACTION INSERT 日志
```

此时行还在 buffer pool，事务未提交，`begin_xid=-T`。

### 3.3 UPDATE / DELETE

MVCC UPDATE：

```text
旧行：end_xid = -T
新行：begin_xid = -T, end_xid = INF
operations_ = [DELETE(old_rid), INSERT(new_rid)]
```

旧索引项不立即删，留给老快照；新版本插入新索引项。

DELETE 同样只改 `end_xid`，不物理删除。

### 3.4 COMMIT

```text
MvccTrx::commit()
  -> 提交锁
  -> 分配 commit_seq
  -> 写 TRANSACTION COMMIT 日志
  -> wait_lsn(commit_seq)       // 当前只等 write，不等 fsync
  -> TxnStatusTable.commit
  -> 发布 published_commit_seq
  -> 回填 begin/end 的 -T 为 commit_seq
```

提交点先建立在 WAL + 状态表；数据页可以晚点刷。

### 3.5 刷脏页

```text
DiskBufferPool::flush_page_internal(frame)
  -> log_handler_.flush_page(page)
       wait_lsn(page.lsn)       // 保证页面的 WAL 至少写入
  -> dblwr.add_page()
  -> frame.clear_dirty()
```

真正写数据文件由 `dblwr.flush_page()` 完成：

```text
fsync(dblwr.db)
  -> 逐页写回 t.data / *.index
  -> invalid 标记
```

所以页面在落盘前，先有完整副本在 dblwr；即使真实页只写了一半，
重启也能用 dblwr 的 checksum 有效副本修复。

---

## 4. 恢复路径

`Db::init()` 的顺序：

```text
1. 打开 ObLsm
2. 创建 TrxKit
3. 打开 clog
4. init_meta(): 读 sys.db 的 checkpoint_lsn / commit_seq
5. open_all_tables(): 扫描 *.table，打开 *.data / *.index
6. init_dblwr_buffer():
     dblwr.load_pages()
     dblwr.recover() = flush_page()
        把有效 dblwr 页写回数据/索引文件
7. recover():
     WAL 从 checkpoint_lsn 开始 replay
8. log_handler.start()
9. log_replayer.on_done()
     没有 COMMIT 的事务 rollback
10. MvccGc 修复/回收 -T 死版本
```

### 4.1 WAL 重放

`IntegratedLogReplayer` 按模块分发：

```text
BUFFER_POOL
  -> redo_allocate_page / redo_deallocate_page

RECORD_MANAGER
  -> 取 page
  -> if page.lsn >= log.lsn: skip
  -> INIT_PAGE / INSERT / DELETE / UPDATE
  -> frame->set_lsn(log.lsn)

BPLUS_TREE
  -> 重做索引页/root 修改

TRANSACTION
  -> 重建 MvccTrx 的 operations/status
  -> COMMIT 标记 committed
  -> 无 COMMIT 的事务在 on_done 回滚
```

核心幂等条件：

```cpp
if (frame_lsn >= entry.lsn()) {
  return RC::SUCCESS;  // 页已经包含这条日志的修改
}
```

### 4.2 一个完整例子

```text
checkpoint = 800

T950: update id=2 v=20; commit;
U980: update id=3 v=99; -- 未提交
sync target = 1000
kill -9
```

如果 U980 仍然活跃，checkpoint gate 会检测到 active transaction，
只刷页、不推进 checkpoint；`sys.db` 仍是 800。

重启：

```text
读 sys.db -> checkpoint=800
dblwr 恢复有效页（可能已包含 LSN 1000）
WAL replay(800):
  - T950/U980 的 record redo
  - page.lsn >= log.lsn 的页跳过
TRANSACTION replay:
  - T950 有 COMMIT
  - U980 没有 COMMIT
on_done:
  - U980 rollback，删除新版本，恢复旧版本 end
结果:
  id=2 v=20 可见
  id=3 v=99 不可见
```

---

## 5. checkpoint 与 WAL

完整 checkpoint 的步骤和每个崩溃点见
[`miniob-checkpoint-wal.md`](miniob-checkpoint-wal.md)。这里只强调与文件的关系：

```text
MvccGc 回填/清理
  -> 采样 target_lsn
  -> 刷所有表页到 dblwr
  -> dblwr.flush_page()
  -> fsync 所有数据/索引文件
  -> wait_lsn(target)
  -> 写 sys.db + fsync 文件/目录
```

当前**不删除 WAL**。`LogFileManager` 没有 `remove/truncate` 接口；
`LSN < checkpoint_lsn` 的日志仍然保留，所以恢复可以回退到旧 checkpoint 重放。

---

## 6. 各层职责与边界

| 组件 | 保证 | 不保证 |
|---|---|---|
| `*.table` | schema、字段 offset、索引定义 | 掉电时无 fsync 的表元数据 |
| `*.data` / `*.index` | 页级数据 + page LSN/checksum | 事务原子性、跨页原子性 |
| `dblwr.db` | 单页写的 torn-page 修复 | 事务回滚、WAL 持久化 |
| `clog` | 页 redo、事务操作/状态重建 | 当前没有 fsync、没有尾部半写恢复 |
| `sys.db` | checkpoint 起点、commit_seq | 表清单、行数据 |
| `TxnStatusTable` | `-T` 的 COMMITTED/ACTIVE/ABORTED 解析 | 掉电后自身不持久，需日志重建 |
| `MvccGc` | 回填 `-T`、回收死版本 | 页面级并发安全的形式化证明 |

当前仍然存在的关键缺口：

1. **WAL 没有 fsync**：`wait_lsn` 不是掉电持久化屏障；
2. **WAL 没有半写/尾部截断恢复**：只写了一半的日志会阻断恢复；
3. **`.table` 没有 tmp+fsync+rename**；
4. **LSM 文件**没有纳入 heap checkpoint 的统一 fsync；
5. **WAL 不截断**，日志只增不减；
6. dblwr invalid 标记与真实页写入之间没有独立的 fsync 屏障，当前靠旧 checkpoint
   和完整 WAL 兜底。

---

## 7. 代码与测试索引

- 文件命名：`src/observer/storage/common/meta_util.h`
- 控制文件：`src/observer/storage/db/db.cpp` 的 `init_meta/flush_meta`
- 表元数据：`src/observer/storage/table/table_meta.cpp`
- 记录页：`src/observer/storage/record/record_manager.h/.cpp`
- 记录 WAL：`src/observer/storage/record/record_log.cpp`
- 页/缓冲池：`src/observer/storage/buffer/disk_buffer_pool.*`
- double write：`src/observer/storage/buffer/double_write_buffer.*`
- WAL：`src/observer/storage/clog/*`
- checkpoint：`docs/docs/design/miniob-checkpoint-wal.md`
- 恢复/检查点测试：`unittest/observer/mvcc_recovery_atomic_test.cpp`
- 并发压力：`test/case/concurrency/mvcc_concurrency_test.py`

Skipped: 未做真实掉电、WAL 尾部半写和 LSM 文件 fsync 的故障注入。
Risk: 当前恢复证明的是“进程崩溃 + 保留 WAL”，不是“掉电后 COMMIT 仍持久”。
