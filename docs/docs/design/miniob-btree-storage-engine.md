---
title: MiniOB B+Tree 聚簇表存储引擎
---

# MiniOB B+Tree 聚簇表存储引擎

## 目标与范围

新增 `ENGINE=btree` 表引擎，把整行数据直接存放在 B+树叶子页中（聚簇表），
而不是 heap 的堆文件。表按主键聚簇；没有主键时使用 8 字节隐藏自增 rowid。
同时补齐 `ENGINE=lsm` 的表引擎能力，见 [MiniOB LSM-Tree 存储引擎](miniob-lsm-tree.md)。

存储引擎在三个层次选择：

1. 启动参数 `-E heap|lsm|btree` 指定数据库默认引擎（缺省 heap）。
2. `CREATE TABLE ... ENGINE=heap|lsm|btree` 覆盖单张表。
3. 选择的引擎写入 `.table` 元数据，重开数据库时按元数据恢复。

## 叶子布局与 Key 编码

B+树仍是 `src/observer/storage/index/bplus_tree.*` 中的同一份实现，通过头部版本
（`IndexFileHeader::CURRENT_VERSION = 2`）与 `key_mode` 泛化：

| 模式 | key | value | 用途 |
| --- | --- | --- | --- |
| `ATTR_RID` | 属性 + RID | RID | heap 二级索引（行为不变） |
| `ATTR_MEMCMP_VALUE` | 属性 + 主键编码 | 主键编码 | btree/lsm 二级索引 |
| `COMPOSITE` | 主键字段按声明顺序拼接 | 整行数据 | 有主键的 btree 表 |
| `ROWID` | 8 字节自增 rowid | 整行数据 | 无主键的 btree 表 |

旧索引文件头部的版本字段为 0，打开时按 `ATTR_RID` + `sizeof(RID)` 解释，
因此 heap 既有索引文件可直接使用。

有主键时的编码：

```text
key   = pk_field_1 | pk_field_2 | ...
value = record bytes（table_meta.record_size()）
```

比较器按字段的 `AttrType` 逐个比较主键字段；无主键时按 uint64 rowid 比较。
主键重复在树插入时返回 `RECORD_DUPLICATE_KEY`，与 heap 不同（heap 不检查重复）。

扫描器在 `COMPOSITE` 单字段时按 CHARS 规则补齐边界键；多字段主键不暴露单字段索引。

## 二级索引与回表

`BtreePkIndex`（`src/observer/storage/index/btree_pk_index.*`）实现两类索引：

- 二级索引：独立 B+树文件，key = 索引属性 + 主键编码，value = 主键编码。
  扫描得到主键编码后调用 `Table::get_record_by_key` 回到聚簇树取整行。
- 主键索引：单列主键时，把聚簇树本身包装成共享 `BtreePkIndex` 暴露给优化器
  （`find_index_by_field` 命中主键字段时返回它）。它的叶子 value 是整行，
  `get_record` 直接复制 value 并用主键字段重建 `record.key()`，供后续 delete 使用。

`create index` 的流程与 heap 一致：建树 → 全表扫描回填 → 写回表元数据。
重启时按 `.table` 中的索引元数据重开索引文件。

## rowid 恢复

rowid 模式下 `next_rowid` 同时存在于内存与树头部：

- 每次插入取 `next_rowid_++`；头部在 `sync()` 时落盘。
- 打开表时取 `max(header.next_rowid, 扫描到的最大 rowid + 1)`，因此即使崩溃前
  头部未同步，也不会复用已存在的 rowid。
- 恢复扫描是全表 O(n)；当前表规模下可接受，后续可改为只读最右叶子。

## 崩溃一致性

聚簇树复用 B+树的 mtr 逻辑日志（`LogModule::BPLUS_TREE`）：页面修改、分裂、合并
记录逻辑日志，恢复时按 buffer pool 重放。rowid 与 record 数据在同一棵树中，
插入/删除要么按日志重做，要么回滚，不存在“数据在但主键编码不在”的分裂状态。

二级索引与聚簇表的更新不在同一个日志事务里（与 heap 的索引维护相同），
索引失败时按 heap 的模式回滚已插入项与树记录。当前没有跨索引的原子提交保证。

## 约束与未覆盖

- 只支持 vacuous 事务；`-t mvcc` 下对 btree 表的 insert/delete 返回 `UNSUPPORTED`。
- 不支持 chunk 执行模式（`get_chunk_scanner` 返回 `UNSUPPORTED`）。
- 不支持 UPDATE（SQL 层没有 update 执行算子）。
- 表记录大小 + 主键大小必须能放进叶子页（至少 2 条），否则建表返回 `INVALID_ARGUMENT`。
- 没有实现 `get_record(rid)`：btree 表没有堆 RID，点查走 `get_record_by_key`。
- 扫描顺序是主键序（无主键时 rowid 序，即插入序），与 heap 的物理序不同。

## 验证

- `unittest/observer/btree_table_engine_test.cpp`：rowid/单列主键/复合主键的
  插入、扫描序、重复主键、删除、二级索引与主键索引扫描、重开数据库。
- `unittest/observer/bplus_tree_modes_test.cpp`：四种 key 模式、`get_value`、
  旧格式头部兼容。
- `test/case/test/btree_engine.test`：SQL 层的建表、DML、二级索引、隐藏 rowid。
- 手工端到端：observer + obclient 验证建表、插入、删除、索引查询与重启。

## 与 heap 的差异

| 方面 | heap | btree |
| --- | --- | --- |
| 行位置 | 堆页 + RID | 主键/rowid 聚簇叶子 |
| 扫描顺序 | 物理页顺序 | 主键序 / 插入序 |
| 重复主键 | 允许（无主键约束检查） | `RECORD_DUPLICATE_KEY` |
| 点查 | RID | 主键编码 |
| 事务 | vacuous/mvcc | vacuous only |
| 二级索引 value | RID | 主键编码 |
