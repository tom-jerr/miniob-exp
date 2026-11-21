# B+Tree Pages 存储引擎 + LSM 存储补齐

## Context

目标：MiniOB 支持 MySQL 风格的 B+tree pages 表存储（数据行直接存在 B+tree 叶子页，聚簇于主键，无主键用隐藏自增 rowid），而不是堆存储；同时把已存在但残缺的 LSM 表引擎补齐到与 heap 相同的能力（DML + 二级索引 + 重启可用）。

选定方案（用户已确认）：
1. 引擎按表选择：新增 `CREATE TABLE ... ENGINE=btree|lsm|heap` 语法；无 ENGINE 子句时沿用启动参数 `-E`（默认 heap 不变）。
2. LSM：完整对齐 heap —— delete/get/索引/重启都可用。
3. B+tree 引擎：完整支持二级索引（InnoDB 风格：二级索引叶子存主键编码，回表按主键查聚簇树）。

现状事实（本会话已核实）：
- `Table` 已通过 `unique_ptr<TableEngine> engine_` 委托（src/observer/storage/table/table.h:107），接口在 table_engine.h（insert_record/delete_record/*_with_trx/get_record(RID)/create_index/get_record_scanner/get_chunk_scanner/visit_record/sync/find_index*/open/init）。
- 引擎按 `TableMeta::storage_engine()`（持久化于 .table JSON）在 Table::create/open 里选择（table.cpp:106-114、:176-183），实例级默认来自 `-E`（db.h:121-131 `Db::get_storage_engine()`，strcasecmp "heap"/"lsm"）。
- `CreateTableSqlNode.storage_engine` 字段已存在（parse_defs.h:163）但语法、stmt、Db 均未接线；yacc 只有 `STORAGE FORMAT EQ ID`（yacc_sql.y:455）。
- 共享 B+tree（storage/index/bplus_tree.{h,cpp}）：叶子条目 = key(attr+RID) + value(RID)，`IndexNodeHandler::value_size()` 硬编码 `sizeof(RID)`（bplus_tree.cpp:62），`calc_leaf_page_capacity(attr_length)` 按 attr+8+8 算（bplus_tree.cpp:41）；header 页结构 `IndexFileHeader`（bplus_tree.h:156，root_page/internal_max_size/leaf_max_size/attr_length/key_length/attr_type）；`BplusTreeHandler` 持有 `KeyComparator key_comparator_`（bplus_tree.h:640，open 时按 attr_type 初始化，bplus_tree.cpp open 尾部）；`BplusTreeScanner` 用 `RID::min()/max()` 填充 key 边界（bplus_tree.cpp:1885-1951），`next_entry(RID&)` 返回叶子 value；节点插入/删除/分裂/合并全部按 item_size=key+value 驱动，天然支持变长 value。
- 崩溃恢复：B+tree 的 mtr 日志（LogModule::BPLUS_TREE）经 `BplusTreeLogger::commit()` 写入 clog（bplus_tree_log.cpp:102-137），恢复时 `IntegratedLogReplayer` 按 buffer_pool_id 重放（bplus_tree_log.cpp:147-183）——聚簇树复用 BplusTreeHandler 即自动获得重做日志。
- LSM：`ObLsm` 本身完整（put/get/remove/iterator/transaction/WAL/manifest/compaction），`Db::init` 无条件打开 `dbpath/lsm`（db.cpp:66-70）。`LsmTableEngine` 只实现了 insert_record（key=`Codec::encode(table_id, 自增id)`，忽略主键）+ 全表扫描；`open()` 返回 UNIMPLEMENTED → 当前 `-E lsm` 下 CREATE TABLE 直接失败。`LsmRecordScanner::next` 会把 lsm key 写入 `record.key()`。`ObLsmTransaction` 有 get/put/remove/commit（oblsm/include/ob_lsm_transaction.h:57-85）。
- DML 入口：InsertPhysicalOperator → `trx->insert_record(table, record)`；DeletePhysicalOperator → `trx_->delete_record`；IndexScanPhysicalOperator → `index->create_scanner` → `next_entry(&rid)` → `table->get_record(rid)`（index_scan_physical_operator.cpp:67）；VacuousTrx 是纯转发（vacuous_trx.cpp:35-40），与引擎无关。UPDATE 在 SQL 层没有执行算子（无 update physical operator，VacuousTrx::update_record 返回 UNIMPLEMENTED）——两个新引擎均不实现 update。
- 执行模式默认 TUPLE_ITERATOR（session.h:125）；CHUNK_ITERATOR 仅 `set execution_mode` 后使用，且 chunk 扫描器（ChunkFileScanner）绑死 heap RecordFileHandler → btree/lsm 的 get_chunk_scanner 返回 UNSUPPORTED（与 lsm 现状一致）。
- CMake：src/observer/CMakeLists.txt 与 unittest/observer/CMakeLists.txt 均 `GLOB *.cpp` → 新增 .cpp 自动纳入构建与单测可执行文件（每 .cpp 一个 gtest 可执行，文件名即测试名）。
- IvfflatIndex 是全部 UNIMPLEMENTED 的桩（ivfflat_index.h）。
- 工作区预存改动：`deps/3rd/jsoncpp`、`deps/3rd/libevent` 子模块指针是用户既有状态，不要动，提交只按路径暂存本项目文件。

## Approach

### Task 1 — BplusTree 泛化（value 大小 / key 模式 / 比较器 / 扫描器），heap 索引行为零变化

文件：src/observer/storage/index/bplus_tree.h、bplus_tree.cpp、bplus_tree_index.h/.cpp；unittest 中直接调用 `BplusTreeHandler::insert_entry(const char*, const RID*)` 的测试（实现时用 `grep -rn "insert_entry(" unittest/ src/` 找全调用点）。

1. `IndexFileHeader` 末尾追加字段（bplus_tree.h:156）：`int32_t format_version; int32_t value_size; int32_t key_mode; int64_t next_rowid;` 并加 `static constexpr int32_t CURRENT_VERSION = 2;`。旧索引文件 page0 尾部为零（页面分配时清零）→ `format_version != 2` 即视为 legacy。
2. 新枚举（bplus_tree.h）：`enum class BplusTreeKeyMode { ATTR_RID = 0, ATTR_MEMCMP_VALUE, COMPOSITE, ROWID };`
3. `KeyComparator`（bplus_tree.h:79）改为按模式分发：
   - `init(AttrType type, int length)` 保持现签名 → mode=ATTR_RID（原行为：attr 比较后按 RID::compare 比较 8 字节后缀）。
   - 新增 `init_attr_value(AttrType type, int attr_length, int value_length)` → mode=ATTR_MEMCMP_VALUE（后缀 memcmp value_length 字节）。
   - 新增 `init_composite(vector<KeyComparator::Field>)`，`struct Field { AttrType type; int offset; int len; };` → mode=COMPOSITE，逐字段 `DataType::type_instance(type)->compare(Value(left), Value(right))`（复用 AttrComparator 现做法，bplus_tree.h:60-77）。
   - 新增 `init_rowid()` → mode=ROWID，8 字节 memcpy 成 uint64 数值比较。
   - `attr_comparator()` 访问器保留（扫描器 open 预检用）；COMPOSITE/ROWID 模式下调用方不得调用（见第 4 条）。
4. `BplusTreeHandler`：
   - `create(LogHandler&, BufferPoolManager&, file, AttrType, attr_length, internal_max_size=-1, leaf_max_size=-1)` 保留（默认等价旧行为：key_length=attr_length+sizeof(RID)，value_size=sizeof(RID)，key_mode=ATTR_RID）；新增重载带 `int key_size, int value_size, BplusTreeKeyMode key_mode`。create 写入 header：`format_version=2, key_length=key_size, value_size, key_mode, next_rowid=0`。
   - `open(LogHandler&, DiskBufferPool&)`：读 header 后，`format_version != CURRENT_VERSION` → 设 `file_header_.value_size = sizeof(RID)`、key_mode=ATTR_RID（legacy 文件兼容，key_length 原样用）；`format_version==2` → 按 header 字段初始化。之后按 key_mode 初始化 `key_comparator_`：ATTR_RID/ATTR_MEMCMP_VALUE 用 attr_type+attr_length(+value_size)；ROWID 用 init_rowid()；COMPOSITE 先不初始化比较器，等待 `set_composite_spec()`。
   - 新增 `RC set_composite_spec(const vector<KeyComparator::Field> &fields)`：仅 COMPOSITE 模式调用，初始化 key_comparator_；非 COMPOSITE 返回 RC::INVALID_ARGUMENT。
   - `insert_entry(const char *user_key, const RID *rid)` → 改为 `RC insert_entry(const char *user_key, const char *value, int value_len)`（value_len 必须 == header.value_size，否则 RC::INVALID_ARGUMENT；ATTR 模式下内部 make 全键 = user_key + value 后缀）。同步改 `delete_entry(const char *user_key, const RID *rid)` → `delete_entry(const char *user_key, const char *value, int value_len)`。`create_new_tree`、`insert_entry_into_leaf_node`、`insert_entry_into_parent` 的参数一并从 RID 改为 (const char *value, int value_len)（bplus_tree.h:606-633 区）。
   - `make_key(const char *user_key, const RID &rid)`（bplus_tree.h:646）→ `make_key(const char *user_key, bool is_min)`：COMPOSITE/ROWID 模式 key_size==user_key 长度，直接 memcpy user_key（调用方保证长度 == key_size）；ATTR* 模式 = user_key(attr_length) + value_size 字节 0x00(is_min)/0xFF。
   - 新增 `RC get_value(const char *user_key, int key_len, const char *&value, int &value_len)`：内部 [k,k] 精确扫描取第一条的 value（复用 BplusTreeScanner::open(true,true) + 新 next_entry 重载）；空树/未找到返回 RC::RECORD_NOT_EXIST（rc.h:43 已定义）；key_mode 为 COMPOSITE/ROWID 时 key_len 必须 == key_size（key 无后缀），ATTR* 模式 key_len == attr_length（后缀由 make_key 填充）。
   - `calc_internal_page_capacity(int attr_length)` → 改按 `key_size`；`calc_leaf_page_capacity(int attr_length)` → `calc_leaf_page_capacity(int key_size, int value_size)`（bplus_tree.cpp:34-44）。create 里 internal_max_size/leaf_max_size 缺省时用新函数。
   - `IndexNodeHandler::value_size()`（bplus_tree.cpp:62）→ `return header_.value_size;`；InternalIndexNodeHandler::value_size() 覆盖保持 sizeof(PageNum)。`item_size()` 不动（key_size+value_size 自然正确）。
5. `BplusTreeScanner`：
   - `next_entry(RID &rid)` 保留（ATTR_RID 模式下语义不变，heap 索引路径用）；新增 `RC next_entry(const char *&value, int &value_len, const char *&key)`：value = `leaf_node.value_at(iter_index_)`，key = `leaf_node.key_at(iter_index_)`（整条叶子 key，长度 = header.key_length）；一次推进同时给出两值，迭代前进逻辑与现 fetch_item 相同（抽公共推进，不复制两套）。
   - `open()` 中左右边界填充改用 `tree_handler_.make_key(user_key, is_min)`（RID::min()/max() 不再直接出现于扫描器，bplus_tree.cpp:1885/1949 两处）；`open()` 开头的 `attr_comparator()` 预检（bplus_tree.cpp:1867-1876）仅在 key_mode 为 ATTR_* 时执行，COMPOSITE/ROWID 跳过（该预检只服务单字段索引的 left>right 校验）。
   - `KeyPrinter` 不动；`validate_tree()`/to_string 在非 ATTR 模式打印原始字节十六进制（新增文件内 helper，无公开 API）。
6. `BplusTreeIndex`（bplus_tree_index.{h,cpp}）：`insert_entry(const char *record, const RID *rid)` → 按 Step 2 的 Index 接口新签名实现（本步先改为接收 value 参数，value 由调用方传 rid 字节）；内部 `index_handler_.insert_entry(record + field_meta_.offset(), value, value_len)`。`BplusTreeIndexScanner::next_entry(RID*)` 改按 Step 2 接口（本步先返回 8 字节 value 的 RID 视图，Step 2 统一）。

验收：`bash build.sh debug` 通过；`cd build_debug && python3 ../test/case/miniob_test.py`（全量 SQL 套件）绿；`build_debug/unittest/observer/bplus_tree_test` 绿。

### Task 2 — Index 接口值参数化 + 索引扫描回表抽象

文件：storage/index/index.h、bplus_tree_index.{h,cpp}、ivfflat_index.h、storage/table/heap_table_engine.cpp、sql/operator/index_scan_physical_operator.cpp。

1. `Index`（index.h）：
   - `virtual RC insert_entry(const char *record, const RID *rid) = 0;` → `virtual RC insert_entry(const char *record, const char *value, int value_len) = 0;`；`delete_entry` 同样加 `const char *value, int value_len`。
   - 新增 `virtual RC get_record(Table *table, const char *value, int value_len, Record &record) { return RC::UNSUPPORTED; }`（默认不实现；仅当 value_len==sizeof(RID) 的实现类自行解释为 rid）。
   - `IndexScanner::next_entry(RID *rid) = 0;` → `virtual RC next_entry(const char *&value, int &value_len) = 0;`。
2. `BplusTreeIndex`：insert/delete 转发 value 到树；`get_record(Table*, value, len, record)`：len==sizeof(RID) → `table->get_record(*(const RID*)value, record)`，否则 RC::INVALID_ARGUMENT。`BplusTreeIndexScanner::next_entry(const char *&value, int &value_len)` → `tree_scanner_.next_entry(value, value_len, key)` 并丢弃 key（Step 1 的三出参重载）。
3. `IvfflatIndex`：机械更新 override 签名（value 参数未用，保持 UNSUPPORTED 桩）；若类缺 `create_scanner` override（当前未实现），补一个返回 nullptr 的 override 以过编译。
4. `HeapTableEngine::insert_entry_of_indexes/delete_entry_of_indexes`（heap_table_engine.cpp:170-215 区）与 `create_index` 的填充循环：value 传 `(const char *)&rid, sizeof(RID)`。
5. `IndexScanPhysicalOperator::next`（index_scan_physical_operator.cpp:67）：`const char *value; int value_len;` → `index_scanner_->next_entry(value, value_len)` → `index_->get_record(table_, value, value_len, current_record_)`；`trx_->visit_record` 之后逻辑不动。
6. 用 `grep -rn "insert_entry(\|delete_entry(\|next_entry(" src/observer unittest/` 找全剩余调用点（包括 bplus_tree_index 内部、单测）逐一更新。

验收：build + 全量 SQL 套件绿（heap 语义不变，仅中间层签名变化）。

### Task 3 — 引擎选择接线（枚举 / -E / ENGINE= 语法 / Db / Table 分支）

1. `common/types.h:58` `StorageEngine` 加 `BTREE`（在 LSM 后、UNKNOWN_ENGINE 前）。
2. `Db::get_storage_engine()`（db.h:121-131）加 `else if (0 == strcasecmp(storage_engine_.c_str(), "btree")) engine = StorageEngine::BTREE;`。
3. `Db::create_table`（db.h:70、db.cpp:153）加末参 `const StorageEngine storage_engine = StorageEngine::UNKNOWN_ENGINE`（默认值保持现有调用可编译）；函数体开头 `StorageEngine actual = storage_engine == StorageEngine::UNKNOWN_ENGINE ? get_storage_engine() : storage_engine;`，`table->create(...)` 传 actual。
4. yacc（yacc_sql.y）：
   - `%token ENGINE`；`%type <cstring> storage_engine`。
   - create_table 产生式（yacc_sql.y:324 附近）追加 `storage_engine`：`CREATE TABLE ID LBRACE attr_def_list primary_key RBRACE storage_format storage_engine`，动作里 `create_table.storage_engine = $9;`。
   - 新规则：
     ```
     storage_engine:
         /* empty */ { $$ = nullptr; }
       | ENGINE EQ ID { $$ = $3; }
       ;
     ```
   - lex_sql.l 加关键字规则 `engine`（对齐文件内既有关键字写法，如 STORAGE/FORMAT）。
5. `CreateTableStmt`（sql/stmt/create_table_stmt.{h,cpp}）：成员 `StorageEngine storage_engine_ = StorageEngine::UNKNOWN_ENGINE;` + getter；`create()` 里解析 `create_table.storage_engine`：null/空 → 保持 UNKNOWN_ENGINE；"heap"/"lsm"/"btree"（strcasecmp）→ 对应枚举；其它 → 返回 RC::INVALID_ARGUMENT（日志含非法值）。
6. `CreateTableExecutor`（sql/executor/create_table_executor.cpp:35）调 `db->create_table(..., create_table_stmt->storage_format(), create_table_stmt->storage_engine())`。
7. `Table::create`/`Table::open`（table.cpp:106-114、:176-183）加分支 `else if (table_meta_.storage_engine() == StorageEngine::BTREE) { rc = RC::UNSUPPORTED; LOG_WARN("btree engine not ready: %s", ...); return rc; }` —— 本步结束时 engine=btree 建表报明确的 UNSUPPORTED（Step 4 替换该分支），heap/lsm 表行为不变。
8. main.cpp usage 文本（main.cpp:50）改为 `{heap(default), lsm, btree}`。

验收：build 绿；heap/lsm 既有套件绿；obclient 手测 `create table t1(id int) engine=btree;` 返回明确 unsupported 错误；`engine=lsm` 建表仍走原路径（错误不变，Step 6 修复）。

### Task 4 — BtreeTableEngine + BtreeRecordScanner（聚簇树 CRUD + 三种 key 模式）

新文件：src/observer/storage/table/btree_table_engine.{h,cpp}（GLOB 自动纳入）；src/observer/storage/record/btree_record_scanner.{h,cpp}。

1. `class BtreeTableEngine : public TableEngine`（btree_table_engine.h，结构对齐 heap_table_engine.h）：
   - 成员：`DiskBufferPool *data_buffer_pool_`、`BplusTreeHandler tree_`、`vector<Index *> indexes_`（Step 5 填充）、`Db *db_`、`Table *table_`、`vector<KeyComparator::Field> pk_specs_`（空 = rowid 模式）、`uint64_t next_rowid_ = 0`、`int pk_size_ = 0`（pk 模式 = 各 pk 字段 len 之和；rowid 模式 = 8）。
   - `open()`：`data_file = table_data_file(db_->path().c_str(), table_meta_->name())`（meta_util.h）；`db_->buffer_pool_manager().open_file(db_->log_handler(), data_file.c_str(), data_buffer_pool_)`；`tree_.open(db_->log_handler(), *data_buffer_pool_)`。随后：header `format_version != 2`（新建空文件）→ 初始化：pk_specs_ 从 `table_meta_->primary_keys()` 按声明顺序经 `table_meta_->field(name)` 取 FieldMeta 的 type/offset/len（类型 AttrType）；pk_specs_ 非空 → key_mode=COMPOSITE、key_size=pk_size_、`tree_.set_composite_spec(pk_specs_)`；为空 → key_mode=ROWID、key_size=8。value_size=`table_meta_->record_size()`。用 `calc_leaf_page_capacity(key_size, record_size)` 校验 ≥ 2，否则 RC::INVALID_ARGUMENT（日志含表名与大小）。写 header（root_page=BP_INVALID_PAGE_NUM、format_version=2、key_length、value_size、key_mode、next_rowid=0）后 `tree_.sync()`。文件已 v2 → 直接用 header 重建 pk_specs_（rowid 模式无 spec）并 set_composite_spec；rowid 模式追加：扫描树最右叶子（`BplusTreeScanner` 全量 next_entry 取最大 key 的 8 字节）得 max_rowid，`next_rowid_ = max(tree_.file_header().next_rowid, max_rowid + 1)`。
   - key 构造 helper（文件内私有函数）：
     - pk 模式 `build_key(const char *record, bytes &key)`：按 pk_specs_ 顺序 memcpy record+offset 共 len 字节；`extract_key(const Record &r, bytes &key)`：`r.key()` 非空且长度==pk_size_ → 用 r.key() 字节；否则 build_key(r.data())。
     - rowid 模式：`build_key_rowid(uint64_t id, bytes &key)`（8 字节 memcpy）；`extract_key(Record)`：r.key() 长度==8 → 用之；否则 RC::INVALID_ARGUMENT（rowid 不进 record 数据，无回退）。
   - `insert_record(Record &record)`：造 key（pk：build_key；rowid：`next_rowid_++`）；`tree_.insert_entry(key, record.data(), record_size)`；返回 RC::RECORD_DUPLICATE_KEY 时回退 rowid 计数（rowid 模式 `next_rowid_--`）并原样返回；成功后 `record.set_rid({0,0})`、`record.set_key(string(key 字节, 长度))`；再对 indexes_ 逐 `insert_entry(record.data(), key.data(), key_len)`（Step 5 起生效，先空循环），任一失败按 heap_table_engine.cpp:56-71 的回滚模式回退（先删已插索引再树删）。
   - `delete_record(const Record &record)`：extract_key → 先 indexes_ 逐个 `delete_entry(record.data(), key, len)`（ASSERT SUCCESS 同 heap_table_engine.cpp:96-101）→ `tree_.delete_entry(key, key_len...)`（按 Step 1 签名：delete_entry(user_key, value, value_len)，此处 value=record 数据字节）。
   - `get_record(const RID &rid, Record &record)` → RC::UNSUPPORTED（btree 表无堆 rid；回表走 get_record_by_key）。
   - 新增 `get_record_by_key(const char *key, int key_len, Record &record)`（见 Step 5 的 TableEngine 接口）：`tree_.get_value(key, key_len, value, value_len)` → `record.copy_data(value, value_len)`、`record.set_rid({0,0})`、`record.set_key(string(key, key_len))`。
   - `get_record_scanner(RecordScanner *&scanner, Trx *trx, ReadWriteMode mode)`：`scanner = new BtreeRecordScanner(table_, tree_); scanner->open_scan()`。
   - `visit_record` → RC::UNSUPPORTED；`get_chunk_scanner` → RC::UNSUPPORTED；三个 `*_with_trx` → RC::UNSUPPORTED（UPDATE 无 SQL 算子；MVCC 见下条）。
   - `sync()`：`tree_.sync()` + indexes_ 逐个 sync（照 heap_table_engine.cpp:233-248 结构）。
   - `create_index`：Step 5 实现，本步 RC::UNSUPPORTED；`find_index*`：本步只查 indexes_（Step 5 加 primary_index_）。
   - `init()` → RC::SUCCESS（init 与 open 合一，空实现即可）。
2. `BtreeRecordScanner`（storage/record/btree_record_scanner.{h,cpp}，实现 `RecordScanner` 接口，参考 heap_record_scanner.h 的接口形状）：内部 `BplusTreeScanner` 全量（open(nullptr,0,true,nullptr,0,true)）；`next(Record&)`：`tree_scanner_.next_entry(value, value_len, key)`（Step 1 的三出参重载）→ `record.copy_data(value, value_len)`（复制，叶锁随推进释放）、`set_rid({0,0})`、`set_key(string(key, tree_.file_header().key_length))`（整条树 key 即 pk 编码或 8 字节 rowid，delete 依赖它回取）。
3. MVCC 守卫（storage/trx/mvcc_trx.cpp）：`MvccTrx::insert_record`/`delete_record`/`update_record` 入口各加：`if (table->table_meta().storage_engine() == StorageEngine::BTREE) { LOG_WARN("btree engine only supports vacuous trx"); return RC::UNSUPPORTED; }`。
4. `TableEngine` 接口（table_engine.h）加 `virtual RC get_record_by_key(const char *key, int key_len, Record &record) = 0;`；heap/lsm 实现返回 RC::UNSUPPORTED；`Table`（table.h/.cpp）加同名转发。`Table::create`/`Table::open` 的 BTREE 分支（Step 3 写的 UNSUPPORTED）替换为 `engine_ = make_unique<BtreeTableEngine>(&table_meta_, db_, this);`（含 include）。

验收：build 绿；单测（Step 7 的 btree_table_engine_test 早期版：建 rowid 表、单列/复合 pk 表，插 N 行、重复 pk 报错、按 key 取、全扫序、删、重开库）绿；obclient 手测 `create table t1(id int primary key, name char(20)) engine=btree; insert; select; delete; select;` 输出正确；重启 observer 后 select 仍在。

### Task 5 — BtreePkIndex（主键索引暴露 + 二级索引）与 create_index

新文件：src/observer/storage/index/btree_pk_index.{h,cpp}。

1. `class BtreePkIndex : public Index`（index.h 基类；IndexMeta 用 index_meta.h 的 {name, field}）：
   - 成员：`BplusTreeHandler tree_`（独立索引文件）、`DiskBufferPool *bp_`、`Table *table_`、`vector<KeyComparator::Field> pk_specs_`、`int pk_size_`。
   - `create(Table*, const char *file_name, const IndexMeta&, const FieldMeta& field_meta)`：`bp` 经 `table->db()->buffer_pool_manager()`；`tree_.create(log_handler, bpm, file_name, field_meta.type(), field_meta.len(), /*key_size=*/field_meta.len() + pk_size_, /*value_size=*/pk_size_, BplusTreeKeyMode::ATTR_MEMCMP_VALUE)`。file_name 用 `table_index_file(db->path(), table->name(), index_name)`（meta_util.h）。创建后填充交给引擎的 create_index（本类只管树）。
   - `open(...)` 对称（同参数；无 spec 需要，比较器模式在 header 里）。
   - `insert_entry(const char *record, const char *value, int value_len)`：value 为 pk/rowid 编码（由引擎传，value_len==pk_size_ 否则 INVALID_ARGUMENT）；`tree_.insert_entry(record + field_meta_.offset(), value, value_len)`。
   - `delete_entry` 对称。
   - `create_scanner(left_key, left_len, left_inclusive, right_key, right_len, right_inclusive)` → 内部 BplusTreeScanner 包装（照 BplusTreeIndexScanner，bplus_tree_index.cpp:57-90 结构），`next_entry(const char *&value, int &value_len)` → 树扫描 value 重载。
   - `get_record(Table *table, const char *value, int value_len, Record &record)` → `table->get_record_by_key(value, value_len, record)`。
   - `sync()` → tree_.sync()。
2. 主键暴露：`BtreeTableEngine::find_index_by_field(field_name)`：先查 indexes_；未命中且 pk_specs_.size()==1 且 pk 字段名==field_name → 返回 `primary_index_`（`unique_ptr<BtreePkIndex>` 包裹聚簇树本身：create/open 时不给 file_name、树对象直接引用 `tree_` —— 具体实现：BtreePkIndex 加构造参数 `BplusTreeHandler *shared_tree`（非空则 create/open 只绑定不建文件）；scanner/get_record 复用同一份代码）。`find_index(name)` 不返回主键索引（无名）。
3. `BtreeTableEngine::create_index(Trx *trx, const FieldMeta *field_meta, const char *index_name)`：镜像 heap_table_engine.cpp:125-212 的完整序列：校验参数（index_name/field_meta 为空 → INVALID_ARGUMENT）→ `IndexMeta new_meta; new_meta.init(index_name, *field_meta)` → `new BtreePkIndex()` + `index_file = table_index_file(db_->path().c_str(), table_meta_->name(), index_name)` + `index->create(table_, index_file.c_str(), new_meta, *field_meta)`（失败 → delete index，返回）→ `get_record_scanner` 全扫填充：每行 extract_key 得 key → `index->insert_entry(record.data(), key.data(), key_len)`（失败 → close/delete scanner，返回）→ 关闭并 delete scanner → `indexes_.push_back(index)` → 元数据持久化逐行照 heap_table_engine.cpp:170-211：`TableMeta new_table_meta(*table_meta_); new_table_meta.add_index(new_meta);` → 写 `table_meta_file(db_->path().c_str(), table_meta_->name()) + ".tmp"` → `new_table_meta.serialize(fs)` → `rename(tmp, meta_file)` → `table_meta_->swap(new_table_meta)`；任一步失败仅报错返回（不回收 indexes_/索引文件，与 heap 现有行为一致）。
4. `insert_record`/`delete_record` 的 indexes_ 循环（Step 4 已留空）填充 value = extract_key 结果；`sync()` 循环包含 indexes_。
5. 更新 Step 4 的 `create_index`/`find_index*` 占位为上述实现；`primary_index_` 在 open() 构造（单 pk 时）。

验收：build 绿；obclient 手测：btree 表（单 pk、复合 pk、无 pk）各建二级索引，`select ... where 索引列=常量` 与范围查询结果正确（EXPLAIN 显示走索引）、插入重复索引值不报错（非唯一）、删行后索引查询不返回该行、重启后索引仍在；heap 全量套件仍绿。

### Task 6 — LSM 引擎补齐

文件：storage/table/lsm_table_engine.{h,cpp}、storage/common/codec.{h,cpp}、storage/index/lsm_secondary_index.{h,cpp}（新）。

1. `Codec`（codec.h:449 附近）：新增 `static RC decode(bytes &encoded_key, int64_t &table_id, uint64_t &rid)` —— 按 encode(table_id, rid)（codec.h:388-399）的逆序读出 table_prefix/table_id/rowkey_prefix/rid；现有 `decode(bytes&, int64_t&)` 保留不动。
2. `LsmTableEngine`：
   - `open()` → RC::SUCCESS（数据在 `db_->lsm()`，Db::init 已打开；注释说明）；`init()` → RC::SUCCESS。
   - `delete_record(const Record &record)`：`record.key()` 空 → RC::INVALID_ARGUMENT；`Codec::decode(key 字节, table_id, rid)`；`lsm_->remove(key)`；再对 indexes_ 逐 `delete_entry(record.data(), (const char*)&rid, sizeof(rid))`。
   - `get_record(const RID&, Record&)` → RC::UNSUPPORTED（lsm 无 rid；点查走 LsmSecondaryIndex::get_record）。
   - `visit_record` → RC::SUCCESS（现有调用方 vacuous/lsm trx 均不依赖它做检查，与 lsm_mvcc_trx.cpp:41 注释一致）。
   - `create_index`：镜像 heap 序列（heap_table_engine.cpp:125-160）：`new LsmSecondaryIndex()` + `table_index_file(...)` + create（内部用 `BplusTreeHandler`，value_size=8、key_mode=ATTR_RID、attr_length=field len —— 与 heap 完全相同的树参数，仅 value 语义为 inc_id）→ add_index + 写 meta → 全扫填充：每行从 `record.key()` decode 出 rid → `index->insert_entry(record.data(), (const char*)&rid, sizeof(rid))`。
   - `insert_record(Record &record)`：现有逻辑基础上追加——`uint64_t rid = inc_id_.fetch_add(1)`（把现内联 encode 的 fetch_add 提出来用），encode 后 `record.set_key(string(lsm_key))`，然后 indexes_ 逐个 `insert_entry(record.data(), (const char*)&rid, sizeof(rid))`；任一索引失败 → 从索引回滚已插项 + `lsm_->remove(lsm_key)`（照 heap 回滚模式 heap_table_engine.cpp:56-71）。
   - `insert_record_with_trx(Record &record, Trx *trx)` / `delete_record_with_trx(const Record &record, Trx *trx)`：`auto lsm_trx = dynamic_cast<LsmMvccTrx*>(trx);` 为空 → RC::UNSUPPORTED；否则通过 `lsm_trx->get_trx()` 的 `put/remove`（ob_lsm_transaction.h:62,70）写入同一 key/value 或删除，索引维护与上面相同；`update_record_with_trx` 保持 UNSUPPORTED。
   - `find_index/find_index_by_field`：照 heap 实现遍历 indexes_（lsm_table_engine.h 里两个 nullptr 替换为 cpp 实现）。
   - `get_chunk_scanner` 保持 UNSUPPORTED。
3. `class LsmSecondaryIndex : public BplusTreeIndex`（新文件，或直接在 lsm_table_engine.cpp 内实现——选独立头文件以匹配 index/ 目录惯例）：仅覆盖 `get_record(Table *table, const char *value, int value_len, Record &record)`：value_len==8 → `uint64_t rid; memcpy(&rid, value, 8);` → `Codec::encode(table->table_id(), rid, key)` → `table->db()->lsm()->get(key, &value_str)` → `record.copy_data(...)` + `record.set_key(key)`；长度不符 → RC::INVALID_ARGUMENT。
4. `-t mvcc` 与 engine=lsm 混用不在本步范围（现状即如此），不加守卫。

验收：build 绿；obclient 手测（`-E lsm` 或 `engine=lsm`）：CREATE TABLE 成功（修复 open）、insert/select/delete、`create index` 后点查/范围查、重启 observer 后数据与索引仍在（Db::init 重开 lsm_）；`-t lsm` 下 insert/delete 提交可见、rollback 后不可见（若 LsmMvccTrx 路径可跑通；跑不通则记录并只验收 vacuous 路径——见 Assumptions）。

### Task 7 — 测试与文档

1. `unittest/observer/btree_table_engine_test.cpp`（新，gtest；照 pax_storage_test.cpp 的 Db 构造模式——vacuous trx + disk/vacuous log handler，直接调 Db::create_table/Table 接口，不经 SQL）：覆盖 rowid 表、单列 pk、复合 pk 各：插入 100+ 行（触发分裂）、全扫按 key 序、get_record_by_key 命中/未命中、重复 pk → RECORD_DUPLICATE_KEY、删除若干后再扫、二级索引建/查/删行后不可见、`db 关闭重开（Db::open 重新 open_all_tables）`后数据仍在、rowid 重启后 next_rowid 不回退（插新行不冲突）。
2. `unittest/observer/lsm_table_engine_test.cpp`（新）：insert/scan/delete/create_index 点查/重启重开数据仍在。
3. SQL 用例：`test/case/test/btree_engine.test`、`test/case/test/lsm_engine.test`（照 test/case/test/ 现有 .test 格式；各含：`engine=btree` / `engine=lsm` 建表（含无 pk、单 pk、复合 pk）、insert、select、where 常量点查（走索引）、delete、create index、范围查）；跑法见 Verification。若 harness 需要 result 文件（test/case/result/），用 harness 提供的更新机制生成（`python3 ../test/case/miniob_test.py --help` 里找 update/record 选项），不接受手改 result。
4. 设计文档：新建 `docs/docs/design/miniob-btree-storage-engine.md`：聚簇布局（key/value 编码、三种 key 模式）、二级索引存主键编码与回表路径、rowid 恢复策略（header 持久化 + open 时右叶子扫描）、崩溃一致性（复用 BplusTree mtr 日志，buffer_pool_id 重放）、约束（vacuous trx only、chunk 模式不支持、record+key 必须装下叶子页）、与 heap 的差异（重复主键报错）。同步在 `docs/docs/design/miniob-lsm-tree.md` 的"基于 ObLsm 的表引擎"一节补一行现状（DML/索引已齐、key 仍为自增列）。

## Critical files & anchors

- `src/observer/storage/index/bplus_tree.cpp:60-70`（value_size/key_size/item_size）、`:34-44`（容量计算）、`:868`（create 写 header key_length）、open 尾部（key_comparator_ 初始化）——Step 1 的核心改动锚点；改完必须保证 heap 索引文件旧格式可 open（format_version 判别）。
- `src/observer/storage/table/heap_table_engine.cpp:56-115`（insert/delete 与索引回滚序列）、`:125-160`（create_index 序列）——Btree/Lsm 引擎 insert/delete/create_index 逐行镜像的对象，回滚语义必须一致。
- `src/observer/storage/index/bplus_tree.cpp:1867-1990`（BplusTreeScanner::open 边界填充与预检）——改 make_key 与跳过 COMPOSITE 预检处。
- `src/observer/storage/table/table.cpp:106-114` 与 `:176-183`（引擎选择分支）——BTREE 分支两处，Step 3 与 Step 4 各改一次。
- `src/observer/sql/operator/index_scan_physical_operator.cpp:60-90`（rid→value→index->get_record 的改写点）。
- `src/observer/storage/record/lsm_record_scanner.cpp:60-70`（record.key() 写入处，LSM delete 依赖它）。

## Verification

前置：仓库根目录 `bash build.sh debug` 生成 build_debug；先跑一次全量 `cd build_debug && python3 ../test/case/miniob_test.py` 记录基线（若基线失败先报告，不修）。

每步验收已列在 Step 内；最终一次性验收：
1. `cd build_debug && python3 ../test/case/miniob_test.py` —— 全量 SQL 套件绿（heap 默认路径回归）。
2. `python3 ../test/case/miniob_test.py --test-cases=btree_engine,lsm_engine` —— 新用例绿（若 --test-cases 参数形式不同，以 `python3 ../test/case/miniob_test.py --help` 为准）。
3. `./unittest/observer/btree_table_engine_test`、`./unittest/observer/lsm_table_engine_test`、`./unittest/observer/bplus_tree_test`、`./unittest/observer/pax_storage_test` 全绿。
4. 手工端到端（重启持久性，单测未覆盖 observer 进程级）：`./bin/observer -f ../etc/observer.ini -s /tmp/miniob.sock &`，`./bin/obclient -s /tmp/miniob.sock` 依次执行：
   ```
   create table t(id int primary key, name char(20)) engine=btree;
   insert into t values (2,'b'); insert into t values (1,'a');
   select * from t;            -- 期望按 id 升序: 1 a; 2 b
   create index i1 on t(name);
   select * from t where name='a';   -- 期望 1 a
   ```
   kill observer → 重启同命令 → `select * from t;` 数据仍在；`create table l(id int, v char(10)) engine=lsm; insert; select; delete; create index; 重启 select;` 同验。
5. 提交：只 `git add` 本任务改动的路径（src/observer、unittest/observer、test/case/test、test/case/result、docs/docs/design、src/oblsm 未动则不提交；deps/3rd 两个子模块指针不碰），提交信息如 `feat: btree page storage engine + lsm engine parity`。

## Assumptions & contingencies

- btree 引擎 v1 仅支持 vacuous trx（MVCC 守卫返回 UNSUPPORTED）。这是明确的用户可见取舍：实现 btree 表的 MVCC undo 是另一个项目；如用户要，需重新走设计。
- LSM key 保持自增列（pk 不进 key），与 lsm 设计文档"后续支持主键"的路线一致，本任务不实现 pk-keyed LSM。
- 若 bplus_tree_test 等既有单测直接构造 `IndexFileHeader`/假设 sizeof，Step 1 需同步更新（grep 已列调用点策略）；若发现其它硬编码 sizeof(RID) 的 value 假设（如 bplus_tree_log_entry 的 INIT_HEADER 序列化），随 Step 1 一并泛化（该序列化读 header 结构体，自动含新字段）。
- 若 `-t lsm`（LsmMvccTrx）路径在 Step 6 验收跑不通（ObLsmTransaction 语义与假设不符），回退方案：insert/delete_with_trx 直接走 `lsm_` 的 put/remove（vacuous 语义），在文档标注 lsm 事务为既有未完成项，不阻塞交付。
- 若某测试表的 record_size + key_size 装不进叶子页（容量 <2），报错是设计内行为（RC::INVALID_ARGUMENT + 日志）；遇到真实用例冲突再议（不回退、不静默降级）。
