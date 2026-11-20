/* Copyright (c) 2021 OceanBase and/or its affiliates. All rights reserved.
miniob is licensed under Mulan PSL v2.
You can use this software according to the terms and conditions of the Mulan PSL v2.
You may obtain a copy of Mulan PSL v2 at:
         http://license.coscl.org.cn/MulanPSL2
THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
See the Mulan PSL v2 for more details. */

#pragma once

#include "storage/table/table_engine.h"
#include "storage/index/index.h"
#include "storage/index/bplus_tree.h"
#include "storage/index/btree_pk_index.h"

class Table;

/**
 * @brief 以 B+树叶子页聚簇保存整行数据的表引擎
 * @details 有主键时 key 为主键字段按声明顺序拼接，叶子 value 为整行；
 * 无主键时 key 为 8 字节隐藏 rowid，next_rowid 持久化在树头部。
 * 仅支持 vacuous trx；chunk 扫描与 UPDATE 不支持。
 */
class BtreeTableEngine : public TableEngine
{
public:
  friend class Table;
  BtreeTableEngine(TableMeta *table_meta, Db *db, Table *table) : TableEngine(table_meta), db_(db), table_(table) {}
  ~BtreeTableEngine() override;

  RC create() override;
  RC open() override;
  RC init() override { return RC::SUCCESS; }

  RC insert_record(Record &record) override;
  RC delete_record(const Record &record) override;
  RC insert_record_with_trx(Record &record, Trx *trx) override { return RC::UNSUPPORTED; }
  RC delete_record_with_trx(const Record &record, Trx *trx) override { return RC::UNSUPPORTED; }
  RC update_record_with_trx(const Record &old_record, const Record &new_record, Trx *trx) override;
  RC get_record(const RID &rid, Record &record) override { return RC::UNSUPPORTED; }
  RC get_record_by_key(const char *key, int key_len, Record &record) override;

  RC create_index(Trx *trx, const FieldMeta *field_meta, const char *index_name) override;
  RC get_record_scanner(RecordScanner *&scanner, Trx *trx, ReadWriteMode mode) override;
  RC get_chunk_scanner(ChunkFileScanner &scanner, Trx *trx, ReadWriteMode mode) override { return RC::UNSUPPORTED; }
  RC visit_record(const RID &rid, function<bool(Record &)> visitor) override { return RC::UNSUPPORTED; }
  RC sync() override;

  Index *find_index(const char *index_name) const override;
  Index *find_index_by_field(const char *field_name) const override;

  const BplusTreeHandler &tree_handler() const { return tree_; }

private:
  RC open_data_file();
  RC build_key_specs();
  RC build_primary_index();
  RC open_indexes();
  RC build_key_from_record(const Record &record, string &key) const;
  RC build_key_from_data(const char *data, string &key) const;
  RC recover_next_rowid();

private:
  DiskBufferPool              *data_buffer_pool_ = nullptr;
  BplusTreeHandler             tree_;
  vector<Index *>              indexes_;
  Db                          *db_;
  Table                       *table_;
  vector<KeyComparator::Field> pk_specs_;  ///< 空表示 rowid 模式
  int                          pk_size_ = 0;
  uint64_t                     next_rowid_ = 0;
  unique_ptr<BtreePkIndex>     primary_index_;  ///< 单列主键时暴露的聚簇索引
};
