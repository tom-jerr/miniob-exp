/* Copyright (c) 2021 OceanBase and/or its affiliates. All rights reserved.
miniob is licensed under Mulan PSL v2.
You can use this software according to the terms and conditions of the Mulan PSL v2.
You may obtain a copy of Mulan PSL v2 at:
         http://license.coscl.org.cn/MulanPSL2
THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
See the Mulan PSL v2 for more details. */

#include "storage/index/btree_pk_index.h"
#include "common/lang/string.h"
#include "common/log/log.h"
#include "storage/index/bplus_tree_index.h"
#include "storage/db/db.h"
#include "storage/table/table.h"

BtreePkIndex::~BtreePkIndex()
{
  if (inited_ && !shared_) {
    index_handler_.close();
  }
}

int BtreePkIndex::calc_pk_size(const TableMeta &table_meta)
{
  int size = 0;
  for (const string &field_name : table_meta.primary_keys()) {
    const FieldMeta *field = table_meta.field(field_name.c_str());
    if (field == nullptr) {
      return -1;
    }
    size += field->len();
  }
  if (size == 0) {
    size = static_cast<int>(sizeof(uint64_t));
  }
  return size;
}

RC BtreePkIndex::create(Table *table, const char *file_name, const IndexMeta &index_meta, const FieldMeta &field_meta)
{
  if (inited_) {
    return RC::RECORD_OPENNED;
  }

  Index::init(index_meta, field_meta);

  pk_size_ = calc_pk_size(table->table_meta());
  if (pk_size_ <= 0) {
    LOG_WARN("failed to calculate primary key size. table=%s", table->name());
    return RC::INVALID_ARGUMENT;
  }

  BufferPoolManager &bpm = table->db()->buffer_pool_manager();
  RC rc = index_handler_.create(table->db()->log_handler(), bpm, file_name, field_meta.type(), field_meta.len(),
      field_meta.len() + pk_size_, pk_size_, BplusTreeKeyMode::ATTR_MEMCMP_VALUE);
  if (rc != RC::SUCCESS) {
    LOG_WARN("failed to create index tree. table=%s, index=%s, rc=%s", table->name(), index_meta.name(), strrc(rc));
    return rc;
  }

  tree_   = &index_handler_;
  inited_ = true;
  return RC::SUCCESS;
}

RC BtreePkIndex::open(Table *table, const char *file_name, const IndexMeta &index_meta, const FieldMeta &field_meta)
{
  if (inited_) {
    return RC::RECORD_OPENNED;
  }

  Index::init(index_meta, field_meta);

  pk_size_ = calc_pk_size(table->table_meta());
  if (pk_size_ <= 0) {
    LOG_WARN("failed to calculate primary key size. table=%s", table->name());
    return RC::INVALID_ARGUMENT;
  }

  BufferPoolManager &bpm = table->db()->buffer_pool_manager();
  RC rc = index_handler_.open(table->db()->log_handler(), bpm, file_name);
  if (rc != RC::SUCCESS) {
    LOG_WARN("failed to open index tree. table=%s, index=%s, rc=%s", table->name(), index_meta.name(), strrc(rc));
    return rc;
  }

  tree_   = &index_handler_;
  inited_ = true;
  return RC::SUCCESS;
}

RC BtreePkIndex::insert_entry(const char *record, const char *value, int value_len)
{
  if (tree_ == nullptr || record == nullptr || value == nullptr || value_len != pk_size_) {
    return RC::INVALID_ARGUMENT;
  }
  return tree_->insert_entry(record + field_meta_.offset(), value, value_len);
}

RC BtreePkIndex::delete_entry(const char *record, const char *value, int value_len)
{
  if (tree_ == nullptr || record == nullptr || value == nullptr || value_len != pk_size_) {
    return RC::INVALID_ARGUMENT;
  }
  return tree_->delete_entry(record + field_meta_.offset(), value, value_len);
}

RC BtreePkIndex::get_record(Table *table, const char *value, int value_len, Record &record)
{
  if (tree_ == nullptr || value == nullptr) {
    return RC::INVALID_ARGUMENT;
  }

  if (!shared_) {
    if (value_len != pk_size_) {
      return RC::INVALID_ARGUMENT;
    }
    return table->get_record_by_key(value, value_len, record);
  }

  // 主键索引扫描返回的 value 就是聚簇叶子的整行数据
  RC rc = record.copy_data(value, value_len);
  if (rc != RC::SUCCESS) {
    return rc;
  }
  record.set_rid(RID{0, 0});
  record.set_key(string(value + field_meta_.offset(), field_meta_.len()));
  return RC::SUCCESS;
}

IndexScanner *BtreePkIndex::create_scanner(const char *left_key, int left_len, bool left_inclusive, const char *right_key,
    int right_len, bool right_inclusive)
{
  if (tree_ == nullptr) {
    return nullptr;
  }

  BplusTreeIndexScanner *scanner = new BplusTreeIndexScanner(*tree_);
  RC                     rc      = scanner->open(left_key, left_len, left_inclusive, right_key, right_len, right_inclusive);
  if (rc != RC::SUCCESS) {
    LOG_WARN("failed to open index scanner. rc=%s", strrc(rc));
    delete scanner;
    return nullptr;
  }
  return scanner;
}

RC BtreePkIndex::sync()
{
  if (tree_ == nullptr) {
    return RC::SUCCESS;
  }
  return tree_->sync();
}
