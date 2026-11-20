/* Copyright (c) 2021 OceanBase and/or its affiliates. All rights reserved.
miniob is licensed under Mulan PSL v2.
You can use this software according to the terms and conditions of the Mulan PSL v2.
You may obtain a copy of Mulan PSL v2 at:
         http://license.coscl.org.cn/MulanPSL2
THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
See the Mulan PSL v2 for more details. */

#include "storage/table/btree_table_engine.h"
#include "common/lang/string.h"
#include "common/log/log.h"
#include "storage/common/meta_util.h"
#include "storage/record/btree_record_scanner.h"
#include "storage/table/table.h"

namespace {

constexpr int ROWID_KEY_SIZE = static_cast<int>(sizeof(uint64_t));

}  // namespace

RC BtreeTableEngine::create()
{
  RC rc = open_data_file();
  if (rc != RC::SUCCESS) {
    return rc;
  }

  rc = build_key_specs();
  if (rc != RC::SUCCESS) {
    return rc;
  }

  const bool rowid_mode = pk_specs_.empty();
  const int  key_size   = rowid_mode ? ROWID_KEY_SIZE : pk_size_;
  const AttrType    attr_type   = rowid_mode ? AttrType::INTS : pk_specs_[0].type;
  const int         attr_length = rowid_mode ? ROWID_KEY_SIZE : pk_specs_[0].len;
  const BplusTreeKeyMode key_mode = rowid_mode ? BplusTreeKeyMode::ROWID : BplusTreeKeyMode::COMPOSITE;

  rc = tree_.create(db_->log_handler(), *data_buffer_pool_, attr_type, attr_length, key_size,
      table_meta_->record_size(), key_mode);
  if (rc != RC::SUCCESS) {
    LOG_WARN("failed to create btree table. table=%s, rc=%s", table_meta_->name(), strrc(rc));
    return rc;
  }

  if (key_mode == BplusTreeKeyMode::COMPOSITE) {
    rc = tree_.set_composite_spec(pk_specs_);
    if (rc != RC::SUCCESS) {
      return rc;
    }
  }

  next_rowid_ = 0;
  return RC::SUCCESS;
}

RC BtreeTableEngine::open()
{
  RC rc = open_data_file();
  if (rc != RC::SUCCESS) {
    return rc;
  }

  rc = tree_.open(db_->log_handler(), *data_buffer_pool_);
  if (rc != RC::SUCCESS) {
    LOG_WARN("failed to open btree table. table=%s, rc=%s", table_meta_->name(), strrc(rc));
    return rc;
  }

  rc = build_key_specs();
  if (rc != RC::SUCCESS) {
    return rc;
  }

  const IndexFileHeader &header = tree_.file_header();
  if (header.value_size != table_meta_->record_size()) {
    LOG_WARN("btree table record size mismatch. table=%s, stored=%d, meta=%d",
        table_meta_->name(), header.value_size, table_meta_->record_size());
    return RC::INVALID_ARGUMENT;
  }

  if (pk_specs_.empty()) {
    if (header.key_mode != static_cast<int32_t>(BplusTreeKeyMode::ROWID) || header.key_length != ROWID_KEY_SIZE) {
      LOG_WARN("btree table key mode mismatch. table=%s, key_mode=%d, key_length=%d",
          table_meta_->name(), header.key_mode, header.key_length);
      return RC::INVALID_ARGUMENT;
    }
    return recover_next_rowid();
  }

  if (header.key_mode != static_cast<int32_t>(BplusTreeKeyMode::COMPOSITE) || header.key_length != pk_size_) {
    LOG_WARN("btree table key mode mismatch. table=%s, key_mode=%d, key_length=%d, pk_size=%d",
        table_meta_->name(), header.key_mode, header.key_length, pk_size_);
    return RC::INVALID_ARGUMENT;
  }

  return tree_.set_composite_spec(pk_specs_);
}

RC BtreeTableEngine::open_data_file()
{
  const string data_file = table_data_file(db_->path().c_str(), table_meta_->name());
  RC rc = db_->buffer_pool_manager().open_file(db_->log_handler(), data_file.c_str(), data_buffer_pool_);
  if (rc != RC::SUCCESS) {
    LOG_WARN("failed to open btree data file. file=%s, rc=%s", data_file.c_str(), strrc(rc));
  }
  return rc;
}

RC BtreeTableEngine::build_key_specs()
{
  pk_specs_.clear();
  pk_size_ = 0;

  for (const string &field_name : table_meta_->primary_keys()) {
    const FieldMeta *field = table_meta_->field(field_name.c_str());
    if (field == nullptr) {
      LOG_WARN("primary key field not found. table=%s, field=%s", table_meta_->name(), field_name.c_str());
      return RC::SCHEMA_FIELD_MISSING;
    }
    pk_specs_.push_back({field->type(), field->offset(), field->len()});
    pk_size_ += field->len();
  }
  return RC::SUCCESS;
}

RC BtreeTableEngine::build_key_from_data(const char *data, string &key) const
{
  if (data == nullptr || pk_specs_.empty()) {
    return RC::INVALID_ARGUMENT;
  }

  key.clear();
  key.reserve(pk_size_);
  for (const KeyComparator::Field &field : pk_specs_) {
    key.append(data + field.offset, field.len);
  }
  return RC::SUCCESS;
}

RC BtreeTableEngine::build_key_from_record(const Record &record, string &key) const
{
  if (pk_specs_.empty()) {
    if (record.key().size() != static_cast<size_t>(ROWID_KEY_SIZE)) {
      LOG_WARN("rowid key is missing or has invalid length. table=%s", table_meta_->name());
      return RC::INVALID_ARGUMENT;
    }
    key = record.key();
    return RC::SUCCESS;
  }

  if (!record.key().empty() && static_cast<int>(record.key().size()) == pk_size_) {
    key = record.key();
    return RC::SUCCESS;
  }
  return build_key_from_data(record.data(), key);
}

RC BtreeTableEngine::recover_next_rowid()
{
  next_rowid_ = static_cast<uint64_t>(tree_.file_header().next_rowid);

  BplusTreeScanner scanner(tree_);
  RC rc = scanner.open(nullptr, 0, true, nullptr, 0, true);
  if (rc != RC::SUCCESS) {
    LOG_WARN("failed to open btree scanner while recovering rowid. table=%s, rc=%s",
        table_meta_->name(), strrc(rc));
    return rc;
  }

  const char *value     = nullptr;
  const char *key       = nullptr;
  int         value_len = 0;
  while (RC::SUCCESS == (rc = scanner.next_entry(value, value_len, key))) {
    uint64_t rowid = 0;
    memcpy(&rowid, key, sizeof(rowid));
    if (rowid + 1 > next_rowid_) {
      next_rowid_ = rowid + 1;
    }
  }
  scanner.close();

  if (rc != RC::RECORD_EOF) {
    LOG_WARN("btree scan failed while recovering rowid. table=%s, rc=%s", table_meta_->name(), strrc(rc));
    return rc;
  }

  tree_.set_next_rowid(static_cast<int64_t>(next_rowid_));
  return RC::SUCCESS;
}

RC BtreeTableEngine::insert_record(Record &record)
{
  const int record_size = table_meta_->record_size();
  if (record.data() == nullptr || record.len() != record_size) {
    LOG_WARN("invalid record. table=%s, record length=%d, expected=%d",
        table_meta_->name(), record.len(), record_size);
    return RC::INVALID_ARGUMENT;
  }

  const bool rowid_mode = pk_specs_.empty();
  string     key;
  if (rowid_mode) {
    const uint64_t rowid = next_rowid_++;
    key.assign(reinterpret_cast<const char *>(&rowid), sizeof(rowid));
  } else {
    RC rc = build_key_from_record(record, key);
    if (rc != RC::SUCCESS) {
      return rc;
    }
  }

  RC rc = tree_.insert_entry(key.data(), record.data(), record_size);
  if (rc != RC::SUCCESS) {
    if (rowid_mode) {
      next_rowid_--;
    }
    return rc;
  }

  record.set_rid(RID{0, 0});
  record.set_key(key);

  for (Index *index : indexes_) {
    rc = index->insert_entry(record.data(), key.data(), key.size());
    if (rc != RC::SUCCESS) {
      for (Index *inserted : indexes_) {
        inserted->delete_entry(record.data(), key.data(), key.size());
      }
      tree_.delete_entry(key.data(), record.data(), record_size);
      record.set_key("");
      if (rowid_mode) {
        next_rowid_--;
      }
      return rc;
    }
  }
  return RC::SUCCESS;
}

RC BtreeTableEngine::delete_record(const Record &record)
{
  string key;
  RC     rc = build_key_from_record(record, key);
  if (rc != RC::SUCCESS) {
    return rc;
  }

  for (Index *index : indexes_) {
    rc = index->delete_entry(record.data(), key.data(), key.size());
    ASSERT(RC::SUCCESS == rc, "failed to delete entry from index. table name=%s, index name=%s, rc=%s",
        table_meta_->name(), index->index_meta().name(), strrc(rc));
  }

  return tree_.delete_entry(key.data(), record.data(), table_meta_->record_size());
}

RC BtreeTableEngine::get_record_by_key(const char *key, int key_len, Record &record)
{
  const char *value     = nullptr;
  int         value_len = 0;
  RC rc = tree_.get_value(key, key_len, value, value_len);
  if (rc != RC::SUCCESS) {
    return rc;
  }

  rc = record.copy_data(value, value_len);
  if (rc != RC::SUCCESS) {
    return rc;
  }
  record.set_rid(RID{0, 0});
  record.set_key(string(key, key_len));
  return RC::SUCCESS;
}

RC BtreeTableEngine::create_index(Trx *trx, const FieldMeta *field_meta, const char *index_name)
{
  return RC::UNSUPPORTED;
}

RC BtreeTableEngine::get_record_scanner(RecordScanner *&scanner, Trx *trx, ReadWriteMode mode)
{
  BtreeRecordScanner *btree_scanner = new BtreeRecordScanner(table_, tree_);
  RC                  rc            = btree_scanner->open_scan();
  if (rc != RC::SUCCESS) {
    LOG_WARN("failed to open btree record scanner. table=%s, rc=%s", table_meta_->name(), strrc(rc));
    delete btree_scanner;
    scanner = nullptr;
    return rc;
  }
  scanner = btree_scanner;
  return RC::SUCCESS;
}

RC BtreeTableEngine::sync()
{
  RC rc = tree_.sync();
  if (rc != RC::SUCCESS) {
    LOG_WARN("failed to sync btree. table=%s, rc=%s", table_meta_->name(), strrc(rc));
    return rc;
  }

  for (Index *index : indexes_) {
    rc = index->sync();
    if (rc != RC::SUCCESS) {
      LOG_WARN("failed to sync index. table=%s, index=%s, rc=%s",
          table_meta_->name(), index->index_meta().name(), strrc(rc));
      return rc;
    }
  }
  return RC::SUCCESS;
}

Index *BtreeTableEngine::find_index(const char *index_name) const
{
  for (Index *index : indexes_) {
    if (0 == strcmp(index->index_meta().name(), index_name)) {
      return index;
    }
  }
  return nullptr;
}

Index *BtreeTableEngine::find_index_by_field(const char *field_name) const
{
  for (Index *index : indexes_) {
    if (0 == strcmp(index->index_meta().field(), field_name)) {
      return index;
    }
  }
  return nullptr;
}
