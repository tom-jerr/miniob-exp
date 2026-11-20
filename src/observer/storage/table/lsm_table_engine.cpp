/* Copyright (c) 2021 Meiyi and/or its affiliates. All rights reserved.
miniob is licensed under Mulan PSL v2.
You can use this software according to the terms and conditions of the Mulan PSL v2.
You may obtain a copy of Mulan PSL v2 at:
         http://license.coscl.org.cn/MulanPSL2
THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
See the Mulan PSL v2 for more details. */

#include "storage/table/lsm_table_engine.h"
#include "common/lang/string.h"
#include "common/log/log.h"
#include "storage/common/codec.h"
#include "storage/common/meta_util.h"
#include "storage/index/bplus_tree_index.h"
#include "storage/index/lsm_secondary_index.h"
#include "storage/record/lsm_record_scanner.h"
#include "storage/table/table.h"
#include "storage/trx/lsm_mvcc_trx.h"

#include <errno.h>
#include <fstream>

namespace {

ObLsmTransaction *lsm_transaction(Trx *trx)
{
  LsmMvccTrx *lsm_trx = dynamic_cast<LsmMvccTrx *>(trx);
  return lsm_trx == nullptr ? nullptr : lsm_trx->get_trx();
}

}  // namespace

LsmTableEngine::~LsmTableEngine()
{
  for (Index *index : indexes_) {
    delete index;
  }
  indexes_.clear();
}

RC LsmTableEngine::insert_record(Record &record) { return write_record(record, nullptr); }

RC LsmTableEngine::insert_record_with_trx(Record &record, Trx *trx)
{
  ObLsmTransaction *lsm_trx = lsm_transaction(trx);
  if (lsm_trx == nullptr) {
    return RC::UNSUPPORTED;
  }
  return write_record(record, lsm_trx);
}

RC LsmTableEngine::write_record(Record &record, ObLsmTransaction *trx)
{
  if (record.data() == nullptr || record.len() <= 0) {
    return RC::INVALID_ARGUMENT;
  }

  const uint64_t rid = inc_id_.fetch_add(1);
  bytes          lsm_key;
  RC             rc = Codec::encode(table_->table_id(), rid, lsm_key);
  if (rc != RC::SUCCESS) {
    inc_id_--;
    return rc;
  }

  const string_view key_view(reinterpret_cast<char *>(lsm_key.data()), lsm_key.size());
  const string_view value_view(record.data(), record.len());
  rc = trx == nullptr ? lsm_->put(key_view, value_view) : trx->put(key_view, value_view);
  if (rc != RC::SUCCESS) {
    inc_id_--;
    return rc;
  }

  record.set_rid(RID{0, 0});
  record.set_key(string(key_view));

  rc = insert_entry_of_indexes(record.data(), rid);
  if (rc != RC::SUCCESS) {
    delete_entry_of_indexes(record.data(), rid, false);
    if (trx == nullptr) {
      lsm_->remove(key_view);
    } else {
      trx->remove(key_view);
    }
    record.set_key("");
    inc_id_--;
  }
  return rc;
}

RC LsmTableEngine::delete_record(const Record &record) { return erase_record(record, nullptr); }

RC LsmTableEngine::delete_record_with_trx(const Record &record, Trx *trx)
{
  ObLsmTransaction *lsm_trx = lsm_transaction(trx);
  if (lsm_trx == nullptr) {
    return RC::UNSUPPORTED;
  }
  return erase_record(record, lsm_trx);
}

RC LsmTableEngine::erase_record(const Record &record, ObLsmTransaction *trx)
{
  if (record.key().empty()) {
    return RC::INVALID_ARGUMENT;
  }

  bytes   lsm_key(record.key().begin(), record.key().end());
  int64_t table_id = 0;
  uint64_t rid     = 0;
  RC rc = Codec::decode(lsm_key, table_id, rid);
  if (rc != RC::SUCCESS) {
    return rc;
  }
  if (table_id != table_->table_id()) {
    LOG_WARN("table id mismatch. table=%s, stored=%ld", table_->name(), table_id);
    return RC::INVALID_ARGUMENT;
  }

  if (record.data() != nullptr) {
    for (Index *index : indexes_) {
      rc = index->delete_entry(record.data(), reinterpret_cast<const char *>(&rid), sizeof(rid));
      ASSERT(RC::SUCCESS == rc, "failed to delete entry from index. table name=%s, index name=%s, rc=%s",
          table_->name(), index->index_meta().name(), strrc(rc));
    }
  }

  const string_view key_view(reinterpret_cast<char *>(lsm_key.data()), lsm_key.size());
  return trx == nullptr ? lsm_->remove(key_view) : trx->remove(key_view);
}

RC LsmTableEngine::insert_entry_of_indexes(const char *record, uint64_t rid)
{
  RC rc = RC::SUCCESS;
  for (Index *index : indexes_) {
    rc = index->insert_entry(record, reinterpret_cast<const char *>(&rid), sizeof(rid));
    if (rc != RC::SUCCESS) {
      break;
    }
  }
  return rc;
}

RC LsmTableEngine::delete_entry_of_indexes(const char *record, uint64_t rid, bool error_on_not_exists)
{
  RC rc = RC::SUCCESS;
  for (Index *index : indexes_) {
    rc = index->delete_entry(record, reinterpret_cast<const char *>(&rid), sizeof(rid));
    if (rc != RC::SUCCESS && (rc != RC::RECORD_INVALID_KEY || error_on_not_exists)) {
      break;
    }
  }
  return rc;
}

RC LsmTableEngine::get_record_scanner(RecordScanner *&scanner, Trx *trx, ReadWriteMode mode)
{
  scanner = new LsmRecordScanner(table_, db_->lsm(), trx);
  RC rc   = scanner->open_scan();
  if (rc != RC::SUCCESS) {
    LOG_ERROR("failed to open scanner. rc=%s", strrc(rc));
  }
  return rc;
}

RC LsmTableEngine::open() { return open_indexes(); }

RC LsmTableEngine::open_indexes()
{
  const int index_num = table_meta_->index_num();
  for (int i = 0; i < index_num; i++) {
    const IndexMeta *index_meta = table_meta_->index(i);
    const FieldMeta *field_meta = table_meta_->field(index_meta->field());
    if (field_meta == nullptr) {
      LOG_WARN("index field not found. table=%s, index=%s, field=%s",
          table_meta_->name(), index_meta->name(), index_meta->field());
      return RC::INTERNAL;
    }

    LsmSecondaryIndex *index      = new LsmSecondaryIndex();
    const string       index_file = table_index_file(db_->path().c_str(), table_meta_->name(), index_meta->name());
    RC                 rc         = index->open(table_, index_file.c_str(), *index_meta, *field_meta);
    if (rc != RC::SUCCESS) {
      LOG_WARN("failed to open index. table=%s, index=%s, file=%s, rc=%s",
          table_meta_->name(), index_meta->name(), index_file.c_str(), strrc(rc));
      delete index;
      return rc;
    }
    indexes_.push_back(index);
  }
  return RC::SUCCESS;
}

RC LsmTableEngine::create_index(Trx *trx, const FieldMeta *field_meta, const char *index_name)
{
  if (common::is_blank(index_name) || field_meta == nullptr) {
    LOG_WARN("invalid arguments. table=%s", table_meta_->name());
    return RC::INVALID_ARGUMENT;
  }

  IndexMeta new_index_meta;
  RC        rc = new_index_meta.init(index_name, *field_meta);
  if (rc != RC::SUCCESS) {
    return rc;
  }

  LsmSecondaryIndex *index      = new LsmSecondaryIndex();
  const string       index_file = table_index_file(db_->path().c_str(), table_meta_->name(), index_name);
  rc = index->create(table_, index_file.c_str(), new_index_meta, *field_meta);
  if (rc != RC::SUCCESS) {
    LOG_WARN("failed to create index. table=%s, index=%s, rc=%s", table_meta_->name(), index_name, strrc(rc));
    delete index;
    return rc;
  }

  RecordScanner *scanner = nullptr;
  rc                     = get_record_scanner(scanner, trx, ReadWriteMode::READ_ONLY);
  if (rc != RC::SUCCESS) {
    delete index;
    return rc;
  }

  Record record;
  while (OB_SUCC(rc = scanner->next(record))) {
    bytes    lsm_key(record.key().begin(), record.key().end());
    int64_t  table_id = 0;
    uint64_t rid      = 0;
    rc                = Codec::decode(lsm_key, table_id, rid);
    if (rc == RC::SUCCESS) {
      rc = index->insert_entry(record.data(), reinterpret_cast<const char *>(&rid), sizeof(rid));
    }
    if (rc != RC::SUCCESS) {
      break;
    }
  }
  if (rc == RC::RECORD_EOF) {
    rc = RC::SUCCESS;
  }
  scanner->close_scan();
  delete scanner;
  if (rc != RC::SUCCESS) {
    LOG_WARN("failed to fill index. table=%s, index=%s, rc=%s", table_meta_->name(), index_name, strrc(rc));
    delete index;
    return rc;
  }

  indexes_.push_back(index);

  TableMeta new_table_meta(*table_meta_);
  rc = new_table_meta.add_index(new_index_meta);
  if (rc != RC::SUCCESS) {
    return rc;
  }

  const string tmp_file = table_meta_file(db_->path().c_str(), table_meta_->name()) + ".tmp";
  fstream      fs;
  fs.open(tmp_file, std::ios_base::out | std::ios_base::binary | std::ios_base::trunc);
  if (!fs.is_open()) {
    LOG_WARN("failed to open meta file for write. file=%s", tmp_file.c_str());
    return RC::IOERR_OPEN;
  }
  if (new_table_meta.serialize(fs) < 0) {
    LOG_WARN("failed to serialize table meta. file=%s", tmp_file.c_str());
    return RC::IOERR_WRITE;
  }
  fs.close();

  const string meta_file = table_meta_file(db_->path().c_str(), table_meta_->name());
  if (0 != rename(tmp_file.c_str(), meta_file.c_str())) {
    LOG_WARN("failed to rename meta file. from=%s, to=%s, errno=%d", tmp_file.c_str(), meta_file.c_str(), errno);
    return RC::IOERR_WRITE;
  }
  table_meta_->swap(new_table_meta);

  LOG_INFO("successfully added index. table=%s, index=%s", table_meta_->name(), index_name);
  return RC::SUCCESS;
}

RC LsmTableEngine::sync()
{
  for (Index *index : indexes_) {
    RC rc = index->sync();
    if (rc != RC::SUCCESS) {
      return rc;
    }
  }
  return RC::SUCCESS;
}

Index *LsmTableEngine::find_index(const char *index_name) const
{
  for (Index *index : indexes_) {
    if (0 == strcmp(index->index_meta().name(), index_name)) {
      return index;
    }
  }
  return nullptr;
}

Index *LsmTableEngine::find_index_by_field(const char *field_name) const
{
  for (Index *index : indexes_) {
    if (0 == strcmp(index->index_meta().field(), field_name)) {
      return index;
    }
  }
  return nullptr;
}
