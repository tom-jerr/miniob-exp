/* Copyright (c) 2021 OceanBase and/or its affiliates. All rights reserved.
miniob is licensed under Mulan PSL v2.
You can use this software according to the terms and conditions of the Mulan PSL v2.
You may obtain a copy of Mulan PSL v2 at:
         http://license.coscl.org.cn/MulanPSL2
THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
See the Mulan PSL v2 for more details. */

//
// Created by Wangyunlai on 2023/04/24.
//

#include "storage/trx/mvcc_trx.h"
#include "storage/db/db.h"
#include "storage/field/field.h"
#include "storage/trx/mvcc_trx_log.h"
#include "common/lang/algorithm.h"

MvccTrxKit::~MvccTrxKit()
{
  vector<Trx *> tmp_trxes;
  tmp_trxes.swap(trxes_);

  for (Trx *trx : tmp_trxes) {
    delete trx;
  }
}

RC MvccTrxKit::init()
{
  // 事务使用一些特殊的字段，放到每行记录中，表示行记录的可见性。
  fields_ = vector<FieldMeta>{
      // field_id in trx fields is invisible.
      FieldMeta("__trx_xid_begin", AttrType::INTS, 0 /*attr_offset*/, 8 /*attr_len*/, false /*visible*/, -1/*field_id*/),
      FieldMeta("__trx_xid_end", AttrType::INTS, 0 /*attr_offset*/, 8 /*attr_len*/, false /*visible*/, -2/*field_id*/)};

  LOG_INFO("init mvcc trx kit done.");
  return RC::SUCCESS;
}

const vector<FieldMeta> *MvccTrxKit::trx_fields() const { return &fields_; }

int64_t MvccTrxKit::next_trx_id() { return ++current_trx_id_; }

int64_t MvccTrxKit::next_commit_seq() { return commit_seq_.fetch_add(1) + 1; }

void MvccTrxKit::advance_commit_seq(int64_t commit_seq)
{
  int64_t current = commit_seq_.load(std::memory_order_acquire);
  while (current < commit_seq && !commit_seq_.compare_exchange_weak(current, commit_seq)) {
  }
}

int64_t MvccTrxKit::max_trx_id() const { return numeric_limits<int64_t>::max(); }

RC MvccTrxKit::validate_trx_fields(const TableMeta &table_meta) const
{
  span<const FieldMeta> trx_fields = table_meta.trx_fields();
  if (trx_fields.size() < 2) {
    LOG_ERROR("table %s has no transaction fields", table_meta.name());
    return RC::INVALID_ARGUMENT;
  }

  for (size_t i = 0; i < 2; i++) {
    if (trx_fields[i].len() != static_cast<int>(sizeof(int64_t))) {
      LOG_ERROR("table %s has incompatible transaction field %s length %d; expected 8 (old 4-byte-xid table?)",
          table_meta.name(), trx_fields[i].name(), trx_fields[i].len());
      return RC::INVALID_ARGUMENT;
    }
  }
  return RC::SUCCESS;
}

Trx *MvccTrxKit::create_trx(LogHandler &log_handler)
{
  Trx *trx = new MvccTrx(*this, log_handler);
  if (trx != nullptr) {
    lock_.lock();
    trxes_.push_back(trx);
    lock_.unlock();
  }
  return trx;
}

Trx *MvccTrxKit::create_trx(LogHandler &log_handler, int64_t trx_id)
{
  Trx *trx = new MvccTrx(*this, log_handler, trx_id);
  if (trx != nullptr) {
    lock_.lock();
    trxes_.push_back(trx);
    if (current_trx_id_ < trx_id) {
      current_trx_id_ = trx_id;
    }
    lock_.unlock();
  }
  return trx;
}

void MvccTrxKit::destroy_trx(Trx *trx)
{
  lock_.lock();
  for (auto iter = trxes_.begin(), itend = trxes_.end(); iter != itend; ++iter) {
    if (*iter == trx) {
      trxes_.erase(iter);
      break;
    }
  }
  lock_.unlock();

  delete trx;
}

void MvccTrxKit::all_trxes(vector<Trx *> &trxes)
{
  lock_.lock();
  trxes = trxes_;
  lock_.unlock();
}

LogReplayer *MvccTrxKit::create_log_replayer(Db &db, LogHandler &log_handler)
{
  return new MvccTrxLogReplayer(db, *this, log_handler);
}

////////////////////////////////////////////////////////////////////////////////

MvccTrx::MvccTrx(MvccTrxKit &kit, LogHandler &log_handler) : Trx(TrxKit::Type::MVCC), trx_kit_(kit), log_handler_(log_handler)
{}

MvccTrx::MvccTrx(MvccTrxKit &kit, LogHandler &log_handler, int64_t trx_id) 
  : Trx(TrxKit::Type::MVCC), trx_kit_(kit), log_handler_(log_handler), trx_id_(trx_id)
{
  started_    = true;
  recovering_ = true;
}

MvccTrx::~MvccTrx() {}

RC MvccTrx::insert_record(Table *table, Record &record)
{
  if (table->table_meta().storage_engine() == StorageEngine::BTREE) {
    LOG_WARN("btree storage engine only supports vacuous trx. table=%s", table->name());
    return RC::UNSUPPORTED;
  }

  Field begin_field;
  Field end_field;
  trx_fields(table, begin_field, end_field);

  begin_field.set_int64(record, -trx_id_);
  end_field.set_int64(record, trx_kit_.max_trx_id());

  RC rc = table->insert_record(record);
  if (rc != RC::SUCCESS) {
    LOG_WARN("failed to insert record into table. rc=%s", strrc(rc));
    return rc;
  }

  rc = log_handler_.insert_record(trx_id_, table, record.rid());
  ASSERT(rc == RC::SUCCESS, "failed to append insert record log. trx id=%d, table id=%d, rid=%s, record len=%d, rc=%s",
         trx_id_, table->table_id(), record.rid().to_string().c_str(), record.len(), strrc(rc));

  operations_.push_back(Operation(Operation::Type::INSERT, table, record.rid()));
  return rc;
}

RC MvccTrx::delete_record(Table *table, Record &record)
{
  if (table->table_meta().storage_engine() == StorageEngine::BTREE) {
    LOG_WARN("btree storage engine only supports vacuous trx. table=%s", table->name());
    return RC::UNSUPPORTED;
  }

  Field begin_field;
  Field end_field;
  trx_fields(table, begin_field, end_field);

  RC delete_result = RC::SUCCESS;

  RC rc = table->visit_record(record.rid(), [this, table, &delete_result, &end_field](Record &inplace_record) -> bool {
    RC rc = this->visit_record(table, inplace_record, ReadWriteMode::READ_WRITE);
    if (OB_FAIL(rc)) {
      delete_result = rc;
      return false;
    }

    end_field.set_int64(inplace_record, -trx_id_);
    return true;
  });

  if (OB_FAIL(rc)) {
    LOG_WARN("failed to visit record. rc=%s", strrc(rc));
    return rc;
  }

  if (OB_FAIL(delete_result)) {
    LOG_TRACE("record is not visible. rid=%s, rc=%s", record.rid().to_string().c_str(), strrc(delete_result));
    return delete_result;
  }

  rc = log_handler_.delete_record(trx_id_, table, record.rid());
  ASSERT(rc == RC::SUCCESS, "failed to append delete record log. trx id=%d, table id=%d, rid=%s, record len=%d, rc=%s",
      trx_id_, table->table_id(), record.rid().to_string().c_str(), record.len(), strrc(rc));

  operations_.push_back(Operation(Operation::Type::DELETE, table, record.rid()));

  return RC::SUCCESS;
}

RC MvccTrx::visit_record(Table *table, Record &record, ReadWriteMode mode)
{
  Field begin_field;
  Field end_field;
  trx_fields(table, begin_field, end_field);

  const int64_t begin_raw = begin_field.get_int64(record);
  const int64_t end_raw   = end_field.get_int64(record);

  enum class XidKind
  {
    SEQ,
    SELF,
    ACTIVE,
    ABORTED,
  };
  struct ResolvedXid
  {
    XidKind kind;
    int64_t seq;
  };

  auto resolve = [this](int64_t xid) -> ResolvedXid {
    if (xid == 0) {
      return {XidKind::ABORTED, 0};
    }
    if (xid > 0) {
      return {XidKind::SEQ, xid};
    }

    const int64_t other = -xid;
    if (other == trx_id_) {
      return {XidKind::SELF, 0};
    }

    TxnStatusTable::Status status = trx_kit_.status_table().get(other);
    if (status.state == TxnStatusTable::State::COMMITTED) {
      return {XidKind::SEQ, status.commit_seq};
    }
    if (status.state == TxnStatusTable::State::ABORTED) {
      return {XidKind::ABORTED, 0};
    }
    return {XidKind::ACTIVE, 0};
  };

  const ResolvedXid begin = resolve(begin_raw);
  const ResolvedXid end   = resolve(end_raw);

  const bool begin_visible = begin.kind == XidKind::SELF ||
                             (begin.kind == XidKind::SEQ && begin.seq <= snapshot_seq_);
  if (!begin_visible) {
    LOG_TRACE("record invisible by begin. trx=%ld snapshot=%ld begin=%ld end=%ld",
        trx_id_, snapshot_seq_, begin_raw, end_raw);
    return RC::RECORD_INVISIBLE;
  }

  switch (end.kind) {
    case XidKind::SELF: return RC::RECORD_INVISIBLE;
    case XidKind::SEQ: return end.seq > snapshot_seq_ ? RC::SUCCESS : RC::RECORD_INVISIBLE;
    case XidKind::ABORTED: return RC::SUCCESS;
    case XidKind::ACTIVE: {
      if (mode == ReadWriteMode::READ_WRITE) {
        LOG_TRACE("concurrency conflict. trx=%ld begin=%ld end=%ld", trx_id_, begin_raw, end_raw);
        return RC::LOCKED_CONCURRENCY_CONFLICT;
      }
      return RC::SUCCESS;
    }
  }
  return RC::SUCCESS;
}

/**
 * @brief 获取指定表上的事务使用的字段
 *
 * @param table 指定的表
 * @param begin_xid_field 返回处理begin_xid的字段
 * @param end_xid_field   返回处理end_xid的字段
 */
void MvccTrx::trx_fields(Table *table, Field &begin_xid_field, Field &end_xid_field) const
{
  const TableMeta      &table_meta = table->table_meta();
  span<const FieldMeta> trx_fields = table_meta.trx_fields();
  ASSERT(trx_fields.size() >= 2, "invalid trx fields number. %d", trx_fields.size());

  begin_xid_field.set_table(table);
  begin_xid_field.set_field(&trx_fields[0]);
  end_xid_field.set_table(table);
  end_xid_field.set_field(&trx_fields[1]);
}

RC MvccTrx::start_if_need()
{
  if (!started_) {
    ASSERT(operations_.empty(), "try to start a new trx while operations is not empty");
    trx_id_       = trx_kit_.next_trx_id();
    snapshot_seq_ = trx_kit_.current_commit_seq();
    trx_kit_.status_table().register_trx(trx_id_);
    LOG_DEBUG("current thread change to new trx with %ld, snapshot=%ld", trx_id_, snapshot_seq_);
    started_ = true;
  }
  return RC::SUCCESS;
}

RC MvccTrx::commit()
{
  if (!started_) {
    return RC::SUCCESS;
  }

  const int64_t commit_seq = trx_kit_.next_commit_seq();
  RC            rc         = log_handler_.commit(trx_id_, commit_seq);
  if (OB_FAIL(rc)) {
    LOG_WARN("failed to append commit log. trx=%ld rc=%s", trx_id_, strrc(rc));
    return rc;
  }

  // 提交点：状态表一次原子发布，随后所有 -T 元组同时可见
  trx_kit_.status_table().commit_trx(trx_id_, commit_seq);
  started_ = false;

  rc = commit_with_trx_id(commit_seq);
  if (rc != RC::SUCCESS) {
    // 提交点已确立，回填失败留给恢复/GC 重试，不把已提交事务报成失败
    LOG_WARN("commit point published but backfill failed. trx=%ld commit_seq=%ld rc=%s", trx_id_, commit_seq, strrc(rc));
  }
  return RC::SUCCESS;
}

RC MvccTrx::commit_with_trx_id(int64_t commit_xid)
{
  RC rc = RC::SUCCESS;

  for (const Operation &operation : operations_) {
    switch (operation.type()) {
      case Operation::Type::INSERT: {
        RID    rid(operation.page_num(), operation.slot_num());
        Table *table = operation.table();
        Field  begin_xid_field, end_xid_field;
        trx_fields(table, begin_xid_field, end_xid_field);

        auto record_updater = [this, &begin_xid_field, commit_xid](Record &record) -> bool {
          LOG_DEBUG("before commit insert record. trx id=%d, begin xid=%d, commit xid=%d, lbt=%s",
                    trx_id_, begin_xid_field.get_int64(record), commit_xid, lbt());
          const int64_t current_begin = begin_xid_field.get_int64(record);
          if (current_begin == commit_xid) {
            return true;
          }
          if (current_begin != -this->trx_id_) {
            LOG_WARN("unexpected begin xid while committing. trx=%ld begin=%ld commit=%ld",
                trx_id_, current_begin, commit_xid);
            return true;
          }
          begin_xid_field.set_int64(record, commit_xid);
          return true;
        };

        rc = operation.table()->visit_record(rid, record_updater);
        ASSERT(rc == RC::SUCCESS, "failed to get record while committing. rid=%s, rc=%s",
               rid.to_string().c_str(), strrc(rc));
      } break;

      case Operation::Type::DELETE: {
        Table *table = operation.table();
        RID    rid(operation.page_num(), operation.slot_num());

        Field begin_xid_field, end_xid_field;
        trx_fields(table, begin_xid_field, end_xid_field);

        auto record_updater = [this, &end_xid_field, commit_xid](Record &record) -> bool {
          const int64_t current_end = end_xid_field.get_int64(record);
          if (current_end == commit_xid) {
            return true;
          }
          if (current_end != -trx_id_) {
            LOG_WARN("unexpected end xid while committing. trx=%ld end=%ld commit=%ld",
                trx_id_, current_end, commit_xid);
            return true;
          }
          end_xid_field.set_int64(record, commit_xid);
          return true;
        };

        rc = operation.table()->visit_record(rid, record_updater);
        ASSERT(rc == RC::SUCCESS, "failed to get record while committing. rid=%s, rc=%s",
               rid.to_string().c_str(), strrc(rc));
      } break;

      default: {
        ASSERT(false, "unsupported operation. type=%d", static_cast<int>(operation.type()));
      }
    }
  }

  if (!recovering_) {
    rc = log_handler_.commit(static_cast<int32_t>(trx_id_), commit_xid);
  }

  operations_.clear();

  LOG_TRACE("append trx commit log. trx id=%d, commit_xid=%d, rc=%s", trx_id_, commit_xid, strrc(rc));
  return rc;
}

RC MvccTrx::rollback()
{
  RC rc = RC::SUCCESS;
  if (!started_) {
    return RC::SUCCESS;
  }
  trx_kit_.status_table().abort_trx(trx_id_);
  started_ = false;

  for (auto iter = operations_.rbegin(), itend = operations_.rend(); iter != itend; ++iter) {
    const Operation &operation = *iter;
    switch (operation.type()) {
      case Operation::Type::INSERT: {
        RID    rid(operation.page_num(), operation.slot_num());
        Table *table = operation.table();
        // 这里也可以不删除，仅仅给数据加个标识位，等垃圾回收器来收割也行
        Record record;
        record.set_rid(rid);
        if (recovering_) {
          // 恢复的时候，需要额外判断下当前记录是否还是当前事务拥有。是的话才能删除记录
          rc = table->get_record(rid, record);
          if (OB_SUCC(rc)) {
            Field begin_xid_field, end_xid_field;
            trx_fields(table, begin_xid_field, end_xid_field);
            if (begin_xid_field.get_int64(record) != -trx_id_) {
              continue;
            }
          } else if (RC::RECORD_NOT_EXIST == rc) {
            continue;
          } else {
            LOG_WARN("failed to get record while rollback. table=%s, rid=%s, rc=%s", 
                     table->name(), rid.to_string().c_str(), strrc(rc));
            return rc;
          }
        }
        rc = table->delete_record(record);
        ASSERT(rc == RC::SUCCESS, "failed to delete record while rollback. rid=%s, rc=%s",
               rid.to_string().c_str(), strrc(rc));
      } break;

      case Operation::Type::DELETE: {
        Table *table = operation.table();
        RID    rid(operation.page_num(), operation.slot_num());

        ASSERT(rc == RC::SUCCESS, "failed to get record while rollback. rid=%s, rc=%s",
              rid.to_string().c_str(), strrc(rc));
        Field begin_xid_field, end_xid_field;
        trx_fields(table, begin_xid_field, end_xid_field);

        auto record_updater = [this, &end_xid_field](Record &record) -> bool {
          if (recovering_ && end_xid_field.get_int64(record) != -trx_id_) {
            return false;
          }

          ASSERT(end_xid_field.get_int64(record) == -trx_id_, 
                "got an invalid record while rollback. end xid=%d, this trx id=%d", 
                end_xid_field.get_int64(record), trx_id_);

          end_xid_field.set_int64(record, trx_kit_.max_trx_id());
          return true;
        };

        rc = table->visit_record(rid, record_updater);
        ASSERT(rc == RC::SUCCESS, "failed to get record while committing. rid=%s, rc=%s",
               rid.to_string().c_str(), strrc(rc));
      } break;

      default: {
        ASSERT(false, "unsupported operation. type=%d", static_cast<int>(operation.type()));
      }
    }
  }

  operations_.clear();

  if (!recovering_) {
    rc = log_handler_.rollback(trx_id_);
  }
  LOG_TRACE("append trx rollback log. trx id=%d, rc=%s", trx_id_, strrc(rc));
  return rc;
}

RC find_table(Db *db, const LogEntry &log_entry, Table *&table)
{
  auto *trx_log_header = reinterpret_cast<const MvccTrxLogHeader *>(log_entry.data());
  switch (MvccTrxLogOperation(trx_log_header->operation_type).type()) {
    case MvccTrxLogOperation::Type::INSERT_RECORD:
    case MvccTrxLogOperation::Type::DELETE_RECORD: {
      auto *trx_log_record = reinterpret_cast<const MvccTrxRecordLogEntry *>(log_entry.data());
      table                = db->find_table(trx_log_record->table_id);
      if (nullptr == table) {
        LOG_WARN("no such table to redo. log record=%s", trx_log_record->to_string().c_str());
        return RC::SCHEMA_TABLE_NOT_EXIST;
      }
    } break;
    default: {
      // do nothing
    } break;
  }
  return RC::SUCCESS;
}

RC MvccTrx::redo(Db *db, const LogEntry &log_entry)
{
  auto *trx_log_header = reinterpret_cast<const MvccTrxLogHeader *>(log_entry.data());
  Table *table = nullptr;
  RC     rc    = find_table(db, log_entry, table);
  if (OB_FAIL(rc)) {
    return rc;
  }

  switch (MvccTrxLogOperation(trx_log_header->operation_type).type()) {
    case MvccTrxLogOperation::Type::INSERT_RECORD: {
      auto *trx_log_record = reinterpret_cast<const MvccTrxRecordLogEntry *>(log_entry.data());
      operations_.push_back(Operation(Operation::Type::INSERT, table, trx_log_record->rid));
    } break;

    case MvccTrxLogOperation::Type::DELETE_RECORD: {
      auto *trx_log_record = reinterpret_cast<const MvccTrxRecordLogEntry *>(log_entry.data());
      operations_.push_back(Operation(Operation::Type::DELETE, table, trx_log_record->rid));
    } break;

    case MvccTrxLogOperation::Type::COMMIT: {
      // auto *trx_log_record = reinterpret_cast<const MvccTrxCommitLogEntry *>(log_entry.data());
      // commit_with_trx_id(trx_log_record->commit_trx_id);
      // 遇到了提交日志，说明前面的记录都已经提交成功了
    } break;

    case MvccTrxLogOperation::Type::ROLLBACK: {
      // do nothing
      // 遇到了回滚日志，前面的回滚操作也都执行完成了
    } break;

    default: {
      ASSERT(false, "unsupported redo log. log_record=%s", log_entry.to_string().c_str());
      return RC::INTERNAL;
    } break;
  }

  return RC::SUCCESS;
}
