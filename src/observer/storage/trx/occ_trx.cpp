#include "storage/trx/occ_trx.h"

#include <cstring>

#include "common/log/log.h"
#include "storage/table/table_meta.h"

using namespace common;

OccTrx::OccTrx(MvccTrxKit &kit, LogHandler &log_handler) : MvccTrx(kit, log_handler) { set_type(TrxKit::Type::OCC); }

OccTrx::OccTrx(MvccTrxKit &kit, LogHandler &log_handler, int64_t trx_id)
    : MvccTrx(kit, log_handler, trx_id)
{
  set_type(TrxKit::Type::OCC);
}

bool OccTrx::already_read(Table *table, const RID &rid) const
{
  for (const ReadEntry &entry : read_set_) {
    if (entry.table == table && entry.rid == rid) {
      return true;
    }
  }
  return false;
}

RC OccTrx::visit_record(Table *table, Record &record, ReadWriteMode mode)
{
  RC rc = MvccTrx::visit_record(table, record, mode);
  if (rc != RC::SUCCESS) {
    return rc;
  }

  span<const FieldMeta> trx_fields = table->table_meta().trx_fields();
  if (trx_fields.size() < 2 || already_read(table, record.rid())) {
    return RC::SUCCESS;
  }

  ReadEntry entry;
  entry.table = table;
  entry.rid   = record.rid();
  memcpy(&entry.begin_raw, record.data() + trx_fields[0].offset(), sizeof(entry.begin_raw));
  memcpy(&entry.end_raw, record.data() + trx_fields[1].offset(), sizeof(entry.end_raw));
  read_set_.push_back(entry);
  return RC::SUCCESS;
}

RC OccTrx::commit()
{
  if (!started()) {
    return RC::SUCCESS;
  }

  const int64_t snapshot = snapshot_seq();
  for (const ReadEntry &entry : read_set_) {
    span<const FieldMeta> trx_fields = entry.table->table_meta().trx_fields();
    if (trx_fields.size() < 2) {
      continue;
    }

    Record current;
    RC     rc = entry.table->get_record(entry.rid, current);
    if (rc == RC::RECORD_NOT_EXIST) {
      LOG_INFO("occ read validation failed: row disappeared. trx=%d rid=%s",
          id(), entry.rid.to_string().c_str());
      rollback();
      return RC::LOCKED_CONCURRENCY_CONFLICT;
    }
    if (OB_FAIL(rc)) {
      LOG_WARN("failed to read record during occ validation. trx=%d rid=%s rc=%s",
          id(), entry.rid.to_string().c_str(), strrc(rc));
      rollback();
      return rc;
    }

    int64_t begin_raw = 0;
    int64_t end_raw   = 0;
    memcpy(&begin_raw, current.data() + trx_fields[0].offset(), sizeof(begin_raw));
    memcpy(&end_raw, current.data() + trx_fields[1].offset(), sizeof(end_raw));

    if (begin_raw != entry.begin_raw || end_raw == entry.end_raw) {
      continue;
    }

    bool committed_after_snapshot = false;
    if (end_raw > 0) {
      committed_after_snapshot = end_raw > snapshot;
    } else if (end_raw < 0) {
      TxnStatusTable::Status status = trx_kit().status_table().get(-end_raw);
      committed_after_snapshot =
          status.state == TxnStatusTable::State::COMMITTED && status.commit_seq > snapshot;
    }

    if (committed_after_snapshot) {
      LOG_INFO("occ read validation failed: version changed. trx=%d rid=%s snapshot=%ld",
          id(), entry.rid.to_string().c_str(), snapshot);
      rollback();
      return RC::LOCKED_CONCURRENCY_CONFLICT;
    }
  }

  return MvccTrx::commit();
}

Trx *OccTrxKit::create_trx(LogHandler &log_handler) { return new OccTrx(*this, log_handler); }

Trx *OccTrxKit::create_trx(LogHandler &log_handler, int64_t trx_id)
{
  return new OccTrx(*this, log_handler, trx_id);
}
