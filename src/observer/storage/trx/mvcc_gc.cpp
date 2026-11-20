#include "storage/trx/mvcc_gc.h"

#include <algorithm>
#include <vector>

#include "common/log/log.h"
#include "storage/db/db.h"
#include "storage/record/record_scanner.h"
#include "storage/table/table.h"
#include "storage/table/table_meta.h"
#include "storage/trx/mvcc_trx.h"
#include "storage/trx/trx_status.h"

using namespace std;
using namespace common;

namespace {

// 可见性水位：所有已启动事务中最早的快照；没有活跃事务时就是当前提交点。
int64_t gc_watermark(MvccTrxKit &kit)
{
  int64_t watermark = kit.current_commit_seq();
  vector<Trx *> trxes;
  kit.all_trxes(trxes);
  for (Trx *trx : trxes) {
    auto *mvcc_trx = dynamic_cast<MvccTrx *>(trx);
    if (mvcc_trx != nullptr && mvcc_trx->started()) {
      watermark = min(watermark, mvcc_trx->snapshot_seq());
    }
  }
  return watermark;
}

bool xid_is_aborted(TxnStatusTable &status_table, int64_t xid)
{
  return status_table.get(xid).state == TxnStatusTable::State::ABORTED;
}

bool xid_is_committed_below(TxnStatusTable &status_table, int64_t xid, int64_t watermark)
{
  TxnStatusTable::Status status = status_table.get(xid);
  return status.state == TxnStatusTable::State::COMMITTED && status.commit_seq <= watermark;
}

bool is_dead_version(TxnStatusTable &status_table, int64_t begin_raw, int64_t end_raw, int64_t watermark)
{
  if (begin_raw == 0) {
    return true;
  }
  if (begin_raw < 0 && xid_is_aborted(status_table, -begin_raw)) {
    return true;
  }
  if (end_raw < 0) {
    int64_t xid = -end_raw;
    return xid_is_aborted(status_table, xid) || xid_is_committed_below(status_table, xid, watermark);
  }
  return end_raw > 0 && end_raw <= watermark;
}

}  // namespace

RC MvccGc::collect(Db &db)
{
  auto *mvcc_kit = dynamic_cast<MvccTrxKit *>(&db.trx_kit());
  if (mvcc_kit == nullptr) {
    return RC::SUCCESS;
  }
  MvccTrxKit &kit = *mvcc_kit;

  const int64_t watermark = gc_watermark(kit);

  vector<string> table_names;
  db.all_tables(table_names);
  for (const string &table_name : table_names) {
    Table *table = db.find_table(table_name.c_str());
    if (table == nullptr) {
      continue;
    }
    span<const FieldMeta> trx_fields = table->table_meta().trx_fields();
    if (trx_fields.size() < 2) {
      continue;
    }
    const FieldMeta &begin_field = trx_fields[0];
    const FieldMeta &end_field   = trx_fields[1];

    // 先收集 RID，避免边扫描页面边删除槽位。
    vector<RID> dead_rids;
    RecordScanner *scanner = nullptr;
    RC rc = table->get_record_scanner(scanner, nullptr, ReadWriteMode::READ_ONLY);
    if (OB_FAIL(rc)) {
      LOG_WARN("failed to scan table for gc. table=%s, rc=%s", table->name(), strrc(rc));
      continue;
    }
    Record record;
    while (RC::SUCCESS == scanner->next(record)) {
      int64_t begin_raw = 0;
      int64_t end_raw   = 0;
      memcpy(&begin_raw, record.data() + begin_field.offset(), sizeof(begin_raw));
      memcpy(&end_raw, record.data() + end_field.offset(), sizeof(end_raw));
      if (is_dead_version(kit.status_table(), begin_raw, end_raw, watermark)) {
        dead_rids.push_back(record.rid());
      }
    }
    scanner->close_scan();
    delete scanner;

    for (const RID &rid : dead_rids) {
      Record dead;
      rc = table->get_record(rid, dead);
      if (rc == RC::RECORD_NOT_EXIST) {
        continue;
      }
      if (OB_FAIL(rc)) {
        LOG_WARN("failed to read dead record. table=%s, rid=%s, rc=%s",
            table->name(), rid.to_string().c_str(), strrc(rc));
        continue;
      }
      // get_record 返回页面内指针，删除前复制。
      Record owned;
      rc = owned.copy_data(dead.data(), dead.len());
      if (OB_FAIL(rc)) {
        continue;
      }
      owned.set_rid(rid);
      owned.set_key(dead.key());
      rc = table->delete_record(owned);
      if (OB_FAIL(rc)) {
        LOG_WARN("failed to delete dead record. table=%s, rid=%s, rc=%s",
            table->name(), rid.to_string().c_str(), strrc(rc));
      }
    }
  }
  return RC::SUCCESS;
}
