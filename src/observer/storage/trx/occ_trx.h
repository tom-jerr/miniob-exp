#pragma once

#include <vector>

#include "storage/trx/mvcc_trx.h"

class LogHandler;

// Snapshot-isolation OCC：读到已提交版本时记录 read set，
// 提交前校验这些版本是否仍对快照有效；写写冲突仍在执行期由
// MvccTrx 的 tuple xid 检查返回。
class OccTrx : public MvccTrx
{
public:
  OccTrx(MvccTrxKit &kit, LogHandler &log_handler);
  OccTrx(MvccTrxKit &kit, LogHandler &log_handler, int64_t trx_id);

  RC visit_record(Table *table, Record &record, ReadWriteMode mode) override;
  RC commit() override;

private:
  struct ReadEntry
  {
    Table  *table     = nullptr;
    RID     rid;
    int64_t begin_raw = 0;
    int64_t end_raw   = 0;
  };

  bool already_read(Table *table, const RID &rid) const;

private:
  std::vector<ReadEntry> read_set_;
};

class OccTrxKit : public MvccTrxKit
{
public:
  Trx *create_trx(LogHandler &log_handler) override;
  Trx *create_trx(LogHandler &log_handler, int64_t trx_id) override;
};
