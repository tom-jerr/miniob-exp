#pragma once

#include "sql/stmt/stmt.h"

// SYNC 语句：触发一次 Db::sync（MVCC 死版本 GC + 刷盘）。
class SyncStmt : public Stmt
{
public:
  SyncStmt()           = default;
  virtual ~SyncStmt()  = default;

  StmtType type() const override { return StmtType::SYNC; }

  static RC create(Stmt *&stmt)
  {
    stmt = new SyncStmt();
    return RC::SUCCESS;
  }
};
