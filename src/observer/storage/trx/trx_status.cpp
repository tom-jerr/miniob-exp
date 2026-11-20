/* Copyright (c) 2021 OceanBase and/or its affiliates. All rights reserved.
miniob is licensed under Mulan PSL v2.
You can use this software according to the terms and conditions of the Mulan PSL v2.
You may obtain a copy of Mulan PSL v2 at:
         http://license.coscl.org.cn/MulanPSL2
THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
See the Mulan PSL v2 for more details. */

#include "storage/trx/trx_status.h"
#include "common/lang/mutex.h"

using namespace common;

RC TxnStatusTable::register_trx(int64_t trx_id)
{
  scoped_lock lock(lock_);
  auto        iter = statuses_.find(trx_id);
  if (iter != statuses_.end()) {
    return RC::INVALID_ARGUMENT;
  }
  statuses_.emplace(trx_id, Status{State::ACTIVE, 0});
  return RC::SUCCESS;
}

RC TxnStatusTable::commit_trx(int64_t trx_id, int64_t commit_seq)
{
  scoped_lock lock(lock_);
  Status     &status = statuses_[trx_id];
  status.state       = State::COMMITTED;
  status.commit_seq  = commit_seq;
  return RC::SUCCESS;
}

RC TxnStatusTable::abort_trx(int64_t trx_id)
{
  scoped_lock lock(lock_);
  Status     &status = statuses_[trx_id];
  status.state       = State::ABORTED;
  status.commit_seq  = 0;
  return RC::SUCCESS;
}

TxnStatusTable::Status TxnStatusTable::get(int64_t trx_id) const
{
  scoped_lock lock(lock_);
  auto        iter = statuses_.find(trx_id);
  if (iter == statuses_.end()) {
    return Status{State::ACTIVE, 0};
  }
  return iter->second;
}
