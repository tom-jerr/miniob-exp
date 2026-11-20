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

#include "common/lang/mutex.h"
#include "common/lang/unordered_map.h"
#include "common/sys/rc.h"

/**
 * @brief 事务状态表
 * @details 让提交只做一次原子状态切换。元组上的负事务号通过该表解析为
 * COMMITTED(commit_seq) / ACTIVE / ABORTED，因此提交回填可以延后。
 */
class TxnStatusTable
{
public:
  enum class State
  {
    ACTIVE,
    COMMITTED,
    ABORTED,
  };

  struct Status
  {
    State   state     = State::ACTIVE;
    int64_t commit_seq = 0;
  };

  RC     register_trx(int64_t trx_id);
  RC     commit_trx(int64_t trx_id, int64_t commit_seq);
  RC     abort_trx(int64_t trx_id);
  Status get(int64_t trx_id) const;

private:
  mutable common::Mutex           lock_;
  unordered_map<int64_t, Status> statuses_;
};
