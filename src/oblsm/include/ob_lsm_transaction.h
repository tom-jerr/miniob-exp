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

#include "common/sys/rc.h"
#include "common/lang/string_view.h"
#include "common/lang/string.h"
#include "common/lang/vector.h"
#include "common/lang/memory.h"
#include <unordered_set>
#include "oblsm/include/ob_lsm_iterator.h"
#include "oblsm/include/ob_lsm_options.h"
#include "oblsm/include/ob_lsm.h"
#include "oblsm/memtable/ob_skiplist.h"
#include "oblsm/table/ob_sstable.h"
#include "oblsm/util/ob_arena.h"
#include "oblsm/util/ob_coding.h"
#include "common/lang/map.h"

namespace oceanbase {

/**
 * @class ObLsmTransaction
 * @brief A class representing a transaction in oblsm supporting MVCC + OCC and Spill-to-Disk WriteBatch.
 */
class ObLsmTransaction
{
public:
  ObLsmTransaction(ObLsm *db, uint64_t ts);

  ~ObLsmTransaction();

  RC get(const string_view &key, string *value);

  RC put(const string_view &key, const string_view &value);

  RC remove(const string_view &key);

  ObLsmIterator *new_iterator(ObLsmReadOptions options);

  RC commit();

  RC rollback();

  void                      set_isolation_level(TransactionIsolationLevel level) { isolation_level_ = level; }
  TransactionIsolationLevel isolation_level() const { return isolation_level_; }
  uint64_t                  ts() const { return ts_; }
  size_t                    spill_count() const { return spill_sstables_.size(); }

private:
  RC             spill_inner_store_to_disk();
  void           cleanup_spill_files();
  ObLsmIterator *new_write_batch_iterator() const;

  ObLsm                    *db_              = nullptr;
  uint64_t                  ts_              = 0;
  uint64_t                  trx_uid_         = 0;
  TransactionIsolationLevel isolation_level_ = TransactionIsolationLevel::SNAPSHOT_ISOLATION;

  map<string, string>           inner_store_;
  size_t                        current_batch_bytes_ = 0;
  size_t                        max_batch_bytes_     = 4 * 1024 * 1024;
  vector<shared_ptr<ObSSTable>> spill_sstables_;

  std::unordered_set<string> read_set_;
  std::unordered_set<string> write_set_;
  bool                       committed_   = false;
  bool                       rolled_back_ = false;
};

/**
 * @class TrxInnerMapIterator
 * @brief An iterator for traversing the transaction's in-memory store
 */
class TrxInnerMapIterator : public ObLsmIterator
{
public:
  explicit TrxInnerMapIterator(const map<string, string> &store) : store_(store), iter_(store_.end()), valid_(false) {}
  ~TrxInnerMapIterator() override = default;

  bool valid() const override { return valid_ && iter_ != store_.end(); }

  void seek_to_first() override
  {
    iter_  = store_.begin();
    valid_ = (iter_ != store_.end());
  }

  void seek_to_last() override
  {
    if (store_.empty()) {
      iter_  = store_.end();
      valid_ = false;
    } else {
      iter_  = std::prev(store_.end());
      valid_ = true;
    }
  }

  void seek(const string_view &key) override
  {
    iter_  = store_.lower_bound(string(key));
    valid_ = (iter_ != store_.end());
  }

  void next() override
  {
    if (valid_ && iter_ != store_.end()) {
      ++iter_;
      valid_ = (iter_ != store_.end());
    }
  }

  string_view key() const override { return iter_->first; }
  string_view value() const override { return iter_->second; }

private:
  const map<string, string>          &store_;
  map<string, string>::const_iterator iter_;
  bool                                valid_ = false;
};
}  // namespace oceanbase