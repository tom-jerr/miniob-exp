/* Copyright (c) 2021 OceanBase and/or its affiliates. All rights reserved.
miniob is licensed under Mulan PSL v2.
You can use this software according to the terms and conditions of the Mulan PSL v2.
You may obtain a copy of Mulan PSL v2 at:
         http://license.coscl.org.cn/MulanPSL2
THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
See the Mulan PSL v2 for more details. */

#include "oblsm/include/ob_lsm_transaction.h"
#include "oblsm/ob_lsm_impl.h"
#include "oblsm/table/ob_sstable_builder.h"
#include "oblsm/table/ob_merger.h"
#include "oblsm/util/ob_comparator.h"
#include "common/lang/memory.h"
#include "common/lang/filesystem.h"

namespace oceanbase {

namespace {
static atomic<uint64_t> g_trx_uid_generator{1};

/**
 * @class SpillSSTUserIterator
 * @brief Wraps a Spill SSTable's internal iterator and exposes user_key and raw value (including tombstones).
 */
class SpillSSTUserIterator : public ObLsmIterator
{
public:
  explicit SpillSSTUserIterator(ObLsmIterator *sst_iter) : sst_iter_(sst_iter) {}
  ~SpillSSTUserIterator() override = default;

  bool        valid() const override { return sst_iter_ != nullptr && sst_iter_->valid(); }
  void        seek_to_first() override { sst_iter_->seek_to_first(); }
  void        seek_to_last() override { sst_iter_->seek_to_last(); }
  void        next() override { sst_iter_->next(); }
  string_view key() const override { return extract_user_key(sst_iter_->key()); }
  string_view value() const override { return sst_iter_->value(); }

  void seek(const string_view &user_key) override
  {
    lookup_key_.clear();
    put_numeric<uint64_t>(&lookup_key_, user_key.size() + SEQ_SIZE);
    lookup_key_.append(user_key.data(), user_key.size());
    put_numeric<uint64_t>(&lookup_key_, UINT64_MAX);
    sst_iter_->seek(string_view(lookup_key_.data(), lookup_key_.size()));
  }

private:
  unique_ptr<ObLsmIterator> sst_iter_;
  string                    lookup_key_;
};

/**
 * @class MultiLevelWriteBatchIterator
 * @brief Merges the active in-memory WriteBatch and spilled SSTables in newest-to-oldest priority order.
 *        Produces each modified user_key once with its most recent value (empty value means tombstone).
 */
class MultiLevelWriteBatchIterator : public ObLsmIterator
{
public:
  explicit MultiLevelWriteBatchIterator(vector<unique_ptr<ObLsmIterator>> &&sources)
      : sources_(std::move(sources)), current_idx_(-1)
  {}
  ~MultiLevelWriteBatchIterator() override = default;

  bool valid() const override { return current_idx_ >= 0 && current_idx_ < static_cast<int>(sources_.size()); }

  void seek_to_first() override
  {
    for (auto &src : sources_) {
      src->seek_to_first();
    }
    find_smallest_unique();
  }

  void seek_to_last() override
  {
    for (auto &src : sources_) {
      src->seek_to_last();
    }
    current_idx_ = -1;
    for (size_t i = 0; i < sources_.size(); ++i) {
      if (sources_[i]->valid()) {
        if (current_idx_ == -1 || comp_.compare(sources_[i]->key(), sources_[current_idx_]->key()) > 0) {
          current_idx_ = static_cast<int>(i);
        }
      }
    }
  }

  void seek(const string_view &key) override
  {
    for (auto &src : sources_) {
      src->seek(key);
    }
    find_smallest_unique();
  }

  void next() override
  {
    if (!valid()) {
      return;
    }
    string curr_key(sources_[current_idx_]->key());
    for (auto &src : sources_) {
      if (src->valid() && comp_.compare(src->key(), curr_key) == 0) {
        src->next();
      }
    }
    find_smallest_unique();
  }

  string_view key() const override { return sources_[current_idx_]->key(); }
  string_view value() const override { return sources_[current_idx_]->value(); }

private:
  void find_smallest_unique()
  {
    current_idx_ = -1;
    // sources_ are ordered from newest (index 0) to oldest (index N-1).
    // Strict '<' ensures the newest source wins on tie.
    for (size_t i = 0; i < sources_.size(); ++i) {
      if (sources_[i]->valid()) {
        if (current_idx_ == -1 || comp_.compare(sources_[i]->key(), sources_[current_idx_]->key()) < 0) {
          current_idx_ = static_cast<int>(i);
        }
      }
    }
  }

  ObDefaultComparator               comp_;
  vector<unique_ptr<ObLsmIterator>> sources_;
  int                               current_idx_ = -1;
};

}  // namespace

/**
 * @brief merge TrxInnerMapIterator (or MultiLevelWriteBatchIterator) and ObUserIterator
 * @details Merges two iterators of different types into one.
 * If the two iterators have the same key, only
 * produce the key once and prefer the entry from left.
 * If the preferred entry from left has an empty value (tombstone), skip the key completely.
 */
class TrxIterator : public ObLsmIterator
{
public:
  TrxIterator(ObLsmIterator *left, ObLsmIterator *right) : left_(left), right_(right), use_left_(true), valid_(false) {}
  ~TrxIterator() override = default;

  bool valid() const override { return valid_; }

  void seek_to_first() override
  {
    if (left_) {
      left_->seek_to_first();
    }
    if (right_) {
      right_->seek_to_first();
    }
    align_to_valid_entry();
  }

  void seek_to_last() override
  {
    if (left_) {
      left_->seek_to_last();
    }
    if (right_) {
      right_->seek_to_last();
    }
    bool l_valid = left_ && left_->valid() && !left_->value().empty();
    bool r_valid = right_ && right_->valid();
    if (l_valid && r_valid) {
      int cmp   = comp_.compare(left_->key(), right_->key());
      use_left_ = (cmp >= 0);
      valid_    = true;
    } else if (l_valid) {
      use_left_ = true;
      valid_    = true;
    } else if (r_valid) {
      use_left_ = false;
      valid_    = true;
    } else {
      valid_ = false;
    }
  }

  void seek(const string_view &key) override
  {
    if (left_) {
      left_->seek(key);
    }
    if (right_) {
      right_->seek(key);
    }
    align_to_valid_entry();
  }

  void next() override
  {
    if (!valid_) {
      return;
    }
    bool l_valid = left_ && left_->valid();
    bool r_valid = right_ && right_->valid();
    if (l_valid && r_valid) {
      int cmp = comp_.compare(left_->key(), right_->key());
      if (cmp < 0) {
        left_->next();
      } else if (cmp > 0) {
        right_->next();
      } else {
        left_->next();
        right_->next();
      }
    } else if (use_left_ && l_valid) {
      left_->next();
    } else if (!use_left_ && r_valid) {
      right_->next();
    }
    align_to_valid_entry();
  }

  string_view key() const override { return use_left_ ? left_->key() : right_->key(); }
  string_view value() const override { return use_left_ ? left_->value() : right_->value(); }

private:
  void align_to_valid_entry()
  {
    while (true) {
      bool l_valid = left_ && left_->valid();
      bool r_valid = right_ && right_->valid();
      if (!l_valid && !r_valid) {
        valid_ = false;
        return;
      }

      if (l_valid && r_valid) {
        int cmp = comp_.compare(left_->key(), right_->key());
        if (cmp < 0) {
          if (left_->value().empty()) {
            // Deleted in transaction
            left_->next();
            continue;
          }
          use_left_ = true;
          valid_    = true;
          return;
        } else if (cmp > 0) {
          use_left_ = false;
          valid_    = true;
          return;
        } else {
          // Equal keys: transaction's write/delete overrides DB snapshot
          if (left_->value().empty()) {
            left_->next();
            right_->next();
            continue;
          }
          use_left_ = true;
          valid_    = true;
          return;
        }
      } else if (l_valid) {
        if (left_->value().empty()) {
          left_->next();
          continue;
        }
        use_left_ = true;
        valid_    = true;
        return;
      } else {
        use_left_ = false;
        valid_    = true;
        return;
      }
    }
  }

  unique_ptr<ObLsmIterator> left_;
  unique_ptr<ObLsmIterator> right_;
  ObDefaultComparator       comp_;
  bool                      use_left_ = true;
  bool                      valid_    = false;
};

ObLsmTransaction::ObLsmTransaction(ObLsm *db, uint64_t ts)
    : db_(db), ts_(ts), trx_uid_(g_trx_uid_generator.fetch_add(1))
{
  ObLsmImpl *impl = dynamic_cast<ObLsmImpl *>(db_);
  if (impl != nullptr) {
    isolation_level_ = impl->options().isolation_level;
    max_batch_bytes_ = impl->options().max_write_batch_size;
  }
}

ObLsmTransaction::~ObLsmTransaction() { cleanup_spill_files(); }

void ObLsmTransaction::cleanup_spill_files()
{
  for (auto &sst : spill_sstables_) {
    if (sst != nullptr) {
      sst->remove();
    }
  }
  spill_sstables_.clear();
}

RC ObLsmTransaction::spill_inner_store_to_disk()
{
  if (inner_store_.empty()) {
    return RC::SUCCESS;
  }
  ObLsmImpl *impl = dynamic_cast<ObLsmImpl *>(db_);
  if (impl == nullptr) {
    return RC::INTERNAL;
  }

  uint32_t spill_idx  = static_cast<uint32_t>(spill_sstables_.size());
  string   spill_name = "trx_" + to_string(trx_uid_) + "_spill_" + to_string(spill_idx) + SSTABLE_SUFFIX;
  string   spill_path = filesystem::path(impl->path()) / spill_name;

  ObSSTableBuilder builder(impl->default_comparator(), nullptr);
  RC               rc = builder.open(spill_path, 0x80000000u | spill_idx);
  if (OB_FAIL(rc)) {
    return rc;
  }

  uint64_t spill_seq = spill_idx + 1;
  for (const auto &[ukey, val] : inner_store_) {
    string internal_key;
    internal_key.reserve(ukey.size() + SEQ_SIZE);
    internal_key.append(ukey);
    put_numeric<uint64_t>(&internal_key, spill_seq);
    rc = builder.add(internal_key, val);
    if (OB_FAIL(rc)) {
      return rc;
    }
  }

  rc = builder.finish();
  if (OB_FAIL(rc)) {
    return rc;
  }

  spill_sstables_.push_back(builder.get_built_table());
  inner_store_.clear();
  current_batch_bytes_ = 0;
  return RC::SUCCESS;
}

ObLsmIterator *ObLsmTransaction::new_write_batch_iterator() const
{
  if (spill_sstables_.empty()) {
    return new TrxInnerMapIterator(inner_store_);
  }

  vector<unique_ptr<ObLsmIterator>> sources;
  sources.emplace_back(make_unique<TrxInnerMapIterator>(inner_store_));
  for (int i = static_cast<int>(spill_sstables_.size()) - 1; i >= 0; --i) {
    sources.emplace_back(make_unique<SpillSSTUserIterator>(spill_sstables_[i]->new_iterator()));
  }
  return new MultiLevelWriteBatchIterator(std::move(sources));
}

RC ObLsmTransaction::get(const string_view &key, string *value)
{
  string k(key);
  // 1. Check active in-memory WriteBatch
  auto it = inner_store_.find(k);
  if (it != inner_store_.end()) {
    if (it->second.empty()) {
      return RC::NOT_EXIST;
    }
    if (value != nullptr) {
      *value = it->second;
    }
    return RC::SUCCESS;
  }

  // 2. Check spilled WriteBatch SSTables from newest to oldest
  for (int i = static_cast<int>(spill_sstables_.size()) - 1; i >= 0; --i) {
    SpillSSTUserIterator spill_iter(spill_sstables_[i]->new_iterator());
    spill_iter.seek(key);
    if (spill_iter.valid() && spill_iter.key() == key) {
      if (spill_iter.value().empty()) {
        return RC::NOT_EXIST;
      }
      if (value != nullptr) {
        value->assign(spill_iter.value().data(), spill_iter.value().size());
      }
      return RC::SUCCESS;
    }
  }

  // 3. Record in OCC read_set_ when reading from DB snapshot
  if (isolation_level_ == TransactionIsolationLevel::SERIALIZABLE) {
    read_set_.insert(k);
  }

  // 4. Read from underlying DB at snapshot timestamp ts_
  ObLsmReadOptions read_opts;
  read_opts.seq = static_cast<int64_t>(ts_);
  unique_ptr<ObLsmIterator> db_iter(db_->new_iterator(read_opts));
  if (db_iter == nullptr) {
    return RC::NOT_EXIST;
  }
  db_iter->seek(key);
  if (db_iter->valid() && db_iter->key() == key) {
    if (db_iter->value().empty()) {
      return RC::NOT_EXIST;
    }
    if (value != nullptr) {
      value->assign(db_iter->value().data(), db_iter->value().size());
    }
    return RC::SUCCESS;
  }
  return RC::NOT_EXIST;
}

RC ObLsmTransaction::put(const string_view &key, const string_view &value)
{
  string k(key);
  string v(value);

  auto it = inner_store_.find(k);
  if (it != inner_store_.end()) {
    current_batch_bytes_ -= it->second.size();
    current_batch_bytes_ += v.size();
    it->second = std::move(v);
  } else {
    current_batch_bytes_ += (k.size() + v.size());
    inner_store_.emplace(k, std::move(v));
  }
  write_set_.insert(std::move(k));

  if (max_batch_bytes_ > 0 && current_batch_bytes_ > max_batch_bytes_) {
    return spill_inner_store_to_disk();
  }
  return RC::SUCCESS;
}

RC ObLsmTransaction::remove(const string_view &key) { return put(key, ""); }

ObLsmIterator *ObLsmTransaction::new_iterator(ObLsmReadOptions options)
{
  if (options.seq == -1) {
    options.seq = static_cast<int64_t>(ts_);
  }
  ObLsmIterator *left_iter  = new_write_batch_iterator();
  ObLsmIterator *right_iter = db_->new_iterator(options);
  return new TrxIterator(left_iter, right_iter);
}

RC ObLsmTransaction::commit()
{
  if (committed_ || rolled_back_) {
    return RC::INTERNAL;
  }
  if (write_set_.empty() && inner_store_.empty() && spill_sstables_.empty()) {
    committed_ = true;
    return RC::SUCCESS;
  }

  ObLsmImpl *impl = dynamic_cast<ObLsmImpl *>(db_);
  RC         rc   = RC::SUCCESS;
  if (impl != nullptr) {
    unique_ptr<ObLsmIterator> wb_iter(new_write_batch_iterator());
    rc = impl->commit_transaction(ts_, isolation_level_, read_set_, write_set_, wb_iter.get());
  } else if (db_ != nullptr) {
    vector<pair<string, string>> kvs;
    unique_ptr<ObLsmIterator>    wb_iter(new_write_batch_iterator());
    wb_iter->seek_to_first();
    while (wb_iter->valid()) {
      kvs.emplace_back(string(wb_iter->key()), string(wb_iter->value()));
      wb_iter->next();
    }
    rc = db_->batch_put(kvs);
  } else {
    return RC::INTERNAL;
  }

  if (OB_SUCC(rc)) {
    committed_ = true;
    inner_store_.clear();
    current_batch_bytes_ = 0;
    cleanup_spill_files();
  }
  return rc;
}

RC ObLsmTransaction::rollback()
{
  rolled_back_ = true;
  inner_store_.clear();
  read_set_.clear();
  write_set_.clear();
  current_batch_bytes_ = 0;
  cleanup_spill_files();
  return RC::SUCCESS;
}

}  // namespace oceanbase
