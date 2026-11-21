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
#include "common/lang/string.h"
#include "common/lang/string_view.h"
#include "common/lang/memory.h"
#include "oblsm/ob_lsm_define.h"
#include "oblsm/memtable/ob_skiplist.h"
#include "oblsm/memtable/ob_bplus_tree_mem.h"
#include "oblsm/util/ob_comparator.h"
#include "oblsm/util/ob_arena.h"
#include "oblsm/util/ob_coding.h"
#include "oblsm/include/ob_lsm_iterator.h"

namespace oceanbase {

/**
 * @class ObMemTable
 * @brief MemTable implementation for LSM-Tree supporting pluggable index structures (SkipList or B+Tree).
 */
class ObMemTable : public enable_shared_from_this<ObMemTable>
{
public:
  explicit ObMemTable(MemTableType type = MemTableType::SKIPLIST)
      : type_(type), comparator_(), table_(comparator_), bptree_table_(comparator_) {};

  ~ObMemTable() = default;

  MemTableType type() const { return type_; }

  /**
   * @brief Retrieves a shared pointer to the current ObMemTable instance.
   *
   * This method utilizes `std::enable_shared_from_this` to provide a shared pointer
   * to the current object. Useful when the current object needs to be shared safely
   * among multiple components.
   *
   * @return A shared pointer to the current `ObMemTable` instance.
   */
  shared_ptr<ObMemTable> get_shared_ptr() { return shared_from_this(); }

  /**
   * @brief Inserts a key-value pair into the memtable.
   *
   * Each entry is versioned using the provided `seq` number. If the same key is
   * inserted multiple times, the version with the highest sequence number will
   * take precedence when queried.
   *
   * @param seq A sequence number used for versioning the key-value entry.
   * @param key The key to be inserted.
   * @param value The value associated with the key.
   */
  void put(uint64_t seq, const string_view &key, const string_view &value);

  /**
   * @brief Estimates the memory usage of the memtable.
   *
   * Returns the approximate memory usage of the memtable, including the
   * skip list and associated memory allocations.
   *
   * @return The approximate memory usage in bytes.
   */
  size_t appro_memory_usage() const { return arena_.memory_usage(); }

  /**
   * @brief Creates a new iterator for traversing the contents of the memtable.
   *
   * This method returns a heap-allocated iterator for iterating over key-value
   * pairs stored in the memtable. The caller is responsible for managing the
   * lifetime of the returned iterator.
   *
   * @return A pointer to the newly created `ObLsmIterator` for the memtable.
   */
  ObLsmIterator *new_iterator();

  struct KeyComparator
  {
    const ObInternalKeyComparator comparator;
    explicit KeyComparator() {}
    int operator()(const char *a, const char *b) const;
  };

  typedef ObSkipList<const char *, KeyComparator>     Table;
  typedef ObMemBPlusTree<const char *, KeyComparator> BPlusTable;

private:
  template <typename TableType>
  friend class ObMemTableTypedIterator;

  MemTableType  type_ = MemTableType::SKIPLIST;
  KeyComparator comparator_;
  Table         table_;
  BPlusTable    bptree_table_;
  ObArena       arena_;
};

/**
 * @class ObMemTableTypedIterator
 * @brief A generic iterator for traversing an `ObMemTable` backed by either ObSkipList or ObMemBPlusTree.
 */
template <typename TableType>
class ObMemTableTypedIterator : public ObLsmIterator
{
public:
  explicit ObMemTableTypedIterator(shared_ptr<ObMemTable> mem, TableType *table) : mem_(mem), iter_(table) {}

  ObMemTableTypedIterator(const ObMemTableTypedIterator &)            = delete;
  ObMemTableTypedIterator &operator=(const ObMemTableTypedIterator &) = delete;

  ~ObMemTableTypedIterator() override = default;

  void seek(const string_view &k) override
  {
    tmp_.clear();
    iter_.seek(k.data());
  }
  void seek_to_first() override { iter_.seek_to_first(); }
  void seek_to_last() override { iter_.seek_to_last(); }

  bool        valid() const override { return iter_.valid(); }
  void        next() override { iter_.next(); }
  string_view key() const override { return get_length_prefixed_string(iter_.key()); }
  string_view value() const override
  {
    string_view key_slice = get_length_prefixed_string(iter_.key());
    return get_length_prefixed_string(key_slice.data() + key_slice.size());
  }

private:
  shared_ptr<ObMemTable>       mem_;
  typename TableType::Iterator iter_;
  string                       tmp_;  // For seek key
};

using ObMemTableIterator      = ObMemTableTypedIterator<ObMemTable::Table>;
using ObMemTableBPlusIterator = ObMemTableTypedIterator<ObMemTable::BPlusTable>;

}  // namespace oceanbase