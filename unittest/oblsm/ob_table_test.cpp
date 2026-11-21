/* Copyright (c) 2021 OceanBase and/or its affiliates. All rights reserved.
miniob is licensed under Mulan PSL v2.
You can use this software according to the terms and conditions of the Mulan PSL v2.
You may obtain a copy of Mulan PSL v2 at:
         http://license.coscl.org.cn/MulanPSL2
THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
See the Mulan PSL v2 for more details. */

#include "gtest/gtest.h"

#include "common/lang/filesystem.h"
#include "common/lang/thread.h"
#include "oblsm/util/ob_comparator.h"
#include "oblsm/table/ob_sstable_builder.h"
#include "oblsm/table/ob_sstable.h"

using namespace oceanbase;

TEST(table_test, table_test_basic)
{
  ObDefaultComparator    comparator;
  shared_ptr<ObMemTable> table = make_shared<ObMemTable>();
  uint64_t               seq   = 0;
  size_t                 count = 5;
  for (size_t i = 0; i < count; i++) {
    string key(to_string(i));
    table->put(seq++, key, key);
  }
  ObSSTableBuilder tb(&comparator, nullptr);
  ASSERT_EQ(tb.build(table, "test.sst", 0), RC::SUCCESS);
  shared_ptr<ObSSTable> sst      = tb.get_built_table();
  ObLsmIterator        *sst_iter = sst->new_iterator();
  sst_iter->seek_to_first();
  while (sst_iter->valid()) {
    cout << sst_iter->key() << " " << sst_iter->value() << endl;
    sst_iter->next();
  }
  delete sst_iter;
  sst->remove();
}

TEST(table_test, pluggable_memtable_skiplist_vs_bplustree_sstable_and_concurrent_cache_read)
{
  ObDefaultComparator comparator;
  auto               *block_cache = new_lru_cache<uint64_t, shared_ptr<ObBlock>>(16);

  shared_ptr<ObMemTable> sl_mem = make_shared<ObMemTable>(MemTableType::SKIPLIST);
  shared_ptr<ObMemTable> bp_mem = make_shared<ObMemTable>(MemTableType::BPLUS_TREE);

  const size_t count = 1500;
  for (size_t i = 0; i < count; ++i) {
    string k = "user_key_" + to_string((i * 73) % count);
    string v = "payload_value_" + to_string(i) + "_padding_for_multi_blocks";
    sl_mem->put(i + 1, k, v);
    bp_mem->put(i + 1, k, v);
  }

  ObSSTableBuilder sl_tb(&comparator, block_cache);
  ObSSTableBuilder bp_tb(&comparator, block_cache);
  ASSERT_EQ(sl_tb.build(sl_mem, "test_sl.sst", 10), RC::SUCCESS);
  ASSERT_EQ(bp_tb.build(bp_mem, "test_bp.sst", 20), RC::SUCCESS);

  shared_ptr<ObSSTable> sl_sst = sl_tb.get_built_table();
  shared_ptr<ObSSTable> bp_sst = bp_tb.get_built_table();
  ASSERT_GT(sl_sst->block_count(), 1u);
  ASSERT_EQ(sl_sst->block_count(), bp_sst->block_count());

  // Verify both SSTables contain identical ordered key-value streams
  unique_ptr<ObLsmIterator> it1(sl_sst->new_iterator());
  unique_ptr<ObLsmIterator> it2(bp_sst->new_iterator());
  it1->seek_to_first();
  it2->seek_to_first();
  size_t verified = 0;
  while (it1->valid() && it2->valid()) {
    ASSERT_EQ(it1->key(), it2->key());
    ASSERT_EQ(it1->value(), it2->value());
    verified++;
    it1->next();
    it2->next();
  }
  ASSERT_FALSE(it1->valid());
  ASSERT_FALSE(it2->valid());
  ASSERT_EQ(verified, count);

  // Multi-threaded concurrent scan over SSTables exercising shared BlockCache
  std::vector<std::thread> readers;
  for (int t = 0; t < 6; ++t) {
    readers.emplace_back([t, &sl_sst, &bp_sst, count]() {
      shared_ptr<ObSSTable>     target = (t % 2 == 0) ? sl_sst : bp_sst;
      unique_ptr<ObLsmIterator> iter(target->new_iterator());
      iter->seek_to_first();
      size_t c = 0;
      while (iter->valid()) {
        c++;
        iter->next();
      }
      EXPECT_EQ(c, count);
    });
  }
  for (auto &th : readers) {
    th.join();
  }

  sl_sst->remove();
  bp_sst->remove();
  delete block_cache;
}

int main(int argc, char **argv)
{
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}