/* Copyright (c) 2021 OceanBase and/or its affiliates. All rights reserved.
miniob is licensed under Mulan PSL v2.
You can use this software according to the terms and conditions of the Mulan PSL v2.
You may obtain a copy of Mulan PSL v2 at:
         http://license.coscl.org.cn/MulanPSL2
THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
See the Mulan PSL v2 for more details. */

// Covers the generalized BplusTree used by the clustered btree table engine:
// ROWID / COMPOSITE / ATTR_MEMCMP_VALUE key modes, value lookup, and the
// legacy (format_version != 2) header reopen path.

#include <algorithm>
#include <cstdint>
#include <cstring>
#include <string>
#include <vector>

#include "common/lang/filesystem.h"
#include "common/lang/memory.h"
#include "common/log/log.h"
#include "storage/buffer/disk_buffer_pool.h"
#include "storage/buffer/double_write_buffer.h"
#include "storage/clog/vacuous_log_handler.h"
#include "storage/index/bplus_tree.h"
#include "gtest/gtest.h"

using namespace common;

namespace {

constexpr int FIRST_INDEX_PAGE = 1;

class BplusTreeModesTest : public ::testing::Test
{
protected:
  void SetUp() override
  {
    filesystem::path dir = "bplus_tree_modes";
    filesystem::remove_all(dir);
    filesystem::create_directories(dir);
    std::string name    = ::testing::UnitTest::GetInstance()->current_test_info()->name();
    file_               = dir / (name + ".btree");
    ASSERT_EQ(RC::SUCCESS, bpm_.init(make_unique<VacuousDoubleWriteBuffer>()));
    ASSERT_EQ(RC::SUCCESS, bpm_.create_file(file_.c_str()));
    ASSERT_EQ(RC::SUCCESS, bpm_.open_file(log_handler_, file_.c_str(), bp_));
    ASSERT_NE(nullptr, bp_);
  }

  void TearDown() override { filesystem::remove_all("bplus_tree_modes"); }

  VacuousLogHandler log_handler_;
  BufferPoolManager  bpm_;
  DiskBufferPool    *bp_ = nullptr;
  filesystem::path   file_;
};

struct CompositeKey
{
  int32_t id;
  char    name[8];
};

}  // namespace

TEST_F(BplusTreeModesTest, rowid_mode_orders_values_numerically)
{
  BplusTreeHandler tree;
  ASSERT_EQ(RC::SUCCESS,
      tree.create(log_handler_, *bp_, AttrType::INTS, 4, sizeof(uint64_t), sizeof(RID), BplusTreeKeyMode::ROWID));

  uint64_t ids[] = {50, 10, 30, 20, 40};
  RID      rid{1, 0};
  for (uint64_t id : ids) {
    rid.slot_num = static_cast<int>(id);
    ASSERT_EQ(RC::SUCCESS, tree.insert_entry(reinterpret_cast<const char *>(&id),
                                 reinterpret_cast<const char *>(&rid), sizeof(rid)));
  }

  uint64_t duplicate = ids[0];
  ASSERT_EQ(RC::RECORD_DUPLICATE_KEY,
      tree.insert_entry(reinterpret_cast<const char *>(&duplicate), reinterpret_cast<const char *>(&rid), sizeof(rid)));

  BplusTreeScanner scanner(tree);
  ASSERT_EQ(RC::SUCCESS, scanner.open(nullptr, 0, true, nullptr, 0, true));
  uint64_t    prev      = 0;
  int         count     = 0;
  const char *value     = nullptr;
  const char *key       = nullptr;
  int         value_len = 0;
  while (RC::SUCCESS == scanner.next_entry(value, value_len, key)) {
    uint64_t scanned_id = 0;
    memcpy(&scanned_id, key, sizeof(scanned_id));
    ASSERT_LT(prev, scanned_id);
    ASSERT_EQ(static_cast<int>(sizeof(RID)), value_len);
    RID value_rid;
    memcpy(&value_rid, value, sizeof(value_rid));
    ASSERT_EQ(static_cast<int>(scanned_id), value_rid.slot_num);
    prev = scanned_id;
    ++count;
  }
  ASSERT_EQ(5, count);

  uint64_t probe = 30;
  ASSERT_EQ(RC::SUCCESS, tree.get_value(reinterpret_cast<const char *>(&probe), sizeof(probe), value, value_len));
  ASSERT_EQ(static_cast<int>(sizeof(RID)), value_len);
  RID found;
  memcpy(&found, value, sizeof(found));
  ASSERT_EQ(30, found.slot_num);

  probe = 35;
  ASSERT_EQ(RC::RECORD_NOT_EXIST, tree.get_value(reinterpret_cast<const char *>(&probe), sizeof(probe), value, value_len));
}

TEST_F(BplusTreeModesTest, composite_mode_orders_by_declared_fields)
{
  BplusTreeHandler tree;
  ASSERT_EQ(RC::SUCCESS,
      tree.create(log_handler_, *bp_, AttrType::INTS, 4, sizeof(CompositeKey), 16, BplusTreeKeyMode::COMPOSITE));
  ASSERT_EQ(RC::SUCCESS,
      tree.set_composite_spec({{AttrType::INTS, 0, 4}, {AttrType::CHARS, 4, 8}}));

  vector<CompositeKey> keys;
  CompositeKey         key{};
  key.id = 2;
  strncpy(key.name, "bb", sizeof(key.name));
  keys.push_back(key);
  key.id = 1;
  strncpy(key.name, "zz", sizeof(key.name));
  keys.push_back(key);
  key.id = 1;
  strncpy(key.name, "aa", sizeof(key.name));
  keys.push_back(key);
  key.id = 3;
  strncpy(key.name, "cc", sizeof(key.name));
  keys.push_back(key);
  key.id = 1;
  strncpy(key.name, "mm", sizeof(key.name));
  keys.push_back(key);

  char value[16] = {};
  for (size_t i = 0; i < keys.size(); i++) {
    memset(value, 0, sizeof(value));
    memcpy(value, &i, sizeof(i));
    ASSERT_EQ(RC::SUCCESS,
        tree.insert_entry(reinterpret_cast<const char *>(&keys[i]), value, sizeof(value)));
  }

  vector<CompositeKey> expected = keys;
  sort(expected.begin(), expected.end(), [](const CompositeKey &left, const CompositeKey &right) {
    if (left.id != right.id) {
      return left.id < right.id;
    }
    return memcmp(left.name, right.name, sizeof(left.name)) < 0;
  });

  BplusTreeScanner scanner(tree);
  ASSERT_EQ(RC::SUCCESS, scanner.open(nullptr, 0, true, nullptr, 0, true));
  const char *scanned_value = nullptr;
  const char *scanned_key   = nullptr;
  int         value_len     = 0;
  int         count         = 0;
  while (RC::SUCCESS == scanner.next_entry(scanned_value, value_len, scanned_key)) {
    CompositeKey scanned{};
    memcpy(&scanned, scanned_key, sizeof(scanned));
    const CompositeKey &want = expected[count];
    ASSERT_EQ(want.id, scanned.id);
    ASSERT_EQ(0, memcmp(want.name, scanned.name, sizeof(scanned.name)));
    ASSERT_EQ(16, value_len);
    ++count;
  }
  ASSERT_EQ(static_cast<int>(keys.size()), count);
}

TEST_F(BplusTreeModesTest, attr_value_mode_keeps_duplicate_attr_and_looks_up_value)
{
  BplusTreeHandler tree;
  ASSERT_EQ(RC::SUCCESS,
      tree.create(log_handler_, *bp_, AttrType::INTS, 4, 4 + sizeof(uint64_t), sizeof(uint64_t),
          BplusTreeKeyMode::ATTR_MEMCMP_VALUE));

  int32_t  attr      = 7;
  uint64_t values[]  = {0x0101010101010101ULL, 0x0303030303030303ULL, 0x0202020202020202ULL};
  for (uint64_t value : values) {
    ASSERT_EQ(RC::SUCCESS, tree.insert_entry(reinterpret_cast<const char *>(&attr),
                                 reinterpret_cast<const char *>(&value), sizeof(value)));
  }

  ASSERT_EQ(RC::RECORD_DUPLICATE_KEY,
      tree.insert_entry(reinterpret_cast<const char *>(&attr), reinterpret_cast<const char *>(&values[0]),
          sizeof(values[0])));

  BplusTreeScanner scanner(tree);
  ASSERT_EQ(RC::SUCCESS, scanner.open(reinterpret_cast<const char *>(&attr), sizeof(attr), true,
                            reinterpret_cast<const char *>(&attr), sizeof(attr), true));
  const char *scanned_value = nullptr;
  const char *scanned_key   = nullptr;
  int         value_len     = 0;
  int         count         = 0;
  while (RC::SUCCESS == scanner.next_entry(scanned_value, value_len, scanned_key)) {
    ASSERT_EQ(static_cast<int>(sizeof(uint64_t)), value_len);
    ++count;
  }
  ASSERT_EQ(3, count);

  const char *found_value = nullptr;
  int         found_len   = 0;
  ASSERT_EQ(RC::SUCCESS,
      tree.get_value(reinterpret_cast<const char *>(&attr), sizeof(attr), found_value, found_len));
  ASSERT_EQ(static_cast<int>(sizeof(uint64_t)), found_len);
  uint64_t found = 0;
  memcpy(&found, found_value, sizeof(found));
  ASSERT_EQ(values[0], found);

  ASSERT_EQ(RC::INVALID_ARGUMENT, tree.get_value(reinterpret_cast<const char *>(&attr), sizeof(uint64_t), found_value, found_len));
}

TEST_F(BplusTreeModesTest, legacy_header_reopens_as_attr_rid)
{
  BplusTreeHandler tree;
  ASSERT_EQ(RC::SUCCESS, tree.create(log_handler_, *bp_, AttrType::INTS, 4));
  ASSERT_EQ(2, tree.file_header().format_version);

  Frame *frame = nullptr;
  ASSERT_EQ(RC::SUCCESS, bp_->get_this_page(FIRST_INDEX_PAGE, &frame));
  IndexFileHeader *header = reinterpret_cast<IndexFileHeader *>(frame->data());
  header->format_version  = 0;
  frame->mark_dirty();
  bp_->unpin_page(frame);

  BplusTreeHandler reopened;
  ASSERT_EQ(RC::SUCCESS, reopened.open(log_handler_, *bp_));
  ASSERT_EQ(static_cast<int>(sizeof(RID)), reopened.file_header().value_size);
  ASSERT_EQ(static_cast<int32_t>(BplusTreeKeyMode::ATTR_RID), reopened.file_header().key_mode);

  int key = 1;
  RID rid{0, 1};
  ASSERT_EQ(RC::SUCCESS, reopened.insert_entry(reinterpret_cast<const char *>(&key),
                               reinterpret_cast<const char *>(&rid), sizeof(rid)));
  BplusTreeScanner scanner(reopened);
  ASSERT_EQ(RC::SUCCESS, scanner.open(nullptr, 0, true, nullptr, 0, true));
  RID  scanned;
  bool has_entry = (RC::SUCCESS == scanner.next_entry(scanned));
  ASSERT_TRUE(has_entry);
  ASSERT_EQ(1, scanned.slot_num);
  ASSERT_EQ(RC::RECORD_EOF, scanner.next_entry(scanned));
}
