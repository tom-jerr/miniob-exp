/* Copyright (c) 2021 OceanBase and/or its affiliates. All rights reserved.
miniob is licensed under Mulan PSL v2.
You can use this software according to the terms and conditions of the Mulan PSL v2.
You may obtain a copy of Mulan PSL v2 at:
         http://license.coscl.org.cn/MulanPSL2
THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
See the Mulan PSL v2 for more details. */

// Covers the clustered B+tree table engine at the Db/Table level:
// rowid mode, single/composite primary keys, duplicate keys, delete,
// exact key lookup, and restart with rowid recovery.

#include <cstring>
#include <filesystem>
#include <memory>
#include <string>
#include <vector>

#include "common/lang/memory.h"
#include "common/types.h"
#include "storage/db/db.h"
#include "storage/record/record_scanner.h"
#include "storage/table/table.h"
#include "storage/trx/trx.h"
#include "gtest/gtest.h"

using namespace std;
using namespace common;

namespace {

vector<AttrInfoSqlNode> int_attrs(const vector<string> &names)
{
  vector<AttrInfoSqlNode> attrs;
  for (const string &name : names) {
    AttrInfoSqlNode attr;
    attr.name   = name;
    attr.type   = AttrType::INTS;
    attr.length = sizeof(int);
    attrs.push_back(attr);
  }
  return attrs;
}

Record make_int_record(Table *table, const vector<int> &values)
{
  vector<Value> value_list;
  for (int value : values) {
    Value v;
    v.set_int(value);
    value_list.push_back(v);
  }
  Record record;
  EXPECT_EQ(RC::SUCCESS, table->make_record(value_list.size(), value_list.data(), record));
  return record;
}

vector<Record> scan_all(Table *table)
{
  RecordScanner *scanner = nullptr;
  EXPECT_EQ(RC::SUCCESS, table->get_record_scanner(scanner, nullptr, ReadWriteMode::READ_ONLY));

  vector<Record> records;
  Record         record;
  RC             rc = RC::SUCCESS;
  while (RC::SUCCESS == (rc = scanner->next(record))) {
    records.push_back(record);
  }
  EXPECT_EQ(RC::RECORD_EOF, rc);
  scanner->close_scan();
  delete scanner;
  return records;
}

int record_int(const Record &record, int index)
{
  int value = 0;
  memcpy(&value, record.data() + index * sizeof(int), sizeof(value));
  return value;
}

}  // namespace

class BtreeTableEngineTest : public ::testing::Test
{
protected:
  void SetUp() override
  {
    filesystem::remove_all(dir_);
    filesystem::create_directories(dir_);
  }

  void TearDown() override { filesystem::remove_all(dir_); }

  void init_db()
  {
    db_ = make_unique<Db>();
    ASSERT_EQ(RC::SUCCESS, db_->init("btree_db", dir_.c_str(), "vacuous", "vacuous"));
  }

  Table *create_table(const char *name, const vector<string> &fields, const vector<string> &primary_keys)
  {
    vector<AttrInfoSqlNode> attrs = int_attrs(fields);
    EXPECT_EQ(RC::SUCCESS,
        db_->create_table(name, attrs, primary_keys, StorageFormat::ROW_FORMAT, StorageEngine::BTREE));
    return db_->find_table(name);
  }

  filesystem::path dir_ = "btree_table_engine_test";
  unique_ptr<Db>    db_;
};

TEST_F(BtreeTableEngineTest, rowid_mode_scan_delete_lookup_and_restart)
{
  init_db();
  Table *table = create_table("t_rowid", {"id", "v"}, {});
  ASSERT_NE(nullptr, table);

  const int row_num = 300;
  for (int i = 0; i < row_num; i++) {
    Record record = make_int_record(table, {i, i * 2});
    ASSERT_EQ(RC::SUCCESS, table->insert_record(record));
  }

  vector<Record> records = scan_all(table);
  ASSERT_EQ(row_num, static_cast<int>(records.size()));
  for (int i = 0; i < row_num; i++) {
    ASSERT_EQ(i, record_int(records[i], 0));
    ASSERT_EQ(i * 2, record_int(records[i], 1));
  }

  Record fetched;
  uint64_t rowid = 5;
  ASSERT_EQ(RC::SUCCESS, table->get_record_by_key(reinterpret_cast<const char *>(&rowid), sizeof(rowid), fetched));
  ASSERT_EQ(5, record_int(fetched, 0));
  rowid = row_num;
  ASSERT_EQ(RC::RECORD_NOT_EXIST, table->get_record_by_key(reinterpret_cast<const char *>(&rowid), sizeof(rowid), fetched));

  Record to_delete = records[10];
  ASSERT_EQ(RC::SUCCESS, table->delete_record(to_delete));
  ASSERT_EQ(row_num - 1, static_cast<int>(scan_all(table).size()));

  ASSERT_EQ(RC::SUCCESS, db_->sync());
  db_.reset();
  init_db();

  table = db_->find_table("t_rowid");
  ASSERT_NE(nullptr, table);
  records = scan_all(table);
  ASSERT_EQ(row_num - 1, static_cast<int>(records.size()));
  ASSERT_EQ(0, record_int(records[0], 0));

  Record appended = make_int_record(table, {row_num, -1});
  ASSERT_EQ(RC::SUCCESS, table->insert_record(appended));
  ASSERT_EQ(row_num, static_cast<int>(scan_all(table).size()));

  rowid = row_num;
  ASSERT_EQ(RC::SUCCESS, table->get_record_by_key(reinterpret_cast<const char *>(&rowid), sizeof(rowid), fetched));
  ASSERT_EQ(row_num, record_int(fetched, 0));
}

TEST_F(BtreeTableEngineTest, single_primary_key_orders_and_rejects_duplicates)
{
  init_db();
  Table *table = create_table("t_pk", {"id", "v"}, {"id"});
  ASSERT_NE(nullptr, table);

  const int values[] = {50, 10, 90, 30, 70, 20, 80, 60, 40, 100};
  for (int value : values) {
    Record record = make_int_record(table, {value, value * 10});
    ASSERT_EQ(RC::SUCCESS, table->insert_record(record));
  }

  vector<Record> records = scan_all(table);
  ASSERT_EQ(10, static_cast<int>(records.size()));
  for (int i = 0; i < 10; i++) {
    ASSERT_EQ((i + 1) * 10, record_int(records[i], 0));
    ASSERT_EQ((i + 1) * 10 * 10, record_int(records[i], 1));
  }

  Record duplicate = make_int_record(table, {30, 0});
  ASSERT_EQ(RC::RECORD_DUPLICATE_KEY, table->insert_record(duplicate));

  int    key = 40;
  Record fetched;
  ASSERT_EQ(RC::SUCCESS, table->get_record_by_key(reinterpret_cast<const char *>(&key), sizeof(key), fetched));
  ASSERT_EQ(400, record_int(fetched, 1));

  ASSERT_EQ(RC::SUCCESS, table->delete_record(records[3]));
  records = scan_all(table);
  ASSERT_EQ(9, static_cast<int>(records.size()));
  ASSERT_EQ(10, record_int(records[0], 0));
  ASSERT_EQ(50, record_int(records[3], 0));
}

TEST_F(BtreeTableEngineTest, composite_primary_key_orders_by_fields)
{
  init_db();
  Table *table = create_table("t_composite", {"a", "b", "v"}, {"a", "b"});
  ASSERT_NE(nullptr, table);

  const int keys[][2] = {{2, 1}, {1, 2}, {1, 1}, {2, 2}, {1, 3}};
  for (const auto &key : keys) {
    Record record = make_int_record(table, {key[0], key[1], key[0] * 10 + key[1]});
    ASSERT_EQ(RC::SUCCESS, table->insert_record(record));
  }

  vector<Record> records = scan_all(table);
  ASSERT_EQ(5, static_cast<int>(records.size()));
  ASSERT_EQ(1, record_int(records[0], 0));
  ASSERT_EQ(1, record_int(records[0], 1));
  ASSERT_EQ(1, record_int(records[1], 0));
  ASSERT_EQ(2, record_int(records[1], 1));
  ASSERT_EQ(1, record_int(records[2], 0));
  ASSERT_EQ(3, record_int(records[2], 1));
  ASSERT_EQ(2, record_int(records[3], 0));
  ASSERT_EQ(1, record_int(records[3], 1));
  ASSERT_EQ(2, record_int(records[4], 0));
  ASSERT_EQ(2, record_int(records[4], 1));

  Record duplicate = make_int_record(table, {1, 2, 0});
  ASSERT_EQ(RC::RECORD_DUPLICATE_KEY, table->insert_record(duplicate));

  int    key[2] = {2, 2};
  Record fetched;
  ASSERT_EQ(RC::SUCCESS,
      table->get_record_by_key(reinterpret_cast<const char *>(key), sizeof(key), fetched));
  ASSERT_EQ(22, record_int(fetched, 2));
}
