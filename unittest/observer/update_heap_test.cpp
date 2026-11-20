/* Copyright (c) 2021 OceanBase and/or its affiliates. All rights reserved.
miniob is licensed under Mulan PSL v2.
You can use this software according to the terms and conditions of the Mulan PSL v2.
You may obtain a copy of Mulan PSL v2 at:
         http://license.coscl.org.cn/MulanPSL2
THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
See the Mulan PSL v2 for more details. */

// Covers heap UPDATE at the engine level: in-place row update with secondary
// index maintenance, including rollback on an invalid record.

#include <cstring>
#include <filesystem>
#include <memory>

#include "common/lang/memory.h"
#include "common/types.h"
#include "storage/db/db.h"
#include "storage/index/index.h"
#include "storage/record/record_scanner.h"
#include "storage/table/table.h"
#include "storage/trx/trx.h"
#include "gtest/gtest.h"

using namespace std;
using namespace common;

namespace {

int record_int(const Record &record, int index)
{
  int value = 0;
  memcpy(&value, record.data() + index * sizeof(int), sizeof(value));
  return value;
}

vector<Record> scan_all(Table *table)
{
  RecordScanner *scanner = nullptr;
  EXPECT_EQ(RC::SUCCESS, table->get_record_scanner(scanner, nullptr, ReadWriteMode::READ_ONLY));
  vector<Record> records;
  Record         record;
  while (RC::SUCCESS == scanner->next(record)) {
    Record owned;
    EXPECT_EQ(RC::SUCCESS, owned.copy_data(record.data(), record.len()));
    owned.set_rid(record.rid());
    records.push_back(std::move(owned));
  }
  scanner->close_scan();
  delete scanner;
  return records;
}

RC update_one(Table *table, const Record &old_record, const Record *new_record)
{
  return table->update_record_with_trx(old_record, *new_record, nullptr);
}

}  // namespace

TEST(UpdateHeap, in_place_update_and_index_maintenance)
{
  filesystem::path dir = "update_heap_test";
  filesystem::remove_all(dir);
  filesystem::create_directories(dir);

  auto db = make_unique<Db>();
  ASSERT_EQ(RC::SUCCESS, db->init("update_heap_db", dir.c_str(), "vacuous", "vacuous"));

  vector<AttrInfoSqlNode> attrs(2);
  attrs[0].name   = "id";
  attrs[0].type   = AttrType::INTS;
  attrs[0].length = sizeof(int);
  attrs[1].name   = "v";
  attrs[1].type   = AttrType::INTS;
  attrs[1].length = sizeof(int);
  ASSERT_EQ(RC::SUCCESS, db->create_table("t_up", attrs, {}));
  Table *table = db->find_table("t_up");
  ASSERT_NE(nullptr, table);

  for (int i = 1; i <= 3; i++) {
    vector<Value> values(2);
    values[0].set_int(i);
    values[1].set_int(i * 10);
    Record record;
    ASSERT_EQ(RC::SUCCESS, table->make_record(values.size(), values.data(), record));
    ASSERT_EQ(RC::SUCCESS, table->insert_record(record));
  }

  const FieldMeta *v_field = table->table_meta().field("v");
  ASSERT_NE(nullptr, v_field);
  ASSERT_EQ(RC::SUCCESS, table->create_index(nullptr, v_field, "i_v"));
  // create_index swaps the table meta; re-resolve the field pointer
  v_field = table->table_meta().field("v");
  ASSERT_NE(nullptr, v_field);
  Index *v_index = table->find_index("i_v");
  ASSERT_NE(nullptr, v_index);

  vector<Record> records = scan_all(table);
  ASSERT_EQ(3, static_cast<int>(records.size()));
  Record old_record = records[1];
  ASSERT_EQ(2, record_int(old_record, 0));

  Record     new_record = old_record;
  Value      new_value;
  new_value.set_int(99);
  ASSERT_EQ(RC::SUCCESS, new_record.reset_filed(v_field->offset(), v_field->len()));
  ASSERT_EQ(RC::SUCCESS, table->set_value_to_record(new_record.data(), new_value, v_field));
  ASSERT_EQ(RC::SUCCESS, update_one(table, old_record, &new_record));

  records = scan_all(table);
  ASSERT_EQ(3, static_cast<int>(records.size()));
  for (const Record &record : records) {
    if (record_int(record, 0) == 2) {
      ASSERT_EQ(99, record_int(record, 1));
    }
  }

  int           key       = 99;
  const char   *value     = nullptr;
  int           value_len = 0;
  IndexScanner *scanner   = v_index->create_scanner(reinterpret_cast<const char *>(&key), sizeof(key), true,
      reinterpret_cast<const char *>(&key), sizeof(key), true);
  ASSERT_NE(nullptr, scanner);
  ASSERT_EQ(RC::SUCCESS, scanner->next_entry(value, value_len));
  Record fetched;
  ASSERT_EQ(RC::SUCCESS, v_index->get_record(table, value, value_len, fetched));
  ASSERT_EQ(2, record_int(fetched, 0));
  ASSERT_EQ(RC::RECORD_EOF, scanner->next_entry(value, value_len));
  scanner->destroy();

  key = 20;
  scanner = v_index->create_scanner(reinterpret_cast<const char *>(&key), sizeof(key), true,
      reinterpret_cast<const char *>(&key), sizeof(key), true);
  ASSERT_NE(nullptr, scanner);
  ASSERT_EQ(RC::RECORD_EOF, scanner->next_entry(value, value_len));
  scanner->destroy();

  // Invalid new record must not change the stored row.
  Record bad_record;
  bad_record.set_data(const_cast<char *>(new_record.data()), 1);
  ASSERT_EQ(RC::INVALID_ARGUMENT, update_one(table, old_record, &bad_record));
  records = scan_all(table);
  bool found_old = false;
  for (const Record &record : records) {
    if (record_int(record, 0) == 2 && record_int(record, 1) == 99) {
      found_old = true;
    }
  }
  ASSERT_TRUE(found_old);

  db.reset();
  filesystem::remove_all(dir);
}
