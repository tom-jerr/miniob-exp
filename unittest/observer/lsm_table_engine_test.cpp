/* Copyright (c) 2021 OceanBase and/or its affiliates. All rights reserved.
miniob is licensed under Mulan PSL v2.
You can use this software according to the terms and conditions of the Mulan PSL v2.
You may obtain a copy of Mulan PSL v2 at:
         http://license.coscl.org.cn/MulanPSL2
THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
See the Mulan PSL v2 for more details. */

// Covers the LSM table engine at the Db/Table level: insert/scan/delete,
// secondary index lookup, and persistence across reopen (WAL replay).

#include <cstring>
#include <filesystem>
#include <memory>
#include <vector>

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

vector<AttrInfoSqlNode> int_attrs()
{
  vector<AttrInfoSqlNode> attrs(2);
  attrs[0].name   = "id";
  attrs[0].type   = AttrType::INTS;
  attrs[0].length = sizeof(int);
  attrs[1].name   = "v";
  attrs[1].type   = AttrType::INTS;
  attrs[1].length = sizeof(int);
  return attrs;
}

Record make_int_record(Table *table, int id, int v)
{
  vector<Value> values(2);
  values[0].set_int(id);
  values[1].set_int(v);
  Record record;
  EXPECT_EQ(RC::SUCCESS, table->make_record(values.size(), values.data(), record));
  return record;
}

vector<Record> scan_all(Table *table, Trx *trx)
{
  RecordScanner *scanner = nullptr;
  EXPECT_EQ(RC::SUCCESS, table->get_record_scanner(scanner, trx, ReadWriteMode::READ_ONLY));
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

TEST(LsmTableEngine, crud_index_and_reopen)
{
  filesystem::path dir = "lsm_table_engine_test";
  filesystem::remove_all(dir);
  filesystem::create_directories(dir);

  const char *dbname = "lsm_test_db";
  auto        db     = make_unique<Db>();
  ASSERT_EQ(RC::SUCCESS, db->init(dbname, dir.c_str(), "vacuous", "vacuous"));

  vector<AttrInfoSqlNode> attrs = int_attrs();
  ASSERT_EQ(RC::SUCCESS,
      db->create_table("t_lsm", attrs, {}, StorageFormat::ROW_FORMAT, StorageEngine::LSM));
  Table *table = db->find_table("t_lsm");
  ASSERT_NE(nullptr, table);

  Trx *trx = db->trx_kit().create_trx(db->log_handler());
  ASSERT_NE(nullptr, trx);
  ASSERT_EQ(RC::SUCCESS, trx->start_if_need());

  const int row_num = 20;
  for (int i = 0; i < row_num; i++) {
    Record record = make_int_record(table, i, i * 10);
    ASSERT_EQ(RC::SUCCESS, table->insert_record(record));
  }

  vector<Record> records = scan_all(table, trx);
  ASSERT_EQ(row_num, static_cast<int>(records.size()));
  for (int i = 0; i < row_num; i++) {
    ASSERT_EQ(i, record_int(records[i], 0));
    ASSERT_EQ(i * 10, record_int(records[i], 1));
  }

  const FieldMeta *v_field = table->table_meta().field("v");
  ASSERT_NE(nullptr, v_field);
  ASSERT_EQ(RC::SUCCESS, table->create_index(trx, v_field, "idx_v"));

  Index *v_index = table->find_index("idx_v");
  ASSERT_NE(nullptr, v_index);

  int           key       = 20;
  const char   *value     = nullptr;
  int           value_len = 0;
  IndexScanner *scanner   = v_index->create_scanner(
      reinterpret_cast<const char *>(&key), sizeof(key), true, reinterpret_cast<const char *>(&key), sizeof(key), true);
  ASSERT_NE(nullptr, scanner);
  ASSERT_EQ(RC::SUCCESS, scanner->next_entry(value, value_len));
  Record fetched;
  ASSERT_EQ(RC::SUCCESS, v_index->get_record(table, value, value_len, fetched));
  ASSERT_EQ(2, record_int(fetched, 0));
  ASSERT_EQ(RC::RECORD_EOF, scanner->next_entry(value, value_len));
  scanner->destroy();

  Record to_delete;
  for (Record &record : scan_all(table, trx)) {
    if (record_int(record, 0) == 5) {
      to_delete = record;
      break;
    }
  }
  ASSERT_EQ(5, record_int(to_delete, 0));
  ASSERT_EQ(RC::SUCCESS, table->delete_record(to_delete));

  key = 50;
  scanner = v_index->create_scanner(
      reinterpret_cast<const char *>(&key), sizeof(key), true, reinterpret_cast<const char *>(&key), sizeof(key), true);
  ASSERT_NE(nullptr, scanner);
  ASSERT_EQ(RC::RECORD_EOF, scanner->next_entry(value, value_len));
  scanner->destroy();

  db->trx_kit().destroy_trx(trx);
  trx = nullptr;

  ASSERT_EQ(RC::SUCCESS, db->sync());
  db.reset();

  db = make_unique<Db>();
  ASSERT_EQ(RC::SUCCESS, db->init(dbname, dir.c_str(), "vacuous", "vacuous"));
  table = db->find_table("t_lsm");
  ASSERT_NE(nullptr, table);
  trx = db->trx_kit().create_trx(db->log_handler());
  ASSERT_NE(nullptr, trx);
  ASSERT_EQ(RC::SUCCESS, trx->start_if_need());
  ASSERT_EQ(row_num - 1, static_cast<int>(scan_all(table, trx).size()));

  v_index = table->find_index("idx_v");
  ASSERT_NE(nullptr, v_index);
  key = 40;
  scanner = v_index->create_scanner(
      reinterpret_cast<const char *>(&key), sizeof(key), true, reinterpret_cast<const char *>(&key), sizeof(key), true);
  ASSERT_NE(nullptr, scanner);
  ASSERT_EQ(RC::SUCCESS, scanner->next_entry(value, value_len));
  ASSERT_EQ(RC::SUCCESS, v_index->get_record(table, value, value_len, fetched));
  ASSERT_EQ(4, record_int(fetched, 0));
  ASSERT_EQ(RC::RECORD_EOF, scanner->next_entry(value, value_len));
  scanner->destroy();

  db->trx_kit().destroy_trx(trx);
  db.reset();
  filesystem::remove_all(dir);
}
