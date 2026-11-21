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
  ASSERT_EQ(RC::SUCCESS, db->create_table("t_lsm", attrs, {}, StorageFormat::ROW_FORMAT, StorageEngine::LSM));
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

  key     = 50;
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
  key     = 40;
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

TEST(LsmTableEngine, update_rewrites_value_and_indexes_and_survives_reopen)
{
  filesystem::path dir = "lsm_table_engine_test";
  filesystem::remove_all(dir);
  filesystem::create_directories(dir);

  const char *dbname = "lsm_update_db";
  auto        db     = make_unique<Db>();
  ASSERT_EQ(RC::SUCCESS, db->init(dbname, dir.c_str(), "vacuous", "vacuous"));

  vector<AttrInfoSqlNode> attrs = int_attrs();
  ASSERT_EQ(RC::SUCCESS, db->create_table("t_up", attrs, {}, StorageFormat::ROW_FORMAT, StorageEngine::LSM));
  Table *table = db->find_table("t_up");
  ASSERT_NE(nullptr, table);

  Trx *trx = db->trx_kit().create_trx(db->log_handler());
  ASSERT_NE(nullptr, trx);
  ASSERT_EQ(RC::SUCCESS, trx->start_if_need());

  for (int i = 1; i <= 3; i++) {
    Record record = make_int_record(table, i, i * 10);
    ASSERT_EQ(RC::SUCCESS, table->insert_record(record));
  }

  ASSERT_EQ(RC::SUCCESS, table->create_index(trx, table->table_meta().field("v"), "i_v"));
  const FieldMeta *id_field = table->table_meta().field("id");
  const FieldMeta *v_field  = table->table_meta().field("v");
  ASSERT_NE(nullptr, id_field);
  ASSERT_NE(nullptr, v_field);
  Index *v_index = table->find_index("i_v");
  ASSERT_NE(nullptr, v_index);

  vector<Record> records = scan_all(table, trx);
  ASSERT_EQ(3, static_cast<int>(records.size()));
  Record old_record = records[1];
  ASSERT_EQ(2, record_int(old_record, 0));

  Value  value;
  Record new_record = old_record;
  value.set_int(99);
  ASSERT_EQ(RC::SUCCESS, new_record.reset_filed(v_field->offset(), v_field->len()));
  ASSERT_EQ(RC::SUCCESS, table->set_value_to_record(new_record.data(), value, v_field));
  ASSERT_EQ(RC::SUCCESS, table->update_record_with_trx(old_record, new_record, trx));

  records = scan_all(table, trx);
  ASSERT_EQ(3, static_cast<int>(records.size()));
  for (const Record &record : records) {
    if (record_int(record, 0) == 2) {
      ASSERT_EQ(99, record_int(record, 1));
    }
  }

  int           probe     = 99;
  const char   *idx_value = nullptr;
  int           idx_len   = 0;
  IndexScanner *scanner   = v_index->create_scanner(reinterpret_cast<const char *>(&probe),
      sizeof(probe),
      true,
      reinterpret_cast<const char *>(&probe),
      sizeof(probe),
      true);
  ASSERT_NE(nullptr, scanner);
  ASSERT_EQ(RC::SUCCESS, scanner->next_entry(idx_value, idx_len));
  Record fetched;
  ASSERT_EQ(RC::SUCCESS, v_index->get_record(table, idx_value, idx_len, fetched));
  ASSERT_EQ(2, record_int(fetched, 0));
  ASSERT_EQ(RC::RECORD_EOF, scanner->next_entry(idx_value, idx_len));
  scanner->destroy();

  // id is a normal column for the LSM engine; change it and re-check.
  Record id_changed = new_record;
  value.set_int(5);
  ASSERT_EQ(RC::SUCCESS, id_changed.reset_filed(id_field->offset(), id_field->len()));
  ASSERT_EQ(RC::SUCCESS, table->set_value_to_record(id_changed.data(), value, id_field));
  ASSERT_EQ(RC::SUCCESS, table->update_record_with_trx(new_record, id_changed, trx));
  records           = scan_all(table, trx);
  bool found_new_id = false;
  for (const Record &record : records) {
    if (record_int(record, 0) == 5) {
      found_new_id = true;
      ASSERT_EQ(99, record_int(record, 1));
    }
  }
  ASSERT_TRUE(found_new_id);

  ASSERT_EQ(RC::SUCCESS, trx->commit());
  db->trx_kit().destroy_trx(trx);
  trx = nullptr;
  ASSERT_EQ(RC::SUCCESS, db->sync());
  db.reset();

  db = make_unique<Db>();
  ASSERT_EQ(RC::SUCCESS, db->init(dbname, dir.c_str(), "vacuous", "vacuous"));
  table = db->find_table("t_up");
  ASSERT_NE(nullptr, table);
  trx = db->trx_kit().create_trx(db->log_handler());
  ASSERT_EQ(RC::SUCCESS, trx->start_if_need());
  records = scan_all(table, trx);
  ASSERT_EQ(3, static_cast<int>(records.size()));
  bool found_persisted = false;
  for (const Record &record : records) {
    if (record_int(record, 0) == 5) {
      found_persisted = true;
      ASSERT_EQ(99, record_int(record, 1));
    }
  }
  ASSERT_TRUE(found_persisted);

  v_index = table->find_index("i_v");
  ASSERT_NE(nullptr, v_index);
  probe   = 99;
  scanner = v_index->create_scanner(reinterpret_cast<const char *>(&probe),
      sizeof(probe),
      true,
      reinterpret_cast<const char *>(&probe),
      sizeof(probe),
      true);
  ASSERT_NE(nullptr, scanner);
  ASSERT_EQ(RC::SUCCESS, scanner->next_entry(idx_value, idx_len));
  ASSERT_EQ(RC::SUCCESS, v_index->get_record(table, idx_value, idx_len, fetched));
  ASSERT_EQ(5, record_int(fetched, 0));
  scanner->destroy();

  ASSERT_EQ(RC::SUCCESS, trx->commit());
  db->trx_kit().destroy_trx(trx);
  db.reset();
  filesystem::remove_all(dir);
}

TEST(LsmTableEngine, lsm_mvcc_occ_two_clients_interleaved_and_concurrent_update)
{
  filesystem::path dir = "lsm_table_engine_mvcc_test";
  filesystem::remove_all(dir);
  filesystem::create_directories(dir);

  const char *dbname = "lsm_mvcc_db";
  auto        db     = make_unique<Db>();
  ASSERT_EQ(RC::SUCCESS, db->init(dbname, dir.c_str(), "lsm", "vacuous"));

  vector<AttrInfoSqlNode> attrs = int_attrs();
  ASSERT_EQ(RC::SUCCESS, db->create_table("t1", attrs, {}, StorageFormat::ROW_FORMAT, StorageEngine::LSM));
  Table           *table   = db->find_table("t1");
  const FieldMeta *v_field = table->table_meta().field("v");
  ASSERT_NE(nullptr, table);
  ASSERT_NE(nullptr, v_field);

  // insert into t1 values(0, 0);
  Record r0 = make_int_record(table, 0, 0);
  ASSERT_EQ(RC::SUCCESS, table->insert_record(r0));

  // client 1: begin;  client 2: begin;
  Trx *c1 = db->trx_kit().create_trx(db->log_handler());
  Trx *c2 = db->trx_kit().create_trx(db->log_handler());
  ASSERT_EQ(RC::SUCCESS, c1->start_if_need());
  ASSERT_EQ(RC::SUCCESS, c2->start_if_need());

  // client 1: insert into t1 values(1, 1);
  Record r1 = make_int_record(table, 1, 1);
  ASSERT_EQ(RC::SUCCESS, c1->insert_record(table, r1));

  // client 1 sees (0,0) and (1,1); client 2 only sees (0,0)
  vector<Record> c1_rows = scan_all(table, c1);
  vector<Record> c2_rows = scan_all(table, c2);
  ASSERT_EQ(2u, c1_rows.size());
  ASSERT_EQ(1u, c2_rows.size());
  EXPECT_EQ(0, record_int(c2_rows[0], 0));

  // client 1: delete from t1 where id = 0; update t1 set v = 101 where id = 1;
  ASSERT_EQ(RC::SUCCESS, c1->delete_record(table, c1_rows[0]));
  Record r1_updated = c1_rows[1];
  Value  v101;
  v101.set_int(101);
  ASSERT_EQ(RC::SUCCESS, r1_updated.reset_filed(v_field->offset(), v_field->len()));
  ASSERT_EQ(RC::SUCCESS, table->set_value_to_record(r1_updated.data(), v101, v_field));
  ASSERT_EQ(RC::SUCCESS, c1->update_record(table, c1_rows[1], r1_updated));

  // client 1 sees only (1, 101); client 2 still sees only (0, 0)
  c1_rows = scan_all(table, c1);
  c2_rows = scan_all(table, c2);
  ASSERT_EQ(1u, c1_rows.size());
  EXPECT_EQ(1, record_int(c1_rows[0], 0));
  EXPECT_EQ(101, record_int(c1_rows[0], 1));
  ASSERT_EQ(1u, c2_rows.size());
  EXPECT_EQ(0, record_int(c2_rows[0], 0));

  // client 2: insert into t1 values(3, 3);
  Record r3 = make_int_record(table, 3, 3);
  ASSERT_EQ(RC::SUCCESS, c2->insert_record(table, r3));
  c2_rows = scan_all(table, c2);
  ASSERT_EQ(2u, c2_rows.size());

  // client 1: commit;
  ASSERT_EQ(RC::SUCCESS, c1->commit());

  // client 2 still sees its own snapshot {(0,0), (3,3)} before commit
  c2_rows = scan_all(table, c2);
  ASSERT_EQ(2u, c2_rows.size());
  EXPECT_EQ(0, record_int(c2_rows[0], 0));
  EXPECT_EQ(3, record_int(c2_rows[1], 0));

  // client 2: commit;
  ASSERT_EQ(RC::SUCCESS, c2->commit());

  // New transaction sees merged result: {(1, 101), (3, 3)}
  Trx *c3 = db->trx_kit().create_trx(db->log_handler());
  ASSERT_EQ(RC::SUCCESS, c3->start_if_need());
  vector<Record> c3_rows = scan_all(table, c3);
  ASSERT_EQ(2u, c3_rows.size());
  EXPECT_EQ(1, record_int(c3_rows[0], 0));
  EXPECT_EQ(101, record_int(c3_rows[0], 1));
  EXPECT_EQ(3, record_int(c3_rows[1], 0));
  EXPECT_EQ(3, record_int(c3_rows[1], 1));

  db->trx_kit().destroy_trx(c1);
  db->trx_kit().destroy_trx(c2);
  db->trx_kit().destroy_trx(c3);
  db.reset();
  filesystem::remove_all(dir);
}
