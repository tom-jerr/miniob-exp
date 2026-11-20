/* Copyright (c) 2021 OceanBase and/or its affiliates. All rights reserved.
miniob is licensed under Mulan PSL v2.
You can use this software according to the terms and conditions of the Mulan PSL v2.
You may obtain a copy of Mulan PSL v2 at:
         http://license.coscl.org.cn/MulanPSL2
THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
See the Mulan PSL v2 for more details. */

// Covers MVCC UPDATE: old version marked deleted, new version inserted;
// snapshots keep old values, commit publishes the new one, rollback discards it,
// and concurrent writers conflict.

#include <cstring>
#include <filesystem>
#include <memory>

#include "common/lang/memory.h"
#include "common/types.h"
#include "storage/db/db.h"
#include "storage/field/field.h"
#include "storage/index/index.h"
#include "storage/record/record_scanner.h"
#include "storage/table/table.h"
#include "storage/trx/mvcc_trx.h"
#include "storage/trx/trx.h"
#include "gtest/gtest.h"

using namespace std;
using namespace common;

namespace {

int field_int(Table *table, const Record &record, const char *field_name)
{
  const FieldMeta *field = table->table_meta().field(field_name);
  EXPECT_NE(nullptr, field);
  int value = 0;
  memcpy(&value, record.data() + field->offset(), sizeof(value));
  return value;
}

vector<Record> scan_all(Table *table, Trx *trx)
{
  RecordScanner *scanner = nullptr;
  EXPECT_EQ(RC::SUCCESS, table->get_record_scanner(scanner, trx, ReadWriteMode::READ_ONLY));
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

int index_visible_count(Table *table, Trx *trx, Index *index, int key)
{
  IndexScanner *scanner = index->create_scanner(reinterpret_cast<const char *>(&key), sizeof(key), true,
      reinterpret_cast<const char *>(&key), sizeof(key), true);
  EXPECT_NE(nullptr, scanner);
  const char *value     = nullptr;
  int         value_len = 0;
  int         count     = 0;
  Record      record;
  while (RC::SUCCESS == scanner->next_entry(value, value_len)) {
    if (index->get_record(table, value, value_len, record) != RC::SUCCESS) {
      continue;
    }
    if (trx->visit_record(table, record, ReadWriteMode::READ_ONLY) == RC::SUCCESS) {
      count++;
    }
  }
  scanner->destroy();
  return count;
}

Record find_by_id(Table *table, Trx *trx, int id)
{
  for (const Record &record : scan_all(table, trx)) {
    if (field_int(table, record, "id") == id) {
      return record;
    }
  }
  return Record();
}

}  // namespace

TEST(MvccUpdate, snapshot_and_commit_and_rollback_and_conflict)
{
  filesystem::path dir = "mvcc_update_test";
  filesystem::remove_all(dir);
  filesystem::create_directories(dir);

  auto db = make_unique<Db>();
  ASSERT_EQ(RC::SUCCESS, db->init("mvcc_update_db", dir.c_str(), "mvcc", "vacuous"));
  MvccTrxKit &kit = static_cast<MvccTrxKit &>(db->trx_kit());

  vector<AttrInfoSqlNode> attrs(2);
  attrs[0].name   = "id";
  attrs[0].type   = AttrType::INTS;
  attrs[0].length = sizeof(int);
  attrs[1].name   = "v";
  attrs[1].type   = AttrType::INTS;
  attrs[1].length = sizeof(int);
  ASSERT_EQ(RC::SUCCESS, db->create_table("t_mv", attrs, {}));
  Table *table = db->find_table("t_mv");
  ASSERT_NE(nullptr, table);

  Trx *seed = kit.create_trx(db->log_handler());
  ASSERT_EQ(RC::SUCCESS, seed->start_if_need());
  for (int i = 1; i <= 3; i++) {
    vector<Value> values(2);
    values[0].set_int(i);
    values[1].set_int(i * 10);
    Record record;
    ASSERT_EQ(RC::SUCCESS, table->make_record(values.size(), values.data(), record));
    ASSERT_EQ(RC::SUCCESS, seed->insert_record(table, record));
  }
  ASSERT_EQ(RC::SUCCESS, seed->commit());

  ASSERT_EQ(RC::SUCCESS, table->create_index(nullptr, table->table_meta().field("v"), "i_v"));
  const FieldMeta *v_field = table->table_meta().field("v");
  ASSERT_NE(nullptr, v_field);
  Index *v_index = table->find_index("i_v");
  ASSERT_NE(nullptr, v_index);

  Trx *writer = kit.create_trx(db->log_handler());
  ASSERT_EQ(RC::SUCCESS, writer->start_if_need());

  Trx *old_snapshot = kit.create_trx(db->log_handler());
  ASSERT_EQ(RC::SUCCESS, old_snapshot->start_if_need());

  Record old_record = find_by_id(table, writer, 2);
  ASSERT_EQ(2, field_int(table, old_record, "id"));
  Record     new_record = old_record;
  Value      new_value;
  new_value.set_int(99);
  ASSERT_EQ(RC::SUCCESS, new_record.reset_filed(v_field->offset(), v_field->len()));
  ASSERT_EQ(RC::SUCCESS, table->set_value_to_record(new_record.data(), new_value, v_field));
  ASSERT_EQ(RC::SUCCESS, writer->update_record(table, old_record, new_record));

  // The writer sees the new version, not the old one.
  vector<Record> writer_rows = scan_all(table, writer);
  ASSERT_EQ(3, static_cast<int>(writer_rows.size()));
  for (const Record &record : writer_rows) {
    ASSERT_FALSE(field_int(table, record, "id") == 2 && field_int(table, record, "v") == 20);
  }
  // The old snapshot still sees the old value.
  Record old_from_read = find_by_id(table, old_snapshot, 2);
  ASSERT_EQ(20, field_int(table, old_from_read, "v"));

  ASSERT_EQ(RC::SUCCESS, writer->commit());
  ASSERT_EQ(20, field_int(table, find_by_id(table, old_snapshot, 2), "v"));

  Trx *fresh = kit.create_trx(db->log_handler());
  ASSERT_EQ(RC::SUCCESS, fresh->start_if_need());
  ASSERT_EQ(99, field_int(table, find_by_id(table, fresh, 2), "v"));

  ASSERT_EQ(1, index_visible_count(table, fresh, v_index, 99));
  ASSERT_EQ(0, index_visible_count(table, fresh, v_index, 20));
  ASSERT_EQ(1, index_visible_count(table, old_snapshot, v_index, 20));

  // Rollback restores the old version and removes the new one.
  Trx *rollback_trx = kit.create_trx(db->log_handler());
  ASSERT_EQ(RC::SUCCESS, rollback_trx->start_if_need());
  Record rollback_old = find_by_id(table, rollback_trx, 3);
  Record rollback_new = rollback_old;
  new_value.set_int(77);
  ASSERT_EQ(RC::SUCCESS, rollback_new.reset_filed(v_field->offset(), v_field->len()));
  ASSERT_EQ(RC::SUCCESS, table->set_value_to_record(rollback_new.data(), new_value, v_field));
  ASSERT_EQ(RC::SUCCESS, rollback_trx->update_record(table, rollback_old, rollback_new));
  ASSERT_EQ(RC::SUCCESS, rollback_trx->rollback());

  Trx *after_rollback = kit.create_trx(db->log_handler());
  ASSERT_EQ(RC::SUCCESS, after_rollback->start_if_need());
  ASSERT_EQ(30, field_int(table, find_by_id(table, after_rollback, 3), "v"));

  // Two writers on the same row: second gets a conflict and does not change it.
  Trx *t6 = kit.create_trx(db->log_handler());
  ASSERT_EQ(RC::SUCCESS, t6->start_if_need());
  Record t6_old = find_by_id(table, t6, 1);
  Record t6_new = t6_old;
  new_value.set_int(111);
  ASSERT_EQ(RC::SUCCESS, t6_new.reset_filed(v_field->offset(), v_field->len()));
  ASSERT_EQ(RC::SUCCESS, table->set_value_to_record(t6_new.data(), new_value, v_field));
  ASSERT_EQ(RC::SUCCESS, t6->update_record(table, t6_old, t6_new));

  Trx *t7 = kit.create_trx(db->log_handler());
  ASSERT_EQ(RC::SUCCESS, t7->start_if_need());
  Record t7_old = find_by_id(table, t7, 1);
  ASSERT_EQ(1, field_int(table, t7_old, "id"));
  Record t7_new = t7_old;
  new_value.set_int(222);
  ASSERT_EQ(RC::SUCCESS, t7_new.reset_filed(v_field->offset(), v_field->len()));
  ASSERT_EQ(RC::SUCCESS, table->set_value_to_record(t7_new.data(), new_value, v_field));
  ASSERT_EQ(RC::LOCKED_CONCURRENCY_CONFLICT, t7->update_record(table, t7_old, t7_new));
  ASSERT_EQ(RC::SUCCESS, t7->rollback());
  ASSERT_EQ(RC::SUCCESS, t6->rollback());

  kit.destroy_trx(seed);
  kit.destroy_trx(writer);
  kit.destroy_trx(old_snapshot);
  kit.destroy_trx(fresh);
  kit.destroy_trx(rollback_trx);
  kit.destroy_trx(after_rollback);
  kit.destroy_trx(t6);
  kit.destroy_trx(t7);
  db.reset();
  filesystem::remove_all(dir);
}
