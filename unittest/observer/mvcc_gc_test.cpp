/* Copyright (c) 2021 OceanBase and/or its affiliates. All rights reserved.
miniob is licensed under Mulan PSL v2.
You can use this software according to the terms and conditions of the Mulan PSL v2.
You may obtain a copy of Mulan PSL v2 at:
         http://license.coscl.org.cn/MulanPSL2
THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
See the Mulan PSL v2 for more details. */

#include <cstring>
#include <filesystem>
#include <memory>

#include "common/lang/memory.h"
#include "common/types.h"
#include "storage/db/db.h"
#include "storage/index/index.h"
#include "storage/index/index_meta.h"
#include "storage/record/record_scanner.h"
#include "storage/table/table.h"
#include "storage/trx/mvcc_gc.h"
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

Record find_by_id(Table *table, Trx *trx, int id)
{
  RecordScanner *scanner = nullptr;
  EXPECT_EQ(RC::SUCCESS, table->get_record_scanner(scanner, trx, ReadWriteMode::READ_ONLY));
  Record record;
  while (RC::SUCCESS == scanner->next(record)) {
    if (field_int(table, record, "id") == id) {
      Record copy;
      EXPECT_EQ(RC::SUCCESS, copy.copy_data(record.data(), record.len()));
      copy.set_rid(record.rid());
      scanner->close_scan();
      delete scanner;
      return copy;
    }
  }
  scanner->close_scan();
  delete scanner;
  return Record();
}

int scan_count(Table *table, Trx *trx)
{
  RecordScanner *scanner = nullptr;
  EXPECT_EQ(RC::SUCCESS, table->get_record_scanner(scanner, trx, ReadWriteMode::READ_ONLY));
  Record record;
  int    count = 0;
  while (RC::SUCCESS == scanner->next(record)) {
    count++;
  }
  scanner->close_scan();
  delete scanner;
  return count;
}

int raw_scan_count(Table *table)
{
  return scan_count(table, nullptr);
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

int index_raw_count(Index *index, int key)
{
  IndexScanner *scanner = index->create_scanner(reinterpret_cast<const char *>(&key), sizeof(key), true,
      reinterpret_cast<const char *>(&key), sizeof(key), true);
  EXPECT_NE(nullptr, scanner);
  const char *value     = nullptr;
  int         value_len = 0;
  int         count     = 0;
  while (RC::SUCCESS == scanner->next_entry(value, value_len)) {
    count++;
  }
  scanner->destroy();
  return count;
}

}  // namespace

TEST(MvccGc, collects_dead_versions_and_index_entries)
{
  filesystem::path db_path = "mvcc_gc_test/db";
  filesystem::remove_all("mvcc_gc_test");
  filesystem::create_directories(db_path);

  auto db = make_unique<Db>();
  ASSERT_EQ(RC::SUCCESS, db->init("gc_db", db_path.c_str(), "mvcc", "disk"));

  vector<AttrInfoSqlNode> attrs(2);
  attrs[0].name   = "id";
  attrs[0].type   = AttrType::INTS;
  attrs[0].length = sizeof(int);
  attrs[1].name   = "v";
  attrs[1].type   = AttrType::INTS;
  attrs[1].length = sizeof(int);
  ASSERT_EQ(RC::SUCCESS, db->create_table("t_gc", attrs, {}));
  Table *table = db->find_table("t_gc");
  ASSERT_NE(nullptr, table);

  const FieldMeta *indexed_field = table->table_meta().field("v");
  ASSERT_NE(nullptr, indexed_field);
  ASSERT_EQ(RC::SUCCESS, table->create_index(nullptr, indexed_field, "i_v"));
  Index *v_index = table->find_index("i_v");
  ASSERT_NE(nullptr, v_index);

  MvccTrxKit &kit = static_cast<MvccTrxKit &>(db->trx_kit());
  Trx        *seed = kit.create_trx(db->log_handler());
  ASSERT_EQ(RC::SUCCESS, seed->start_if_need());
  for (int i = 1; i <= 2; i++) {
    vector<Value> values(2);
    values[0].set_int(i);
    values[1].set_int(i * 10);
    Record record;
    ASSERT_EQ(RC::SUCCESS, table->make_record(values.size(), values.data(), record));
    ASSERT_EQ(RC::SUCCESS, seed->insert_record(table, record));
  }
  ASSERT_EQ(RC::SUCCESS, seed->commit());

  const FieldMeta *v_field = table->table_meta().field("v");
  ASSERT_NE(nullptr, v_field);

  // Old snapshot must pin the old version of row 1.
  Trx *old_reader = kit.create_trx(db->log_handler());
  ASSERT_EQ(RC::SUCCESS, old_reader->start_if_need());

  Trx *writer = kit.create_trx(db->log_handler());
  ASSERT_EQ(RC::SUCCESS, writer->start_if_need());
  // Find row 1 via the writer's scan.
  Record old_record;
  RecordScanner *scanner = nullptr;
  ASSERT_EQ(RC::SUCCESS, table->get_record_scanner(scanner, writer, ReadWriteMode::READ_ONLY));
  Record scanned;
  bool   found = false;
  while (RC::SUCCESS == scanner->next(scanned)) {
    if (field_int(table, scanned, "id") == 1) {
      old_record.copy_data(scanned.data(), scanned.len());
      old_record.set_rid(scanned.rid());
      old_record.set_key(scanned.key());
      found = true;
      break;
    }
  }
  scanner->close_scan();
  delete scanner;
  ASSERT_TRUE(found);
  Record new_record = old_record;
  Value  value;
  value.set_int(99);
  ASSERT_EQ(RC::SUCCESS, new_record.reset_filed(v_field->offset(), v_field->len()));
  ASSERT_EQ(RC::SUCCESS, table->set_value_to_record(new_record.data(), value, v_field));
  ASSERT_EQ(RC::SUCCESS, writer->update_record(table, old_record, new_record));

  // Delete row 2 is left as runtime code; this test focuses on update.
  ASSERT_EQ(RC::SUCCESS, writer->commit());

  MvccGc gc;
  ASSERT_EQ(RC::SUCCESS, gc.collect(*db));
  // Pinned old snapshot still sees the old version and its index entry.
  ASSERT_EQ(1, index_visible_count(table, old_reader, v_index, 10));
  ASSERT_EQ(2, scan_count(table, old_reader));

  ASSERT_EQ(RC::SUCCESS, old_reader->rollback());
  ASSERT_EQ(RC::SUCCESS, gc.collect(*db));

  Trx *fresh = kit.create_trx(db->log_handler());
  ASSERT_EQ(RC::SUCCESS, fresh->start_if_need());
  ASSERT_EQ(2, scan_count(table, fresh));
  ASSERT_EQ(1, index_visible_count(table, fresh, v_index, 99));
  ASSERT_EQ(0, index_visible_count(table, fresh, v_index, 10));
  ASSERT_EQ(1, index_raw_count(v_index, 99));
  ASSERT_EQ(0, index_raw_count(v_index, 10));
  ASSERT_EQ(2, raw_scan_count(table));
  ASSERT_EQ(RC::SUCCESS, fresh->rollback());

  db.reset();
  filesystem::remove_all("mvcc_gc_test");
}


TEST(MvccGc, aborted_delete_mark_does_not_delete_live_row)
{
  filesystem::path db_path = "mvcc_gc_abort_test/db";
  filesystem::remove_all("mvcc_gc_abort_test");
  filesystem::create_directories(db_path);

  auto db = make_unique<Db>();
  ASSERT_EQ(RC::SUCCESS, db->init("gc_abort_db", db_path.c_str(), "mvcc", "disk"));
  vector<AttrInfoSqlNode> attrs(2);
  attrs[0].name = "id"; attrs[0].type = AttrType::INTS; attrs[0].length = sizeof(int);
  attrs[1].name = "v";  attrs[1].type = AttrType::INTS; attrs[1].length = sizeof(int);
  ASSERT_EQ(RC::SUCCESS, db->create_table("t_gc_abort", attrs, {}));
  Table *table = db->find_table("t_gc_abort");
  ASSERT_NE(nullptr, table);

  auto *kit = static_cast<MvccTrxKit *>(&db->trx_kit());
  Trx  *seed = kit->create_trx(db->log_handler());
  ASSERT_EQ(RC::SUCCESS, seed->start_if_need());
  vector<Value> values(2);
  values[0].set_int(1); values[1].set_int(10);
  Record record;
  ASSERT_EQ(RC::SUCCESS, table->make_record(values.size(), values.data(), record));
  ASSERT_EQ(RC::SUCCESS, seed->insert_record(table, record));
  ASSERT_EQ(RC::SUCCESS, seed->commit());
  kit->destroy_trx(seed);

  // Simulate a crash between status=ABORTED and rollback restoring end_xid.
  Trx *deleter = kit->create_trx(db->log_handler());
  ASSERT_EQ(RC::SUCCESS, deleter->start_if_need());
  Record row = find_by_id(table, deleter, 1);
  ASSERT_EQ(RC::SUCCESS, deleter->delete_record(table, row));
  ASSERT_EQ(RC::SUCCESS, kit->status_table().abort_trx(deleter->id()));

  MvccGc gc;
  ASSERT_EQ(RC::SUCCESS, gc.collect(*db));
  Trx *check = kit->create_trx(db->log_handler());
  ASSERT_EQ(RC::SUCCESS, check->start_if_need());
  ASSERT_EQ(1, scan_count(table, check));
  ASSERT_EQ(1, field_int(table, find_by_id(table, check, 1), "id"));
  ASSERT_EQ(RC::SUCCESS, check->rollback());
  kit->destroy_trx(check);
  // 崩溃语义：GC 已修复后不会再跑该事务的 rollback。
  kit->destroy_trx(deleter);
  db.reset();
  filesystem::remove_all("mvcc_gc_abort_test");
}
