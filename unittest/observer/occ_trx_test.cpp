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
#include "storage/record/record_scanner.h"
#include "storage/table/table.h"
#include "storage/trx/occ_trx.h"
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

Record copy_record(const Record &record)
{
  Record copy;
  EXPECT_EQ(RC::SUCCESS, copy.copy_data(record.data(), record.len()));
  copy.set_rid(record.rid());
  copy.set_key(record.key());
  return copy;
}

Record find_visible(Table *table, Trx *trx, int id)
{
  RecordScanner *scanner = nullptr;
  EXPECT_EQ(RC::SUCCESS, table->get_record_scanner(scanner, trx, ReadWriteMode::READ_ONLY));
  Record record;
  while (RC::SUCCESS == scanner->next(record)) {
    if (field_int(table, record, "id") == id) {
      Record copy = copy_record(record);
      scanner->close_scan();
      delete scanner;
      return copy;
    }
  }
  scanner->close_scan();
  delete scanner;
  return Record();
}

Record find_raw(Table *table, int id)
{
  RecordScanner *scanner = nullptr;
  EXPECT_EQ(RC::SUCCESS, table->get_record_scanner(scanner, nullptr, ReadWriteMode::READ_ONLY));
  Record record;
  while (RC::SUCCESS == scanner->next(record)) {
    if (field_int(table, record, "id") == id) {
      Record copy = copy_record(record);
      scanner->close_scan();
      delete scanner;
      return copy;
    }
  }
  scanner->close_scan();
  delete scanner;
  return Record();
}

RC visit(Trx *trx, Table *table, const Record &raw, ReadWriteMode mode = ReadWriteMode::READ_ONLY)
{
  Record record = copy_record(raw);
  return trx->visit_record(table, record, mode);
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

RC make_updated(Table *table, const Record &old_record, int value, Record &new_record)
{
  const FieldMeta *v_field = table->table_meta().field("v");
  EXPECT_NE(nullptr, v_field);
  new_record = old_record;
  Value new_value;
  new_value.set_int(value);
  RC rc = new_record.reset_filed(v_field->offset(), v_field->len());
  if (OB_FAIL(rc)) {
    return rc;
  }
  return table->set_value_to_record(new_record.data(), new_value, v_field);
}

}  // namespace

class OccTrxTest : public ::testing::Test
{
protected:
  void SetUp() override
  {
    filesystem::remove_all("occ_trx_test");
    filesystem::create_directories("occ_trx_test/db");
    db = make_unique<Db>();
    ASSERT_EQ(RC::SUCCESS, db->init("occ_db", "occ_trx_test/db", "occ", "disk"));

    vector<AttrInfoSqlNode> attrs(2);
    attrs[0].name   = "id";
    attrs[0].type   = AttrType::INTS;
    attrs[0].length = sizeof(int);
    attrs[1].name   = "v";
    attrs[1].type   = AttrType::INTS;
    attrs[1].length = sizeof(int);
    ASSERT_EQ(RC::SUCCESS, db->create_table("t_occ", attrs, {}));
    table = db->find_table("t_occ");
    ASSERT_NE(nullptr, table);

    kit = dynamic_cast<MvccTrxKit *>(&db->trx_kit());
    ASSERT_NE(nullptr, kit);

    Trx *seed = kit->create_trx(db->log_handler());
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
    kit->destroy_trx(seed);
  }

  void TearDown() override
  {
    db.reset();
    filesystem::remove_all("occ_trx_test");
  }

protected:
  unique_ptr<Db> db;
  Table         *table = nullptr;
  MvccTrxKit    *kit   = nullptr;
};

TEST_F(OccTrxTest, read_write_conflict_aborts_at_commit)
{
  Trx *t1 = kit->create_trx(db->log_handler());
  ASSERT_EQ(RC::SUCCESS, t1->start_if_need());
  ASSERT_EQ(RC::SUCCESS, visit(t1, table, find_raw(table, 1)));

  Trx *t2 = kit->create_trx(db->log_handler());
  ASSERT_EQ(RC::SUCCESS, t2->start_if_need());
  Record old_record = find_raw(table, 1);
  Record new_record;
  ASSERT_EQ(RC::SUCCESS, make_updated(table, old_record, 99, new_record));
  ASSERT_EQ(RC::SUCCESS, t2->update_record(table, old_record, new_record));
  ASSERT_EQ(RC::SUCCESS, t2->commit());

  ASSERT_EQ(RC::LOCKED_CONCURRENCY_CONFLICT, t1->commit());

  Trx *fresh = kit->create_trx(db->log_handler());
  ASSERT_EQ(RC::SUCCESS, fresh->start_if_need());
  Record row = find_visible(table, fresh, 1);
  ASSERT_EQ(99, field_int(table, row, "v"));
  ASSERT_EQ(3, scan_count(table, fresh));
  ASSERT_EQ(RC::SUCCESS, fresh->rollback());
  kit->destroy_trx(fresh);
  kit->destroy_trx(t1);
  kit->destroy_trx(t2);
}

TEST_F(OccTrxTest, disjoint_writes_commit)
{
  Trx *t1 = kit->create_trx(db->log_handler());
  Trx *t2 = kit->create_trx(db->log_handler());
  ASSERT_EQ(RC::SUCCESS, t1->start_if_need());
  ASSERT_EQ(RC::SUCCESS, t2->start_if_need());
  ASSERT_EQ(RC::SUCCESS, visit(t1, table, find_raw(table, 1)));
  ASSERT_EQ(RC::SUCCESS, visit(t2, table, find_raw(table, 2)));

  Record row1 = find_raw(table, 1);
  Record row2 = find_raw(table, 2);
  Record updated1;
  Record updated2;
  ASSERT_EQ(RC::SUCCESS, make_updated(table, row1, 11, updated1));
  ASSERT_EQ(RC::SUCCESS, make_updated(table, row2, 22, updated2));
  ASSERT_EQ(RC::SUCCESS, t1->update_record(table, row1, updated1));
  ASSERT_EQ(RC::SUCCESS, t2->update_record(table, row2, updated2));
  ASSERT_EQ(RC::SUCCESS, t1->commit());
  ASSERT_EQ(RC::SUCCESS, t2->commit());

  Trx *fresh = kit->create_trx(db->log_handler());
  ASSERT_EQ(RC::SUCCESS, fresh->start_if_need());
  ASSERT_EQ(11, field_int(table, find_visible(table, fresh, 1), "v"));
  ASSERT_EQ(22, field_int(table, find_visible(table, fresh, 2), "v"));
  ASSERT_EQ(RC::SUCCESS, fresh->rollback());
  kit->destroy_trx(fresh);
  kit->destroy_trx(t1);
  kit->destroy_trx(t2);
}

TEST_F(OccTrxTest, write_write_conflict_is_detected_at_update)
{
  Trx *t1 = kit->create_trx(db->log_handler());
  ASSERT_EQ(RC::SUCCESS, t1->start_if_need());
  Record old_record = find_raw(table, 1);
  Record new_record;
  ASSERT_EQ(RC::SUCCESS, make_updated(table, old_record, 77, new_record));
  ASSERT_EQ(RC::SUCCESS, t1->update_record(table, old_record, new_record));

  Trx *t2 = kit->create_trx(db->log_handler());
  ASSERT_EQ(RC::SUCCESS, t2->start_if_need());
  Record t2_old = find_raw(table, 1);
  Record t2_new;
  ASSERT_EQ(RC::SUCCESS, make_updated(table, t2_old, 88, t2_new));
  ASSERT_EQ(RC::LOCKED_CONCURRENCY_CONFLICT, t2->update_record(table, t2_old, t2_new));
  ASSERT_EQ(RC::SUCCESS, t2->rollback());

  ASSERT_EQ(RC::SUCCESS, t1->commit());
  Trx *fresh = kit->create_trx(db->log_handler());
  ASSERT_EQ(RC::SUCCESS, fresh->start_if_need());
  ASSERT_EQ(77, field_int(table, find_visible(table, fresh, 1), "v"));
  ASSERT_EQ(RC::SUCCESS, fresh->rollback());
  kit->destroy_trx(fresh);
  kit->destroy_trx(t1);
  kit->destroy_trx(t2);
}

TEST_F(OccTrxTest, uncommitted_row_is_invisible)
{
  Trx *writer = kit->create_trx(db->log_handler());
  ASSERT_EQ(RC::SUCCESS, writer->start_if_need());
  vector<Value> values(2);
  values[0].set_int(4);
  values[1].set_int(40);
  Record inserted;
  ASSERT_EQ(RC::SUCCESS, table->make_record(values.size(), values.data(), inserted));
  ASSERT_EQ(RC::SUCCESS, writer->insert_record(table, inserted));

  Trx *reader = kit->create_trx(db->log_handler());
  ASSERT_EQ(RC::SUCCESS, reader->start_if_need());
  ASSERT_EQ(RC::RECORD_INVISIBLE, visit(reader, table, find_raw(table, 4)));
  ASSERT_EQ(RC::SUCCESS, reader->commit());

  ASSERT_EQ(RC::SUCCESS, writer->rollback());
  ASSERT_EQ(3, scan_count(table, reader));
  kit->destroy_trx(writer);
  kit->destroy_trx(reader);
}
