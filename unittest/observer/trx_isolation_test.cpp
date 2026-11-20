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
#include <utility>
#include <vector>

#include "common/lang/memory.h"
#include "common/types.h"
#include "storage/db/db.h"
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

RC visit(Trx *trx, Table *table, const Record &raw, ReadWriteMode mode = ReadWriteMode::READ_ONLY)
{
  Record record = copy_record(raw);
  return trx->visit_record(table, record, mode);
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

Table *create_table(Db *db, const char *name)
{
  vector<AttrInfoSqlNode> attrs(2);
  attrs[0].name   = "id";
  attrs[0].type   = AttrType::INTS;
  attrs[0].length = sizeof(int);
  attrs[1].name   = "v";
  attrs[1].type   = AttrType::INTS;
  attrs[1].length = sizeof(int);
  EXPECT_EQ(RC::SUCCESS, db->create_table(name, attrs, {}));
  return db->find_table(name);
}

void seed(MvccTrxKit *kit, Db *db, Table *table, const vector<pair<int, int>> &rows)
{
  Trx *trx = kit->create_trx(db->log_handler());
  EXPECT_EQ(RC::SUCCESS, trx->start_if_need());
  for (const auto &[id, value] : rows) {
    vector<Value> values(2);
    values[0].set_int(id);
    values[1].set_int(value);
    Record record;
    EXPECT_EQ(RC::SUCCESS, table->make_record(values.size(), values.data(), record));
    EXPECT_EQ(RC::SUCCESS, trx->insert_record(table, record));
  }
  EXPECT_EQ(RC::SUCCESS, trx->commit());
  kit->destroy_trx(trx);
}

}  // namespace

TEST(TrxIsolation, mvcc_snapshot_is_not_serializable)
{
  filesystem::remove_all("trx_isolation_test");
  filesystem::create_directories("trx_isolation_test/db");
  auto db = make_unique<Db>();
  ASSERT_EQ(RC::SUCCESS, db->init("iso_db", "trx_isolation_test/db", "mvcc", "disk"));
  auto *kit = dynamic_cast<MvccTrxKit *>(&db->trx_kit());
  ASSERT_NE(nullptr, kit);

  Table *dirty = create_table(db.get(), "t_dirty");
  Table *repeat = create_table(db.get(), "t_repeat");
  Table *lost = create_table(db.get(), "t_lost");
  Table *skew = create_table(db.get(), "t_skew");
  seed(kit, db.get(), dirty, {{1, 10}});
  seed(kit, db.get(), repeat, {{1, 10}});
  seed(kit, db.get(), lost, {{1, 10}});
  seed(kit, db.get(), skew, {{1, 1}, {2, 1}});

  // Dirty read: an uncommitted update stays invisible to another snapshot.
  Trx *writer = kit->create_trx(db->log_handler());
  ASSERT_EQ(RC::SUCCESS, writer->start_if_need());
  Record dirty_old = find_visible(dirty, writer, 1);
  Record dirty_new;
  ASSERT_EQ(RC::SUCCESS, make_updated(dirty, dirty_old, 99, dirty_new));
  ASSERT_EQ(RC::SUCCESS, writer->update_record(dirty, dirty_old, dirty_new));
  Trx *reader = kit->create_trx(db->log_handler());
  ASSERT_EQ(RC::SUCCESS, reader->start_if_need());
  ASSERT_EQ(10, field_int(dirty, find_visible(dirty, reader, 1), "v"));
  ASSERT_EQ(RC::SUCCESS, writer->commit());
  ASSERT_EQ(10, field_int(dirty, find_visible(dirty, reader, 1), "v"));
  ASSERT_EQ(RC::SUCCESS, reader->rollback());
  kit->destroy_trx(reader);
  kit->destroy_trx(writer);

  // Repeatable read: a snapshot keeps seeing its version after a commit.
  Trx *s1 = kit->create_trx(db->log_handler());
  Trx *s2 = kit->create_trx(db->log_handler());
  ASSERT_EQ(RC::SUCCESS, s1->start_if_need());
  ASSERT_EQ(RC::SUCCESS, s2->start_if_need());
  ASSERT_EQ(10, field_int(repeat, find_visible(repeat, s1, 1), "v"));
  Record repeat_old = find_visible(repeat, s2, 1);
  Record repeat_new;
  ASSERT_EQ(RC::SUCCESS, make_updated(repeat, repeat_old, 99, repeat_new));
  ASSERT_EQ(RC::SUCCESS, s2->update_record(repeat, repeat_old, repeat_new));
  ASSERT_EQ(RC::SUCCESS, s2->commit());
  ASSERT_EQ(10, field_int(repeat, find_visible(repeat, s1, 1), "v"));
  ASSERT_EQ(RC::SUCCESS, s1->rollback());
  kit->destroy_trx(s1);
  kit->destroy_trx(s2);

  // Lost update: first updater wins; the stale writer conflicts.
  Trx *u1 = kit->create_trx(db->log_handler());
  Trx *u2 = kit->create_trx(db->log_handler());
  ASSERT_EQ(RC::SUCCESS, u1->start_if_need());
  ASSERT_EQ(RC::SUCCESS, u2->start_if_need());
  Record u1_old = find_visible(lost, u1, 1);
  Record u2_old = find_visible(lost, u2, 1);
  ASSERT_EQ(10, field_int(lost, u2_old, "v"));
  Record u1_new;
  Record u2_new;
  ASSERT_EQ(RC::SUCCESS, make_updated(lost, u1_old, 20, u1_new));
  ASSERT_EQ(RC::SUCCESS, make_updated(lost, u2_old, 30, u2_new));
  ASSERT_EQ(RC::SUCCESS, u1->update_record(lost, u1_old, u1_new));
  ASSERT_EQ(RC::SUCCESS, u1->commit());
  ASSERT_EQ(RC::LOCKED_CONCURRENCY_CONFLICT, u2->update_record(lost, u2_old, u2_new));
  ASSERT_EQ(RC::SUCCESS, u2->rollback());
  kit->destroy_trx(u1);
  kit->destroy_trx(u2);

  Trx *check = kit->create_trx(db->log_handler());
  ASSERT_EQ(RC::SUCCESS, check->start_if_need());
  ASSERT_EQ(20, field_int(lost, find_visible(lost, check, 1), "v"));
  ASSERT_EQ(RC::SUCCESS, check->rollback());
  kit->destroy_trx(check);

  // Write skew: MVCC SI allows disjoint writes based on the same reads.
  Trx *w1 = kit->create_trx(db->log_handler());
  Trx *w2 = kit->create_trx(db->log_handler());
  ASSERT_EQ(RC::SUCCESS, w1->start_if_need());
  ASSERT_EQ(RC::SUCCESS, w2->start_if_need());
  Record w1_x = find_visible(skew, w1, 1);
  Record w1_y = find_visible(skew, w1, 2);
  Record w2_x = find_visible(skew, w2, 1);
  Record w2_y = find_visible(skew, w2, 2);
  ASSERT_EQ(1, field_int(skew, w1_x, "v"));
  ASSERT_EQ(1, field_int(skew, w2_y, "v"));
  Record w1_x_new;
  Record w2_y_new;
  ASSERT_EQ(RC::SUCCESS, make_updated(skew, w1_x, 0, w1_x_new));
  ASSERT_EQ(RC::SUCCESS, make_updated(skew, w2_y, 0, w2_y_new));
  ASSERT_EQ(RC::SUCCESS, w1->update_record(skew, w1_x, w1_x_new));
  ASSERT_EQ(RC::SUCCESS, w2->update_record(skew, w2_y, w2_y_new));
  ASSERT_EQ(RC::SUCCESS, w1->commit());
  ASSERT_EQ(RC::SUCCESS, w2->commit());
  kit->destroy_trx(w1);
  kit->destroy_trx(w2);
  (void)w1_y;
  (void)w2_x;

  Trx *skew_check = kit->create_trx(db->log_handler());
  ASSERT_EQ(RC::SUCCESS, skew_check->start_if_need());
  ASSERT_EQ(0, field_int(skew, find_visible(skew, skew_check, 1), "v"));
  ASSERT_EQ(0, field_int(skew, find_visible(skew, skew_check, 2), "v"));
  ASSERT_EQ(RC::SUCCESS, skew_check->rollback());
  kit->destroy_trx(skew_check);

  db.reset();
  filesystem::remove_all("trx_isolation_test");
}

TEST(TrxIsolation, occ_aborts_on_read_after_conflicting_commit)
{
  filesystem::remove_all("trx_isolation_occ");
  filesystem::create_directories("trx_isolation_occ/db");
  auto db = make_unique<Db>();
  ASSERT_EQ(RC::SUCCESS, db->init("iso_occ", "trx_isolation_occ/db", "occ", "disk"));
  auto *kit = dynamic_cast<MvccTrxKit *>(&db->trx_kit());
  ASSERT_NE(nullptr, kit);

  Table *table = create_table(db.get(), "t_occ");
  seed(kit, db.get(), table, {{1, 10}});

  Trx *reader = kit->create_trx(db->log_handler());
  ASSERT_EQ(RC::SUCCESS, reader->start_if_need());
  ASSERT_EQ(RC::SUCCESS, visit(reader, table, find_visible(table, reader, 1)));

  Trx *writer = kit->create_trx(db->log_handler());
  ASSERT_EQ(RC::SUCCESS, writer->start_if_need());
  Record old_record = find_visible(table, writer, 1);
  Record new_record;
  ASSERT_EQ(RC::SUCCESS, make_updated(table, old_record, 99, new_record));
  ASSERT_EQ(RC::SUCCESS, writer->update_record(table, old_record, new_record));
  ASSERT_EQ(RC::SUCCESS, writer->commit());

  ASSERT_EQ(RC::LOCKED_CONCURRENCY_CONFLICT, reader->commit());
  kit->destroy_trx(reader);
  kit->destroy_trx(writer);

  Trx *check = kit->create_trx(db->log_handler());
  ASSERT_EQ(RC::SUCCESS, check->start_if_need());
  ASSERT_EQ(99, field_int(table, find_visible(table, check, 1), "v"));
  ASSERT_EQ(RC::SUCCESS, check->rollback());
  kit->destroy_trx(check);

  db.reset();
  filesystem::remove_all("trx_isolation_occ");
}
