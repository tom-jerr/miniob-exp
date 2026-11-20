/* Copyright (c) 2021 OceanBase and/or its affiliates. All rights reserved.
miniob is licensed under Mulan PSL v2.
You can use this software according to the terms and conditions of the Mulan PSL v2.
You may obtain a copy of Mulan PSL v2 at:
         http://license.coscl.org.cn/MulanPSL2
THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
See the Mulan PSL v2 for more details. */

// Covers the TxnStatusTable commit point: visibility is atomic even before
// tuple xids are backfilled, snapshots stay stable, and rollback hides rows.

#include <cstring>
#include <filesystem>
#include <memory>

#include "common/lang/memory.h"
#include "common/types.h"
#include "storage/db/db.h"
#include "storage/field/field.h"
#include "storage/record/record_scanner.h"
#include "storage/table/table.h"
#include "storage/trx/mvcc_trx.h"
#include "storage/trx/trx.h"
#include "gtest/gtest.h"

using namespace std;
using namespace common;

namespace {

int scan_count(Table *table, Trx *trx)
{
  RecordScanner *scanner = nullptr;
  EXPECT_EQ(RC::SUCCESS, table->get_record_scanner(scanner, trx, ReadWriteMode::READ_ONLY));
  int    count = 0;
  Record record;
  while (RC::SUCCESS == scanner->next(record)) {
    count++;
  }
  scanner->close_scan();
  delete scanner;
  return count;
}

void insert_rows(Table *table, Trx *trx, int start, int count)
{
  for (int i = start; i < start + count; i++) {
    vector<Value> values(1);
    values[0].set_int(i);
    Record record;
    ASSERT_EQ(RC::SUCCESS, table->make_record(values.size(), values.data(), record));
    ASSERT_EQ(RC::SUCCESS, trx->insert_record(table, record));
  }
}

}  // namespace

TEST(MvccCommitAtomic, status_table_publishes_all_rows_at_once)
{
  filesystem::path dir = "mvcc_commit_atomic_test";
  filesystem::remove_all(dir);
  filesystem::create_directories(dir);

  const char *dbname = "mvcc_atomic_db";
  auto        db     = make_unique<Db>();
  ASSERT_EQ(RC::SUCCESS, db->init(dbname, dir.c_str(), "mvcc", "vacuous"));

  vector<AttrInfoSqlNode> attrs(1);
  attrs[0].name   = "id";
  attrs[0].type   = AttrType::INTS;
  attrs[0].length = sizeof(int);
  ASSERT_EQ(RC::SUCCESS, db->create_table("t_atomic", attrs, {}));
  Table *table = db->find_table("t_atomic");
  ASSERT_NE(nullptr, table);

  MvccTrxKit &kit = static_cast<MvccTrxKit &>(db->trx_kit());

  Trx *t1 = kit.create_trx(db->log_handler());
  ASSERT_EQ(RC::SUCCESS, t1->start_if_need());
  insert_rows(table, t1, 0, 3);

  Trx *t2 = kit.create_trx(db->log_handler());
  ASSERT_EQ(RC::SUCCESS, t2->start_if_need());
  ASSERT_EQ(0, scan_count(table, t2));

  ASSERT_EQ(RC::SUCCESS, t1->commit());
  const int64_t t1_id = t1->id();

  // The old snapshot does not see the committed transaction.
  ASSERT_EQ(0, scan_count(table, t2));
  ASSERT_EQ(RC::SUCCESS, t2->commit());

  Trx *t3 = kit.create_trx(db->log_handler());
  ASSERT_EQ(RC::SUCCESS, t3->start_if_need());
  ASSERT_EQ(3, scan_count(table, t3));

  // Simulate a crash after the commit log but before tuple backfill: put two
  // rows back to -T1 and verify the status table still makes them visible.
  span<const FieldMeta> trx_fields = table->table_meta().trx_fields();
  ASSERT_EQ(2, static_cast<int>(trx_fields.size()));
  Field begin_field(table, &trx_fields[0]);

  int restored = 0;
  vector<RID> rids;
  {
    RecordScanner *scanner = nullptr;
    ASSERT_EQ(RC::SUCCESS, table->get_record_scanner(scanner, t3, ReadWriteMode::READ_ONLY));
    Record record;
    while (restored < 2 && RC::SUCCESS == scanner->next(record)) {
      rids.push_back(record.rid());
      restored++;
    }
    scanner->close_scan();
    delete scanner;
  }
  ASSERT_EQ(2, restored);
  for (const RID &rid : rids) {
    ASSERT_EQ(RC::SUCCESS, table->visit_record(rid, [&begin_field, t1_id](Record &inplace) -> bool {
      begin_field.set_int64(inplace, -t1_id);
      return true;
    }));
  }

  Trx *t4 = kit.create_trx(db->log_handler());
  ASSERT_EQ(RC::SUCCESS, t4->start_if_need());
  ASSERT_EQ(3, scan_count(table, t4));

  // Rollback: inserted rows must never become visible.
  Trx *t5 = kit.create_trx(db->log_handler());
  ASSERT_EQ(RC::SUCCESS, t5->start_if_need());
  insert_rows(table, t5, 100, 2);
  ASSERT_EQ(RC::SUCCESS, t5->rollback());

  Trx *t6 = kit.create_trx(db->log_handler());
  ASSERT_EQ(RC::SUCCESS, t6->start_if_need());
  ASSERT_EQ(3, scan_count(table, t6));

  t3->commit();
  t4->commit();
  t6->commit();
  kit.destroy_trx(t1);
  kit.destroy_trx(t2);
  kit.destroy_trx(t3);
  kit.destroy_trx(t4);
  kit.destroy_trx(t5);
  kit.destroy_trx(t6);

  db.reset();
  filesystem::remove_all(dir);
}
