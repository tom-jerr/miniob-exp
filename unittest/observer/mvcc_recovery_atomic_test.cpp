/* Copyright (c) 2021 OceanBase and/or its affiliates. All rights reserved.
miniob is licensed under Mulan PSL v2.
You can use this software according to the terms and conditions of the Mulan PSL v2.
You may obtain a copy of Mulan PSL v2 at:
         http://license.coscl.org.cn/MulanPSL2
THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
See the Mulan PSL v2 for more details. */

// Crash recovery for MVCC: a durable COMMIT log publishes the transaction
// even when data pages and tuple backfill were not flushed; a transaction
// without a COMMIT log is rolled back on reopen.

#include <cstring>
#include <filesystem>
#include <memory>

#include "common/lang/memory.h"
#include "common/types.h"
#include "storage/clog/log_entry.h"
#include "storage/clog/log_handler.h"
#include "storage/db/db.h"
#include "storage/record/record_scanner.h"
#include "storage/table/table.h"
#include "storage/trx/mvcc_trx.h"
#include "storage/trx/mvcc_trx_log.h"
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

int scan_count(Table *table, Trx *trx, vector<Record> *records = nullptr)
{
  RecordScanner *scanner = nullptr;
  EXPECT_EQ(RC::SUCCESS, table->get_record_scanner(scanner, trx, ReadWriteMode::READ_ONLY));
  Record record;
  int    count = 0;
  while (RC::SUCCESS == scanner->next(record)) {
    count++;
    if (records != nullptr) {
      Record owned;
      EXPECT_EQ(RC::SUCCESS, owned.copy_data(record.data(), record.len()));
      owned.set_rid(record.rid());
      records->push_back(std::move(owned));
    }
  }
  scanner->close_scan();
  delete scanner;
  return count;
}

Record scan_first(Table *table, Trx *trx, int id)
{
  RecordScanner *scanner = nullptr;
  EXPECT_EQ(RC::SUCCESS, table->get_record_scanner(scanner, trx, ReadWriteMode::READ_ONLY));
  Record record;
  while (RC::SUCCESS == scanner->next(record)) {
    if (field_int(table, record, "id") == id) {
      Record copy;
      EXPECT_EQ(RC::SUCCESS, copy.copy_data(record.data(), record.len()));
      copy.set_rid(record.rid());
      copy.set_key(record.key());
      scanner->close_scan();
      delete scanner;
      return copy;
    }
  }
  scanner->close_scan();
  delete scanner;
  return Record();
}

RC append_commit_log(Db *db, Trx *trx, int64_t commit_seq)
{
  MvccTrxCommitLogEntry entry{};
  entry.header.operation_type = MvccTrxLogOperation(MvccTrxLogOperation::Type::COMMIT).index();
  entry.header.trx_id         = trx->id();
  entry.commit_trx_id         = commit_seq;

  LSN lsn = 0;
  RC  rc  = db->log_handler().append(
      lsn, LogModule::Id::TRANSACTION, span<const char>(reinterpret_cast<const char *>(&entry), sizeof(entry)));
  if (OB_FAIL(rc)) {
    return rc;
  }
  return db->log_handler().wait_lsn(lsn);
}

void copy_db(const filesystem::path &from, const filesystem::path &to)
{
  filesystem::remove_all(to);
  filesystem::create_directories(to);
  filesystem::copy(from, to, filesystem::copy_options::recursive | filesystem::copy_options::overwrite_existing);
}

}  // namespace

TEST(MvccRecoveryAtomic, commit_log_without_flush_publishes_all_rows)
{
  filesystem::path root = "mvcc_recovery_atomic_test";
  filesystem::remove_all(root);
  filesystem::create_directories(root);
  filesystem::path db_path  = root / "db1";
  filesystem::path db_path2 = root / "db2";
  filesystem::create_directories(db_path);
  filesystem::create_directories(db_path2);

  auto db = make_unique<Db>();
  ASSERT_EQ(RC::SUCCESS, db->init("rec_db", db_path.c_str(), "mvcc", "disk"));

  vector<AttrInfoSqlNode> attrs(2);
  attrs[0].name   = "id";
  attrs[0].type   = AttrType::INTS;
  attrs[0].length = sizeof(int);
  attrs[1].name   = "v";
  attrs[1].type   = AttrType::INTS;
  attrs[1].length = sizeof(int);
  ASSERT_EQ(RC::SUCCESS, db->create_table("t_rec", attrs, {}));
  Table *table = db->find_table("t_rec");
  ASSERT_NE(nullptr, table);

  MvccTrxKit &kit = static_cast<MvccTrxKit &>(db->trx_kit());
  Trx        *trx = kit.create_trx(db->log_handler());
  ASSERT_EQ(RC::SUCCESS, trx->start_if_need());
  for (int i = 1; i <= 3; i++) {
    vector<Value> values(2);
    values[0].set_int(i);
    values[1].set_int(i * 10);
    Record record;
    ASSERT_EQ(RC::SUCCESS, table->make_record(values.size(), values.data(), record));
    ASSERT_EQ(RC::SUCCESS, trx->insert_record(table, record));
  }
  ASSERT_EQ(RC::SUCCESS, append_commit_log(db.get(), trx, trx->id() + 1000));
  ASSERT_EQ(RC::SUCCESS, db->log_handler().wait_lsn(db->log_handler().current_lsn()));

  // Simulate a crash before data-page/tuple backfill: only the logs are copied.
  copy_db(db_path, db_path2);
  db.reset();

  auto db2 = make_unique<Db>();
  ASSERT_EQ(RC::SUCCESS, db2->init("rec_db", db_path2.c_str(), "mvcc", "disk"));
  Table *table2 = db2->find_table("t_rec");
  ASSERT_NE(nullptr, table2);
  MvccTrxKit &kit2 = static_cast<MvccTrxKit &>(db2->trx_kit());
  Trx        *trx2 = kit2.create_trx(db2->log_handler());
  ASSERT_EQ(RC::SUCCESS, trx2->start_if_need());
  ASSERT_EQ(3, scan_count(table2, trx2));
  ASSERT_EQ(RC::SUCCESS, trx2->rollback());
  kit2.destroy_trx(trx2);

  // A clean sync checkpoints past the commit log. Reopen must still see the
  // committed rows (commit_seq is restored, not reset to zero).
  ASSERT_EQ(RC::SUCCESS, db2->sync());
  db2.reset();

  auto db3 = make_unique<Db>();
  ASSERT_EQ(RC::SUCCESS, db3->init("rec_db", db_path2.c_str(), "mvcc", "disk"));
  Table *table3 = db3->find_table("t_rec");
  ASSERT_NE(nullptr, table3);
  MvccTrxKit &kit3 = static_cast<MvccTrxKit &>(db3->trx_kit());
  Trx        *trx3 = kit3.create_trx(db3->log_handler());
  ASSERT_EQ(RC::SUCCESS, trx3->start_if_need());
  ASSERT_EQ(3, scan_count(table3, trx3));
  ASSERT_EQ(RC::SUCCESS, trx3->rollback());
  kit3.destroy_trx(trx3);
  db3.reset();
  filesystem::remove_all(root);
}

TEST(MvccRecoveryAtomic, no_commit_log_rolls_back)
{
  filesystem::path root = "mvcc_recovery_atomic_test";
  filesystem::remove_all(root);
  filesystem::create_directories(root);
  filesystem::path db_path  = root / "db1";
  filesystem::path db_path2 = root / "db2";
  filesystem::create_directories(db_path);
  filesystem::create_directories(db_path2);

  auto db = make_unique<Db>();
  ASSERT_EQ(RC::SUCCESS, db->init("rec_db", db_path.c_str(), "mvcc", "disk"));

  vector<AttrInfoSqlNode> attrs(1);
  attrs[0].name   = "id";
  attrs[0].type   = AttrType::INTS;
  attrs[0].length = sizeof(int);
  ASSERT_EQ(RC::SUCCESS, db->create_table("t_rec", attrs, {}));
  Table *table = db->find_table("t_rec");
  ASSERT_NE(nullptr, table);

  MvccTrxKit &kit = static_cast<MvccTrxKit &>(db->trx_kit());
  Trx        *trx = kit.create_trx(db->log_handler());
  ASSERT_EQ(RC::SUCCESS, trx->start_if_need());
  for (int i = 1; i <= 3; i++) {
    vector<Value> values(1);
    values[0].set_int(i);
    Record record;
    ASSERT_EQ(RC::SUCCESS, table->make_record(values.size(), values.data(), record));
    ASSERT_EQ(RC::SUCCESS, trx->insert_record(table, record));
  }
  ASSERT_EQ(RC::SUCCESS, db->log_handler().wait_lsn(db->log_handler().current_lsn()));

  copy_db(db_path, db_path2);
  db.reset();

  auto db2 = make_unique<Db>();
  ASSERT_EQ(RC::SUCCESS, db2->init("rec_db", db_path2.c_str(), "mvcc", "disk"));
  Table *table2 = db2->find_table("t_rec");
  ASSERT_NE(nullptr, table2);
  MvccTrxKit &kit2 = static_cast<MvccTrxKit &>(db2->trx_kit());
  Trx        *trx2 = kit2.create_trx(db2->log_handler());
  ASSERT_EQ(RC::SUCCESS, trx2->start_if_need());
  ASSERT_EQ(0, scan_count(table2, trx2));
  ASSERT_EQ(RC::SUCCESS, trx2->rollback());
  db2.reset();
  filesystem::remove_all(root);
}

TEST(MvccRecoveryAtomic, update_commit_log_publishes_new_version_only)
{
  filesystem::path root = "mvcc_recovery_atomic_test";
  filesystem::remove_all(root);
  filesystem::create_directories(root);
  filesystem::path db_path  = root / "db1";
  filesystem::path db_path2 = root / "db2";
  filesystem::create_directories(db_path);
  filesystem::create_directories(db_path2);

  auto db = make_unique<Db>();
  ASSERT_EQ(RC::SUCCESS, db->init("rec_db", db_path.c_str(), "mvcc", "disk"));

  vector<AttrInfoSqlNode> attrs(2);
  attrs[0].name   = "id";
  attrs[0].type   = AttrType::INTS;
  attrs[0].length = sizeof(int);
  attrs[1].name   = "v";
  attrs[1].type   = AttrType::INTS;
  attrs[1].length = sizeof(int);
  ASSERT_EQ(RC::SUCCESS, db->create_table("t_rec", attrs, {}));
  Table *table = db->find_table("t_rec");
  ASSERT_NE(nullptr, table);

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

  Trx *writer = kit.create_trx(db->log_handler());
  ASSERT_EQ(RC::SUCCESS, writer->start_if_need());
  vector<Record> rows;
  ASSERT_EQ(2, scan_count(table, writer, &rows));
  const FieldMeta *v_field = table->table_meta().field("v");
  ASSERT_NE(nullptr, v_field);
  Record old_record = rows[1];
  ASSERT_EQ(2, field_int(table, old_record, "id"));
  Record new_record = old_record;
  Value  value;
  value.set_int(99);
  ASSERT_EQ(RC::SUCCESS, new_record.reset_filed(v_field->offset(), v_field->len()));
  ASSERT_EQ(RC::SUCCESS, table->set_value_to_record(new_record.data(), value, v_field));
  ASSERT_EQ(RC::SUCCESS, writer->update_record(table, old_record, new_record));
  ASSERT_EQ(RC::SUCCESS, append_commit_log(db.get(), writer, seed->id() + 1000));
  ASSERT_EQ(RC::SUCCESS, db->log_handler().wait_lsn(db->log_handler().current_lsn()));

  copy_db(db_path, db_path2);
  db.reset();

  auto db2 = make_unique<Db>();
  ASSERT_EQ(RC::SUCCESS, db2->init("rec_db", db_path2.c_str(), "mvcc", "disk"));
  Table *table2 = db2->find_table("t_rec");
  ASSERT_NE(nullptr, table2);
  MvccTrxKit &kit2 = static_cast<MvccTrxKit &>(db2->trx_kit());
  Trx        *trx2 = kit2.create_trx(db2->log_handler());
  ASSERT_EQ(RC::SUCCESS, trx2->start_if_need());
  vector<Record> recovered;
  ASSERT_EQ(2, scan_count(table2, trx2, &recovered));
  bool found_new = false;
  bool found_old = false;
  for (const Record &record : recovered) {
    if (field_int(table2, record, "id") == 2) {
      if (field_int(table2, record, "v") == 99) {
        found_new = true;
      }
      if (field_int(table2, record, "v") == 20) {
        found_old = true;
      }
    }
  }
  ASSERT_TRUE(found_new);
  ASSERT_FALSE(found_old);
  ASSERT_EQ(RC::SUCCESS, trx2->rollback());
  kit2.destroy_trx(trx2);

  ASSERT_EQ(RC::SUCCESS, db2->sync());
  db2.reset();

  auto db3 = make_unique<Db>();
  ASSERT_EQ(RC::SUCCESS, db3->init("rec_db", db_path2.c_str(), "mvcc", "disk"));
  Table *table3 = db3->find_table("t_rec");
  ASSERT_NE(nullptr, table3);
  MvccTrxKit &kit3 = static_cast<MvccTrxKit &>(db3->trx_kit());
  Trx        *trx3 = kit3.create_trx(db3->log_handler());
  ASSERT_EQ(RC::SUCCESS, trx3->start_if_need());
  ASSERT_EQ(2, scan_count(table3, trx3));
  ASSERT_EQ(99, field_int(table3, scan_first(table3, trx3, 2), "v"));
  ASSERT_EQ(RC::SUCCESS, trx3->rollback());
  kit3.destroy_trx(trx3);
  db3.reset();
  filesystem::remove_all(root);
}
