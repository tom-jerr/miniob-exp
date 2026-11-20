/* Copyright (c) 2021 OceanBase and/or its affiliates. All rights reserved.
miniob is licensed under Mulan PSL v2.
You can use this software according to the terms and conditions of the Mulan PSL v2.
You may obtain a copy of Mulan PSL v2 at:
         http://license.coscl.org.cn/MulanPSL2
THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
See the Mulan PSL v2 for more details. */

// Covers the 8-byte MVCC tuple xid fields and the persisted-format guard for
// old 4-byte-xid tables.

#include <cstdint>
#include <filesystem>
#include <memory>

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

TEST(MvccXid, eight_byte_fields_and_guard)
{
  filesystem::path dir = "mvcc_xid_test";
  filesystem::remove_all(dir);
  filesystem::create_directories(dir);

  const char *dbname = "mvcc_xid_db";
  auto        db     = make_unique<Db>();
  ASSERT_EQ(RC::SUCCESS, db->init(dbname, dir.c_str(), "mvcc", "disk"));

  const vector<FieldMeta> *trx_fields = db->trx_kit().trx_fields();
  ASSERT_NE(nullptr, trx_fields);
  ASSERT_EQ(2, static_cast<int>(trx_fields->size()));
  ASSERT_EQ(static_cast<int>(sizeof(int64_t)), (*trx_fields)[0].len());
  ASSERT_EQ(static_cast<int>(sizeof(int64_t)), (*trx_fields)[1].len());
  ASSERT_EQ(INT64_MAX, db->trx_kit().max_trx_id());

  vector<AttrInfoSqlNode> attrs(1);
  attrs[0].name   = "id";
  attrs[0].type   = AttrType::INTS;
  attrs[0].length = sizeof(int);
  ASSERT_EQ(RC::SUCCESS, db->create_table("t_xid", attrs, {}));
  Table *table = db->find_table("t_xid");
  ASSERT_NE(nullptr, table);
  ASSERT_EQ(RC::SUCCESS, db->trx_kit().validate_trx_fields(table->table_meta()));

  Trx *trx = db->trx_kit().create_trx(db->log_handler());
  ASSERT_NE(nullptr, trx);
  ASSERT_EQ(RC::SUCCESS, trx->start_if_need());

  vector<Value> values(1);
  values[0].set_int(7);
  Record record;
  ASSERT_EQ(RC::SUCCESS, table->make_record(values.size(), values.data(), record));
  ASSERT_EQ(RC::SUCCESS, trx->insert_record(table, record));
  ASSERT_EQ(RC::SUCCESS, trx->commit());
  db->trx_kit().destroy_trx(trx);

  ASSERT_EQ(RC::SUCCESS, db->sync());
  db.reset();

  db = make_unique<Db>();
  ASSERT_EQ(RC::SUCCESS, db->init(dbname, dir.c_str(), "mvcc", "disk"));
  table = db->find_table("t_xid");
  ASSERT_NE(nullptr, table);
  ASSERT_EQ(RC::SUCCESS, db->trx_kit().validate_trx_fields(table->table_meta()));

  trx = db->trx_kit().create_trx(db->log_handler());
  ASSERT_EQ(RC::SUCCESS, trx->start_if_need());
  RecordScanner *scanner = nullptr;
  ASSERT_EQ(RC::SUCCESS, table->get_record_scanner(scanner, trx, ReadWriteMode::READ_ONLY));
  Record scanned;
  ASSERT_EQ(RC::SUCCESS, scanner->next(scanned));
  ASSERT_EQ(RC::RECORD_EOF, scanner->next(scanned));
  scanner->close_scan();
  delete scanner;
  ASSERT_EQ(RC::SUCCESS, trx->commit());
  db->trx_kit().destroy_trx(trx);

  // A table created by the old 4-byte layout must be rejected explicitly.
  vector<FieldMeta> old_fields;
  old_fields.emplace_back("__trx_xid_begin", AttrType::INTS, 0, 4, false, -1);
  old_fields.emplace_back("__trx_xid_end", AttrType::INTS, 0, 4, false, -2);
  TableMeta old_meta;
  ASSERT_EQ(RC::SUCCESS, old_meta.init(1, "old_xid", &old_fields, attrs, {}, StorageFormat::ROW_FORMAT,
                                StorageEngine::HEAP));
  ASSERT_EQ(RC::INVALID_ARGUMENT, db->trx_kit().validate_trx_fields(old_meta));

  db.reset();
  filesystem::remove_all(dir);
}
