/* Copyright (c) 2021 OceanBase and/or its affiliates. All rights reserved.
miniob is licensed under Mulan PSL v2.
You can use this software according to the terms and conditions of the Mulan PSL v2.
You may obtain a copy of Mulan PSL v2 at:
         http://license.coscl.org.cn/MulanPSL2
THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
See the Mulan PSL v2 for more details. */

// Verifies the LSM WAL path: put/get/scan, tombstone removal, and replay on
// reopen.

#include <filesystem>
#include <string>

#include "oblsm/include/ob_lsm.h"
#include "oblsm/include/ob_lsm_iterator.h"
#include "oblsm/include/ob_lsm_options.h"
#include "gtest/gtest.h"

using namespace oceanbase;
using namespace std;

TEST(ObLsmRecovery, put_get_scan_remove_and_reopen)
{
  filesystem::path dir = "oblsm_recovery_test";
  filesystem::remove_all(dir);
  filesystem::create_directories(dir);

  ObLsmOptions options;
  ObLsm       *db = nullptr;
  ASSERT_EQ(RC::SUCCESS, ObLsm::open(options, dir.string(), &db));
  ASSERT_NE(nullptr, db);

  ASSERT_EQ(RC::SUCCESS, db->put("k1", "v1"));
  ASSERT_EQ(RC::SUCCESS, db->put("k2", "v2"));
  ASSERT_EQ(RC::SUCCESS, db->put("k3", "v3"));

  {
    ObLsmIterator *iter = db->new_iterator(ObLsmReadOptions());
    ASSERT_NE(nullptr, iter);
    iter->seek_to_first();
    int count = 0;
    while (iter->valid()) {
      count++;
      iter->next();
    }
    delete iter;
    ASSERT_EQ(3, count);
  }

  string value;
  ASSERT_EQ(RC::SUCCESS, db->get("k1", &value));
  ASSERT_EQ("v1", value);

  ASSERT_EQ(RC::SUCCESS, db->remove("k2"));
  ASSERT_EQ(RC::NOT_EXIST, db->get("k2", &value));
  {
    ObLsmIterator *iter = db->new_iterator(ObLsmReadOptions());
    iter->seek_to_first();
    int count = 0;
    while (iter->valid()) {
      count++;
      iter->next();
    }
    delete iter;
    ASSERT_EQ(2, count);
  }

  delete db;
  db = nullptr;

  ASSERT_EQ(RC::SUCCESS, ObLsm::open(options, dir.string(), &db));
  ASSERT_EQ(RC::SUCCESS, db->get("k1", &value));
  ASSERT_EQ("v1", value);
  ASSERT_EQ(RC::NOT_EXIST, db->get("k2", &value));
  ASSERT_EQ(RC::SUCCESS, db->get("k3", &value));
  ASSERT_EQ("v3", value);
  delete db;

  filesystem::remove_all(dir);
}
