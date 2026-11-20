/* Copyright (c) 2021 OceanBase and/or its affiliates. All rights reserved.
miniob is licensed under Mulan PSL v2.
You can use this software according to the terms and conditions of the Mulan PSL v2.
You may obtain a copy of Mulan PSL v2 at:
         http://license.coscl.org.cn/MulanPSL2
THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
See the Mulan PSL v2 for more details. */

#include "storage/index/lsm_secondary_index.h"
#include "common/log/log.h"
#include "storage/common/codec.h"
#include "storage/db/db.h"
#include "storage/table/table.h"

RC LsmSecondaryIndex::get_record(Table *table, const char *value, int value_len, Record &record)
{
  if (value == nullptr || value_len != static_cast<int>(sizeof(uint64_t))) {
    LOG_WARN("lsm index value must be a rid. value length=%d", value_len);
    return RC::INVALID_ARGUMENT;
  }

  uint64_t rid = 0;
  memcpy(&rid, value, sizeof(rid));

  bytes lsm_key;
  RC    rc = Codec::encode(table->table_id(), rid, lsm_key);
  if (rc != RC::SUCCESS) {
    return rc;
  }

  string lsm_value;
  rc = table->db()->lsm()->get(string_view(reinterpret_cast<char *>(lsm_key.data()), lsm_key.size()), &lsm_value);
  if (rc != RC::SUCCESS) {
    return rc;
  }

  rc = record.copy_data(lsm_value.data(), static_cast<int>(lsm_value.size()));
  if (rc != RC::SUCCESS) {
    return rc;
  }
  record.set_rid(RID{0, 0});
  record.set_key(string(reinterpret_cast<char *>(lsm_key.data()), lsm_key.size()));
  return RC::SUCCESS;
}
