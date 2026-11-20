/* Copyright (c) 2021 OceanBase and/or its affiliates. All rights reserved.
miniob is licensed under Mulan PSL v2.
You can use this software according to the terms and conditions of the Mulan PSL v2.
You may obtain a copy of Mulan PSL v2 at:
         http://license.coscl.org.cn/MulanPSL2
THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
See the Mulan PSL v2 for more details. */

#include "storage/record/btree_record_scanner.h"
#include "common/log/log.h"
#include "storage/table/table.h"

RC BtreeRecordScanner::open_scan()
{
  RC rc = tree_scanner_.open(nullptr, 0, true, nullptr, 0, true);
  if (rc != RC::SUCCESS) {
    LOG_WARN("failed to open btree scanner. table=%s, rc=%s", table_->name(), strrc(rc));
  }
  return rc;
}

RC BtreeRecordScanner::close_scan()
{
  tree_scanner_.close();
  return RC::SUCCESS;
}

RC BtreeRecordScanner::next(Record &record)
{
  const char *value     = nullptr;
  const char *key       = nullptr;
  int         value_len = 0;
  RC          rc        = tree_scanner_.next_entry(value, value_len, key);
  if (rc != RC::SUCCESS) {
    return rc;
  }

  rc = record.copy_data(value, value_len);
  if (rc != RC::SUCCESS) {
    return rc;
  }
  record.set_rid(RID{0, 0});
  record.set_key(string(key, tree_handler_.file_header().key_length));
  return RC::SUCCESS;
}
