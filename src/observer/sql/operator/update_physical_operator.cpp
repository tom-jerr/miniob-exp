/* Copyright (c) 2021 OceanBase and/or its affiliates. All rights reserved.
miniob is licensed under Mulan PSL v2.
You can use this software according to the terms and conditions of the Mulan PSL v2.
You may obtain a copy of Mulan PSL v2 at:
         http://license.coscl.org.cn/MulanPSL2
THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
See the Mulan PSL v2 for more details. */

#include "sql/operator/update_physical_operator.h"
#include "common/log/log.h"
#include "storage/table/table.h"
#include "storage/trx/trx.h"

using namespace common;

RC UpdatePhysicalOperator::open(Trx *trx)
{
  if (children_.empty()) {
    return RC::INVALID_ARGUMENT;
  }

  unique_ptr<PhysicalOperator> &child = children_[0];
  RC rc = child->open(trx);
  if (OB_FAIL(rc)) {
    LOG_WARN("failed to open child operator: %s", strrc(rc));
    return rc;
  }

  trx_ = trx;

  while (OB_SUCC(rc = child->next())) {
    Tuple *tuple = child->current_tuple();
    if (nullptr == tuple) {
      LOG_WARN("failed to get current tuple");
      return RC::INTERNAL;
    }

    RowTuple *row_tuple = static_cast<RowTuple *>(tuple);
    Record   &record    = row_tuple->record();
    Record owned;
    rc = owned.copy_data(record.data(), record.len());
    if (OB_FAIL(rc)) {
      return rc;
    }
    owned.set_rid(record.rid());
    owned.set_key(record.key());
    records_.emplace_back(std::move(owned));
  }
  child->close();
  if (rc != RC::RECORD_EOF) {
    LOG_WARN("failed to scan records. rc=%s", strrc(rc));
    return rc;
  }

  for (Record &old_record : records_) {
    Record new_record = old_record;
    // CHARS 等定长字段先清零，避免短值更新后保留旧尾部
    rc = new_record.reset_filed(field_->offset(), field_->len());
    if (OB_FAIL(rc)) {
      LOG_WARN("failed to reset field before update. field=%s, rc=%s", field_->name(), strrc(rc));
      return rc;
    }
    rc = table_->set_value_to_record(new_record.data(), value_, field_);
    if (OB_FAIL(rc)) {
      LOG_WARN("failed to set field value. field=%s, rc=%s", field_->name(), strrc(rc));
      return rc;
    }

    rc = trx_->update_record(table_, old_record, new_record);
    if (OB_FAIL(rc)) {
      LOG_WARN("failed to update record. rc=%s", strrc(rc));
      return rc;
    }
  }
  return RC::SUCCESS;
}

RC UpdatePhysicalOperator::next() { return RC::RECORD_EOF; }

RC UpdatePhysicalOperator::close() { return RC::SUCCESS; }
