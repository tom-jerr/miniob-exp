/* Copyright (c) 2021 OceanBase and/or its affiliates. All rights reserved.
miniob is licensed under Mulan PSL v2.
You can use this software according to the terms and conditions of the Mulan PSL v2.
You may obtain a copy of Mulan PSL v2 at:
         http://license.coscl.org.cn/MulanPSL2
THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
See the Mulan PSL v2 for more details. */

#pragma once

#include "sql/operator/physical_operator.h"

class FieldMeta;

/**
 * @brief 物理算子，更新单列
 * @details 与 Delete 一样先收集目标行再逐行更新，避免扫描器在更新时失效。
 * @ingroup PhysicalOperator
 */
class UpdatePhysicalOperator : public PhysicalOperator
{
public:
  UpdatePhysicalOperator(Table *table, const FieldMeta *field, const Value &value)
      : table_(table), field_(field), value_(value)
  {}
  ~UpdatePhysicalOperator() override = default;

  PhysicalOperatorType type() const override { return PhysicalOperatorType::UPDATE; }
  OpType               get_op_type() const override { return OpType::UPDATE; }

  RC open(Trx *trx) override;
  RC next() override;
  RC close() override;
  Tuple *current_tuple() override { return nullptr; }

private:
  Table           *table_ = nullptr;
  const FieldMeta *field_ = nullptr;
  Value            value_;
  Trx             *trx_ = nullptr;
  vector<Record>   records_;
};
