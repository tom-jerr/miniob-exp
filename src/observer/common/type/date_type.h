/**
 * @file date_type.h
 * @brief
 * @author lzy (lzy_CS_LN@163.com)
 * @version 1.0
 * @date 2025-08-05
 *
 * @copyright Copyright (c) 2025
 *
 */
#pragma once
#include "common/type/data_type.h"
/**
 * @brief 日期类型
 * @ingroup DataType
 */
class DateType : public DataType
{
public:
  DateType() : DataType(AttrType::DATES) {}
  virtual ~DateType() = default;

  int compare(const Value &left, const Value &right) const override;

  RC cast_to(const Value &val, AttrType type, Value &result) const override;

  RC set_value_from_str(Value &val, const string &data) const override;

  int cast_cost(AttrType type) override;

  RC to_string(const Value &val, string &result) const override;
};