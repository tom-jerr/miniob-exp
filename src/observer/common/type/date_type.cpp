/**
 * @file date_type.cpp
 * @brief 日期类型实现
 * @author lzy (lzy_CS_LN@163.com)
 * @version 1.0
 * @date 2025-08-05
 *
 * @copyright Copyright (c) 2025
 *
 */
#include "common/type/date_type.h"
#include "common/lang/comparator.h"
#include "common/log/log.h"
#include "common/type/attr_type.h"
#include "common/type/data_type.h"
#include "common/value.h"

int DateType::compare(const Value &left, const Value &right) const
{
  ASSERT(left.attr_type() == AttrType::DATES && right.attr_type() == AttrType::DATES, "invalid type");
  return common::compare_date(left.value_.date_value_, right.value_.date_value_);
}

int DateType::cast_cost(AttrType type)
{
  if (type == AttrType::DATES) {
    return 0;
  }
  return INT32_MAX;
}

RC DateType::cast_to(const Value &val, AttrType type, Value &result) const
{
  switch (type) {
    case AttrType::DATES: {
      result.set_date(val.get_date());
      return RC::SUCCESS;
    }
    default: return RC::UNIMPLEMENTED;
  }
  return RC::SUCCESS;
}

RC DateType::set_value_from_str(Value &val, const string &data) const
{
  Date date = Date::from_string(data);
  if (date.is_valid()) {
    val.set_date(date);
    return RC::SUCCESS;
  } else {
    LOG_WARN("invalid date string: %s", data.c_str());
    return RC::INVALID_ARGUMENT;
  }
}

RC DateType::to_string(const Value &val, string &result) const
{
  ASSERT(val.attr_type() == AttrType::DATES, "value is not a date");
  Date date = val.get_date();
  if (date.is_valid()) {
    result = date.to_string();
    return RC::SUCCESS;
  } else {
    LOG_WARN("invalid date value");
    return RC::INVALID_ARGUMENT;
  }
}