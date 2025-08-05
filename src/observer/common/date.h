/**
 * @file date.h
 * @brief
 * @author lzy (lzy_CS_LN@163.com)
 * @version 1.0
 * @date 2025-08-05
 *
 * @copyright Copyright (c) 2025
 *
 */
#pragma once
#include <stdint.h>
#include "common/lang/string.h"
/**
 * @brief Date类型，使用紧凑格式存储日期
 * @details 使用23位整数存储日期：
 * - 前14位：年份（0-16383，足够存储0000-9999年）
 * - 接下来4位：月份（1-12）
 * - 最后5位：日（1-31）
 */
class Date
{
public:
  Date() : value_(0), is_valid_(false) {}
  Date(int year, int month, int day);
  Date(int32_t packed_value) : value_(packed_value), is_valid_(is_valid(year(), month(), day())) {}

  int year() const { return (value_ >> 9) & 0x3FFF; }  // 提取前14位
  int month() const { return (value_ >> 5) & 0x0F; }   // 提取中间4位
  int day() const { return value_ & 0x1F; }            // 提取后5位

  int32_t value() const { return value_; }

  static bool is_valid(int year, int month, int day);
  bool        is_valid() const;
  string      to_string() const;
  static Date from_string(const string &date_str);

  bool operator==(const Date &other) const { return value_ == other.value_; }
  bool operator!=(const Date &other) const { return value_ != other.value_; }
  bool operator<(const Date &other) const { return value_ < other.value_; }
  bool operator<=(const Date &other) const { return value_ <= other.value_; }
  bool operator>(const Date &other) const { return value_ > other.value_; }
  bool operator>=(const Date &other) const { return value_ >= other.value_; }

private:
  int32_t value_;  // 23位紧凑存储格式
  bool    is_valid_;
};
