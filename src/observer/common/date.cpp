/* Copyright (c) 2021 OceanBase and/or its affiliates. All rights reserved.
miniob is licensed under Mulan PSL v2.
You can use this software according to the terms and conditions of the Mulan PSL v2.
You may obtain a copy of Mulan PSL v2 at:
         http://license.coscl.org.cn/MulanPSL2
THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
See the Mulan PSL v2 for more details. */

//
// Created by lzy_CS_LN@163.com 2025/8/5
//

#include "common/date.h"
#include <cstdio>
#include <cstdlib>
#include <stdint.h>

Date::Date(int year, int month, int day)
{
  // 将年月日打包成23位格式
  // 前14位：年份，中间4位：月份，后5位：日
  value_    = ((year & 0x3FFF) << 9) | ((month & 0x0F) << 5) | (day & 0x1F);
  is_valid_ = is_valid(year, month, day);
}

bool Date::is_valid(int y, int m, int d)
{
  // 基本范围检查
  if (y < 0 || y > 9999)
    return false;
  if (m < 1 || m > 12)
    return false;
  if (d < 1 || d > 31)
    return false;

  // 检查具体月份的日期范围
  static const int days_in_month[] = {31, 28, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31};
  int              max_days        = days_in_month[m - 1];

  // 闰年2月处理
  if (m == 2) {
    bool is_leap = (y % 4 == 0 && y % 100 != 0) || (y % 400 == 0);
    if (is_leap)
      max_days = 29;
  }

  return d <= max_days;
}

bool Date::is_valid() const { return is_valid_; }

string Date::to_string() const
{
  char buffer[16];
  snprintf(buffer, sizeof(buffer), "%04d-%02d-%02d", year(), month(), day());
  return string(buffer);
}

Date Date::from_string(const string &date_str)
{
  int year, month, day;
  if (sscanf(date_str.c_str(), "%d-%d-%d", &year, &month, &day) == 3) {
    return Date(year, month, day);
  }
  return Date();  // 无效日期
}
