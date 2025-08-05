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
// Created by Wangyunlai on 2023/7/12.
//

#pragma once

#include "common/sys/rc.h"
#include <string>
#include <vector>

class SQLStageEvent;
class Table;
class SqlResult;

/**
 * @brief 导入数据的执行器
 * @ingroup Executor
 */
class LoadDataExecutor
{
public:
  LoadDataExecutor()          = default;
  virtual ~LoadDataExecutor() = default;

  RC execute(SQLStageEvent *sql_event);

private:
  void load_data(
      Table *table, const char *file_name, std::string terminated, std::string enclosed, SqlResult *sql_result);
};

/**
 * @brief 解析CSV行，针对ClickBench数据集优化
 * @param line CSV行字符串（会被修改）
 * @param file_values 解析后的字段值向量
 * @param terminated 字段分隔符
 * @param enclosed 字段引号字符
 */
void parse_csv_line(
    char *line, std::vector<std::string> &file_values, const std::string &terminated, const std::string &enclosed);
