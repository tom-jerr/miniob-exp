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

#include "sql/executor/load_data_executor.h"
#include "common/csv.h"
#include "common/lang/string.h"
#include "event/session_event.h"
#include "event/sql_event.h"
#include "sql/executor/sql_result.h"
#include "sql/stmt/load_data_stmt.h"
#include "storage/common/chunk.h"
#include <sstream>
#include <string>
#include <ctime>
#include <cstring>

using namespace common;

/**
 * 解析CSV行，针对ClickBench数据集优化，支持自定义分隔符和引号字符
 * ClickBench数据集通常使用Tab分隔符，这个函数针对此进行了优化
 * @param line CSV行字符串
 * @param file_values 解析后的字段值向量
 * @param terminated 字段分隔符（ClickBench通常使用Tab即'\t'）
 * @param enclosed 字段引号字符
 */
void parse_csv_line(char *line, vector<string> &file_values, char terminated, char enclosed)
{
  if (!line || *line == '\0') {
    return;
  }

  char *current      = line;
  char *field_start  = current;
  bool  in_encloseds = false;

  // 使用更高效的方式解析，减少string对象的创建
  while (*current != '\0') {
    if (*current == enclosed) {
      if (in_encloseds && *(current + 1) == enclosed) {
        // 转义的引号 ("" -> ")，需要移除一个引号
        memmove(current, current + 1, strlen(current));
      } else {
        // 切换引号状态
        in_encloseds = !in_encloseds;
        // 移除引号字符
        memmove(current, current + 1, strlen(current));
        continue;  // 不移动current，因为我们已经移除了字符
      }
    } else if (*current == terminated && !in_encloseds) {
      // 字段分隔符且不在引号内
      *current = '\0';  // 终止当前字段
      file_values.emplace_back(field_start);
      current++;
      field_start = current;
      continue;
    }
    current++;
  }

  // 添加最后一个字段
  if (field_start <= current) {
    file_values.emplace_back(field_start);
  }
}

RC LoadDataExecutor::execute(SQLStageEvent *sql_event)
{
  RC            rc         = RC::SUCCESS;
  SqlResult    *sql_result = sql_event->session_event()->sql_result();
  LoadDataStmt *stmt       = static_cast<LoadDataStmt *>(sql_event->stmt());
  Table        *table      = stmt->table();
  const char   *file_name  = stmt->filename();
  load_data(table, file_name, stmt->terminated(), stmt->enclosed(), sql_result);
  return rc;
}

/**
 * 从文件中导入数据时使用。尝试向表中插入解析后的一行数据。
 * @param table  要导入的表
 * @param file_values 从文件中读取到的一行数据，使用分隔符拆分后的几个字段值
 * @param record_values Table::insert_record使用的参数，为了防止频繁的申请内存
 * @param errmsg 如果出现错误，通过这个参数返回错误信息
 * @return 成功返回RC::SUCCESS
 */
RC insert_record_from_file(
    Table *table, vector<string> &file_values, vector<Value> &record_values, stringstream &errmsg)
{

  const int field_num     = record_values.size();
  const int sys_field_num = table->table_meta().sys_field_num();

  if (file_values.size() < record_values.size()) {
    return RC::SCHEMA_FIELD_MISSING;
  }

  RC rc = RC::SUCCESS;

  stringstream deserialize_stream;
  for (int i = 0; i < field_num && RC::SUCCESS == rc; i++) {
    const FieldMeta *field = table->table_meta().field(i + sys_field_num);

    string &file_value = file_values[i];
    if (field->type() != AttrType::CHARS) {
      common::strip(file_value);
    }
    rc = DataType::type_instance(field->type())->set_value_from_str(record_values[i], file_value);
    if (rc != RC::SUCCESS) {
      LOG_WARN("Failed to deserialize value from string: %s, type=%d", file_value.c_str(), field->type());
      return rc;
    }
  }

  if (RC::SUCCESS == rc) {
    Record record;
    rc = table->make_record(field_num, record_values.data(), record);
    if (rc != RC::SUCCESS) {
      errmsg << "insert failed.";
    } else if (RC::SUCCESS != (rc = table->insert_record(record))) {
      errmsg << "insert failed.";
    }
  }
  return rc;
}

// TODO: pax format and row format
void LoadDataExecutor::load_data(
    Table *table, const char *file_name, char terminated, char enclosed, SqlResult *sql_result)
{
  RC           rc = RC::SUCCESS;
  stringstream result_string;
  int          line_num        = 0;
  int          insertion_count = 0;
  stringstream errmsg;

  // 记录开始时间
  struct timespec begin_time;
  clock_gettime(CLOCK_MONOTONIC, &begin_time);

  const int field_num = table->table_meta().field_num() - table->table_meta().sys_field_num();

  try {
    // 使用Fast-CSV库读取文件，配置为适合ClickBench数据集
    io::LineReader line_reader(file_name);

    // 预分配向量空间以提高性能，特别适合ClickBench这样的大数据集
    vector<Value>  record_values(field_num);
    vector<string> file_values;
    file_values.reserve(field_num + 10);  // 预留一些额外空间防止频繁重分配

    // 逐行读取CSV文件，这种方式对大文件更友好
    char *line;
    while ((line = line_reader.next_line()) != nullptr) {
      line_num++;

      // 跳过空行和注释行（ClickBench可能包含）
      if (*line == '\0' || *line == '#') {
        continue;
      }

      // 解析CSV行，支持ClickBench常用的分隔符
      file_values.clear();
      parse_csv_line(line, file_values, terminated, enclosed);

      // 如果字段数量不匹配，跳过这一行
      if (file_values.size() < static_cast<size_t>(field_num)) {
        LOG_WARN("Line %d has insufficient fields (%zu expected %d), skipping", 
                 line_num, file_values.size(), field_num);
        continue;
      }

      // 插入记录
      RC insert_rc = insert_record_from_file(table, file_values, record_values, errmsg);
      if (insert_rc == RC::SUCCESS) {
        insertion_count++;

        // 每处理10000行打印一次进度（对大数据集有用）
        if (insertion_count % 10000 == 0) {
          LOG_INFO("Processed %d records from %s", insertion_count, file_name);
        }
      } else {
        // 如果插入失败，记录错误但继续处理
        LOG_WARN("Failed to insert record at line %d: %s", line_num, errmsg.str().c_str());
        errmsg.clear();
        errmsg.str("");

        // 对于某些错误类型，我们可以选择是否继续
        if (insert_rc == RC::SCHEMA_FIELD_MISSING || insert_rc == RC::SCHEMA_FIELD_TYPE_MISMATCH) {
          // 字段相关错误，跳过这一行继续处理
          continue;
        } else {
          // 其他严重错误，停止处理
          rc = insert_rc;
          break;
        }
      }
    }

  } catch (const io::error::base &e) {
    // Fast-CSV库异常处理
    result_string << "CSV parsing error: " << e.what();
    rc = RC::IOERR_READ;
  } catch (const std::exception &e) {
    // 标准异常处理
    result_string << "Error: " << e.what();
    rc = RC::INTERNAL;
  }

  // 记录结束时间并计算耗时
  struct timespec end_time;
  clock_gettime(CLOCK_MONOTONIC, &end_time);
  long cost_nano = (end_time.tv_sec - begin_time.tv_sec) * 1000000000L + (end_time.tv_nsec - begin_time.tv_nsec);

  if (RC::SUCCESS == rc) {
    result_string << strrc(rc) << ". total " << line_num << " line(s) handled and " << insertion_count
                  << " record(s) loaded, total cost " << cost_nano / 1000000000.0 << " second(s)";
  } else {
    result_string << strrc(rc) << ". total " << line_num << " line(s) handled and " << insertion_count
                  << " record(s) loaded before error, total cost " << cost_nano / 1000000000.0 << " second(s)";
  }

  sql_result->set_return_code(rc);
  sql_result->set_state_string(result_string.str());
}
