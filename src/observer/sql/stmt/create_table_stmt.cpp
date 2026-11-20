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
// Created by Wangyunlai on 2023/6/13.
//

#include "common/log/log.h"
#include "common/types.h"
#include "sql/stmt/create_table_stmt.h"
#include "event/sql_debug.h"

RC CreateTableStmt::create(Db *db, const CreateTableSqlNode &create_table, Stmt *&stmt)
{
  StorageFormat storage_format = get_storage_format(create_table.storage_format.c_str());
  if (storage_format == StorageFormat::UNKNOWN_FORMAT) {
    return RC::INVALID_ARGUMENT;
  }

  StorageEngine storage_engine = StorageEngine::UNKNOWN_ENGINE;
  if (!create_table.storage_engine.empty()) {
    storage_engine = get_storage_engine(create_table.storage_engine.c_str());
    if (storage_engine == StorageEngine::UNKNOWN_ENGINE) {
      LOG_WARN("invalid storage engine: %s", create_table.storage_engine.c_str());
      return RC::INVALID_ARGUMENT;
    }
  }

  stmt = new CreateTableStmt(create_table.relation_name, create_table.attr_infos, create_table.primary_keys, storage_format,
      storage_engine);
  sql_debug("create table statement: table name %s", create_table.relation_name.c_str());
  return RC::SUCCESS;
}

StorageFormat CreateTableStmt::get_storage_format(const char *format_str) {
  StorageFormat format = StorageFormat::UNKNOWN_FORMAT;
  if (strlen(format_str) == 0) {
    format = StorageFormat::ROW_FORMAT;
  } else if (0 == strcasecmp(format_str, "ROW")) {
    format = StorageFormat::ROW_FORMAT;
  } else if (0 == strcasecmp(format_str, "PAX")) {
    format = StorageFormat::PAX_FORMAT;
  } else {
    format = StorageFormat::UNKNOWN_FORMAT;
  }
  return format;
}

StorageEngine CreateTableStmt::get_storage_engine(const char *engine_str)
{
  if (strlen(engine_str) == 0) {
    return StorageEngine::UNKNOWN_ENGINE;
  }
  if (0 == strcasecmp(engine_str, "heap")) {
    return StorageEngine::HEAP;
  }
  if (0 == strcasecmp(engine_str, "lsm")) {
    return StorageEngine::LSM;
  }
  if (0 == strcasecmp(engine_str, "btree")) {
    return StorageEngine::BTREE;
  }
  return StorageEngine::UNKNOWN_ENGINE;
}
