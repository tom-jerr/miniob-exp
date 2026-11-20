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

#include "storage/index/index.h"
#include "storage/index/bplus_tree.h"

class TableMeta;

/**
 * @brief 聚簇表上的索引
 * @details 二级索引的 key 为 索引字段 + 主键编码，value 为主键编码，回表时按主键查聚簇树。
 * 单列主键还会以共享聚簇树的方式暴露一个“主键索引”，扫描时 value 就是整行数据。
 */
class BtreePkIndex : public Index
{
public:
  BtreePkIndex() = default;
  BtreePkIndex(BplusTreeHandler *shared_tree, const IndexMeta &index_meta, const FieldMeta &field_meta, int pk_size)
      : tree_(shared_tree), pk_size_(pk_size), shared_(true)
  {
    Index::init(index_meta, field_meta);
  }
  ~BtreePkIndex() override;

  RC create(Table *table, const char *file_name, const IndexMeta &index_meta, const FieldMeta &field_meta) override;
  RC open(Table *table, const char *file_name, const IndexMeta &index_meta, const FieldMeta &field_meta) override;

  RC insert_entry(const char *record, const char *value, int value_len) override;
  RC delete_entry(const char *record, const char *value, int value_len) override;
  RC get_record(Table *table, const char *value, int value_len, Record &record) override;

  IndexScanner *create_scanner(const char *left_key, int left_len, bool left_inclusive, const char *right_key,
      int right_len, bool right_inclusive) override;

  RC sync() override;

private:
  static int calc_pk_size(const TableMeta &table_meta);

private:
  BplusTreeHandler  index_handler_;
  BplusTreeHandler *tree_ = nullptr;
  int               pk_size_ = 0;
  bool              shared_  = false;
  bool              inited_  = false;
};
