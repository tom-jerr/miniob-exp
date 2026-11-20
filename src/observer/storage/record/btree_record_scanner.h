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

#include "storage/record/record_scanner.h"
#include "storage/index/bplus_tree.h"

class Table;

/**
 * @brief 遍历聚簇 B+树表中的所有记录
 * @details 叶子节点保存整行数据，key 是主键编码（或 8 字节 rowid）
 */
class BtreeRecordScanner : public RecordScanner
{
public:
  BtreeRecordScanner(Table *table, BplusTreeHandler &tree_handler)
      : table_(table), tree_handler_(tree_handler), tree_scanner_(tree_handler)
  {}
  ~BtreeRecordScanner() override { close_scan(); }

  RC open_scan() override;
  RC close_scan() override;
  RC next(Record &record) override;

private:
  Table            *table_ = nullptr;
  BplusTreeHandler &tree_handler_;
  BplusTreeScanner  tree_scanner_;
};
