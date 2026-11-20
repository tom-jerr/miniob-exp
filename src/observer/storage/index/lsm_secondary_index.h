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

#include "storage/index/bplus_tree_index.h"

/**
 * @brief LSM 表的二级索引
 * @details 索引叶子保存自增 rid，回表时用 Codec::encode 重新构造 LSM key 取整行
 */
class LsmSecondaryIndex : public BplusTreeIndex
{
public:
  LsmSecondaryIndex()           = default;
  ~LsmSecondaryIndex() override = default;

  RC get_record(Table *table, const char *value, int value_len, Record &record) override;
};
