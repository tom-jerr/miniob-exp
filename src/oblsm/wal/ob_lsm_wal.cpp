/* Copyright (c) 2021 OceanBase and/or its affiliates. All rights reserved.
   miniob is licensed under Mulan PSL v2.
   You can use this software according to the terms and conditions of the Mulan PSL v2.
   You may obtain a copy of Mulan PSL v2 at:
            http://license.coscl.org.cn/MulanPSL2
   THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
   EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
   MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
   See the Mulan PSL v2 for more details. */

#include "oblsm/wal/ob_lsm_wal.h"
#include "common/log/log.h"
#include "oblsm/util/ob_coding.h"
#include "oblsm/util/ob_file_reader.h"

#include <fstream>

namespace oceanbase {

RC WAL::open(const std::string &filename)
{
  filename_ = filename;
  writer_   = make_unique<ObFileWriter>(filename, true /*append*/);
  return writer_->open_file();
}

RC WAL::recover(const std::string &wal_file, std::vector<WalRecord> &wal_records)
{
  std::ifstream in(wal_file, std::ios::in | std::ios::binary);
  if (!in.is_open()) {
    // 还没有 WAL 文件，等价于空日志
    return RC::SUCCESS;
  }

  while (true) {
    uint64_t seq       = 0;
    size_t   key_size  = 0;
    size_t   value_size = 0;
    if (!in.read(reinterpret_cast<char *>(&seq), sizeof(seq))) {
      break;
    }
    if (!in.read(reinterpret_cast<char *>(&key_size), sizeof(key_size))) {
      break;
    }

    string key(key_size, '\0');
    if (key_size > 0 && !in.read(key.data(), static_cast<std::streamsize>(key_size))) {
      break;
    }
    if (!in.read(reinterpret_cast<char *>(&value_size), sizeof(value_size))) {
      break;
    }

    string value(value_size, '\0');
    if (value_size > 0 && !in.read(value.data(), static_cast<std::streamsize>(value_size))) {
      break;
    }
    wal_records.emplace_back(seq, std::move(key), std::move(value));
  }
  return RC::SUCCESS;
}

RC WAL::put(uint64_t seq, string_view key, string_view val)
{
  if (writer_ == nullptr) {
    LOG_WARN("wal is not open");
    return RC::IOERR_WRITE;
  }

  string record;
  put_numeric<uint64_t>(&record, seq);
  put_numeric<size_t>(&record, key.size());
  record.append(key.data(), key.size());
  put_numeric<size_t>(&record, val.size());
  record.append(val.data(), val.size());

  RC rc = writer_->write(record);
  if (rc != RC::SUCCESS) {
    return rc;
  }
  return writer_->flush();
}

RC WAL::sync()
{
  if (writer_ == nullptr) {
    return RC::SUCCESS;
  }
  return writer_->flush();
}

}  // namespace oceanbase
