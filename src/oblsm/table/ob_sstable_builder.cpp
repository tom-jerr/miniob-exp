/* Copyright (c) 2021 OceanBase and/or its affiliates. All rights reserved.
miniob is licensed under Mulan PSL v2.
You can use this software according to the terms and conditions of the Mulan PSL v2.
You may obtain a copy of Mulan PSL v2 at:
         http://license.coscl.org.cn/MulanPSL2
THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
See the Mulan PSL v2 for more details. */

#include "oblsm/table/ob_sstable_builder.h"
#include "oblsm/util/ob_coding.h"

namespace oceanbase {

// TODO: refactor build with mem_table/iterator logic.
RC ObSSTableBuilder::open(const std::string &file_name, uint32_t sst_id)
{
  reset();
  sst_id_      = sst_id;
  file_writer_ = ObFileWriter::create_file_writer(file_name, false);
  if (file_writer_ == nullptr) {
    return RC::IOERR_OPEN;
  }
  return RC::SUCCESS;
}

RC ObSSTableBuilder::add(const string_view &key, const string_view &value)
{
  if (curr_blk_first_key_.empty()) {
    curr_blk_first_key_.assign(key.data(), key.size());
  }
  RC rc = block_builder_.add(key, value);
  if (rc == RC::FULL) {
    finish_build_block();
    curr_blk_first_key_.assign(key.data(), key.size());
    rc = block_builder_.add(key, value);
  }
  if (rc == RC::SUCCESS) {
    entry_count_++;
  }
  return rc;
}

RC ObSSTableBuilder::finish()
{
  if (!curr_blk_first_key_.empty()) {
    finish_build_block();
  }

  uint32_t meta_offset = curr_offset_;
  string   meta_buf;
  uint32_t meta_cnt = static_cast<uint32_t>(block_metas_.size());
  put_numeric<uint32_t>(&meta_buf, meta_cnt);
  for (const auto &meta : block_metas_) {
    string encoded_meta = meta.encode();
    put_numeric<uint32_t>(&meta_buf, static_cast<uint32_t>(encoded_meta.size()));
    meta_buf.append(encoded_meta);
  }
  put_numeric<uint32_t>(&meta_buf, meta_offset);

  RC rc = file_writer_->write(meta_buf);
  if (OB_FAIL(rc)) {
    return rc;
  }
  rc = file_writer_->flush();
  if (OB_FAIL(rc)) {
    return rc;
  }
  file_size_ = curr_offset_ + meta_buf.size();
  file_writer_->close_file();
  return RC::SUCCESS;
}

RC ObSSTableBuilder::build(shared_ptr<ObMemTable> mem_table, const std::string &file_name, uint32_t sst_id)
{
  RC rc = open(file_name, sst_id);
  if (OB_FAIL(rc)) {
    return rc;
  }

  unique_ptr<ObLsmIterator> iter(mem_table->new_iterator());
  iter->seek_to_first();
  while (iter->valid()) {
    rc = add(iter->key(), iter->value());
    if (OB_FAIL(rc)) {
      return rc;
    }
    iter->next();
  }

  return finish();
}

void ObSSTableBuilder::finish_build_block()
{
  string      last_key       = block_builder_.last_key();
  string_view block_contents = block_builder_.finish();
  file_writer_->write(block_contents);
  block_metas_.push_back(BlockMeta(curr_blk_first_key_, last_key, curr_offset_, block_contents.size()));
  // TODO: block aligned to BLOCK_SIZE
  curr_offset_ += block_contents.size();
  block_builder_.reset();
  curr_blk_first_key_.clear();
}

shared_ptr<ObSSTable> ObSSTableBuilder::get_built_table()
{
  // TODO: sstable should have more metadata
  shared_ptr<ObSSTable> sstable = make_shared<ObSSTable>(sst_id_, file_writer_->file_name(), comparator_, block_cache_);
  sstable->init();
  return sstable;
}

void ObSSTableBuilder::reset()
{
  block_builder_.reset();
  curr_blk_first_key_.clear();
  if (file_writer_ != nullptr) {
    file_writer_.reset(nullptr);
  }
  block_metas_.clear();
  curr_offset_ = 0;
  sst_id_      = 0;
  file_size_   = 0;
  entry_count_ = 0;
}
}  // namespace oceanbase
