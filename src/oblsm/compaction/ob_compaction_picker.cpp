/* Copyright (c) 2021 OceanBase and/or its affiliates. All rights reserved.
miniob is licensed under Mulan PSL v2.
You can use this software according to the terms and conditions of the Mulan PSL v2.
You may obtain a copy of Mulan PSL v2 at:
         http://license.coscl.org.cn/MulanPSL2
THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
See the Mulan PSL v2 for more details. */

#include "oblsm/compaction/ob_compaction_picker.h"
#include "oblsm/util/ob_coding.h"
#include "common/log/log.h"

namespace oceanbase {

// TODO: put it in options
unique_ptr<ObCompaction> TiredCompactionPicker::pick(SSTablesPtr sstables)
{
  if (sstables->size() < options_->default_run_num) {
    return nullptr;
  }
  unique_ptr<ObCompaction> compaction(new ObCompaction(0));
  // TODO(opt): a tricky compaction picker, just pick all sstables if enough sstables.
  for (size_t i = 0; i < sstables->size(); ++i) {
    size_t tire_i_size = (*sstables)[i].size();
    for (size_t j = 0; j < tire_i_size; ++j) {
      compaction->inputs_[0].emplace_back((*sstables)[i][j]);
    }
  }
  // TODO: LOG_DEBUG for debug
  return compaction;
}

ObCompactionPicker *ObCompactionPicker::create(CompactionType type, ObLsmOptions *options)
{
  switch (type) {
    case CompactionType::TIRED: return new TiredCompactionPicker(options);
    case CompactionType::LEVELED: return new LeveledCompactionPicker(options);
    default: return nullptr;
  }
  return nullptr;
}

unique_ptr<ObCompaction> LeveledCompactionPicker::pick(SSTablesPtr sstables)
{
  if (sstables == nullptr || sstables->empty()) {
    return nullptr;
  }

  int    best_level = -1;
  double max_score  = 1.0;

  // Level 0 score: number of files / default_l0_file_num
  if (!(*sstables)[0].empty() && options_->default_l0_file_num > 0) {
    double l0_score = static_cast<double>((*sstables)[0].size()) / static_cast<double>(options_->default_l0_file_num);
    if (l0_score > max_score) {
      max_score  = l0_score;
      best_level = 0;
    }
  }

  // Level 1 .. default_levels - 2 score: total bytes / target_level_size
  size_t target_size = options_->default_l1_level_size;
  for (size_t level = 1; level + 1 < sstables->size(); ++level) {
    size_t level_bytes = 0;
    for (const auto &sst : (*sstables)[level]) {
      level_bytes += sst->size();
    }
    if (target_size > 0) {
      double score = static_cast<double>(level_bytes) / static_cast<double>(target_size);
      if (score > max_score) {
        max_score  = score;
        best_level = static_cast<int>(level);
      }
    }
    target_size *= options_->default_level_ratio;
  }

  if (best_level < 0) {
    return nullptr;
  }

  unique_ptr<ObCompaction> compaction = make_unique<ObCompaction>(best_level);
  ObDefaultComparator      user_comp;

  if (best_level == 0) {
    // Pick all files in L0
    for (const auto &sst : (*sstables)[0]) {
      compaction->inputs_[0].push_back(sst);
    }
  } else {
    // Pick the last file in L_i
    compaction->inputs_[0].push_back((*sstables)[best_level].back());
  }

  if (compaction->inputs_[0].empty()) {
    return nullptr;
  }

  // Compute combined user key range [min_user_key, max_user_key] of inputs_[0]
  string fk0          = compaction->inputs_[0][0]->first_key();
  string lk0          = compaction->inputs_[0][0]->last_key();
  string min_user_key = string(extract_user_key(fk0));
  string max_user_key = string(extract_user_key(lk0));
  for (size_t i = 1; i < compaction->inputs_[0].size(); ++i) {
    string      fki = compaction->inputs_[0][i]->first_key();
    string      lki = compaction->inputs_[0][i]->last_key();
    string_view fk  = extract_user_key(fki);
    string_view lk  = extract_user_key(lki);
    if (user_comp.compare(fk, min_user_key) < 0) {
      min_user_key.assign(fk.data(), fk.size());
    }
    if (user_comp.compare(lk, max_user_key) > 0) {
      max_user_key.assign(lk.data(), lk.size());
    }
  }

  // Pick all overlapping SSTables in best_level + 1
  const auto &next_level_ssts = (*sstables)[best_level + 1];
  for (const auto &sst : next_level_ssts) {
    string      sst_fk    = sst->first_key();
    string      sst_lk    = sst->last_key();
    string_view sst_first = extract_user_key(sst_fk);
    string_view sst_last  = extract_user_key(sst_lk);
    // Overlap condition: !(sst_last < min_user_key || sst_first > max_user_key)
    if (!(user_comp.compare(sst_last, min_user_key) < 0 || user_comp.compare(sst_first, max_user_key) > 0)) {
      compaction->inputs_[1].push_back(sst);
    }
  }

  return compaction;
}

}  // namespace oceanbase