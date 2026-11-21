/* Copyright (c) 2021 OceanBase and/or its affiliates. All rights reserved.
miniob is licensed under Mulan PSL v2.
You can use this software according to the terms and conditions of the Mulan PSL v2.
You may obtain a copy of Mulan PSL v2 at:
         http://license.coscl.org.cn/MulanPSL2
THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
See the Mulan PSL v2 for more details. */

#include "gtest/gtest.h"

#include "common/lang/filesystem.h"
#include "common/lang/thread.h"
#include "common/lang/utility.h"
#include "common/lang/atomic.h"
#include "oblsm/include/ob_lsm.h"
#include "oblsm/ob_lsm_define.h"
#include "unittest/oblsm/ob_lsm_test_base.h"
#include "oblsm/include/ob_lsm_transaction.h"

using namespace oceanbase;

class ObLsmTransactionTest : public ObLsmTestBase
{};

bool check_lsm_scan_result_by_value(ObLsmIterator *iter, const std::vector<std::string> &values)
{
  size_t i = 0;
  iter->seek_to_first();
  while (iter->valid()) {
    if (i >= values.size() || values[i] != iter->value()) {
      return false;
    }
    ++i;
    iter->next();
  }
  return i == values.size();
}

TEST_F(ObLsmTransactionTest, oblsm_test_basic1)
{
  db->put("key1", "value1");
  db->put("key2", "value2");
  db->put("key3", "value3");

  auto iter = db->new_iterator(ObLsmReadOptions());
  ASSERT_TRUE(check_lsm_scan_result_by_value(iter, {"value1", "value2", "value3"}));
  delete iter;

  auto txn1      = db->begin_transaction();
  auto iter_txn1 = txn1->new_iterator(ObLsmReadOptions());
  ASSERT_TRUE(check_lsm_scan_result_by_value(iter_txn1, {"value1", "value2", "value3"}));
  delete iter_txn1;

  txn1->put("key1", "valuetxn1");
  txn1->put("txnkeyx", "txnvaluex");

  auto iter2_txn1 = txn1->new_iterator(ObLsmReadOptions());
  ASSERT_TRUE(check_lsm_scan_result_by_value(iter2_txn1, {"valuetxn1", "value2", "value3", "txnvaluex"}));
  delete iter2_txn1;

  auto txn2 = db->begin_transaction();
  txn2->put("key1", "valuetxn2");
  auto iter_txn2 = txn2->new_iterator(ObLsmReadOptions());
  ASSERT_TRUE(check_lsm_scan_result_by_value(iter_txn2, {"valuetxn2", "value2", "value3"}));
  delete iter_txn2;

  ASSERT_EQ(txn1->commit(), RC::SUCCESS);

  auto iter2_txn2 = txn2->new_iterator(ObLsmReadOptions());
  ASSERT_TRUE(check_lsm_scan_result_by_value(iter2_txn2, {"valuetxn2", "value2", "value3"}));
  delete iter2_txn2;

  auto txn3 = db->begin_transaction();

  auto iter_txn3 = txn3->new_iterator(ObLsmReadOptions());
  ASSERT_TRUE(check_lsm_scan_result_by_value(iter_txn3, {"valuetxn1", "value2", "value3", "txnvaluex"}));
  delete iter_txn3;

  delete txn1;
  delete txn2;
  delete txn3;
}

TEST_F(ObLsmTransactionTest, mvcc_delete_rollback_and_read_your_own_writes)
{
  ASSERT_EQ(db->put("k1", "v1"), RC::SUCCESS);
  ASSERT_EQ(db->put("k2", "v2"), RC::SUCCESS);
  ASSERT_EQ(db->put("k3", "v3"), RC::SUCCESS);

  auto *trx = db->begin_transaction();
  ASSERT_EQ(trx->remove("k2"), RC::SUCCESS);
  ASSERT_EQ(trx->put("k1", "v1_mod"), RC::SUCCESS);
  ASSERT_EQ(trx->put("k4", "v4_new"), RC::SUCCESS);

  string val;
  ASSERT_EQ(trx->get("k1", &val), RC::SUCCESS);
  EXPECT_EQ(val, "v1_mod");
  ASSERT_EQ(trx->get("k2", &val), RC::NOT_EXIST);
  ASSERT_EQ(trx->get("k3", &val), RC::SUCCESS);
  EXPECT_EQ(val, "v3");
  ASSERT_EQ(trx->get("k4", &val), RC::SUCCESS);
  EXPECT_EQ(val, "v4_new");

  auto *it = trx->new_iterator(ObLsmReadOptions());
  ASSERT_TRUE(check_lsm_scan_result_by_value(it, {"v1_mod", "v3", "v4_new"}));
  delete it;

  // Rollback and verify underlying DB is untouched
  ASSERT_EQ(trx->rollback(), RC::SUCCESS);
  delete trx;

  auto *db_it = db->new_iterator(ObLsmReadOptions());
  ASSERT_TRUE(check_lsm_scan_result_by_value(db_it, {"v1", "v2", "v3"}));
  delete db_it;
}

TEST_F(ObLsmTransactionTest, occ_snapshot_isolation_write_write_conflict_first_committer_wins)
{
  ASSERT_EQ(db->put("acct_1", "100"), RC::SUCCESS);
  ASSERT_EQ(db->put("acct_2", "200"), RC::SUCCESS);

  // Two concurrent transactions under default SNAPSHOT_ISOLATION modifying the same key
  auto *t1 = db->begin_transaction();
  auto *t2 = db->begin_transaction();
  EXPECT_EQ(t1->isolation_level(), TransactionIsolationLevel::SNAPSHOT_ISOLATION);
  EXPECT_EQ(t2->isolation_level(), TransactionIsolationLevel::SNAPSHOT_ISOLATION);

  ASSERT_EQ(t1->put("acct_1", "150"), RC::SUCCESS);
  ASSERT_EQ(t2->put("acct_1", "180"), RC::SUCCESS);

  // First committer wins
  ASSERT_EQ(t1->commit(), RC::SUCCESS);
  // Second committer detects write-write conflict via OCC validation
  ASSERT_EQ(t2->commit(), RC::LOCKED_CONCURRENCY_CONFLICT);

  delete t1;
  delete t2;

  string final_val;
  ASSERT_EQ(db->get("acct_1", &final_val), RC::SUCCESS);
  EXPECT_EQ(final_val, "150");

  // Disjoint write sets under SI both succeed even with overlapping read sets (Write Skew allowed in SI)
  auto  *t3 = db->begin_transaction();
  auto  *t4 = db->begin_transaction();
  string v1, v2;
  ASSERT_EQ(t3->get("acct_1", &v1), RC::SUCCESS);
  ASSERT_EQ(t3->put("acct_2", "250"), RC::SUCCESS);

  ASSERT_EQ(t4->get("acct_2", &v2), RC::SUCCESS);
  ASSERT_EQ(t4->put("acct_1", "160"), RC::SUCCESS);

  ASSERT_EQ(t3->commit(), RC::SUCCESS);
  ASSERT_EQ(t4->commit(), RC::SUCCESS);
  delete t3;
  delete t4;
}

TEST_F(ObLsmTransactionTest, occ_serializable_detects_write_skew_read_write_conflict)
{
  ASSERT_EQ(db->put("doctor_on_call_A", "true"), RC::SUCCESS);
  ASSERT_EQ(db->put("doctor_on_call_B", "true"), RC::SUCCESS);

  auto *t1 = db->begin_transaction();
  auto *t2 = db->begin_transaction();
  t1->set_isolation_level(TransactionIsolationLevel::SERIALIZABLE);
  t2->set_isolation_level(TransactionIsolationLevel::SERIALIZABLE);

  string status_a, status_b;
  // T1 reads A and B, updates A -> false
  ASSERT_EQ(t1->get("doctor_on_call_A", &status_a), RC::SUCCESS);
  ASSERT_EQ(t1->get("doctor_on_call_B", &status_b), RC::SUCCESS);
  ASSERT_EQ(t1->put("doctor_on_call_A", "false"), RC::SUCCESS);

  // T2 reads A and B, updates B -> false (classic Write Skew anomaly)
  ASSERT_EQ(t2->get("doctor_on_call_A", &status_a), RC::SUCCESS);
  ASSERT_EQ(t2->get("doctor_on_call_B", &status_b), RC::SUCCESS);
  ASSERT_EQ(t2->put("doctor_on_call_B", "false"), RC::SUCCESS);

  // T1 commits first and succeeds
  ASSERT_EQ(t1->commit(), RC::SUCCESS);
  // T2 must fail OCC validation under SERIALIZABLE because doctor_on_call_A in T2's read_set was modified by T1
  ASSERT_EQ(t2->commit(), RC::LOCKED_CONCURRENCY_CONFLICT);

  delete t1;
  delete t2;

  ASSERT_EQ(db->get("doctor_on_call_A", &status_a), RC::SUCCESS);
  ASSERT_EQ(db->get("doctor_on_call_B", &status_b), RC::SUCCESS);
  EXPECT_EQ(status_a, "false");
  EXPECT_EQ(status_b, "true");
}

TEST(ObLsmSpillTransactionTest, large_write_batch_spill_to_disk_commit_and_rollback)
{
  string spill_db_path = "./testdb_spill_trx";
  filesystem::remove_all(spill_db_path);
  filesystem::create_directory(spill_db_path);

  ObLsmOptions opts;
  opts.max_write_batch_size = 1024;  // 1KB threshold to force multiple spills to disk
  opts.memtable_type        = MemTableType::BPLUS_TREE;

  ObLsm *spill_db = nullptr;
  ASSERT_EQ(ObLsm::open(opts, spill_db_path, &spill_db), RC::SUCCESS);

  ASSERT_EQ(spill_db->put("base_k1", "base_v1"), RC::SUCCESS);
  ASSERT_EQ(spill_db->put("base_k2", "base_v2"), RC::SUCCESS);

  auto        *trx        = spill_db->begin_transaction();
  const size_t total_keys = 300;
  for (size_t i = 0; i < total_keys; ++i) {
    string k = "spill_key_" + to_string(i);
    string v = "spill_value_payload_" + to_string(i) + "_with_extra_bytes_to_trigger_disk_spill";
    ASSERT_EQ(trx->put(k, v), RC::SUCCESS);
  }
  // Verify multiple Spill SSTables were generated on disk
  EXPECT_GT(trx->spill_count(), 3u);

  // Overwrite an early spilled key and delete another early spilled key + base key
  ASSERT_EQ(trx->put("spill_key_5", "updated_after_spill"), RC::SUCCESS);
  ASSERT_EQ(trx->remove("spill_key_10"), RC::SUCCESS);
  ASSERT_EQ(trx->remove("base_k1"), RC::SUCCESS);

  // Point lookup inside transaction across active map, spill SSTables, and base snapshot
  string val;
  ASSERT_EQ(trx->get("spill_key_5", &val), RC::SUCCESS);
  EXPECT_EQ(val, "updated_after_spill");
  ASSERT_EQ(trx->get("spill_key_10", &val), RC::NOT_EXIST);
  ASSERT_EQ(trx->get("base_k1", &val), RC::NOT_EXIST);
  ASSERT_EQ(trx->get("base_k2", &val), RC::SUCCESS);
  EXPECT_EQ(val, "base_v2");
  ASSERT_EQ(trx->get("spill_key_250", &val), RC::SUCCESS);

  // Full scan inside transaction before commit
  unique_ptr<ObLsmIterator> trx_it(trx->new_iterator(ObLsmReadOptions()));
  trx_it->seek_to_first();
  size_t trx_visible_cnt = 0;
  while (trx_it->valid()) {
    trx_visible_cnt++;
    trx_it->next();
  }
  // base_k2 (1) + 300 spill keys - spill_key_10 (1) = 300 keys
  EXPECT_EQ(trx_visible_cnt, total_keys);

  // Commit the spilled transaction and verify all records are atomically persisted
  ASSERT_EQ(trx->commit(), RC::SUCCESS);
  EXPECT_EQ(trx->spill_count(), 0u);
  delete trx;

  ASSERT_EQ(spill_db->get("spill_key_5", &val), RC::SUCCESS);
  EXPECT_EQ(val, "updated_after_spill");
  ASSERT_EQ(spill_db->get("spill_key_10", &val), RC::NOT_EXIST);
  ASSERT_EQ(spill_db->get("base_k1", &val), RC::NOT_EXIST);

  // Test rollback of a spilled transaction cleans up temporary spill files and leaves DB unchanged
  auto *trx_rb = spill_db->begin_transaction();
  for (size_t i = 0; i < 200; ++i) {
    ASSERT_EQ(trx_rb->put("rb_key_" + to_string(i), string(80, 'x')), RC::SUCCESS);
  }
  EXPECT_GT(trx_rb->spill_count(), 1u);
  ASSERT_EQ(trx_rb->rollback(), RC::SUCCESS);
  EXPECT_EQ(trx_rb->spill_count(), 0u);
  delete trx_rb;

  ASSERT_EQ(spill_db->get("rb_key_0", &val), RC::NOT_EXIST);

  delete spill_db;
  filesystem::remove_all(spill_db_path);
}

TEST_F(ObLsmTransactionTest, concurrent_transactions_bank_transfer_invariant_test)
{
  const int num_accounts    = 10;
  const int initial_balance = 1000;
  for (int i = 0; i < num_accounts; ++i) {
    ASSERT_EQ(db->put("account_" + to_string(i), to_string(initial_balance)), RC::SUCCESS);
  }

  const int         num_workers          = 6;
  const int         transfers_per_worker = 40;
  std::atomic<int>  committed_transfers{0};
  std::atomic<int>  aborted_conflicts{0};
  std::atomic<bool> stop_auditor{false};

  // Concurrent auditor thread repeatedly taking MVCC snapshots and verifying total balance == 10000
  std::thread auditor([this, &stop_auditor]() {
    while (!stop_auditor.load(std::memory_order_relaxed)) {
      auto *audit_trx = db->begin_transaction();
      int   total     = 0;
      for (int i = 0; i < num_accounts; ++i) {
        string v;
        ASSERT_EQ(audit_trx->get("account_" + to_string(i), &v), RC::SUCCESS);
        total += std::stoi(v);
      }
      EXPECT_EQ(total, num_accounts * initial_balance);
      delete audit_trx;
    }
  });

  std::vector<std::thread> workers;
  for (int w = 0; w < num_workers; ++w) {
    workers.emplace_back([this, w, &committed_transfers, &aborted_conflicts]() {
      for (int step = 0; step < transfers_per_worker; ++step) {
        int from_acct = (w + step) % num_accounts;
        int to_acct   = (w + step + 3) % num_accounts;
        if (from_acct == to_acct) {
          to_acct = (to_acct + 1) % num_accounts;
        }

        // Retry loop under SERIALIZABLE OCC + MVCC
        while (true) {
          auto *trx = db->begin_transaction();
          trx->set_isolation_level(TransactionIsolationLevel::SERIALIZABLE);

          string b_from_str, b_to_str;
          ASSERT_EQ(trx->get("account_" + to_string(from_acct), &b_from_str), RC::SUCCESS);
          ASSERT_EQ(trx->get("account_" + to_string(to_acct), &b_to_str), RC::SUCCESS);

          int b_from = std::stoi(b_from_str);
          int b_to   = std::stoi(b_to_str);

          ASSERT_EQ(trx->put("account_" + to_string(from_acct), to_string(b_from - 10)), RC::SUCCESS);
          ASSERT_EQ(trx->put("account_" + to_string(to_acct), to_string(b_to + 10)), RC::SUCCESS);

          RC rc = trx->commit();
          delete trx;
          if (rc == RC::SUCCESS) {
            committed_transfers.fetch_add(1);
            break;
          } else {
            ASSERT_EQ(rc, RC::LOCKED_CONCURRENCY_CONFLICT);
            aborted_conflicts.fetch_add(1);
          }
        }
      }
    });
  }

  for (auto &th : workers) {
    th.join();
  }
  stop_auditor.store(true);
  auditor.join();

  EXPECT_EQ(committed_transfers.load(), num_workers * transfers_per_worker);

  int final_total = 0;
  for (int i = 0; i < num_accounts; ++i) {
    string v;
    ASSERT_EQ(db->get("account_" + to_string(i), &v), RC::SUCCESS);
    final_total += std::stoi(v);
  }
  EXPECT_EQ(final_total, num_accounts * initial_balance);
}

int main(int argc, char **argv)
{
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
