# MiniOB TPC-C Benchmark & Consistency Suite

`miniob_tpcc.py` implements a MiniOB-compatible TPC-C workload (9 tables, 5 transaction types:
New-Order, Payment, Order-Status, Delivery, Stock-Level) and verifies all 6 TPC-C Clause 3.3.2
consistency invariants plus a deterministic state digest.

## Local Usage

```bash
# Deterministic single-thread consistency & state digest check
python3 test/tpcc/miniob_tpcc.py \
  --observer build_release/bin/observer \
  --engine btree --trx mvcc --threads 1 --transactions 200 --seed 42 \
  --output-json /tmp/tpcc_det.json

# Concurrent MVCC benchmark & consistency check
python3 test/tpcc/miniob_tpcc.py \
  --observer build_release/bin/observer \
  --engine btree --trx mvcc --threads 4 --transactions 400 --seed 42 \
  --output-json /tmp/tpcc_conc.json

# Compare Head vs Base
python3 test/tpcc/compare_tpcc.py \
  --head /tmp/tpcc_head.json --base /tmp/tpcc_base.json --check-digest
```
