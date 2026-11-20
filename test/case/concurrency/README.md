# Real-concurrency tests

`mvcc_concurrency_test.py` starts a real observer (`-t mvcc` / `-t occ`, `-d disk`)
and drives multiple SQL connections. Each step is asserted before the next one,
so a wrong intermediate result fails at the step that produced it.

Scenario sources:

- Dirty read, non-repeatable read, phantom, lost update: PostgreSQL
  `src/test/isolation` style schedules and the Hermitage anomaly catalogue.
- Read skew and write skew: Hermitage `G2-item` / `G2` write-skew cases; write
  skew is expected to commit under MVCC Snapshot Isolation and is asserted as
  such rather than treated as a failure.
- OCC read-write conflict, disjoint commits: read-set validation tests in the
  style of first-committer-wins OCC.
- `sync;` / GC: visible rows and index lookups are checked before and after GC.
  The stress phase runs four writers (disjoint rows), four full-table readers,
  and a `sync;` loop concurrently under ASan; it fails on any statement error,
  crash, duplicate/missing row, or unexpected final value.

Run:

```bash
python3 test/case/concurrency/mvcc_concurrency_test.py
python3 test/case/concurrency/mvcc_concurrency_test.py --keep  # keeps the data/log dir
```
