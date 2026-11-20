#!/usr/bin/env python3
"""Real-concurrency SQL checks for MiniOB MVCC/OCC.

Scenario layout follows PostgreSQL's isolation regression suite and the
Hermitage anomaly catalogue: dirty read, non-repeatable read, read skew,
phantom, lost update, write skew, plus GC/index maintenance.

Run against build_debug (ASan):
    python3 test/case/concurrency/mvcc_concurrency_test.py
"""

import argparse
import os
import shutil
import socket
import subprocess
import sys
import tempfile
import threading
import time
from pathlib import Path

REPO = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(REPO / "test" / "case"))
from miniob_test import MiniObClient  # noqa: E402


def log(msg):
    print(msg, flush=True)


def fail(msg):
    log(f"FAIL: {msg}")
    raise AssertionError(msg)


def rows_from(out):
    rows = []
    for line in out.splitlines():
        if "|" not in line:
            continue
        parts = [part.strip() for part in line.split("|")]
        try:
            rows.append(tuple(int(part) for part in parts))
        except ValueError:
            continue
    return sorted(rows)


class Server:
    def __init__(self, root, kit, observer_path):
        self.root = Path(root)
        self.kit = kit
        self.observer = Path(observer_path)
        self.data = self.root / "data"
        self.root.mkdir(parents=True, exist_ok=True)
        self.sock = str(self.root / "miniob.sock")
        self.log_file = open(self.root / f"observer-{kit}.log", "w")
        self.proc = None

    def start(self):
        self.root.mkdir(parents=True, exist_ok=True)
        self.data.mkdir(parents=True, exist_ok=True)
        observer = self.observer
        config = REPO / "etc" / "observer.ini"
        env = dict(os.environ)
        env["ASAN_OPTIONS"] = "detect_leaks=0:abort_on_error=1"
        self.proc = subprocess.Popen(
            [str(observer), "-f", str(config), "-s", self.sock, "-t", self.kit, "-d", "disk"],
            cwd=self.data,
            stdout=self.log_file,
            stderr=subprocess.STDOUT,
            env=env,
        )
        deadline = time.time() + 15
        while time.time() < deadline:
            if self.proc.poll() is not None:
                self.dump()
                fail(f"observer-{self.kit} exited during startup")
            if os.path.exists(self.sock):
                try:
                    probe = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
                    probe.connect(self.sock)
                    probe.close()
                    return
                except OSError:
                    pass
            time.sleep(0.05)
        self.dump()
        fail(f"observer-{self.kit} did not start")

    def connect(self):
        client = MiniObClient(0, self.sock)
        if not client.is_valid():
            fail("failed to connect client")
        return Session(client)

    def dump(self):
        self.log_file.flush()
        p = self.root / f"observer-{self.kit}.log"
        if p.exists():
            lines = p.read_text(errors="replace").splitlines()[-40:]
            log("\n".join(lines))

    def stop(self):
        if self.proc is not None and self.proc.poll() is None:
            self.proc.terminate()
            try:
                self.proc.wait(timeout=10)
            except subprocess.TimeoutExpired:
                self.proc.kill()
                self.proc.wait()
        self.log_file.close()


class Session:
    def __init__(self, client):
        self.client = client

    def run(self, sql):
        ok, out = self.client.run_sql(sql)
        if not ok:
            fail(f"protocol failure for {sql!r}")
        return (out or "").strip()

    def success(self, sql):
        out = self.run(sql)
        if out.upper().startswith("FAILURE"):
            fail(f"expected success: {sql} -> {out}")
        return out

    def failure(self, sql):
        out = self.run(sql)
        if not out.upper().startswith("FAILURE"):
            fail(f"expected failure: {sql} -> {out}")
        return out

    def check_rows(self, sql, expected, step):
        out = self.run(sql)
        got = rows_from(out)
        want = sorted(tuple(row) for row in expected)
        if got != want:
            fail(f"{step}: {sql} -> {got}, expected {want}")
        log(f"PASS: {step}")

    def check_success(self, sql, step):
        self.success(sql)
        log(f"PASS: {step}")

    def check_failure(self, sql, step):
        self.failure(sql)
        log(f"PASS: {step}")

    def close(self):
        self.client.close()


def isolation_mvcc(root, observer):
    server = Server(root, "mvcc", observer)
    server.start()
    try:
        c1 = server.connect()
        c2 = server.connect()
        c1.success("create table iso(id int, v int);")
        c1.success("insert into iso values(1,10);")
        c1.success("insert into iso values(2,10);")
        c1.success("create index i_v on iso(v);")
        log("PASS: setup mvcc table")

        c1.success("begin;")
        c1.success("update iso set v=99 where id=1;")
        c2.check_rows("select * from iso where id=1;", [(1, 10)], "dirty read sees old version")
        c1.success("rollback;")
        c2.check_rows("select * from iso where id=1;", [(1, 10)], "dirty read rolled back")

        c1.success("begin;")
        c1.check_rows("select * from iso where id=1;", [(1, 10)], "repeatable read first read")
        c2.success("begin;")
        c2.success("update iso set v=99 where id=1;")
        c2.success("commit;")
        c1.check_rows("select * from iso where id=1;", [(1, 10)], "repeatable read after commit")
        c1.success("commit;")
        c1.check_rows("select * from iso where id=1;", [(1, 99)], "new snapshot sees commit")

        c1.success("begin;")
        c1.check_rows("select * from iso;", [(1, 99), (2, 10)], "read skew first read")
        c2.success("begin;")
        c2.success("update iso set v=0 where id=1;")
        c2.success("update iso set v=20 where id=2;")
        c2.success("commit;")
        c1.check_rows("select * from iso;", [(1, 99), (2, 10)], "read skew second read")
        c1.success("commit;")

        c1.success("begin;")
        c1.check_rows("select * from iso where id=2;", [(2, 20)], "phantom first scan")
        c2.success("insert into iso values(3,30);")
        c1.check_rows("select * from iso where v>=10;", [(2, 20)], "phantom re-scan")
        c1.success("commit;")
        c1.check_rows("select * from iso where v>=10;", [(2, 20), (3, 30)], "phantom after commit")

        # Lost update: both read the same row; the first committer wins.
        c1.success("begin;")
        c2.success("begin;")
        c1.check_rows("select * from iso where id=2;", [(2, 20)], "lost update c1 read")
        c2.check_rows("select * from iso where id=2;", [(2, 20)], "lost update c2 read")
        c1.success("update iso set v=21 where id=2;")
        c1.success("commit;")
        c2.check_failure("update iso set v=22 where id=2;", "lost update stale writer fails")
        c2.success("rollback;")
        c1.check_rows("select * from iso where id=2;", [(2, 21)], "lost update first writer kept")

        # Write skew is allowed under MVCC snapshot isolation.
        c1.success("create table skew(id int, v int);")
        c1.success("insert into skew values(1,10);")
        c1.success("insert into skew values(2,10);")
        c1.success("begin;")
        c2.success("begin;")
        c1.check_rows("select * from skew;", [(1, 10), (2, 10)], "write skew c1 read")
        c2.check_rows("select * from skew;", [(1, 10), (2, 10)], "write skew c2 read")
        c1.success("update skew set v=0 where id=1;")
        c2.success("update skew set v=0 where id=2;")
        c1.success("commit;")
        c2.success("commit;")
        c1.check_rows("select * from skew;", [(1, 0), (2, 0)], "write skew both committed")

        # sync runs GC; visible rows and index results must not change.
        c1.success("create table gc_iso(id int, v int);")
        c1.success("insert into gc_iso values(1,10);")
        c1.success("insert into gc_iso values(2,20);")
        c1.success("insert into gc_iso values(3,30);")
        c1.success("create index i_gc on gc_iso(v);")
        c1.success("begin;")
        c1.success("update gc_iso set v=99 where id=2;")
        c1.success("commit;")
        c1.success("begin;")
        c1.success("update gc_iso set v=88 where id=3;")
        c1.success("rollback;")
        c1.check_rows("select * from gc_iso where v=20;", [], "old index entry before sync")
        c1.check_success("sync;", "sync/GC succeeds")
        c1.check_rows("select * from gc_iso;", [(1, 10), (2, 99), (3, 30)], "rows after GC")
        c1.check_rows("select * from gc_iso where v=20;", [], "dead index entry after GC")
        c1.check_rows("select * from gc_iso where v=99;", [(2, 99)], "new index entry after GC")
        c1.check_rows("select * from gc_iso where v=88;", [], "rolled-back index entry after GC")

        c1.close()
        c2.close()
    finally:
        server.stop()


def isolation_occ(root, observer):
    server = Server(root, "occ", observer)
    server.start()
    try:
        c1 = server.connect()
        c2 = server.connect()
        c1.success("create table occ_iso(id int, v int);")
        c1.success("insert into occ_iso values(1,10);")
        c1.success("insert into occ_iso values(2,20);")
        c1.success("create index i_id on occ_iso(id);")
        log("PASS: setup occ table")

        c1.success("begin;")
        c1.success("update occ_iso set v=99 where id=1;")
        c2.check_rows("select * from occ_iso where id=1;", [(1, 10)], "occ dirty read")
        c1.success("rollback;")

        c1.success("begin;")
        c1.check_rows("select * from occ_iso where id=1;", [(1, 10)], "occ conflict read")
        c2.success("begin;")
        c2.success("update occ_iso set v=99 where id=1;")
        c2.success("commit;")
        c1.check_failure("commit;", "occ read-set conflict at commit")
        c2.check_rows("select * from occ_iso where id=1;", [(1, 99)], "occ winner kept")

        # Index point reads keep the OCC read set small, so disjoint writes pass.
        c1.success("begin;")
        c2.success("begin;")
        c1.check_rows("select * from occ_iso where id=1;", [(1, 99)], "occ disjoint read c1")
        c2.check_rows("select * from occ_iso where id=2;", [(2, 20)], "occ disjoint read c2")
        c1.success("update occ_iso set v=11 where id=1;")
        c2.success("update occ_iso set v=22 where id=2;")
        c1.check_success("commit;", "occ disjoint commit c1")
        c2.check_success("commit;", "occ disjoint commit c2")
        c1.check_rows("select * from occ_iso;", [(1, 11), (2, 22)], "occ disjoint final rows")

        c1.close()
        c2.close()
    finally:
        server.stop()


def gc_stress(root, rounds=40, observer=None):
    server = Server(root, "mvcc", observer)
    server.start()
    errors = []
    try:
        setup = server.connect()
        setup.success("create table stress(id int, v int);")
        setup.success("create index i_stress on stress(id);")
        for i in range(1, 21):
            setup.success(f"insert into stress values({i},0);")
        setup.close()
        log("PASS: setup gc stress table")

        start = threading.Barrier(9)
        stop = threading.Event()

        def writer(writer_id, ids):
            try:
                client = server.connect()
                start.wait()
                for rnd in range(1, rounds + 1):
                    client.success("begin;")
                    for rid in ids:
                        client.success(f"update stress set v={rnd} where id={rid};")
                    if rnd % 7 == 0:
                        client.success("rollback;")
                    else:
                        client.success("commit;")
                client.close()
            except Exception as ex:  # noqa: BLE001
                errors.append(f"writer {writer_id}: {ex}")
                stop.set()

        def reader(reader_id):
            try:
                client = server.connect()
                start.wait()
                seen = 0
                while not stop.is_set():
                    out = client.success("select * from stress;")
                    rows = rows_from(out)
                    ids = [row[0] for row in rows]
                    if len(rows) != 20 or len(set(ids)) != 20:
                        raise AssertionError(f"reader {reader_id} saw {rows}")
                    if any(not (0 <= row[1] <= rounds) for row in rows):
                        raise AssertionError(f"reader {reader_id} bad value {rows}")
                    seen += 1
                client.close()
            except Exception as ex:  # noqa: BLE001
                errors.append(f"reader {reader_id}: {ex}")
                stop.set()

        def gc_worker():
            try:
                client = server.connect()
                start.wait()
                count = 0
                while not stop.is_set():
                    client.success("sync;")
                    count += 1
                    time.sleep(0.02)
                client.close()
            except Exception as ex:  # noqa: BLE001
                errors.append(f"gc: {ex}")
                stop.set()

        threads = []
        for wid in range(4):
            ids = list(range(wid * 5 + 1, wid * 5 + 6))
            threads.append(threading.Thread(target=writer, args=(wid, ids), daemon=True))
        for rid in range(4):
            threads.append(threading.Thread(target=reader, args=(rid,), daemon=True))
        threads.append(threading.Thread(target=gc_worker, daemon=True))
        for thread in threads:
            thread.start()
        for thread in threads[:4]:
            thread.join()
        stop.set()
        for thread in threads[4:]:
            thread.join(timeout=20)
        if errors:
            fail("gc stress: " + "; ".join(errors))
        if server.proc.poll() is not None:
            server.dump()
            fail("observer crashed during gc stress")

        check = server.connect()
        rows = rows_from(check.run("select * from stress;"))
        last = rounds
        while last % 7 == 0:
            last -= 1
        expected = [(i, last) for i in range(1, 21)]
        if rows != sorted(expected):
            fail(f"gc stress final rows {rows}, expected {sorted(expected)}")
        check.check_success("sync;", "final sync/GC")
        rows = rows_from(check.run("select * from stress;"))
        if rows != sorted(expected):
            fail(f"gc stress rows changed after sync {rows}")
        check.close()
        log("PASS: gc stress rows and index intact")
    finally:
        server.stop()




def gc_small_stress(root, rounds=200, observer=None, use_gc=True, use_rollback=True):
    server = Server(root, "mvcc", observer)
    server.start()
    errors = []
    try:
        setup = server.connect()
        setup.success("create table one_row(id int, v int);")
        setup.success("create index i_one on one_row(id);")
        setup.success("insert into one_row values(1,0);")
        setup.close()

        start = threading.Barrier(3 if use_gc else 2)
        stop = threading.Event()

        def writer():
            try:
                client = server.connect()
                start.wait()
                for rnd in range(1, rounds + 1):
                    client.success("begin;")
                    client.success(f"update one_row set v={rnd} where id=1;")
                    if use_rollback and rnd % 5 == 0:
                        client.success("rollback;")
                    else:
                        client.success("commit;")
                client.close()
            except Exception as ex:  # noqa: BLE001
                errors.append(f"writer: {ex}")
                stop.set()

        def reader():
            try:
                client = server.connect()
                start.wait()
                while not stop.is_set():
                    rows = rows_from(client.success("select * from one_row;"))
                    if len(rows) != 1 or rows[0][0] != 1:
                        raise AssertionError(f"reader saw {rows}")
                client.close()
            except Exception as ex:  # noqa: BLE001
                errors.append(f"reader: {ex}")
                stop.set()

        def gc_worker():
            try:
                client = server.connect()
                start.wait()
                while not stop.is_set():
                    client.success("sync;")
                    time.sleep(0.005)
                client.close()
            except Exception as ex:  # noqa: BLE001
                errors.append(f"gc: {ex}")
                stop.set()

        threads = [threading.Thread(target=writer), threading.Thread(target=reader)]
        if use_gc:
            threads.append(threading.Thread(target=gc_worker))
        for thread in threads:
            thread.start()
        threads[0].join()
        stop.set()
        for thread in threads[1:]:
            thread.join(timeout=10)
        if errors:
            fail("small gc stress: " + "; ".join(errors))
        if server.proc.poll() is not None:
            server.dump()
            fail("observer crashed during small gc stress")
        check = server.connect()
        last = rounds
        if use_rollback:
            while last % 5 == 0:
                last -= 1
        check.check_rows("select * from one_row;", [(1, last)], "small stress final row")
        check.close()
        log("PASS: small gc stress one visible version")
    finally:
        server.stop()

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--keep", action="store_true")
    parser.add_argument("--stress-rounds", type=int, default=40)
    parser.add_argument("--observer", default=os.environ.get(
        "MINIOB_OBSERVER", str(REPO / "build_debug" / "bin" / "observer")))
    parser.add_argument("--scenario", choices=["all", "isolation", "gc", "small"], default="all")
    parser.add_argument("--no-gc", action="store_true")
    parser.add_argument("--no-rollback", action="store_true")
    args = parser.parse_args()
    root = Path(tempfile.mkdtemp(prefix="miniob-concurrency-"))
    try:
        log(f"workdir={root}")
        if args.scenario in ("all", "isolation"):
            isolation_mvcc(root / "mvcc", args.observer)
            isolation_occ(root / "occ", args.observer)
        if args.scenario in ("all", "gc"):
            gc_stress(root / "stress", args.stress_rounds, args.observer)
        if args.scenario == "small":
            gc_small_stress(root / "small", args.stress_rounds, args.observer,
                use_gc=not args.no_gc, use_rollback=not args.no_rollback)
        log("ALL CONCURRENCY CHECKS PASSED")
    finally:
        if not args.keep:
            shutil.rmtree(root, ignore_errors=True)
        else:
            log(f"kept {root}")


if __name__ == "__main__":
    main()
