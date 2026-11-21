#!/usr/bin/env python3
"""MiniOB TPC-C benchmark, consistency verifier, and deterministic state checker.

Implements the 9-table TPC-C schema and 5 transaction types (New-Order, Payment,
Order-Status, Delivery, Stock-Level) using MiniOB-compatible SQL over Unix socket.

Verifies all 6 TPC-C Clause 3.3.2 consistency invariants after execution and
computes a deterministic database state digest for cross-commit consistency checks.
"""

import argparse
import hashlib
import json
import os
import random
import shutil
import socket
import subprocess
import sys
import tempfile
import threading
import time
from pathlib import Path

REPO = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(REPO / "test" / "case"))
from miniob_test import MiniObClient  # noqa: E402


def log(msg: str) -> None:
  print(msg, flush=True)


class Server:
  """Manages an isolated MiniOB observer instance for TPC-C."""

  def __init__(self, root: Path, observer_path: str, trx: str = "mvcc", durability: str = "disk"):
    self.root = Path(root)
    self.observer = Path(observer_path)
    self.trx = trx
    self.durability = durability
    self.data = self.root / "data"
    self.root.mkdir(parents=True, exist_ok=True)
    self.sock = str(self.root / "miniob.sock")
    self.log_path = self.root / "observer.log"
    self.log_file = open(self.log_path, "w")
    self.proc = None

  def start(self) -> None:
    self.data.mkdir(parents=True, exist_ok=True)
    config = REPO / "etc" / "observer.ini"
    env = dict(os.environ)
    env["ASAN_OPTIONS"] = "detect_leaks=0:abort_on_error=1"
    self.proc = subprocess.Popen(
        [
            str(self.observer),
            "-f",
            str(config),
            "-s",
            self.sock,
            "-t",
            self.trx,
            "-d",
            self.durability,
        ],
        cwd=self.data,
        stdout=self.log_file,
        stderr=subprocess.STDOUT,
        env=env,
    )
    deadline = time.time() + 20
    while time.time() < deadline:
      if self.proc.poll() is not None:
        self.dump()
        raise RuntimeError(f"observer exited during startup with code {self.proc.returncode}")
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
    raise RuntimeError("observer did not start within 20s")

  def connect(self) -> "Session":
    client = MiniObClient(0, self.sock)
    if not client.is_valid():
      raise RuntimeError("failed to connect to observer unix socket")
    return Session(client)

  def dump(self) -> None:
    self.log_file.flush()
    if self.log_path.exists():
      lines = self.log_path.read_text(errors="replace").splitlines()[-40:]
      log("\n".join(lines))

  def stop(self) -> None:
    if self.proc is not None and self.proc.poll() is None:
      self.proc.terminate()
      try:
        self.proc.wait(timeout=10)
      except subprocess.TimeoutExpired:
        self.proc.kill()
        self.proc.wait()
    self.log_file.close()


class Session:
  """Thin wrapper around MiniObClient with SQL helpers."""

  def __init__(self, client: MiniObClient):
    self.client = client

  def close(self) -> None:
    self.client.close()

  def run(self, sql: str) -> str:
    ok, out = self.client.run_sql(sql)
    if not ok:
      raise RuntimeError(f"transport failure on SQL: {sql}")
    return out.strip()

  def exec_ok(self, sql: str) -> str:
    out = self.run(sql)
    upper = out.upper()
    if upper.startswith("FAILURE") or "FAILED TO PARSE" in upper:
      raise RuntimeError(f"SQL failed: {sql} -> {out}")
    return out

  def query_rows(self, sql: str) -> list[list[str]]:
    out = self.run(sql)
    upper = out.upper()
    if upper.startswith("FAILURE") or "FAILED TO PARSE" in upper:
      raise RuntimeError(f"Query failed: {sql} -> {out}")
    lines = [line.strip() for line in out.splitlines() if line.strip()]
    if len(lines) <= 1:
      return []
    # MiniOB prints header on the first line (e.g. "COL1 | COL2"), rows below.
    rows = []
    for line in lines[1:]:
      rows.append([part.strip() for part in line.split("|")])
    return rows


class TrxConflictError(Exception):
  """Raised when a transaction encounters an expected concurrency conflict."""


def run_sql_in_trx(sess: Session, sql: str) -> str:
  out = sess.run(sql)
  upper = out.upper()
  if "FAILED TO PARSE" in upper:
    raise RuntimeError(f"SQL parse error: {sql} -> {out}")
  if upper.startswith("FAILURE"):
    raise TrxConflictError(f"{sql} -> {out}")
  return out


def query_in_trx(sess: Session, sql: str) -> list[list[str]]:
  out = run_sql_in_trx(sess, sql)
  lines = [line.strip() for line in out.splitlines() if line.strip()]
  if len(lines) <= 1:
    return []
  return [[part.strip() for part in line.split("|")] for line in lines[1:]]


def create_tpcc_schema(sess: Session, engine: str) -> None:
  eng_clause = f" engine={engine}" if engine and engine != "heap" else ""
  ddls = [
      (
          f"create table warehouse(w_id int, w_name char(10), w_street_1 char(20), "
          f"w_city char(20), w_state char(2), w_zip char(9), w_tax float, w_ytd float, "
          f"primary key(w_id)){eng_clause};"
      ),
      (
          f"create table district(d_id int, d_w_id int, d_name char(10), d_street_1 char(20), "
          f"d_city char(20), d_state char(2), d_zip char(9), d_tax float, d_ytd float, "
          f"d_next_o_id int, primary key(d_w_id, d_id)){eng_clause};"
      ),
      (
          f"create table customer(c_id int, c_d_id int, c_w_id int, c_first char(16), "
          f"c_middle char(2), c_last char(16), c_discount float, c_balance float, "
          f"c_ytd_payment float, c_payment_cnt int, c_delivery_cnt int, c_data char(50), "
          f"primary key(c_w_id, c_d_id, c_id)){eng_clause};"
      ),
      (
          f"create table history(h_c_id int, h_c_d_id int, h_c_w_id int, h_d_id int, "
          f"h_w_id int, h_date int, h_amount float, h_data char(24)){eng_clause};"
      ),
      (
          f"create table new_order(no_o_id int, no_d_id int, no_w_id int, "
          f"primary key(no_w_id, no_d_id, no_o_id)){eng_clause};"
      ),
      (
          f"create table orders(o_id int, o_d_id int, o_w_id int, o_c_id int, "
          f"o_entry_d int, o_carrier_id int, o_ol_cnt int, o_all_local int, "
          f"primary key(o_w_id, o_d_id, o_id)){eng_clause};"
      ),
      (
          f"create table order_line(ol_o_id int, ol_d_id int, ol_w_id int, ol_number int, "
          f"ol_i_id int, ol_supply_w_id int, ol_delivery_d int, ol_quantity int, "
          f"ol_amount float, ol_dist_info char(24), "
          f"primary key(ol_w_id, ol_d_id, ol_o_id, ol_number)){eng_clause};"
      ),
      (
          f"create table item(i_id int, i_im_id int, i_name char(24), i_price float, "
          f"i_data char(50), primary key(i_id)){eng_clause};"
      ),
      (
          f"create table stock(s_i_id int, s_w_id int, s_quantity int, s_ytd int, "
          f"s_order_cnt int, s_remote_cnt int, s_data char(50), "
          f"primary key(s_w_id, s_i_id)){eng_clause};"
      ),
      "create index idx_c_last on customer(c_last);",
      "create index idx_o_cid on orders(o_c_id);",
      "create index idx_ol_oid on order_line(ol_o_id);",
  ]
  for ddl in ddls:
    sess.exec_ok(ddl)


def load_tpcc_data(
    sess: Session,
    warehouses: int,
    districts_per_w: int,
    customers_per_d: int,
    items: int,
    initial_orders: int,
    seed: int,
) -> float:
  rng = random.Random(seed)
  initial_ytd_total = 0.0

  # Items
  for i_id in range(1, items + 1):
    price = round(rng.uniform(1.0, 100.0), 2)
    sess.exec_ok(
        f"insert into item values({i_id}, {i_id % 100 + 1}, 'item_{i_id}', {price}, 'idata_{i_id}');"
    )

  for w_id in range(1, warehouses + 1):
    w_ytd = 30000.0 * districts_per_w
    initial_ytd_total += w_ytd
    sess.exec_ok(
        f"insert into warehouse values({w_id}, 'wh_{w_id}', 'street_{w_id}', "
        f"'city_{w_id}', 'ST', '123456789', 0.08, {w_ytd});"
    )

    # Stock
    for i_id in range(1, items + 1):
      qty = rng.randint(20, 100)
      sess.exec_ok(
          f"insert into stock values({i_id}, {w_id}, {qty}, 0, 0, 0, 'sdata_{w_id}_{i_id}');"
      )

    # Districts, Customers, Initial Orders
    for d_id in range(1, districts_per_w + 1):
      d_ytd = 30000.0
      next_o_id = initial_orders + 1
      sess.exec_ok(
          f"insert into district values({d_id}, {w_id}, 'dist_{d_id}', 'dst_{d_id}', "
          f"'dcity_{d_id}', 'ST', '987654321', 0.05, {d_ytd}, {next_o_id});"
      )

      for c_id in range(1, customers_per_d + 1):
        c_last = f"LAST_{c_id % 10}"
        sess.exec_ok(
            f"insert into customer values({c_id}, {d_id}, {w_id}, 'first_{c_id}', 'OE', "
            f"'{c_last}', 0.05, -10.0, 10.0, 1, 0, 'cdata_{c_id}');"
        )

      for o_id in range(1, initial_orders + 1):
        c_id = ((o_id - 1) % customers_per_d) + 1
        ol_cnt = 3
        delivered = o_id <= (initial_orders // 2)
        carrier_id = rng.randint(1, 10) if delivered else 0
        sess.exec_ok(
            f"insert into orders values({o_id}, {d_id}, {w_id}, {c_id}, 1000, {carrier_id}, {ol_cnt}, 1);"
        )
        if not delivered:
          sess.exec_ok(f"insert into new_order values({o_id}, {d_id}, {w_id});")

        for ol_num in range(1, ol_cnt + 1):
          i_id = rng.randint(1, items)
          ol_amount = 0.0 if delivered else round(rng.uniform(1.0, 50.0), 2)
          del_d = 1000 if delivered else 0
          sess.exec_ok(
              f"insert into order_line values({o_id}, {d_id}, {w_id}, {ol_num}, {i_id}, "
              f"{w_id}, {del_d}, 5, {ol_amount}, 'dist_info_{d_id}');"
          )

  return initial_ytd_total


class TpccWorkerConfig:
  def __init__(self, warehouses: int, districts_per_w: int, customers_per_d: int, items: int):
    self.warehouses = warehouses
    self.districts_per_w = districts_per_w
    self.customers_per_d = customers_per_d
    self.items = items


def do_new_order(sess: Session, rng: random.Random, cfg: TpccWorkerConfig) -> None:
  w_id = rng.randint(1, cfg.warehouses)
  d_id = rng.randint(1, cfg.districts_per_w)
  c_id = rng.randint(1, cfg.customers_per_d)
  ol_cnt = rng.randint(2, 5)
  # ~1% simulated invalid item to exercise explicit ROLLBACK
  invalid_rollback = rng.random() < 0.01

  run_sql_in_trx(sess, "begin;")
  try:
    w_rows = query_in_trx(sess, f"select w_tax from warehouse where w_id={w_id};")
    d_rows = query_in_trx(
        sess, f"select d_tax, d_next_o_id from district where d_w_id={w_id} and d_id={d_id};"
    )
    c_rows = query_in_trx(
        sess, f"select c_discount, c_last from customer where c_w_id={w_id} and c_d_id={d_id} and c_id={c_id};"
    )
    if not w_rows or not d_rows or not c_rows:
      raise TrxConflictError("missing warehouse/district/customer row")

    w_tax = float(w_rows[0][0])
    d_tax = float(d_rows[0][0])
    o_id = int(d_rows[0][1])
    c_discount = float(c_rows[0][0])

    run_sql_in_trx(
        sess, f"update district set d_next_o_id={o_id + 1} where d_w_id={w_id} and d_id={d_id};"
    )
    run_sql_in_trx(
        sess, f"insert into orders values({o_id}, {d_id}, {w_id}, {c_id}, 2000, 0, {ol_cnt}, 1);"
    )
    run_sql_in_trx(sess, f"insert into new_order values({o_id}, {d_id}, {w_id});")

    for ol_num in range(1, ol_cnt + 1):
      if invalid_rollback and ol_num == ol_cnt:
        # Item id -1 does not exist -> rollback transaction cleanly
        i_rows = query_in_trx(sess, "select i_price from item where i_id=-1;")
        if not i_rows:
          sess.run("rollback;")
          return
      i_id = rng.randint(1, cfg.items)
      i_rows = query_in_trx(sess, f"select i_price from item where i_id={i_id};")
      if not i_rows:
        sess.run("rollback;")
        return
      i_price = float(i_rows[0][0])

      s_rows = query_in_trx(
          sess,
          f"select s_quantity, s_ytd, s_order_cnt from stock where s_w_id={w_id} and s_i_id={i_id};",
      )
      if not s_rows:
        raise TrxConflictError("missing stock row")
      s_qty = int(s_rows[0][0])
      s_ytd = int(s_rows[0][1])
      s_ocnt = int(s_rows[0][2])

      ol_qty = rng.randint(1, 5)
      new_qty = s_qty - ol_qty if s_qty >= ol_qty + 10 else s_qty + 91 - ol_qty
      run_sql_in_trx(
          sess, f"update stock set s_quantity={new_qty} where s_w_id={w_id} and s_i_id={i_id};"
      )
      run_sql_in_trx(
          sess, f"update stock set s_ytd={s_ytd + ol_qty} where s_w_id={w_id} and s_i_id={i_id};"
      )
      run_sql_in_trx(
          sess, f"update stock set s_order_cnt={s_ocnt + 1} where s_w_id={w_id} and s_i_id={i_id};"
      )

      ol_amount = round(ol_qty * i_price * (1 + w_tax + d_tax) * (1 - c_discount), 2)
      run_sql_in_trx(
          sess,
          f"insert into order_line values({o_id}, {d_id}, {w_id}, {ol_num}, {i_id}, {w_id}, "
          f"0, {ol_qty}, {ol_amount}, 'dist_info_{d_id}');",
      )

    run_sql_in_trx(sess, "commit;")
  except TrxConflictError:
    sess.run("rollback;")
    raise


def do_payment(sess: Session, rng: random.Random, cfg: TpccWorkerConfig) -> None:
  w_id = rng.randint(1, cfg.warehouses)
  d_id = rng.randint(1, cfg.districts_per_w)
  c_id = rng.randint(1, cfg.customers_per_d)
  h_amount = round(rng.uniform(1.0, 500.0), 2)

  run_sql_in_trx(sess, "begin;")
  try:
    w_rows = query_in_trx(sess, f"select w_ytd from warehouse where w_id={w_id};")
    d_rows = query_in_trx(
        sess, f"select d_ytd from district where d_w_id={w_id} and d_id={d_id};"
    )
    c_rows = query_in_trx(
        sess,
        f"select c_balance, c_ytd_payment, c_payment_cnt from customer "
        f"where c_w_id={w_id} and c_d_id={d_id} and c_id={c_id};",
    )
    if not w_rows or not d_rows or not c_rows:
      raise TrxConflictError("missing row in payment")

    w_ytd = round(float(w_rows[0][0]) + h_amount, 2)
    d_ytd = round(float(d_rows[0][0]) + h_amount, 2)
    c_bal = round(float(c_rows[0][0]) - h_amount, 2)
    c_ytd_pay = round(float(c_rows[0][1]) + h_amount, 2)
    c_pay_cnt = int(c_rows[0][2]) + 1

    run_sql_in_trx(sess, f"update warehouse set w_ytd={w_ytd} where w_id={w_id};")
    run_sql_in_trx(
        sess, f"update district set d_ytd={d_ytd} where d_w_id={w_id} and d_id={d_id};"
    )
    run_sql_in_trx(
        sess,
        f"update customer set c_balance={c_bal} where c_w_id={w_id} and c_d_id={d_id} and c_id={c_id};",
    )
    run_sql_in_trx(
        sess,
        f"update customer set c_ytd_payment={c_ytd_pay} where c_w_id={w_id} and c_d_id={d_id} and c_id={c_id};",
    )
    run_sql_in_trx(
        sess,
        f"update customer set c_payment_cnt={c_pay_cnt} where c_w_id={w_id} and c_d_id={d_id} and c_id={c_id};",
    )
    run_sql_in_trx(
        sess,
        f"insert into history values({c_id}, {d_id}, {w_id}, {d_id}, {w_id}, 2000, {h_amount}, 'pay_{w_id}_{d_id}');",
    )
    run_sql_in_trx(sess, "commit;")
  except TrxConflictError:
    sess.run("rollback;")
    raise


def do_order_status(sess: Session, rng: random.Random, cfg: TpccWorkerConfig) -> None:
  w_id = rng.randint(1, cfg.warehouses)
  d_id = rng.randint(1, cfg.districts_per_w)
  c_id = rng.randint(1, cfg.customers_per_d)

  run_sql_in_trx(sess, "begin;")
  try:
    query_in_trx(
        sess,
        f"select c_balance, c_first, c_last from customer "
        f"where c_w_id={w_id} and c_d_id={d_id} and c_id={c_id};",
    )
    o_rows = query_in_trx(
        sess,
        f"select max(o_id) from orders where o_w_id={w_id} and o_d_id={d_id} and o_c_id={c_id};",
    )
    if o_rows and o_rows[0][0].upper() != "NULL":
      o_id = int(o_rows[0][0])
      query_in_trx(
          sess,
          f"select ol_i_id, ol_quantity, ol_amount from order_line "
          f"where ol_w_id={w_id} and ol_d_id={d_id} and ol_o_id={o_id};",
      )
    run_sql_in_trx(sess, "commit;")
  except TrxConflictError:
    sess.run("rollback;")
    raise


def do_delivery(sess: Session, rng: random.Random, cfg: TpccWorkerConfig) -> None:
  w_id = rng.randint(1, cfg.warehouses)
  carrier_id = rng.randint(1, 10)

  run_sql_in_trx(sess, "begin;")
  try:
    for d_id in range(1, cfg.districts_per_w + 1):
      no_rows = query_in_trx(
          sess, f"select min(no_o_id) from new_order where no_w_id={w_id} and no_d_id={d_id};"
      )
      if not no_rows or no_rows[0][0].upper() == "NULL":
        continue
      no_o_id = int(no_rows[0][0])

      run_sql_in_trx(
          sess,
          f"delete from new_order where no_w_id={w_id} and no_d_id={d_id} and no_o_id={no_o_id};",
      )
      o_rows = query_in_trx(
          sess, f"select o_c_id from orders where o_w_id={w_id} and o_d_id={d_id} and o_id={no_o_id};"
      )
      if not o_rows:
        continue
      c_id = int(o_rows[0][0])

      run_sql_in_trx(
          sess,
          f"update orders set o_carrier_id={carrier_id} where o_w_id={w_id} and o_d_id={d_id} and o_id={no_o_id};",
      )
      ol_sum_rows = query_in_trx(
          sess,
          f"select sum(ol_amount) from order_line where ol_w_id={w_id} and ol_d_id={d_id} and ol_o_id={no_o_id};",
      )
      ol_total = float(ol_sum_rows[0][0]) if ol_sum_rows and ol_sum_rows[0][0].upper() != "NULL" else 0.0
      run_sql_in_trx(
          sess,
          f"update order_line set ol_delivery_d=2001 where ol_w_id={w_id} and ol_d_id={d_id} and ol_o_id={no_o_id};",
      )
      c_rows = query_in_trx(
          sess,
          f"select c_balance, c_delivery_cnt from customer where c_w_id={w_id} and c_d_id={d_id} and c_id={c_id};",
      )
      if c_rows:
        new_bal = round(float(c_rows[0][0]) + ol_total, 2)
        new_dcnt = int(c_rows[0][1]) + 1
        run_sql_in_trx(
            sess,
            f"update customer set c_balance={new_bal} where c_w_id={w_id} and c_d_id={d_id} and c_id={c_id};",
        )
        run_sql_in_trx(
            sess,
            f"update customer set c_delivery_cnt={new_dcnt} where c_w_id={w_id} and c_d_id={d_id} and c_id={c_id};",
        )
    run_sql_in_trx(sess, "commit;")
  except TrxConflictError:
    sess.run("rollback;")
    raise


def do_stock_level(sess: Session, rng: random.Random, cfg: TpccWorkerConfig) -> None:
  w_id = rng.randint(1, cfg.warehouses)
  d_id = rng.randint(1, cfg.districts_per_w)
  threshold = rng.randint(10, 20)

  run_sql_in_trx(sess, "begin;")
  try:
    d_rows = query_in_trx(
        sess, f"select d_next_o_id from district where d_w_id={w_id} and d_id={d_id};"
    )
    if d_rows:
      next_o_id = int(d_rows[0][0])
      low_o_id = max(1, next_o_id - 20)
      ol_rows = query_in_trx(
          sess,
          f"select ol_i_id from order_line where ol_w_id={w_id} and ol_d_id={d_id} "
          f"and ol_o_id>={low_o_id} and ol_o_id<{next_o_id};",
      )
      item_ids = sorted({int(r[0]) for r in ol_rows})[:20]
      for i_id in item_ids:
        query_in_trx(
            sess,
            f"select s_quantity from stock where s_w_id={w_id} and s_i_id={i_id} and s_quantity<{threshold};",
        )
    run_sql_in_trx(sess, "commit;")
  except TrxConflictError:
    sess.run("rollback;")
    raise


def verify_consistency_and_digest(
    sess: Session,
    warehouses: int,
    districts_per_w: int,
    initial_ytd_total: float,
) -> tuple[bool, list[str], str, dict]:
  """Verifies TPC-C Clause 3.3.2 consistency conditions and computes state digest."""
  failures = []

  # 1. W_YTD == sum(D_YTD)
  for w_id in range(1, warehouses + 1):
    w_ytd = float(sess.query_rows(f"select w_ytd from warehouse where w_id={w_id};")[0][0])
    d_ytd_sum = float(
        sess.query_rows(f"select sum(d_ytd) from district where d_w_id={w_id};")[0][0]
    )
    if abs(w_ytd - d_ytd_sum) > 0.1:
      failures.append(f"CC1 warehouse {w_id}: w_ytd={w_ytd} != sum(d_ytd)={d_ytd_sum}")

  # 2 & 3. D_NEXT_O_ID - 1 == max(O_ID) == max(NO_O_ID), and new_order contiguous
  for w_id in range(1, warehouses + 1):
    for d_id in range(1, districts_per_w + 1):
      next_o = int(
          sess.query_rows(
              f"select d_next_o_id from district where d_w_id={w_id} and d_id={d_id};"
          )[0][0]
      )
      max_o = int(
          sess.query_rows(
              f"select max(o_id) from orders where o_w_id={w_id} and o_d_id={d_id};"
          )[0][0]
      )
      if next_o - 1 != max_o:
        failures.append(
            f"CC2 ({w_id},{d_id}): d_next_o_id-1={next_o - 1} != max(o_id)={max_o}"
        )
      no_rows = sess.query_rows(
          f"select min(no_o_id), max(no_o_id), count(*) from new_order "
          f"where no_w_id={w_id} and no_d_id={d_id};"
      )
      cnt_no = int(no_rows[0][2])
      if cnt_no > 0:
        min_no = int(no_rows[0][0])
        max_no = int(no_rows[0][1])
        if max_no != max_o:
          failures.append(f"CC2 ({w_id},{d_id}): max(no_o_id)={max_no} != max(o_id)={max_o}")
        if max_no - min_no + 1 != cnt_no:
          failures.append(
              f"CC3 ({w_id},{d_id}): max-min+1={max_no - min_no + 1} != count(new_order)={cnt_no}"
          )

  # 4. sum(O_OL_CNT) == count(order_line)
  for w_id in range(1, warehouses + 1):
    sum_ol_cnt = int(
        sess.query_rows(f"select sum(o_ol_cnt) from orders where o_w_id={w_id};")[0][0]
    )
    cnt_ol = int(sess.query_rows(f"select count(*) from order_line where ol_w_id={w_id};")[0][0])
    if sum_ol_cnt != cnt_ol:
      failures.append(f"CC4 warehouse {w_id}: sum(o_ol_cnt)={sum_ol_cnt} != count(order_line)={cnt_ol}")

  # 5. orders.o_carrier_id == 0 iff present in new_order
  pending_orders = {
      (int(r[0]), int(r[1]), int(r[2]))
      for r in sess.query_rows("select o_w_id, o_d_id, o_id from orders where o_carrier_id=0;")
  }
  new_orders = {
      (int(r[0]), int(r[1]), int(r[2]))
      for r in sess.query_rows("select no_w_id, no_d_id, no_o_id from new_order;")
  }
  if pending_orders != new_orders:
    failures.append(
        f"CC5 mismatch: pending_orders={len(pending_orders)} vs new_orders={len(new_orders)}"
    )

  # 6. sum(H_AMOUNT) == sum(W_YTD) - initial_ytd_total
  h_rows = sess.query_rows("select sum(h_amount), count(*) from history;")
  h_sum = float(h_rows[0][0]) if h_rows[0][0].upper() != "NULL" else 0.0
  h_cnt = int(h_rows[0][1])
  w_ytd_total = float(sess.query_rows("select sum(w_ytd) from warehouse;")[0][0])
  if abs(h_sum - (w_ytd_total - initial_ytd_total)) > 0.2:
    failures.append(
        f"CC6: sum(h_amount)={h_sum} != delta(w_ytd)={w_ytd_total - initial_ytd_total}"
    )

  # Deterministic state snapshot
  snapshot = {
      "warehouse_ytd": round(w_ytd_total, 2),
      "orders_count": int(sess.query_rows("select count(*) from orders;")[0][0]),
      "new_order_count": len(new_orders),
      "order_line_count": int(sess.query_rows("select count(*) from order_line;")[0][0]),
      "history_count": h_cnt,
      "history_amount_sum": round(h_sum, 2),
      "customer_balance_sum": round(
          float(sess.query_rows("select sum(c_balance) from customer;")[0][0]), 2
      ),
      "stock_qty_sum": int(sess.query_rows("select sum(s_quantity) from stock;")[0][0]),
  }
  digest = hashlib.sha256(json.dumps(snapshot, sort_keys=True).encode()).hexdigest()[:16]
  return len(failures) == 0, failures, digest, snapshot


def percentile(values: list[float], pct: float) -> float:
  if not values:
    return 0.0
  ordered = sorted(values)
  idx = min(len(ordered) - 1, max(0, int(round((pct / 100.0) * (len(ordered) - 1)))))
  return ordered[idx]


def main() -> int:
  parser = argparse.ArgumentParser(description="MiniOB TPC-C benchmark and consistency suite")
  parser.add_argument(
      "--observer",
      default=os.environ.get("MINIOB_OBSERVER", str(REPO / "build_release" / "bin" / "observer")),
  )
  parser.add_argument("--engine", choices=["heap", "btree", "lsm"], default="heap")
  parser.add_argument("--trx", choices=["mvcc", "occ", "lsm", "vacuous"], default="mvcc")
  parser.add_argument("--warehouses", type=int, default=1)
  parser.add_argument("--districts", type=int, default=4)
  parser.add_argument("--customers", type=int, default=30)
  parser.add_argument("--items", type=int, default=100)
  parser.add_argument("--initial-orders", type=int, default=20)
  parser.add_argument("--threads", type=int, default=1)
  parser.add_argument("--transactions", type=int, default=200)
  parser.add_argument("--seed", type=int, default=42)
  parser.add_argument("--output-json", default="")
  parser.add_argument("--keep", action="store_true")
  args = parser.parse_args()

  root = Path(tempfile.mkdtemp(prefix="miniob-tpcc-"))
  server = Server(root, args.observer, trx=args.trx, durability="disk")
  try:
    server.start()
    init_sess = server.connect()
    t0 = time.perf_counter()
    create_tpcc_schema(init_sess, args.engine)
    initial_ytd = load_tpcc_data(
        init_sess,
        args.warehouses,
        args.districts,
        args.customers,
        args.items,
        args.initial_orders,
        args.seed,
    )
    load_sec = time.perf_counter() - t0
    init_sess.close()
    log(f"Loaded TPC-C data (engine={args.engine}, trx={args.trx}) in {load_sec:.2f}s")

    cfg = TpccWorkerConfig(args.warehouses, args.districts, args.customers, args.items)
    tx_per_thread = max(1, args.transactions // args.threads)
    stats_lock = threading.Lock()
    latencies_ms: dict[str, list[float]] = {
        "new_order": [],
        "payment": [],
        "order_status": [],
        "delivery": [],
        "stock_level": [],
    }
    committed = {k: 0 for k in latencies_ms}
    aborted = {k: 0 for k in latencies_ms}
    fatal_errors: list[str] = []

    def worker(tid: int) -> None:
      rng = random.Random(args.seed + tid * 1009)
      sess = server.connect()
      try:
        for _ in range(tx_per_thread):
          roll = rng.random()
          if roll < 0.45:
            tx_name, fn = "new_order", do_new_order
          elif roll < 0.88:
            tx_name, fn = "payment", do_payment
          elif roll < 0.92:
            tx_name, fn = "order_status", do_order_status
          elif roll < 0.96:
            tx_name, fn = "delivery", do_delivery
          else:
            tx_name, fn = "stock_level", do_stock_level

          start = time.perf_counter()
          try:
            fn(sess, rng, cfg)
            elapsed_ms = (time.perf_counter() - start) * 1000.0
            with stats_lock:
              committed[tx_name] += 1
              latencies_ms[tx_name].append(elapsed_ms)
          except TrxConflictError:
            with stats_lock:
              aborted[tx_name] += 1
          except Exception as ex:  # noqa: BLE001
            with stats_lock:
              fatal_errors.append(f"worker-{tid} ({tx_name}): {ex}")
            return
      finally:
        sess.close()

    run_start = time.perf_counter()
    workers = [threading.Thread(target=worker, args=(i,)) for i in range(args.threads)]
    for w in workers:
      w.start()
    for w in workers:
      w.join()
    elapsed_sec = max(1e-6, time.perf_counter() - run_start)

    if server.proc.poll() is not None:
      server.dump()
      fatal_errors.append(f"observer crashed with exit code {server.proc.returncode}")

    check_sess = server.connect()
    cc_ok, cc_failures, digest, snapshot = verify_consistency_and_digest(
        check_sess, args.warehouses, args.districts, initial_ytd
    )
    check_sess.close()

    total_committed = sum(committed.values())
    total_aborted = sum(aborted.values())
    all_lats = [v for vals in latencies_ms.values() for v in vals]
    tps = total_committed / elapsed_sec
    tpmc = (committed["new_order"] / elapsed_sec) * 60.0

    report = {
        "config": {
            "engine": args.engine,
            "trx": args.trx,
            "warehouses": args.warehouses,
            "districts": args.districts,
            "customers": args.customers,
            "items": args.items,
            "threads": args.threads,
            "transactions": args.transactions,
            "seed": args.seed,
        },
        "load_sec": round(load_sec, 3),
        "elapsed_sec": round(elapsed_sec, 3),
        "total_committed": total_committed,
        "total_aborted": total_aborted,
        "tps": round(tps, 2),
        "tpmC": round(tpmc, 2),
        "latency_ms": {
            "avg": round(sum(all_lats) / len(all_lats), 3) if all_lats else 0.0,
            "p50": round(percentile(all_lats, 50), 3),
            "p95": round(percentile(all_lats, 95), 3),
            "p99": round(percentile(all_lats, 99), 3),
        },
        "by_tx": {
            k: {
                "committed": committed[k],
                "aborted": aborted[k],
                "avg_ms": round(sum(latencies_ms[k]) / len(latencies_ms[k]), 3)
                if latencies_ms[k]
                else 0.0,
                "p95_ms": round(percentile(latencies_ms[k], 95), 3),
            }
            for k in latencies_ms
        },
        "consistency": {
            "passed": cc_ok and not fatal_errors,
            "failures": cc_failures + fatal_errors,
        },
        "deterministic_digest": digest,
        "snapshot": snapshot,
    }

    if args.output_json:
      out_path = Path(args.output_json)
      out_path.parent.mkdir(parents=True, exist_ok=True)
      out_path.write_text(json.dumps(report, indent=2) + "\n")

    log(
        f"TPC-C Summary: committed={total_committed}, aborted={total_aborted}, "
        f"elapsed={elapsed_sec:.2f}s, TPS={tps:.2f}, tpmC={tpmc:.2f}, "
        f"p95={report['latency_ms']['p95']}ms, digest={digest}"
    )
    if not report["consistency"]["passed"]:
      for err in report["consistency"]["failures"]:
        log(f"CONSISTENCY FAILURE: {err}")
      return 1

    log("TPC-C CONSISTENCY PASSED (all 6 invariants verified)")
    return 0
  finally:
    server.stop()
    if not args.keep:
      shutil.rmtree(root, ignore_errors=True)
    else:
      log(f"kept workdir: {root}")


if __name__ == "__main__":
  sys.exit(main())
