#!/usr/bin/env python3
"""
pgsalesgen.py

Empty DB -> create schema/tables -> fill masters -> generate ~target GB of sales-like data FAST.

Speed upgrades in this version:
- COPY BINARY payload is built by struct.pack_into() into a big bytearray (min allocations)
- channel/status are kept as uint8 indices (no massive Python list[str] creation)
- cp.write() is called with large memoryview chunks (flush by bytes, not rows)

Also fixed:
- Previous version could hang after printing:
    [done] target reached; stopping workers...
  because workers might be blocked on q.get() and never see the stop condition,
  while the coordinator only sent one None per worker after setting stop_evt.
  This version:
    - uses sentinel None to stop workers reliably
    - does not rely on stop_evt for the normal stop path
    - closes/join_thread() on the Queue to avoid lingering feeder threads
    - joins workers without a short timeout (or uses a generous one)

psql-compatible flags:
- -h host, -p port, -U user, -d dbname
(argparse help is remapped to --help / -?)

Usage:
  pgsalesgen -h localhost -p 5432 -U postgres -d emptydb --target-gb 10 --workers 8

"""

from __future__ import annotations

import argparse
import os
import struct
import sys
import time
from dataclasses import dataclass
from datetime import datetime, timezone
from multiprocessing import Event, Process, Queue

import numpy as np
import psycopg

# -----------------------------
# PostgreSQL COPY BINARY helpers
# -----------------------------
PGCOPY_SIGNATURE = b"PGCOPY\n\xff\r\n\0"
EPOCH_2000 = datetime(2000, 1, 1, tzinfo=timezone.utc)
UNIX_EPOCH = datetime(1970, 1, 1, tzinfo=timezone.utc)

S_I16 = struct.Struct("!h")
S_I32 = struct.Struct("!i")
S_I64 = struct.Struct("!q")


# -----------------------------
# Connection (psql-compatible)
# -----------------------------
def build_libpq_dsn(args) -> str:
    if args.dsn:
        return args.dsn

    parts: list[str] = []
    if args.host:
        parts.append(f"host={args.host}")
    if args.port:
        parts.append(f"port={args.port}")
    if args.user:
        parts.append(f"user={args.user}")
    if args.dbname:
        parts.append(f"dbname={args.dbname}")
    if args.password:
        parts.append(f"password={args.password}")
    if args.sslmode:
        parts.append(f"sslmode={args.sslmode}")
    if args.options:
        parts.append(f"options={args.options}")

    return " ".join(parts) if parts else ""


def psql_equivalent_cmd(args) -> str:
    cmd = ["psql"]
    if args.host:
        cmd += ["-h", args.host]
    if args.port:
        cmd += ["-p", str(args.port)]
    if args.user:
        cmd += ["-U", args.user]
    if args.dbname:
        cmd += ["-d", args.dbname]

    prefix = ""
    if args.password:
        prefix += "PGPASSWORD='***' "
    if args.sslmode:
        prefix += f"PGSSLMODE='{args.sslmode}' "
    if args.options:
        prefix += f"PGOPTIONS='{args.options}' "
    return prefix + " ".join(cmd)


# -----------------------------
# Schema / DDL
# -----------------------------
DDL_TEMPLATE = """
CREATE SCHEMA IF NOT EXISTS sales;

-- Drop in dependency order
DROP TABLE IF EXISTS sales.order_items;
DROP TABLE IF EXISTS sales.orders;
DROP TABLE IF EXISTS sales.products;
DROP TABLE IF EXISTS sales.customers;

{create_table} sales.customers (
  customer_id  bigserial PRIMARY KEY,
  created_at   timestamptz NOT NULL DEFAULT now(),
  name         text NOT NULL,
  email        text NOT NULL,
  region       text NOT NULL
);

{create_table} sales.products (
  product_id   bigserial PRIMARY KEY,
  sku          text NOT NULL,
  name         text NOT NULL,
  category     text NOT NULL,
  price_cents  int  NOT NULL
);

-- order_id is BIGINT and provided by generator (worker range), not serial.
{create_table} sales.orders (
  order_id       bigint PRIMARY KEY,
  ordered_at     timestamptz NOT NULL,
  customer_id    bigint NOT NULL,
  channel        text NOT NULL,
  status         text NOT NULL,
  subtotal_cents int NOT NULL,
  tax_cents      int NOT NULL,
  shipping_cents int NOT NULL,
  total_cents    int NOT NULL,
  note           text NOT NULL
);

{create_table} sales.order_items (
  order_item_id    bigserial PRIMARY KEY,
  order_id         bigint NOT NULL,
  product_id       bigint NOT NULL,
  qty              int    NOT NULL,
  unit_price_cents int    NOT NULL,
  line_total_cents int    NOT NULL,
  note             text NOT NULL
);

{fk}
"""


def create_schema_and_tables(conn, logged: bool, with_fk: bool) -> None:
    create_table = "CREATE TABLE" if logged else "CREATE UNLOGGED TABLE"
    fk_sql = ""
    if with_fk:
        fk_sql = """
ALTER TABLE sales.orders
  ADD CONSTRAINT orders_customer_fk
  FOREIGN KEY (customer_id) REFERENCES sales.customers(customer_id);

ALTER TABLE sales.order_items
  ADD CONSTRAINT items_order_fk
  FOREIGN KEY (order_id) REFERENCES sales.orders(order_id);

ALTER TABLE sales.order_items
  ADD CONSTRAINT items_product_fk
  FOREIGN KEY (product_id) REFERENCES sales.products(product_id);
"""
    ddl = DDL_TEMPLATE.format(create_table=create_table, fk=fk_sql)
    conn.execute(ddl)
    conn.commit()


def create_indexes(conn) -> None:
    conn.execute(
        "CREATE INDEX IF NOT EXISTS orders_ordered_at_brin ON sales.orders USING brin (ordered_at);"
    )
    conn.execute(
        "CREATE INDEX IF NOT EXISTS orders_customer_id_idx ON sales.orders (customer_id);"
    )
    conn.execute(
        "CREATE INDEX IF NOT EXISTS items_order_id_idx ON sales.order_items (order_id);"
    )
    conn.execute(
        "CREATE INDEX IF NOT EXISTS items_product_id_idx ON sales.order_items (product_id);"
    )
    conn.execute("ANALYZE sales.customers;")
    conn.execute("ANALYZE sales.products;")
    conn.execute("ANALYZE sales.orders;")
    conn.execute("ANALYZE sales.order_items;")
    conn.commit()


# -----------------------------
# Generation utilities (NumPy)
# -----------------------------
def parse_ymd(s: str) -> datetime:
    y, m, d = map(int, s.split("-"))
    return datetime(y, m, d, tzinfo=timezone.utc)


def us_since_2000_from_unix_seconds(unix_seconds: np.ndarray) -> np.ndarray:
    base_unix_2000 = int((EPOCH_2000 - UNIX_EPOCH).total_seconds())
    return (unix_seconds - base_unix_2000).astype(np.int64) * 1_000_000


def fixed_note(length: int, tag: str) -> str:
    if length <= 0:
        return ""
    return (tag * ((length // max(1, len(tag))) + 1))[:length]


@dataclass(frozen=True)
class BatchConfig:
    avg_items: float
    max_items: int
    order_note_len: int
    item_note_len: int
    start_unix: int
    span_seconds: int
    cust_max: int
    prod_max: int
    unit_price_min: int
    unit_price_max: int
    shipping_threshold: int
    shipping_fee: int
    tax_rate: float


def generate_batch_numpy(
    rng: np.random.Generator,
    start_order_id: int,
    batch_orders: int,
    cfg: BatchConfig,
):
    """
    Returns:
      orders: dict of numpy arrays (no Python list[str])
      items: dict of numpy arrays
      next_order_id: int
    """
    n = batch_orders
    order_ids = np.arange(start_order_id, start_order_id + n, dtype=np.int64)

    offsets = rng.integers(0, cfg.span_seconds, size=n, dtype=np.int64)
    ordered_unix = (cfg.start_unix + offsets).astype(np.int64)
    ordered_us2000 = us_since_2000_from_unix_seconds(ordered_unix)

    customer_ids = rng.integers(1, cfg.cust_max + 1, size=n, dtype=np.int64)

    # store indices only (uint8)
    channel_idx = rng.integers(0, 3, size=n, dtype=np.uint8)  # 0..2
    status_idx = rng.integers(0, 3, size=n, dtype=np.uint8)  # 0..2

    lam = max(0.1, float(cfg.avg_items))
    k = rng.poisson(lam=lam, size=n).astype(np.int64)
    k = np.clip(k, 1, cfg.max_items)
    total_items = int(k.sum())

    item_order_ids = np.repeat(order_ids, k).astype(np.int64)
    product_ids = rng.integers(1, cfg.prod_max + 1, size=total_items, dtype=np.int64)
    qty = rng.integers(1, 6, size=total_items, dtype=np.int32)
    unit = rng.integers(
        cfg.unit_price_min, cfg.unit_price_max + 1, size=total_items, dtype=np.int32
    )
    line_total = (unit.astype(np.int64) * qty.astype(np.int64)).astype(np.int64)

    boundaries = np.concatenate(([0], np.cumsum(k)[:-1]))
    subtotal = np.add.reduceat(line_total, boundaries).astype(np.int64)

    tax = (subtotal.astype(np.float64) * cfg.tax_rate).astype(np.int64)
    shipping = np.where(subtotal >= cfg.shipping_threshold, 0, cfg.shipping_fee).astype(
        np.int64
    )
    total = (subtotal + tax + shipping).astype(np.int64)

    orders = {
        "order_id": order_ids,
        "ordered_us2000": ordered_us2000,
        "customer_id": customer_ids,
        "channel_idx": channel_idx,
        "status_idx": status_idx,
        "subtotal": subtotal,
        "tax": tax,
        "shipping": shipping,
        "total": total,
    }
    items = {
        "order_id": item_order_ids,
        "product_id": product_ids,
        "qty": qty,
        "unit": unit,
        "line_total": line_total,
    }

    return orders, items, int(start_order_id + n)


# -----------------------------
# COPY BINARY writers (FAST)
# -----------------------------
def copy_orders_binary_fast(
    cur,
    orders,
    note_bytes: bytes,
    buffer_mb: int = 16,
):
    """
    Fast COPY BINARY writer for sales.orders.
    - writes into a large bytearray via pack_into
    - flushes by byte size
    """
    channel_vals = (b"web", b"store", b"marketplace")
    status_vals = (b"paid", b"shipped", b"canceled")

    channel_field = [S_I32.pack(len(v)) + v for v in channel_vals]
    status_field = [S_I32.pack(len(v)) + v for v in status_vals]
    note_field = S_I32.pack(len(note_bytes)) + note_bytes

    ncols = 10
    max_text = (
        max(len(x) for x in channel_vals)
        + max(len(x) for x in status_vals)
        + len(note_bytes)
    )
    max_row = 2 + (ncols * 4) + (8 + 8 + 8) + (4 * 4) + max_text  # conservative

    buffer_bytes = max(1, buffer_mb) * 1024 * 1024

    with cur.copy(
        "COPY sales.orders("
        "order_id, ordered_at, customer_id, channel, status, "
        "subtotal_cents, tax_cents, shipping_cents, total_cents, note"
        ") FROM STDIN WITH (FORMAT BINARY)"
    ) as cp:
        cp.write(PGCOPY_SIGNATURE + S_I32.pack(0) + S_I32.pack(0))

        oid = orders["order_id"]
        ous = orders["ordered_us2000"]
        cid = orders["customer_id"]
        chx = orders["channel_idx"]
        stx = orders["status_idx"]
        sub = orders["subtotal"]
        tax = orders["tax"]
        shp = orders["shipping"]
        tot = orders["total"]

        buf = bytearray(buffer_bytes)
        pos = 0

        for i in range(len(oid)):
            if pos + max_row >= buffer_bytes:
                cp.write(memoryview(buf)[:pos])
                pos = 0

            S_I16.pack_into(buf, pos, ncols)
            pos += 2

            # order_id int8
            S_I32.pack_into(buf, pos, 8)
            pos += 4
            S_I64.pack_into(buf, pos, int(oid[i]))
            pos += 8

            # ordered_at timestamptz int8
            S_I32.pack_into(buf, pos, 8)
            pos += 4
            S_I64.pack_into(buf, pos, int(ous[i]))
            pos += 8

            # customer_id int8
            S_I32.pack_into(buf, pos, 8)
            pos += 4
            S_I64.pack_into(buf, pos, int(cid[i]))
            pos += 8

            # channel text (prebuilt)
            cf = channel_field[int(chx[i])]
            buf[pos : pos + len(cf)] = cf
            pos += len(cf)

            # status text (prebuilt)
            sf = status_field[int(stx[i])]
            buf[pos : pos + len(sf)] = sf
            pos += len(sf)

            # subtotal int4
            S_I32.pack_into(buf, pos, 4)
            pos += 4
            S_I32.pack_into(buf, pos, int(sub[i]))
            pos += 4

            # tax int4
            S_I32.pack_into(buf, pos, 4)
            pos += 4
            S_I32.pack_into(buf, pos, int(tax[i]))
            pos += 4

            # shipping int4
            S_I32.pack_into(buf, pos, 4)
            pos += 4
            S_I32.pack_into(buf, pos, int(shp[i]))
            pos += 4

            # total int4
            S_I32.pack_into(buf, pos, 4)
            pos += 4
            S_I32.pack_into(buf, pos, int(tot[i]))
            pos += 4

            # note text (prebuilt)
            buf[pos : pos + len(note_field)] = note_field
            pos += len(note_field)

        if pos:
            cp.write(memoryview(buf)[:pos])

        cp.write(S_I16.pack(-1))


def copy_items_binary_fast(
    cur,
    items,
    note_bytes: bytes,
    buffer_mb: int = 32,
):
    """
    Fast COPY BINARY writer for sales.order_items.
    """
    note_field = S_I32.pack(len(note_bytes)) + note_bytes

    ncols = 6
    max_row = 2 + (ncols * 4) + (8 + 8 + 4 + 4 + 4) + len(note_bytes)
    buffer_bytes = max(1, buffer_mb) * 1024 * 1024

    with cur.copy(
        "COPY sales.order_items("
        "order_id, product_id, qty, unit_price_cents, line_total_cents, note"
        ") FROM STDIN WITH (FORMAT BINARY)"
    ) as cp:
        cp.write(PGCOPY_SIGNATURE + S_I32.pack(0) + S_I32.pack(0))

        oid = items["order_id"]
        pid = items["product_id"]
        qty = items["qty"]
        unit = items["unit"]
        line = items["line_total"]

        buf = bytearray(buffer_bytes)
        pos = 0

        for i in range(len(oid)):
            if pos + max_row >= buffer_bytes:
                cp.write(memoryview(buf)[:pos])
                pos = 0

            S_I16.pack_into(buf, pos, ncols)
            pos += 2

            # order_id int8
            S_I32.pack_into(buf, pos, 8)
            pos += 4
            S_I64.pack_into(buf, pos, int(oid[i]))
            pos += 8

            # product_id int8
            S_I32.pack_into(buf, pos, 8)
            pos += 4
            S_I64.pack_into(buf, pos, int(pid[i]))
            pos += 8

            # qty int4
            S_I32.pack_into(buf, pos, 4)
            pos += 4
            S_I32.pack_into(buf, pos, int(qty[i]))
            pos += 4

            # unit_price int4
            S_I32.pack_into(buf, pos, 4)
            pos += 4
            S_I32.pack_into(buf, pos, int(unit[i]))
            pos += 4

            # line_total int4
            S_I32.pack_into(buf, pos, 4)
            pos += 4
            S_I32.pack_into(buf, pos, int(line[i]))
            pos += 4

            # note
            buf[pos : pos + len(note_field)] = note_field
            pos += len(note_field)

        if pos:
            cp.write(memoryview(buf)[:pos])

        cp.write(S_I16.pack(-1))


# -----------------------------
# Master fillers
# -----------------------------
def fill_masters(conn, customers: int, products: int) -> None:
    conn.execute(
        """
        INSERT INTO sales.customers(name, email, region)
        SELECT
          'Customer-' || gs::text,
          'user' || gs::text || '@example.com',
          (ARRAY['JP','US','EU','APAC','LATAM'])[1 + (random()*4)::int]
        FROM generate_series(1, %s) gs;
        """,
        (customers,),
    )

    conn.execute(
        """
        INSERT INTO sales.products(sku, name, category, price_cents)
        SELECT
          'SKU-' || gs::text,
          'Product-' || gs::text,
          (ARRAY['food','apparel','home','electronics','book','beauty'])[1 + (random()*5)::int],
          (500 + (random()*20000)::int)
        FROM generate_series(1, %s) gs;
        """,
        (products,),
    )
    conn.commit()


# -----------------------------
# Monitoring
# -----------------------------
def current_total_gb(conn) -> float:
    b = conn.execute(
        "SELECT pg_total_relation_size('sales.orders'::regclass) + "
        "pg_total_relation_size('sales.order_items'::regclass)"
    ).fetchone()[0]
    return float(b) / (1024.0**3)


# -----------------------------
# Worker
# -----------------------------
def worker_proc(
    worker_id: int,
    dsn: str,
    q: Queue,
    stop_evt: Event,
    cfg: BatchConfig,
    base_seed: int,
    order_id_stride: int,
    batch_orders: int,
    copy_orders_buf_mb: int,
    copy_items_buf_mb: int,
):
    rng = np.random.default_rng(base_seed + worker_id)
    conn = psycopg.connect(dsn)

    # note bytes: pre-encoded once per worker
    ord_note_bytes = fixed_note(cfg.order_note_len, f"w{worker_id:02d}-ORDER-").encode(
        "utf-8"
    )
    itm_note_bytes = fixed_note(cfg.item_note_len, f"w{worker_id:02d}-ITEM-").encode(
        "utf-8"
    )

    try:
        with conn.cursor() as cur:
            cur.execute("SET synchronous_commit=off")
            cur.execute("SET client_min_messages=warning")
            cur.execute("SET work_mem='256MB'")

        next_order_id = 1 + worker_id * order_id_stride

        while True:
            msg = q.get()  # blocks
            if msg is None:
                break
            if stop_evt.is_set():
                break

            orders, items, next_order_id = generate_batch_numpy(
                rng=rng,
                start_order_id=next_order_id,
                batch_orders=batch_orders,
                cfg=cfg,
            )

            with conn.cursor() as cur:
                copy_orders_binary_fast(
                    cur, orders, note_bytes=ord_note_bytes, buffer_mb=copy_orders_buf_mb
                )
                copy_items_binary_fast(
                    cur, items, note_bytes=itm_note_bytes, buffer_mb=copy_items_buf_mb
                )

            conn.commit()

    except Exception as e:
        print(f"[worker {worker_id}] ERROR: {e}", file=sys.stderr)
        try:
            conn.rollback()
        except Exception:
            pass
        raise
    finally:
        conn.close()


# -----------------------------
# Main
# -----------------------------
def main() -> int:
    # argparse default -h conflicts with psql's -h(host).
    ap = argparse.ArgumentParser(
        description="Empty DB -> create schema/tables -> fill masters -> generate sales-like data FAST.",
        add_help=False,
    )
    ap.add_argument(
        "--help", "-?", action="help", help="show this help message and exit"
    )

    # Connection (psql-compatible)
    ap.add_argument(
        "--dsn",
        default=os.environ.get("PG_DSN"),
        help="libpq DSN. Overrides -h/-p/-U/-d.",
    )
    ap.add_argument(
        "-h",
        "--host",
        default=None,
        help="database server host or socket directory (psql compatible).",
    )
    ap.add_argument(
        "-p",
        "--port",
        type=int,
        default=None,
        help="database server port (psql compatible).",
    )
    ap.add_argument(
        "-U", "--user", default=None, help="database user name (psql compatible)."
    )
    ap.add_argument(
        "-d", "--dbname", default=None, help="database name (psql compatible)."
    )
    ap.add_argument(
        "--password",
        default=None,
        help="database password (or use PGPASSWORD env / .pgpass).",
    )
    ap.add_argument(
        "--sslmode", default=None, help="sslmode (require, verify-full, etc.)."
    )
    ap.add_argument(
        "--options",
        default=None,
        help='libpq options string (e.g., "-c statement_timeout=0").',
    )
    ap.add_argument(
        "--print-psql",
        action="store_true",
        help="Print equivalent psql command and exit.",
    )

    # DDL options
    ap.add_argument(
        "--logged",
        action="store_true",
        help="Create LOGGED tables (default UNLOGGED for speed).",
    )
    ap.add_argument(
        "--with-fk", action="store_true", help="Create foreign keys (slower)."
    )
    ap.add_argument(
        "--create-indexes",
        action="store_true",
        help="Create typical indexes + ANALYZE after load.",
    )

    # Masters
    ap.add_argument(
        "--customers",
        type=int,
        default=2_000_000,
        help="Number of customers to generate.",
    )
    ap.add_argument(
        "--products", type=int, default=200_000, help="Number of products to generate."
    )

    # Generation
    ap.add_argument(
        "--target-gb",
        type=float,
        default=100.0,
        help="Target size (orders+items) in GB.",
    )
    ap.add_argument(
        "--workers",
        type=int,
        default=max(1, os.cpu_count() or 1),
        help="Number of worker processes.",
    )
    ap.add_argument(
        "--batch-orders", type=int, default=200_000, help="Orders per batch per worker."
    )
    ap.add_argument(
        "--avg-items",
        type=float,
        default=3.2,
        help="Average number of items per order.",
    )
    ap.add_argument("--max-items", type=int, default=12, help="Max items per order.")
    ap.add_argument("--order-note-len", type=int, default=80, help="Order note length.")
    ap.add_argument("--item-note-len", type=int, default=120, help="Item note length.")
    ap.add_argument(
        "--start-date", default="2022-01-01", help="Start date YYYY-MM-DD (UTC)."
    )
    ap.add_argument(
        "--end-date", default="2026-01-01", help="End date YYYY-MM-DD (UTC)."
    )
    ap.add_argument(
        "--unit-price-min", type=int, default=500, help="Min unit price (cents)."
    )
    ap.add_argument(
        "--unit-price-max", type=int, default=20_500, help="Max unit price (cents)."
    )
    ap.add_argument(
        "--tax-rate", type=float, default=0.10, help="Tax rate (e.g., 0.10)."
    )
    ap.add_argument(
        "--shipping-threshold",
        type=int,
        default=5000,
        help="Free shipping threshold (cents).",
    )
    ap.add_argument(
        "--shipping-fee",
        type=int,
        default=500,
        help="Shipping fee under threshold (cents).",
    )
    ap.add_argument(
        "--progress-interval",
        type=float,
        default=2.0,
        help="Seconds between progress prints.",
    )
    ap.add_argument("--seed", type=int, default=12345, help="Base RNG seed.")
    ap.add_argument(
        "--order-id-stride",
        type=int,
        default=10_000_000_000,
        help="Per-worker order_id stride (must exceed total orders per worker).",
    )

    # COPY buffer sizes (MB)
    ap.add_argument(
        "--copy-orders-buf-mb",
        type=int,
        default=16,
        help="COPY buffer for orders (MB).",
    )
    ap.add_argument(
        "--copy-items-buf-mb", type=int, default=32, help="COPY buffer for items (MB)."
    )

    # shutdown behavior
    ap.add_argument(
        "--join-timeout-sec",
        type=float,
        default=0.0,
        help="If >0, timeout seconds for joining each worker. 0 means wait indefinitely.",
    )

    args = ap.parse_args()
    dsn = build_libpq_dsn(args)

    if args.print_psql:
        print(psql_equivalent_cmd(args))
        return 0

    start_dt = parse_ymd(args.start_date)
    end_dt = parse_ymd(args.end_date)
    span = int((end_dt - start_dt).total_seconds())
    if span <= 0:
        print("end-date must be after start-date", file=sys.stderr)
        return 2

    # Coordinator connection
    coord = psycopg.connect(dsn)
    coord.execute("SET client_min_messages=warning")
    coord.execute("SET synchronous_commit=off")

    print("[setup] creating schema/tables...")
    create_schema_and_tables(coord, logged=args.logged, with_fk=args.with_fk)

    print(
        f"[setup] inserting masters: customers={args.customers:,} products={args.products:,} ..."
    )
    fill_masters(coord, customers=args.customers, products=args.products)

    cfg = BatchConfig(
        avg_items=args.avg_items,
        max_items=args.max_items,
        order_note_len=args.order_note_len,
        item_note_len=args.item_note_len,
        start_unix=int(start_dt.timestamp()),
        span_seconds=span,
        cust_max=args.customers,
        prod_max=args.products,
        unit_price_min=args.unit_price_min,
        unit_price_max=args.unit_price_max,
        shipping_threshold=args.shipping_threshold,
        shipping_fee=args.shipping_fee,
        tax_rate=args.tax_rate,
    )

    # Work queue
    q: Queue = Queue(maxsize=args.workers * 4)
    stop_evt = Event()

    # Start workers (NOT daemon: allow clean join)
    procs: list[Process] = []
    for wid in range(args.workers):
        p = Process(
            target=worker_proc,
            args=(
                wid,
                dsn,
                q,
                stop_evt,
                cfg,
                args.seed,
                args.order_id_stride,
                args.batch_orders,
                args.copy_orders_buf_mb,
                args.copy_items_buf_mb,
            ),
            daemon=False,
        )
        p.start()
        procs.append(p)

    try:
        last_print = 0.0

        while True:
            gb = current_total_gb(coord)
            now = time.time()
            if now - last_print >= args.progress_interval:
                print(f"[progress] {gb:.2f} GB / {args.target_gb:.2f} GB")
                last_print = now

            if gb >= args.target_gb:
                break

            # enqueue one batch per worker
            for _ in range(args.workers):
                q.put(1)

        print("[done] target reached; stopping workers...")

    finally:
        # Normal stop path: send sentinels so workers break out of q.get()
        stop_evt.set()

        # Make sure we enqueue enough sentinels even if queue is partly full.
        # Block until they're all sent.
        for _ in procs:
            q.put(None)

        # Clean up queue feeder threads
        try:
            q.close()
            q.join_thread()
        except Exception:
            pass

        # Join workers reliably
        for p in procs:
            if args.join_timeout_sec and args.join_timeout_sec > 0:
                p.join(timeout=args.join_timeout_sec)
            else:
                p.join()

            if p.exitcode not in (0, None):
                print(f"[warn] worker exited with code {p.exitcode}", file=sys.stderr)

        coord.close()

    # Optional indexes after load
    if args.create_indexes:
        # need a new connection because coord is closed above
        coord2 = psycopg.connect(dsn)
        try:
            print("[post] creating indexes + analyze...")
            create_indexes(coord2)
        finally:
            coord2.close()

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
