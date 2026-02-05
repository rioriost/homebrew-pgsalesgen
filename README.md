# PostgreSQL Sales Generator

![License](https://img.shields.io/badge/license-MIT-blue.svg)
![Python](https://img.shields.io/badge/Python-3.14%2B-blue)

PostgreSQL Sales Generator

## Table of Contents

- [Prerequisites](#prerequisites)
- [Install](#install)
- [Usage](#usage)
- [Release Notes](#release-notes)
- [License](#license)

## Prerequisites

- Python 3.14 and above
- This module runs on [psycopg](https://www.psycopg.org/)

## Install

- with brew

```bash
brew tap rioriost/pg-salesgen
brew install pg-salesgen
```

## Usage

```
[pg-salesgen --help](usage: pg-salesgen [--help] [--dsn DSN] [-h HOST] [-p PORT] [-U USER]
                   [-d DBNAME] [--password PASSWORD] [--sslmode SSLMODE]
                   [--options OPTIONS] [--print-psql] [--logged] [--with-fk]
                   [--create-indexes] [--customers CUSTOMERS]
                   [--products PRODUCTS] [--target-gb TARGET_GB]
                   [--workers WORKERS] [--batch-orders BATCH_ORDERS]
                   [--avg-items AVG_ITEMS] [--max-items MAX_ITEMS]
                   [--order-note-len ORDER_NOTE_LEN]
                   [--item-note-len ITEM_NOTE_LEN] [--start-date START_DATE]
                   [--end-date END_DATE] [--unit-price-min UNIT_PRICE_MIN]
                   [--unit-price-max UNIT_PRICE_MAX] [--tax-rate TAX_RATE]
                   [--shipping-threshold SHIPPING_THRESHOLD]
                   [--shipping-fee SHIPPING_FEE]
                   [--progress-interval PROGRESS_INTERVAL] [--seed SEED]
                   [--order-id-stride ORDER_ID_STRIDE]
                   [--copy-orders-buf-mb COPY_ORDERS_BUF_MB]
                   [--copy-items-buf-mb COPY_ITEMS_BUF_MB]
                   [--join-timeout-sec JOIN_TIMEOUT_SEC]

Empty DB -> create schema/tables -> fill masters -> generate sales-like data FAST.

options:
  --help, -?            show this help message and exit
  --dsn DSN             libpq DSN. Overrides -h/-p/-U/-d.
  -h, --host HOST       database server host or socket directory (psql
                        compatible).
  -p, --port PORT       database server port (psql compatible).
  -U, --user USER       database user name (psql compatible).
  -d, --dbname DBNAME   database name (psql compatible).
  --password PASSWORD   database password (or use PGPASSWORD env / .pgpass).
  --sslmode SSLMODE     sslmode (require, verify-full, etc.).
  --options OPTIONS     libpq options string (e.g., "-c statement_timeout=0").
  --print-psql          Print equivalent psql command and exit.
  --logged              Create LOGGED tables (default UNLOGGED for speed).
  --with-fk             Create foreign keys (slower).
  --create-indexes      Create typical indexes + ANALYZE after load.
  --customers CUSTOMERS
                        Number of customers to generate.
  --products PRODUCTS   Number of products to generate.
  --target-gb TARGET_GB
                        Target size (orders+items) in GB.
  --workers WORKERS     Number of worker processes.
  --batch-orders BATCH_ORDERS
                        Orders per batch per worker.
  --avg-items AVG_ITEMS
                        Average number of items per order.
  --max-items MAX_ITEMS
                        Max items per order.
  --order-note-len ORDER_NOTE_LEN
                        Order note length.
  --item-note-len ITEM_NOTE_LEN
                        Item note length.
  --start-date START_DATE
                        Start date YYYY-MM-DD (UTC).
  --end-date END_DATE   End date YYYY-MM-DD (UTC).
  --unit-price-min UNIT_PRICE_MIN
                        Min unit price (cents).
  --unit-price-max UNIT_PRICE_MAX
                        Max unit price (cents).
  --tax-rate TAX_RATE   Tax rate (e.g., 0.10).
  --shipping-threshold SHIPPING_THRESHOLD
                        Free shipping threshold (cents).
  --shipping-fee SHIPPING_FEE
                        Shipping fee under threshold (cents).
  --progress-interval PROGRESS_INTERVAL
                        Seconds between progress prints.
  --seed SEED           Base RNG seed.
  --order-id-stride ORDER_ID_STRIDE
                        Per-worker order_id stride (must exceed total orders
                        per worker).
  --copy-orders-buf-mb COPY_ORDERS_BUF_MB
                        COPY buffer for orders (MB).
  --copy-items-buf-mb COPY_ITEMS_BUF_MB
                        COPY buffer for items (MB).
  --join-timeout-sec JOIN_TIMEOUT_SEC
                        If >0, timeout seconds for joining each worker. 0
                        means wait indefinitely.
)
```

## Release Notes

### 0.1.0 Release
- Initial release

## License

MIT License
