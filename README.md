# Retail Data Pipeline

An end-to-end batch data pipeline that loads ~100,000 real e-commerce orders into
**BigQuery** and models them with **dbt** into tested, documented, analytics-ready tables.

**Stack:** Python · BigQuery (GCP) · dbt · DuckDB · SQL · Git

---

## Architecture

```
Olist CSVs  →  Python  →  BigQuery `raw`  →  dbt staging  →  dbt mart  →  tests + docs
                                              (5 views)       (1 view)
```

The pipeline follows the standard modern data stack pattern — **ingest → stage → model → test → document** — with a layered warehouse design.

| Layer | Dataset | Materialisation | Purpose |
|-------|---------|-----------------|---------|
| **raw** | `raw` | tables (loaded by Python) | Source CSVs loaded untouched. Never modified. |
| **staging** | `analytics` | 5 views | One thin model per source table — column selection and renaming only, no logic. |
| **mart** | `analytics` | 1 view (`fct_sales`) | Four staging models joined into an analytics-ready fact table. |

Keeping `raw` immutable means the warehouse can always be rebuilt from the original data
by re-running dbt.

---

## The Data

[Brazilian E-Commerce Public Dataset by Olist](https://www.kaggle.com/datasets/olistbr/brazilian-ecommerce)
— ~100,000 anonymised orders placed between 2016 and 2018, spread across 9 linked CSV files.

The files are relational, not flat: orders link to customers and to order items, which link
to products, via shared ID columns. Rejoining them into a usable fact table is the core of
the transformation layer.

All 9 files are loaded into `raw` to preserve the complete source. The mart is built from
five of them via staging models (`orders`, `customers`, `order_items`, `products`,
`order_payments`); the remaining four are available in `raw` for future models.

---

## Repository Contents

| File | Purpose |
|------|---------|
| `load_bigquery.py` | **Primary ingestion** — loads the 9 raw CSVs into the BigQuery `raw` dataset. |
| `ingest.py` | **Local ingestion** — loads the same CSVs into a local DuckDB file for offline development. |
| `retail_dbt/` | The dbt project — staging models, mart, sources, and tests. |
| `.gitignore` | Excludes the raw CSVs and the local `.duckdb` warehouse — code is versioned, data is not. |

### Why two ingestion scripts?

The project was built locally on DuckDB first, then migrated to BigQuery. The dbt models were
**unchanged** by that migration — only the adapter, the connection profile, and the loading
script differed. Both paths are kept so the same models can be run against either warehouse
(see *Running it*), keeping local iteration fast and free while the cloud path mirrors a
production setup.

---

## Data Model

**`fct_sales`** — grain: **one row per order line item** (~112,650 rows, matching the
`order_items` source).

It starts from `stg_order_items` (which sets the grain) and left-joins orders, customers, and
products:

```
stg_order_items
   ├── LEFT JOIN stg_orders     ON order_id     → order status, purchase timestamp
   ├── LEFT JOIN stg_customers  ON customer_id  → customer state
   └── LEFT JOIN stg_products   ON product_id   → product category
```

Design notes:

- **Grain drives everything.** Because an order can contain several items, `order_id` is
  intentionally **not unique** here — hence a `not_null` test on it rather than a `unique` one.
- **Left joins, not inner**, so a line item is never silently dropped if a lookup is missing.
- **Payments are staged but deliberately not joined.** They sit at a different grain
  (~104,000 payment rows vs ~99,000 orders), so joining them to a line-item fact would fan
  out rows and double-count revenue. They would need their own fact table or an order-level
  aggregate first.
- **Staging stays thin** (e.g. `order_purchase_timestamp` → `purchased_at`); business logic
  and joins live in the mart, keeping the layers separated and staging reusable.

---

## Data Quality & Documentation

- **Tests:** `not_null` constraints on `fct_sales.order_id` and `fct_sales.price`, run via `dbt test`.
- **Idempotent loads:** the BigQuery loader uses `WRITE_TRUNCATE` and the DuckDB loader uses
  `CREATE OR REPLACE`, so re-running produces the same result rather than duplicating data.
- **Robust ingestion:** the BigQuery loader sets `allow_quoted_newlines=True` to handle
  free-text review fields containing embedded line breaks.
- **Documentation:** `dbt docs generate` produces a data dictionary and an interactive lineage
  graph tracing every column from `raw` through staging to the mart.

---

## Setup

### 1. Install dependencies

```bash
pip install duckdb google-cloud-bigquery dbt-duckdb dbt-bigquery
```

### 2. Get the data

Download the [Olist dataset](https://www.kaggle.com/datasets/olistbr/brazilian-ecommerce)
and place the 9 CSVs in a `data/` folder in the project root. (This folder is git-ignored.)

### 3. Authenticate to GCP

```bash
gcloud auth application-default login
gcloud config set project <your-gcp-project-id>
```

The dbt profile uses `method: oauth`, so no service-account key files are needed or stored.

### 4. Configure dbt

dbt's connection settings live outside the repo at `~/.dbt/profiles.yml`
(`C:\Users\<you>\.dbt\profiles.yml` on Windows):

```yaml
retail_dbt:
  target: dev
  outputs:
    dev:
      type: bigquery
      method: oauth
      project: <your-gcp-project-id>
      dataset: analytics
      location: US
      threads: 4
    duckdb:
      type: duckdb
      path: ../retail.duckdb
      threads: 1
```

Also set `PROJECT` at the top of `load_bigquery.py` to your own GCP project ID.

---

## Running it

**Cloud path (BigQuery):**

```bash
python load_bigquery.py                  # upload raw CSVs to BigQuery
cd retail_dbt
dbt run                                   # build staging + fct_sales models
dbt test                                  # run data-quality tests
dbt docs generate && dbt docs serve       # docs + lineage graph
```

**Local path (DuckDB):**

```bash
python ingest.py                          # load raw CSVs into a local DuckDB file
cd retail_dbt
dbt run --target duckdb                    # same models, different warehouse
```

---

## Known Limitations & Next Steps

Honest scope of the current build, and what production-grade would add:

- **No orchestration** — runs are triggered manually. Next step: schedule the
  ingest → run → test sequence with Airflow or Dagster, with retries and alerting.
- **Mart materialised as a view** — `fct_sales` currently rebuilds its four-way join on every
  query. Materialising it as a table would compute the join once at build time.
- **Full refresh only** — models rebuild from scratch each run. Converting `fct_sales` to an
  incremental materialisation would avoid reprocessing unchanged history.
- **Bad-record tolerance** — the loader allows up to 100 malformed rows per file rather than
  failing. In production these should be routed to a quarantine table and alerted on, not
  skipped silently.
- **Hardcoded config** — the GCP project ID is set in the loader; this belongs in an
  environment variable for multi-environment use.
- **No CI** — tests run locally. Next step: run `dbt build` automatically on every pull request.
- **No serving layer** — `fct_sales` is query-ready but not yet connected to a BI tool.
