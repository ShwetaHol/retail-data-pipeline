# Retail Data Pipeline

An **end-to-end batch data pipeline** that loads **~100,000 real e-commerce orders** into
**BigQuery** and models them into **tested, documented, analytics-ready tables**. The
transformation layer is built **twice, in dbt and in Dataform**, on the same data. This shows
both the **warehouse-agnostic** approach (dbt) and the **BigQuery-native** one (Dataform).

**Stack:** Python, BigQuery (GCP), dbt, Dataform, Airflow, Docker, DuckDB, SQL, Git

---

## Architecture

```
Olist CSVs  ->  Python  ->  BigQuery (raw)  ->  transform  ->  tests + docs
                                                |
                                                +-- dbt       ->  analytics dataset
                                                +-- Dataform  ->  dataform dataset
```

The pipeline follows the standard modern data stack pattern (**ingest, stage, model, test,
document**) with a **layered warehouse design**. The same models are built by two tools:

| Layer | dbt output | Dataform output | Materialisation | Purpose |
|-------|-----------|-----------------|-----------------|---------|
| **raw** | `raw` | `raw` | tables (loaded by Python) | Source CSVs loaded untouched. **Never modified.** |
| **staging** | `analytics` | `dataform` | 5 views | One thin model per source table: column selection and renaming only. |
| **mart** | `analytics` | `dataform` | 1 view (`fct_sales`) | Four staging models joined into an analytics-ready fact table. |

**Keeping `raw` immutable means the warehouse can always be rebuilt from the original data.**

---

## The Data

[Brazilian E-Commerce Public Dataset by Olist](https://www.kaggle.com/datasets/olistbr/brazilian-ecommerce):
around **100,000 anonymised orders** placed between **2016 and 2018**, spread across **9 linked CSV files**.

The files are **relational, not flat**. Orders link to customers and to order items, which link
to products, via shared ID columns. **Rejoining them into a usable fact table is the core of
the transformation layer.**

All 9 files are loaded into `raw` to preserve the complete source. The mart is built from
**five** of them via staging models (`orders`, `customers`, `order_items`, `products`,
`order_payments`); the remaining four stay in `raw` for future models.

---

## Repository Contents

| Path | Purpose |
|------|---------|
| `load_bigquery.py` | **Primary ingestion.** Loads the 9 raw CSVs into the BigQuery `raw` dataset. |
| `ingest.py` | **Local ingestion.** Loads the same CSVs into a local DuckDB file for offline development. |
| `retail_dbt/` | The **dbt project**: staging models, mart, sources, and tests. |
| `retail_dataform/` | The **Dataform project**: the same models re-implemented in Dataform's SQLX. |
| `.gitignore` | Excludes the raw CSVs and the local `.duckdb` warehouse. **Code is versioned, data is not.** |

### Why two ingestion scripts?

The project was **built locally on DuckDB first, then migrated to BigQuery**. The dbt models did
**not change** in that migration; only the adapter, the connection profile, and the loading script
differed. Both paths are kept so the **same models can run against either warehouse**, which keeps
local iteration fast and free while the cloud path mirrors a production setup.

### Why two transformation tools?

The pipeline is modelled in **both dbt and Dataform** to show the same logic in two idioms:

- **dbt** is **warehouse-agnostic** (the same models ran on DuckDB and BigQuery) with a large
  ecosystem, but it needs a **separate orchestrator** to schedule it.
- **Dataform** is **BigQuery-native** and fully managed inside GCP, with **scheduling built in** and
  no per-user licence cost, but it is **tied to BigQuery**.

The SQL is **nearly identical** between the two. The main differences are syntactic: `{{ ref('x') }}`
in dbt becomes `${ref("x")}` in Dataform, and **dbt tests become Dataform assertions**.

---

## Data Model

`fct_sales` has a **grain of one row per order line item** (around **112,650 rows**, matching the
`order_items` source). It starts from `stg_order_items`, which **sets the grain**, and left-joins
orders, customers, and products:

```
stg_order_items (base, sets the grain)
  LEFT JOIN stg_orders     ON order_id     ->  order status, purchase timestamp
  LEFT JOIN stg_customers  ON customer_id  ->  customer state
  LEFT JOIN stg_products   ON product_id   ->  product category
```

**Design notes:**

- **Grain drives everything.** Because an order can contain several items, `order_id` is
  **intentionally not unique** here, which is why it has a `not_null` test rather than a `unique` one.
- **Left joins, not inner joins**, so a line item is **never silently dropped** when a lookup is missing.
- **Payments are staged but deliberately not joined.** They sit at a **different grain** (around
  104,000 payment rows against 99,000 orders), so joining them to a line-item fact would **fan out
  rows and double-count revenue**. They would need their own fact table or an order-level aggregate first.
- **Staging stays thin** (for example `order_purchase_timestamp` becomes `purchased_at`). **Business
  logic and joins live in the mart**, which keeps the layers separated and staging reusable.

---

## Data Quality and Documentation

- **Tests and assertions:** `not_null` checks on `fct_sales.order_id` and `fct_sales.price`,
  written as **dbt tests** (`dbt test`) and as **Dataform assertions** (`nonNull`).
- **Idempotent loads:** the BigQuery loader uses `WRITE_TRUNCATE` and the DuckDB loader uses
  `CREATE OR REPLACE`, so **re-running produces the same result** rather than duplicating data.
- **Robust ingestion:** the BigQuery loader sets `allow_quoted_newlines=True` to handle free-text
  review fields that contain embedded line breaks.
- **Documentation:** `dbt docs generate` produces a **data dictionary and an interactive lineage
  graph**; Dataform renders an equivalent compiled graph in the GCP console.

---

## Setup

### 1. Install dependencies

```bash
pip install duckdb google-cloud-bigquery dbt-duckdb dbt-bigquery
```

### 2. Get the data

Download the [Olist dataset](https://www.kaggle.com/datasets/olistbr/brazilian-ecommerce)
and place the 9 CSVs in a `data/` folder in the project root. **This folder is git-ignored.**

### 3. Authenticate to GCP

```bash
gcloud auth application-default login
gcloud config set project <your-gcp-project-id>
```

The dbt profile uses `method: oauth`, so **no service-account key files are needed or stored**.

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

### 5. Dataform configuration

`retail_dataform/workflow_settings.yaml` sets `defaultProject`, `defaultLocation`, and the
output datasets. The Dataform project **reads the same `raw` dataset and writes its models to a
separate `dataform` dataset**, so both implementations coexist without clashing.

---

## Running It

**Cloud path, dbt on BigQuery:**

```bash
python load_bigquery.py                  # upload raw CSVs to BigQuery
cd retail_dbt
dbt run                                  # build staging + fct_sales models
dbt test                                 # run data-quality tests
dbt docs generate && dbt docs serve      # docs + lineage graph
```

**Local path, dbt on DuckDB:**

```bash
python ingest.py                         # load raw CSVs into a local DuckDB file
cd retail_dbt
dbt run --target duckdb                  # same models, different warehouse
```

**Dataform:** the `retail_dataform/` project runs inside the GCP Dataform console against the
same `raw` data, building its models into the `dataform` dataset.

---

## Orchestration in Dataform

Unlike dbt, which needs a **separate orchestrator**, Dataform has **scheduling built into GCP**.
Its two moving parts are:

- A **release configuration**, which compiles the project from a Git branch into a runnable version.
  This repo has one named `production`, tracking `main`.
- A **workflow configuration**, which runs a compiled release on a **cron schedule**.

In this project the release configuration is **executed on demand** rather than on a schedule.
That is a **deliberate consequence of staying on the free BigQuery sandbox**: Dataform only enables
**automatic** releases for repositories connected to a **third-party Git host** such as GitHub, and
that connection requires a **personal access token stored in Secret Manager**, which in turn requires
a **billing-enabled project**. The chain is:

```
automatic scheduling  ->  GitHub connection  ->  token in Secret Manager  ->  billing enabled
```

On a billing-enabled project the remaining steps would be to connect the Dataform repository to this
GitHub repo, store the access token in Secret Manager, and add a workflow configuration with a cron
schedule to run the release automatically.

---

## Orchestration with Airflow

The dbt pipeline is orchestrated by **Apache Airflow**, running locally via **Docker Compose**
(scheduler, webserver, Celery worker, triggerer, Postgres metadata database, and Redis broker).

The DAG (`airflow/dags/retail_pipeline.py`) defines two tasks with a dependency between them:

```
dbt_run  ->  dbt_test
```

- **Schedule:** daily at 06:00 (`0 6 * * *`), with `catchup=False` so it does not backfill
  historical runs from the DAG start date.
- **Tasks:** `BashOperator` tasks invoking `dbt run` and `dbt test` against the mounted dbt project.
- **Dependency:** `dbt_run >> dbt_test`, so tests only execute against a successfully built model set.

### Container setup

Airflow is Linux-only, so the stack runs as **Windows -> WSL2 -> Docker -> Airflow containers**.
Three things had to be bridged into the container:

| Need | Solution |
|------|----------|
| dbt is not in the base Airflow image | A custom image (`airflow/Dockerfile`) extending `apache/airflow:2.10.5` with `dbt-bigquery` |
| The dbt project lives on the host | Bind mount: `../retail_dbt` to `/opt/airflow/retail_dbt` |
| GCP credentials live on the host | Bind mount of the local gcloud config, so dbt's `method: oauth` works unchanged |

A container-specific `profiles.yml` (`airflow/dbt_profiles/`) is mounted to `/home/airflow/.dbt`.
It targets BigQuery only, because the host profile's relative DuckDB path is meaningless inside
a container.

**A note on dependency conflicts.** Installing dbt into the Airflow image upgraded `click`, which
broke Celery's worker startup (`nodesplit` failing on a `None` node name) and left the worker in a
restart loop while tasks sat queued. The fix was pinning `click<8.2` after the dbt install. This is
a common hazard when extending Airflow images: a shared transitive dependency is pulled forward by
the new package and breaks Airflow itself.

### Running Airflow

```bash
cd airflow
docker compose build
docker compose up -d
# UI at http://localhost:8080  (airflow / airflow)
```

---

## Known Limitations and Next Steps

The honest scope of the current build, and what production-grade would add:

- **Ingestion sits outside the DAG.** Airflow orchestrates `dbt run` and `dbt test`; the Python
  ingestion step still runs separately, since it would need its own dependencies and mounts inside
  the Airflow image. Adding it as a first task is the natural next step.
- **Airflow runs locally only.** The stack runs via Docker Compose on a single machine. A production
  deployment would use a managed service such as **Cloud Composer** or **Astronomer**.
- **Dataform scheduling is on demand.** Its release configuration (`production`, tracking `main`) is
  run manually; automatic scheduling needs a billing-enabled project, as explained above.
- **The mart is a view.** `fct_sales` rebuilds its four-way join on every query. Materialising it
  **as a table** would compute the join once at build time.
- **Full refresh only.** Models rebuild from scratch each run. Converting `fct_sales` to an
  **incremental model** would avoid reprocessing unchanged history.
- **Bad-record tolerance.** The loader allows up to 100 malformed rows per file rather than
  failing. In production these should be **routed to a quarantine table and alerted on**.
- **Hardcoded config.** The GCP project ID is set in the loader; this belongs in an **environment
  variable** for multi-environment use.
- **No CI.** Tests run locally. A next step is running `dbt build` automatically **on every pull request**.
- **No serving layer.** `fct_sales` is query-ready but **not yet connected to a BI tool**.