# Retail Data Pipeline 🛒

A hands-on data engineering project where I take messy e-commerce data and turn it
into clean, analytics-ready tables using Python, DuckDB, and dbt.

I built this to learn the modern data stack properly, by doing it end to end rather
than just reading about it.

## What it does

It takes the public [Olist dataset](https://www.kaggle.com/datasets/olistbr/brazilian-ecommerce)
around 100,000 real Brazilian e-commerce orders from 2016–2018, spread across 9
separate CSV files and turns those scattered files into a single, tidy sales table
you can actually ask questions of, like *"which product categories bring in the most revenue?"*

It follows the same pattern real data teams use: **ingest → stage → model → test → document.**

## How it works
CSV files  →  Python  →  DuckDB (raw)  →  dbt staging  →  dbt mart  →  tests + docs

The data moves through three layers, and each one has a job:

- **Raw** — the original CSVs, loaded in untouched. I never edit these, so there's
  always a clean copy to fall back on.
- **Staging** — one lightly-cleaned model per table (tidying column names, picking the
  useful fields).
- **Mart** — where it gets interesting: the staging models get joined into `fct_sales`,
  one row per item sold, with its order, customer, and product details all in one place.

dbt draws the whole flow as a visual lineage graph automatically, which is genuinely
satisfying to see.

## Built with

- **Python** — for the ingestion script
- **DuckDB** — a lightweight local warehouse (runs as a single file, no setup)
- **dbt** — for the transformations, data tests, and documentation
- **Git & GitHub** — version control

## Running it yourself

```bash
# Install what you need
pip install duckdb dbt-duckdb

# Grab the dataset and drop the CSVs into a data/ folder
# https://www.kaggle.com/datasets/olistbr/brazilian-ecommerce

# Load the raw data
python ingest.py

# Transform, test, and document
cd retail_dbt
dbt run
dbt test
dbt docs generate && dbt docs serve
```

## What I learned

This project taught me how the pieces of a real pipeline fit together,why you keep
raw data separate, how dbt figures out the order to build models in from their
dependencies, and how testing and documentation turn a pile of SQL into something
maintainable. The trickiest part was getting dbt pointed at the right warehouse and
wrapping my head around the difference between sources and model references, which is
exactly the kind of thing you only really understand by building it.

## Where I'd take it next

To make this production-grade, I'd move the warehouse to a cloud platform like BigQuery,
add an orchestrator like Airflow to schedule runs, use incremental models so it doesn't
reprocess everything each time, and set up CI to run the tests automatically on every change.
