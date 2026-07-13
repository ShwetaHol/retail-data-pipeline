from google.cloud import bigquery
import glob, os

PROJECT = "retail-pipeline-502300"
DATASET = "raw"

client = bigquery.Client(project=PROJECT)

# Create the raw dataset (in the US region) if it doesn't exist
ds = bigquery.Dataset(f"{PROJECT}.{DATASET}")
ds.location = "US"
client.create_dataset(ds, exists_ok=True)

job_config = bigquery.LoadJobConfig(
    source_format=bigquery.SourceFormat.CSV,
    skip_leading_rows=1,
    autodetect=True,
    write_disposition="WRITE_TRUNCATE",
    allow_quoted_newlines=True,
    max_bad_records=100,
)

for f in glob.glob("data/*.csv"):
    table = os.path.basename(f).replace(".csv", "").replace("olist_", "").replace("_dataset", "")
    table_id = f"{PROJECT}.{DATASET}.{table}"
    with open(f, "rb") as source:
        job = client.load_table_from_file(source, table_id, job_config=job_config)
    job.result()  # wait for the upload to finish
    rows = client.get_table(table_id).num_rows
    print(table, rows)

print("Done.")