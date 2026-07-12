import duckdb,glob,os

con=duckdb.connect("retail.duckdb")

con.execute("CREATE SCHEMA IF NOT EXISTS raw")

for f in glob.glob("data/*.csv"):
    name="raw"+ os.path.basename(f). replace(".csv","") .replace("_dataset",""). replace("olist_","")
    con.execute(f"CREATE OR REPLACE TABLE {name} AS SELECT * FROM read_csv_auto('{f}')")

    print(name,con.execute(f"SELECT COUNT(*) FROM {name}").fetchone()[0])

con.close()