import duckdb
con = duckdb.connect()
con.execute("SET memory_limit='2GB'; SET threads=2; SET temp_directory='/project/tmp/duckdb'")
for n, p in {"spark": "/project/outputs/spark_6months_run1",
             "ray": "/project/outputs/ray_joinfixed_6months_run1"}.items():
    r = con.execute(f"SELECT count(*), count(DISTINCT hash(t)) FROM (SELECT * FROM read_parquet('{p}/*.parquet')) t").fetchone()
    print(n, "rows:", r[0], "distinct rows:", r[1], "duplicates:", r[0] - r[1])
