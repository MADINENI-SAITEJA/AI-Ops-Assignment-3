import duckdb
con = duckdb.connect()
con.execute("SET memory_limit='2GB'; SET threads=2; SET temp_directory='/project/tmp/duckdb'")
paths = {"spark": "/project/outputs/spark_6months_run1",
         "ray":   "/project/outputs/ray_joinfixed_6months_run1"}
res = {}
for n, p in paths.items():
    src = f"read_parquet('{p}/*.parquet')"
    desc = con.execute(f"DESCRIBE SELECT * FROM {src}").fetchall()
    cols = sorted(d[0] for d in desc)
    sel = ", ".join(f'"{c}"' for c in cols)
    cnt, h = con.execute(f"SELECT count(*), sum(hash(t)::HUGEINT) FROM (SELECT {sel} FROM {src}) t").fetchone()
    res[n] = (cnt, h, cols, sorted((d[0], d[1]) for d in desc))
    print(n, "rows:", cnt, "hash:", h)
print("columns match:", res["spark"][2] == res["ray"][2])
print("rows match:   ", res["spark"][0] == res["ray"][0])
print("hash match:   ", res["spark"][1] == res["ray"][1])
if res["spark"][3] != res["ray"][3]:
    print("dtype differences:", set(res["spark"][3]) ^ set(res["ray"][3]))
