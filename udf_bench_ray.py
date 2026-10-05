import json, sys, time, numpy as np, pandas as pd, ray
SRC = "/project/outputs/spark_6months_run1"
COLS = ["trip_distance", "tpep_pickup_datetime", "tpep_dropoff_datetime"]

def speed_mph(d, p, o):
    h = (o - p).total_seconds() / 3600.0
    return d / h

def udf_fn(df):
    s = [speed_mph(d, p, o) for d, p, o in zip(df[COLS[0]], df[COLS[1]], df[COLS[2]])]
    return pd.DataFrame({"s": pd.Series(s, dtype="float64")})

def native_fn(df):
    h = (df[COLS[2]] - df[COLS[1]]).dt.total_seconds().to_numpy() / 3600.0
    return pd.DataFrame({"s": df[COLS[0]].to_numpy() / h})

def t(fn):
    a = time.time(); fn(); return time.time() - a

ray.init(address="ray-head:6379")
ds = lambda: ray.data.read_parquet(SRC, columns=COLS)
ds().materialize()  # warm-up
base = t(lambda: ds().materialize())
udf = t(lambda: ds().map_batches(udf_fn, batch_format="pandas", batch_size=None).materialize())
nat = t(lambda: ds().map_batches(native_fn, batch_format="pandas", batch_size=None).materialize())
out = dict(framework="ray", base_s=base, udf_total_s=udf, native_total_s=nat,
           udf_overhead_s=udf - base, native_overhead_s=nat - base)
print(json.dumps(out, indent=2))
json.dump(out, open("/project/artifacts/udf_ray.json", "w"), indent=2)
