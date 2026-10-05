"""Shared, explicitly defined preprocessing rules for both frameworks."""
from __future__ import annotations

import argparse
import json
import math
import platform
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq

FIELDS = [
    ("VendorID", "int"),
    ("tpep_pickup_datetime", "timestamp"),
    ("tpep_dropoff_datetime", "timestamp"),
    ("passenger_count", "int"),
    ("trip_distance", "float"),
    ("RatecodeID", "int"),
    ("store_and_fwd_flag", "string"),
    ("PULocationID", "int"),
    ("DOLocationID", "int"),
    ("payment_type", "int"),
    ("fare_amount", "float"),
    ("extra", "float"),
    ("mta_tax", "float"),
    ("tip_amount", "float"),
    ("tolls_amount", "float"),
    ("improvement_surcharge", "float"),
    ("total_amount", "float"),
    ("congestion_surcharge", "float"),
    ("airport_fee", "float"),
]
BASE_COLUMNS = [name for name, _ in FIELDS]
INT_COLUMNS = [name for name, kind in FIELDS if kind == "int"]
FLOAT_COLUMNS = [name for name, kind in FIELDS if kind == "float"]
TYPES = {"int": pa.int64(), "float": pa.float64(),
         "timestamp": pa.timestamp("us"), "string": pa.string()}
BASE_SCHEMA = pa.schema([(name, TYPES[kind]) for name, kind in FIELDS])
LOCATION_COLUMNS = [f"{side}_{col}" for side in ("pickup", "dropoff")
                    for col in ("borough", "zone", "service_zone")]
FEATURE_COLUMNS = ["avg_speed_mph", "fare_per_mile", "fare_per_minute"]
OUTPUT_COLUMNS = BASE_COLUMNS + LOCATION_COLUMNS + [
    "trip_duration_us", "pickup_hour"] + FEATURE_COLUMNS
OUTPUT_SCHEMA = pa.schema(list(BASE_SCHEMA) + [
    pa.field(name, pa.string()) for name in LOCATION_COLUMNS] + [
    pa.field("trip_duration_us", pa.int64()),
    pa.field("pickup_hour", pa.int64())] + [
    pa.field(name, pa.float64()) for name in FEATURE_COLUMNS])
DAY_US = 86_400_000_000
RULES = {
    "columns": BASE_COLUMNS,
    "nulls": "Drop rows missing ANY of the 19 normalized source columns.",
    "numeric": "Finite doubles; integral int64 values; normalize signed zero.",
    "timestamps": "Naive local wall-clock values at microsecond precision; no UTC conversion.",
    "pickup_window": "2023-01-01 inclusive to first day after selected final month exclusive.",
    "filters": "1<=passenger_count<=8; 0<trip_distance<=1000; 0<duration<=24h; "
               "fare_amount,total_amount>=0; VendorID,RatecodeID>0; flag in Y,N.",
    "duplicates": "Global DISTINCT on all 19 normalized source columns, across ALL selected files.",
    "joins": "Two INNER equijoins to unique LocationID lookup: pickup and dropoff; blank labels become Unknown.",
    "features": "Identical shared scalar Python function; three double values rounded to 6 decimal places.",
}


def python_features(distance: float, fare: float, duration_us: int):
    """Meaningful per-trip features, with no artificial CPU-burning loop."""
    d, f, t = float(distance), float(fare), int(duration_us)
    if not all(math.isfinite(x) for x in (d, f)) or d <= 0 or t <= 0:
        raise ValueError("Invalid inputs passed to Python feature function")
    return (round(d * 3_600_000_000.0 / t, 6),
            round(f / d, 6), round(f * 60_000_000.0 / t, 6))


def python_identity(distance: float, fare: float, duration_us: int):
    """Cheap Python control with the same input/output shape as the UDF."""
    return float(distance), float(fare), float(duration_us)


def source_names(names):
    """Accept changes in capitalization, but never invent missing columns."""
    lower = {name.lower(): name for name in names}
    missing = [name for name in BASE_COLUMNS if name.lower() not in lower]
    if missing:
        raise ValueError(f"Missing source columns: {missing}")
    return {name: lower[name.lower()] for name in BASE_COLUMNS}


def input_files(data_dir, months):
    if not 1 <= months <= 9:
        raise ValueError("--months must be between 1 and 9")
    root = Path(data_dir).resolve()
    files = [root / f"yellow_tripdata_2023-{month:02d}.parquet"
             for month in range(1, months + 1)]
    missing = [str(f) for f in files if not f.is_file()]
    if missing:
        raise FileNotFoundError("Missing input files:\n" + "\n".join(missing))
    return files


def end_date(months):
    return f"2023-{months + 1:02d}-01"


def input_info(files):
    return [{"file": f.name, "bytes": f.stat().st_size,
             "rows": pq.ParquetFile(f).metadata.num_rows} for f in files]


def load_locations(path):
    """Small dimension table only; the trip table is never collected locally."""
    import pandas as pd
    raw = pd.read_csv(path, keep_default_na=False, dtype=str)
    wanted = ["LocationID", "Borough", "Zone", "service_zone"]
    if any(name not in raw.columns for name in wanted):
        raise ValueError(f"Lookup must contain columns: {wanted}")
    values = pd.to_numeric(raw["LocationID"], errors="raise")
    if (values % 1 != 0).any() or (values <= 0).any():
        raise ValueError("Lookup LocationID must contain positive integers")
    raw["LocationID"] = values.astype("int64")
    if raw["LocationID"].duplicated().any():
        raise ValueError("Duplicate LocationID would multiply joined trip rows")
    for col in wanted[1:]:
        raw[col] = raw[col].str.strip().replace("", "Unknown")
    return raw[wanted]


def ensure_new_output(path):
    path = Path(path).resolve()
    if path.exists():
        raise FileExistsError(f"Output exists. Choose a NEW --output: {path}")
    path.parent.mkdir(parents=True, exist_ok=True)
    return path


def parquet_info(path):
    files = sorted(Path(path).rglob("*.parquet"))
    if not files:
        raise RuntimeError(f"No Parquet files were exported to {path}")
    return {"rows": sum(pq.ParquetFile(f).metadata.num_rows for f in files),
            "files": len(files), "bytes": sum(f.stat().st_size for f in files)}


def write_json(path, result):
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(result, indent=2, allow_nan=False) + "\n")


def environment():
    import pandas, numpy, ray, pyspark
    return {"python": platform.python_version(), "architecture": platform.machine(),
            "spark": pyspark.__version__, "ray": ray.__version__,
            "pyarrow": pa.__version__, "pandas": pandas.__version__,
            "numpy": numpy.__version__}


def parser(framework):
    p = argparse.ArgumentParser(description=f"A3 identical {framework} taxi pipeline")
    p.add_argument("--data-dir", default="/project/data/trips")
    p.add_argument("--zones", default="/project/data/taxi_zone_lookup.csv")
    p.add_argument("--months", type=int, default=9)
    p.add_argument("--partitions", type=int, default=64)
    p.add_argument("--output", required=True)
    p.add_argument("--metrics", required=True)
    p.add_argument("--allow-local-test", action="store_true",
                   help="Correctness testing only; not a valid distributed benchmark")
    return p


def check_spark_workers(master_ui):
    import urllib.request
    with urllib.request.urlopen(master_ui.rstrip("/") + "/json", timeout=15) as reply:
        state = json.load(reply)
    active = [w for w in state.get("workers", []) if w.get("state") == "ALIVE"]
    if len(active) != 2 or any(int(w["cores"]) != 2 for w in active):
        raise RuntimeError("Spark must have exactly TWO active workers, each with TWO cores")
    return [{k: w.get(k) for k in ("id", "host", "cores", "memory", "state")}
            for w in active]


def check_ray_workers(allow_local=False):
    import ray
    nodes = [n for n in ray.nodes() if n["Alive"]]
    workers = [n for n in nodes if n["Resources"].get("CPU", 0) > 0]
    if not allow_local:
        if (len(nodes) != 3 or len(workers) != 2
                or any(n["Resources"].get("CPU") != 2 for n in workers)
                or any(n["Resources"].get("data_worker", 0) < 1 for n in workers)):
            raise RuntimeError("Ray must have a CPU=0 head and TWO CPU=2 nodes with data_worker=1")
    return [{"node_id": n["NodeID"], "address": n["NodeManagerAddress"],
             "resources": n["Resources"]} for n in nodes]
