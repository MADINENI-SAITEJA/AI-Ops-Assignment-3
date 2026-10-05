"""Ray Data pipeline with bucketed, exact global deduplication.

Distributed Ray Data sort/grouping routes identical trips to the same bucket.
Arrow DISTINCT then compares all 19 source fields inside each complete group.
Hash equality is never used as proof that two trips are duplicates.
The baseline joins, feature function and cluster requirements are preserved.
"""
from __future__ import annotations

import time

import numpy as np
import pandas as pd
import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.parquet as pq
import ray
from ray.data import DataContext
from ray.data.context import ShuffleStrategy

from common import (BASE_COLUMNS, BASE_SCHEMA, DAY_US, FEATURE_COLUMNS, FIELDS,
                    FLOAT_COLUMNS, INT_COLUMNS, OUTPUT_COLUMNS, OUTPUT_SCHEMA,
                    RULES, check_ray_workers, end_date, ensure_new_output,
                    environment, input_files, input_info, load_locations,
                    parquet_info, parser, python_features, source_names, write_json)


def clean_batch(batch, months, bucket_count=None):
    frame = batch.to_pandas()
    mapping = source_names(frame.columns)
    frame = frame.rename(columns={actual: canonical for canonical, actual in mapping.items()})
    frame = frame[BASE_COLUMNS].copy()
    for col in INT_COLUMNS + FLOAT_COLUMNS:
        values = pd.to_numeric(frame[col], errors="coerce").astype("float64")
        values = values.where(np.isfinite(values))
        if col in INT_COLUMNS:
            values = values.where((values == np.floor(values)) &
                                  (values >= -2**63) & (values < 2**63))
        frame[col] = values.mask(values == 0, 0.0)
    for col, kind in FIELDS:
        if kind == "timestamp":
            frame[col] = pd.to_datetime(frame[col], errors="coerce").dt.floor("us")
    frame["store_and_fwd_flag"] = frame["store_and_fwd_flag"].astype("string").str.strip(" ").str.upper()
    frame = frame.dropna(subset=BASE_COLUMNS)
    pickup = frame["tpep_pickup_datetime"]
    duration = frame["tpep_dropoff_datetime"] - pickup
    keep = ((pickup >= pd.Timestamp("2023-01-01")) &
            (pickup < pd.Timestamp(end_date(months))) &
            (duration > pd.Timedelta(0)) & (duration <= pd.Timedelta(microseconds=DAY_US)) &
            frame["passenger_count"].between(1, 8) &
            (frame["trip_distance"] > 0) & (frame["trip_distance"] <= 1000) &
            (frame["fare_amount"] >= 0) & (frame["total_amount"] >= 0) &
            (frame["VendorID"] > 0) & (frame["RatecodeID"] > 0) &
            frame["store_and_fwd_flag"].isin(["Y", "N"]))
    frame = frame.loc[keep].copy()
    for col in INT_COLUMNS:
        frame[col] = frame[col].astype("int64")
    table = pa.Table.from_pandas(frame, schema=BASE_SCHEMA, preserve_index=False)
    if bucket_count is not None:
        # Reuse the cleaned frame rather than round-tripping Arrow to pandas.
        # Force a canonical physical unit before hashing, so monthly files
        # with seconds/ms/us/ns timestamps route identical trips consistently.
        for name in ("tpep_pickup_datetime", "tpep_dropoff_datetime"):
            frame[name] = frame[name].astype("datetime64[us]")
        hashes = pd.util.hash_pandas_object(frame[BASE_COLUMNS], index=False)
        buckets = hashes.to_numpy(dtype=np.uint64) % np.uint64(bucket_count)
        table = table.append_column(DEDUP_BUCKET_COLUMN,
                                    pa.array(buckets.astype(np.int64)))
    return table


DEDUP_BUCKET_COLUMN = "_dedup_bucket"
IMPLEMENTATION = "bucketed_arrow_distinct_tunable_v2"


def clean_and_bucket(batch, months, bucket_count):
    return clean_batch(batch, months, bucket_count=bucket_count)


def deduplicate_bucket(batch):
    # map_groups supplies the COMPLETE bucket, including rows from every file.
    # DISTINCT across every source field keeps different rows sharing a hash.
    table = batch.select(BASE_COLUMNS).cast(BASE_SCHEMA)
    if not table.num_rows:
        return table
    return table.group_by(BASE_COLUMNS, use_threads=False).aggregate([]).cast(BASE_SCHEMA)


def add_features(batch):
    # Native joins may return timestamps with a different physical Arrow unit.
    # Convert explicitly before extracting integer microseconds.
    duration = pc.subtract(batch["tpep_dropoff_datetime"].cast(pa.timestamp("us")).cast(pa.int64()),
                           batch["tpep_pickup_datetime"].cast(pa.timestamp("us")).cast(pa.int64()))
    features = [python_features(d, f, t) for d, f, t in
                zip(batch["trip_distance"].to_pylist(), batch["fare_amount"].to_pylist(),
                    duration.to_pylist())]
    batch = batch.append_column("trip_duration_us", duration)
    batch = batch.append_column("pickup_hour", pc.hour(batch["tpep_pickup_datetime"]).cast(pa.int64()))
    for index, name in enumerate(FEATURE_COLUMNS):
        batch = batch.append_column(name, pa.array([v[index] for v in features], type=pa.float64()))
    return batch.select(OUTPUT_COLUMNS).cast(OUTPUT_SCHEMA)


def configure_data_context(block_mib=16, object_store_fraction=None):
    ctx = DataContext.get_current()
    ctx.target_max_block_size = block_mib * 1024**2
    ctx.target_min_block_size = 1024**2
    ctx.shuffle_strategy = ShuffleStrategy.SORT_SHUFFLE_PULL_BASED
    ctx.max_hash_shuffle_aggregators = 2
    ctx.enable_progress_bars = False
    ctx.override_object_store_memory_limit_fraction = object_store_fraction
    return ctx


def pipeline(files, zones_path, months, partitions, allow_local=False,
             batch_rows=16384, bucket_count=None, read_blocks_per_file=None):
    bucket_count = bucket_count or max(256, partitions * 4)
    pieces = []
    for file in files:
        mapping = source_names(pq.ParquetFile(file).schema_arrow.names)
        # Normalize BEFORE union so schema drift cannot change preprocessing.
        ds = ray.data.read_parquet(str(file), columns=list(mapping.values()),
                                   override_num_blocks=(read_blocks_per_file or max(2, partitions // len(files))),
                                   ray_remote_args={"num_cpus": 1})
        pieces.append(ds.map_batches(clean_and_bucket,
                                     fn_kwargs={"months": months, "bucket_count": bucket_count},
                                     batch_format="pyarrow", batch_size=batch_rows, num_cpus=1))
    trips = pieces[0].union(*pieces[1:]) if len(pieces) > 1 else pieces[0]
    # Sort/group only the routing integer; native Arrow performs exact DISTINCT
    # inside complete buckets. This avoids one Python accumulator per trip.
    # Keep SORT_SHUFFLE_PULL_BASED: hash shuffle in Ray 2.49.2 also uses Python
    # row loops and can retain too much heap data in this small cluster.
    trips = trips.groupby(DEDUP_BUCKET_COLUMN, num_partitions=partitions).map_groups(
        deduplicate_bucket, batch_format="pyarrow", num_cpus=1,
        memory=256 * 1024**2, zero_copy_batch=True)
    zones = load_locations(zones_path)
    # Idle shuffle actors reserve no CPU; OS container quotas enforce 1 CPU/node.
    # Worker-only custom resource excludes the CPU=0 coordinator from joins.
    actor_args = {"num_cpus": 0, "memory": 128 * 1024**2,
                  "scheduling_strategy": "SPREAD", "max_concurrency": 1}
    if not allow_local:
        actor_args["resources"] = {"data_worker": 0.01}
    for side, key in (("pickup", "PULocationID"), ("dropoff", "DOLocationID")):
        dim = zones.rename(columns={"LocationID": key, "Borough": f"{side}_borough",
                                    "Zone": f"{side}_zone", "service_zone": f"{side}_service_zone"})
        table = pa.Table.from_pandas(dim, preserve_index=False)
        lookup = ray.data.from_arrow(table)
        trips = trips.join(lookup, join_type="inner", on=(key,),
                            num_partitions=partitions, aggregator_ray_remote_args=actor_args)
    return trips.map_batches(add_features, batch_format="pyarrow", batch_size=batch_rows, num_cpus=1)


def main():
    p = parser("Ray")
    p.add_argument("--address", default="auto")
    p.add_argument("--batch-rows", type=int, default=16384)
    p.add_argument("--block-mib", type=int, default=16)
    p.add_argument("--dedup-buckets", type=int, default=None)
    p.add_argument("--read-blocks-per-file", type=int, default=None,
                   help="Use 1 to permit read/clean fusion; independent files still run in parallel")
    p.add_argument("--object-store-fraction", type=float, default=None,
                   help="Executor buffering fraction; does not resize the object store")
    args = p.parse_args()
    if args.batch_rows < 1 or not 1 <= args.block_mib <= 128:
        p.error("--batch-rows must be positive and --block-mib in [1, 128]")
    if args.read_blocks_per_file is not None and args.read_blocks_per_file < 1:
        p.error("--read-blocks-per-file must be positive")
    if args.dedup_buckets is not None and args.dedup_buckets < 1:
        p.error("--dedup-buckets must be positive")
    if args.object_store_fraction is not None and not 0 < args.object_store_fraction <= 1:
        p.error("--object-store-fraction must be in (0, 1]")
    files = input_files(args.data_dir, args.months)
    info = input_info(files)
    out = ensure_new_output(args.output)
    # Never silently fall back to a single-machine cluster.
    ray.init(address=args.address, log_to_driver=False)
    try:
        nodes = check_ray_workers(args.allow_local_test)
        configure_data_context(args.block_mib, args.object_store_fraction)
        start_epoch = time.time()
        started = time.perf_counter()
        final = pipeline(files, args.zones, args.months, args.partitions, args.allow_local_test,
                         batch_rows=args.batch_rows, bucket_count=args.dedup_buckets,
                         read_blocks_per_file=args.read_blocks_per_file)
        final.write_parquet(str(out), compression="snappy")
        elapsed = time.perf_counter() - started
        finish_epoch = time.time()
        result = {"framework": "ray", "valid_distributed_run": not args.allow_local_test,
                  "e2e_seconds": elapsed, "pipeline_started_epoch": start_epoch,
                  "pipeline_finished_epoch": finish_epoch, "input": info,
                  "input_bytes": sum(x["bytes"] for x in info),
                  "input_rows": sum(x["rows"] for x in info), "output": parquet_info(out),
                  "output_path": str(out), "months": args.months,
                  "partitions": args.partitions, "rules": RULES,
                  "implementation": IMPLEMENTATION,
                  "dedup_buckets": args.dedup_buckets or max(256, args.partitions * 4),
                  "tuning": {"batch_rows": args.batch_rows, "block_mib": args.block_mib,
                             "object_store_fraction_override": args.object_store_fraction,
                             "read_blocks_per_file": args.read_blocks_per_file,
                             "arrow_cpu_threads": pa.cpu_count(),
                             "arrow_io_threads": pa.io_thread_count(),
                             "note": "Object-store capacity is recorded in cluster node resources; "
                                     "the buffering fraction does not change that capacity."},
                  "environment": environment(), "cluster": nodes,
                  "timing_scope": "From constructing reads through completed Parquet export; excludes cluster/driver startup and output inspection."}
        write_json(args.metrics, result)
        from pathlib import Path
        Path(args.metrics.removesuffix(".json") + "_stats.txt").write_text(final.stats())
        print(f"Exported {result['output']['rows']:,} rows. Measurements: {args.metrics}", flush=True)
    finally:
        ray.shutdown()


if __name__ == "__main__":
    main()
