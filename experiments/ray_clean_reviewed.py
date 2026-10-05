"""Ray Data pipeline with bucketed, exact global deduplication.

Distributed Ray Data sort with explicit ranges co-locates identical trips.
Arrow DISTINCT then compares all 19 source fields inside each complete range.
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
from ray.data.datasource import Datasource, ReadTask
from ray.data.block import BlockMetadata

from common import (BASE_COLUMNS, BASE_SCHEMA, DAY_US, FEATURE_COLUMNS, FIELDS,
                    FLOAT_COLUMNS, INT_COLUMNS, OUTPUT_COLUMNS, OUTPUT_SCHEMA,
                    RULES, check_ray_workers, end_date, ensure_new_output,
                    environment, input_files, input_info, load_locations,
                    parquet_info, parser, python_features, source_names, write_json)


def clean_batch_pandas(batch, months, bucket_count=None):
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
IMPLEMENTATION = "arrow_clean_explicit_ranges_push_shuffle_v4"


def clean_batch(batch, months, bucket_count=None):
    """Use native Arrow for numeric Parquet schemas; preserve the older fallback.

    Hashing happens after canonicalization. It only selects a destination:
    duplicates are still compared on every source field by Arrow DISTINCT.
    """
    mapping = source_names(batch.column_names)
    numeric = [batch[mapping[c]].type for c in INT_COLUMNS + FLOAT_COLUMNS]
    times = [batch[mapping[c]].type for c, kind in FIELDS if kind == "timestamp"]
    if (not all(pa.types.is_integer(t) or pa.types.is_floating(t) for t in numeric)
            or not all(pa.types.is_timestamp(t) and t.tz is None for t in times)
            or not pa.types.is_string(batch[mapping["store_and_fwd_flag"]].type)):
        return clean_batch_pandas(batch, months, bucket_count)

    columns, predicates = {}, []
    for name, kind in FIELDS:
        value = batch[mapping[name]]
        if kind in ("int", "float"):
            value = pc.cast(value, pa.float64(), safe=False)
            predicates.append(pc.is_finite(value))
            if kind == "int":
                predicates.extend((pc.equal(value, pc.floor(value)),
                                   pc.greater_equal(value, float(-2**63)),
                                   pc.less(value, float(2**63))))
            value = pc.if_else(pc.equal(value, 0.0), 0.0, value)
        elif kind == "timestamp":
            value = pc.cast(pc.floor_temporal(value, unit="microsecond"),
                            pa.timestamp("us"), safe=False)
        else:
            value = pc.utf8_upper(pc.utf8_trim(value, characters=" "))
        predicates.append(pc.is_valid(value))
        columns[name] = value

    pickup, dropoff = columns["tpep_pickup_datetime"], columns["tpep_dropoff_datetime"]
    predicates.extend((
        pc.greater_equal(pickup, pa.scalar(pd.Timestamp("2023-01-01"), pa.timestamp("us"))),
        pc.less(pickup, pa.scalar(pd.Timestamp(end_date(months)), pa.timestamp("us"))),
        pc.greater(dropoff, pickup),
        pc.less_equal(dropoff, pc.add(pickup, pa.scalar(DAY_US, pa.duration("us")))),
        pc.greater_equal(columns["passenger_count"], 1.0),
        pc.less_equal(columns["passenger_count"], 8.0),
        pc.greater(columns["trip_distance"], 0.0),
        pc.less_equal(columns["trip_distance"], 1000.0),
        pc.greater_equal(columns["fare_amount"], 0.0),
        pc.greater_equal(columns["total_amount"], 0.0),
        pc.greater(columns["VendorID"], 0.0),
        pc.greater(columns["RatecodeID"], 0.0),
        pc.is_in(columns["store_and_fwd_flag"], value_set=pa.array(["Y", "N"]))
    ))
    keep = predicates[0]
    for predicate in predicates[1:]:
        keep = pc.and_kleene(keep, predicate)
    filtered = pa.table(columns).filter(pc.fill_null(keep, False)).cast(BASE_SCHEMA)
    if bucket_count is None:
        return filtered
    frame = filtered.to_pandas()
    for name in ("tpep_pickup_datetime", "tpep_dropoff_datetime"):
        frame[name] = frame[name].astype("datetime64[us]")
    hashes = pd.util.hash_pandas_object(frame[BASE_COLUMNS], index=False)
    buckets = hashes.to_numpy(dtype=np.uint64) % np.uint64(bucket_count)
    return filtered.append_column(DEDUP_BUCKET_COLUMN, pa.array(buckets.astype(np.int64)))


def partition_boundaries(bucket_count, partitions):
    """Distinct integer boundaries: equal routing keys cannot cross a boundary."""
    count = min(bucket_count, partitions)
    # Ray treats [] as 'sample boundaries'. A sentinel makes the one-bucket
    # collision test use one nonempty range and one empty range instead.
    if count == 1:
        return [bucket_count]
    return [(i * bucket_count + count - 1) // count for i in range(1, count)]


def clean_and_bucket(batch, months, bucket_count):
    return clean_batch(batch, months, bucket_count=bucket_count)


def deduplicate_bucket(batch):
    # An explicit sorted range contains complete buckets from ALL input files.
    # Empty sort reducers may return an Arrow table without columns.
    if not batch.num_rows:
        return pa.Table.from_batches([], schema=BASE_SCHEMA)
    table = batch.select(BASE_COLUMNS).cast(BASE_SCHEMA)
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


def configure_data_context(block_mib=64, object_store_fraction=None, shuffle="push"):
    ctx = DataContext.get_current()
    ctx.target_max_block_size = block_mib * 1024**2
    ctx.target_min_block_size = 1024**2
    ctx.shuffle_strategy = (ShuffleStrategy.SORT_SHUFFLE_PUSH_BASED if shuffle == "push"
                            else ShuffleStrategy.SORT_SHUFFLE_PULL_BASED)
    ctx.max_hash_shuffle_aggregators = 2
    ctx.enable_progress_bars = False
    ctx.override_object_store_memory_limit_fraction = object_store_fraction
    return ctx


class NormalizedTaxiParquetDatasource(Datasource):
    """One Ray read operator for all files, with per-file schema normalization.

    Reading and cleaning run in the same worker task. File schemas never need
    to be merged before normalization. There is no order-preserving Union that
    must buffer every month's output before releasing any blocks to the sort.
    The downstream sort and Arrow DISTINCT still deduplicate across ALL files.
    """

    def __init__(self, files, months, bucket_count, batch_rows):
        self.months = months
        self.bucket_count = bucket_count
        self.batch_rows = batch_rows
        self.files = []
        for file in files:
            with pq.ParquetFile(str(file)) as parquet:
                mapping = source_names(parquet.schema_arrow.names)
                # Conservative decoded size estimate, not a measured size.
                estimated_bytes = parquet.metadata.num_rows * 192
            from pathlib import Path
            self.files.append((str(Path(file).resolve()), mapping, estimated_bytes))
        self.output_schema = BASE_SCHEMA.append(pa.field(DEDUP_BUCKET_COLUMN, pa.int64()))

    def get_name(self):
        return "TaxiParquetAndClean"

    def estimate_inmemory_data_size(self):
        return sum(size for _, _, size in self.files)

    def get_read_tasks(self, parallelism):
        # One independent task per input file. Ray schedules them across the
        # two workers; each task yields bounded batches instead of a full file.
        tasks = []
        months, buckets, batch_rows = self.months, self.bucket_count, self.batch_rows
        for path, mapping, estimated_bytes in self.files:
            def read_file(path=path, columns=tuple(mapping.values())):
                with pq.ParquetFile(path) as parquet:
                    for record in parquet.iter_batches(batch_size=batch_rows,
                                                       columns=list(columns), use_threads=False):
                        block = clean_and_bucket(pa.Table.from_batches([record]), months, buckets)
                        if block.num_rows:
                            yield block
            metadata = BlockMetadata(num_rows=None, size_bytes=estimated_bytes,
                                     exec_stats=None, input_files=[path])
            tasks.append(ReadTask(read_file, metadata, schema=self.output_schema))
        return tasks


def pipeline(files, zones_path, months, partitions, allow_local=False,
             batch_rows=65536, bucket_count=None, read_blocks_per_file=None):
    bucket_count = bucket_count or max(256, partitions * 4)
    source = NormalizedTaxiParquetDatasource(files, months, bucket_count, batch_rows)
    requested_blocks = (len(files) * read_blocks_per_file
                        if read_blocks_per_file is not None else max(len(files), partitions))
    trips = ray.data.read_datasource(source, override_num_blocks=requested_blocks,
                                     ray_remote_args={"num_cpus": 1})
    # Ray 2.49.2 sort-based map_groups ignores num_partitions and samples as
    # many ranges as input blocks. Explicit boundaries bound that fan-out and
    # remove the sampling stage. Each reducer emits ONE complete range block;
    # batch_size=None ensures DISTINCT sees that whole range before any output
    # splitting. Both global grouping semantics and all-field equality remain.
    trips = trips.sort(DEDUP_BUCKET_COLUMN,
                       boundaries=partition_boundaries(bucket_count, partitions))
    trips = trips.map_batches(
        deduplicate_bucket, batch_format="pyarrow", batch_size=None, num_cpus=1,
        memory=256 * 1024**2, zero_copy_batch=True)
    zones = load_locations(zones_path)
    # Idle shuffle actors reserve no CPU; Docker enforces the configured per-worker CPU quota.
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
    p.add_argument("--batch-rows", type=int, default=65536)
    p.add_argument("--block-mib", type=int, default=64)
    p.add_argument("--shuffle", choices=("push", "pull"), default="push")
    p.add_argument("--dedup-buckets", type=int, default=None)
    p.add_argument("--read-blocks-per-file", type=int, default=None,
                   help="Requested read blocks per file; all files share one streaming datasource")
    p.add_argument("--object-store-fraction", type=float, default=None,
                   help="Executor buffering fraction; does not resize the object store")
    args = p.parse_args()
    if args.batch_rows < 1 or not 1 <= args.block_mib <= 128:
        p.error("--batch-rows must be positive and --block-mib in [1, 128]")
    if args.partitions < 1:
        p.error("--partitions must be positive")
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
        configure_data_context(args.block_mib, args.object_store_fraction, args.shuffle)
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
                             "sort_shuffle": args.shuffle,
                             "sort_boundaries": partition_boundaries(
                                 args.dedup_buckets or max(256, args.partitions * 4), args.partitions),
                             "cleaning": "Arrow kernels for native numeric Parquet; pandas fallback for other physical schemas",
                             "object_store_fraction_override": args.object_store_fraction,
                             "read_blocks_per_file": args.read_blocks_per_file,
                             "arrow_cpu_threads": pa.cpu_count(),
                             "arrow_io_threads": pa.io_thread_count(),
                             "ingestion": "one Ray Data datasource; each file is read and normalized in bounded batches",
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
