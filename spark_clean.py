"""Run with spark-submit against the manually started two-worker cluster."""
from __future__ import annotations

import time
from functools import reduce

from pyspark.sql import SparkSession, functions as F, types as T

from common import (BASE_COLUMNS, DAY_US, FEATURE_COLUMNS, FIELDS, INT_COLUMNS,
                    FLOAT_COLUMNS, OUTPUT_COLUMNS, RULES, check_spark_workers,
                    end_date, ensure_new_output, environment, input_files,
                    input_info, load_locations, parquet_info, parser,
                    python_features, source_names, write_json)

FEATURE_TYPE = T.StructType([T.StructField(c, T.DoubleType(), False)
                             for c in FEATURE_COLUMNS])


def cleaned_trips(spark, files, months):
    parts = []
    # Cast each file BEFORE union: monthly files can have different physical types.
    for file in files:
        df = spark.read.parquet(str(file))
        mapping = source_names(df.columns)
        types = {"int": "double", "float": "double",
                 "timestamp": "timestamp_ntz", "string": "string"}
        parts.append(df.select(*[F.col(mapping[name]).cast(types[kind]).alias(name)
                                 for name, kind in FIELDS]))
    df = reduce(lambda left, right: left.unionByName(right), parts).dropna()
    predicates = []
    for col in INT_COLUMNS + FLOAT_COLUMNS:
        predicates.append(~F.isnan(col) & (F.abs(F.col(col)) < F.lit(float("inf"))))
    for col in INT_COLUMNS:
        predicates.append((F.col(col) == F.floor(col)) &
                          (F.col(col) >= F.lit(float(-2**63))) &
                          (F.col(col) < F.lit(float(2**63))))
    df = df.filter(reduce(lambda a, b: a & b, predicates))
    expressions = []
    for col, kind in FIELDS:
        value = F.col(col)
        if kind == "int":
            value = value.cast("long")
        elif kind == "float":
            value = F.when(value == 0, F.lit(0.0)).otherwise(value)
        elif kind == "string":
            value = F.upper(F.trim(value))
        expressions.append(value.alias(col))
    df = df.select(*expressions)
    duration = (F.unix_micros(F.col("tpep_dropoff_datetime").cast("timestamp")) -
                F.unix_micros(F.col("tpep_pickup_datetime").cast("timestamp")))
    df = df.filter(
        (F.col("tpep_pickup_datetime") >= F.lit("2023-01-01").cast("timestamp_ntz")) &
        (F.col("tpep_pickup_datetime") < F.lit(end_date(months)).cast("timestamp_ntz")) &
        (duration > 0) & (duration <= DAY_US) &
        F.col("passenger_count").between(1, 8) &
        (F.col("trip_distance") > 0) & (F.col("trip_distance") <= 1000) &
        (F.col("fare_amount") >= 0) & (F.col("total_amount") >= 0) &
        (F.col("VendorID") > 0) & (F.col("RatecodeID") > 0) &
        F.col("store_and_fwd_flag").isin("Y", "N")
    )
    return df.select(BASE_COLUMNS).dropDuplicates(BASE_COLUMNS)


def pipeline(spark, files, zones_path, months):
    trips = cleaned_trips(spark, files, months)
    zones = load_locations(zones_path)
    zone_schema = "LocationID long, Borough string, Zone string, service_zone string"
    lookup = spark.createDataFrame(list(zones.itertuples(index=False, name=None)), zone_schema)
    for side, key in (("pickup", "PULocationID"), ("dropoff", "DOLocationID")):
        dim = lookup.select(F.col("LocationID").alias(key),
                            F.col("Borough").alias(f"{side}_borough"),
                            F.col("Zone").alias(f"{side}_zone"),
                            F.col("service_zone").alias(f"{side}_service_zone"))
        trips = trips.hint("merge").join(dim.hint("merge"), on=key, how="inner")
    trips = trips.withColumn("trip_duration_us",
                             F.unix_micros(F.col("tpep_dropoff_datetime").cast("timestamp")) -
                             F.unix_micros(F.col("tpep_pickup_datetime").cast("timestamp")))
    trips = trips.withColumn("pickup_hour", F.hour("tpep_pickup_datetime").cast("long"))
    features_udf = F.udf(python_features, FEATURE_TYPE)
    trips = trips.withColumn("_features", features_udf("trip_distance", "fare_amount",
                                                       "trip_duration_us"))
    for name in FEATURE_COLUMNS:
        trips = trips.withColumn(name, F.col(f"_features.{name}"))
    return trips.select(OUTPUT_COLUMNS)


def main():
    p = parser("Spark")
    p.add_argument("--master-ui", default="http://spark-master:8080")
    p.add_argument("--hold-ui-seconds", type=int, default=0)
    args = p.parse_args()
    files = input_files(args.data_dir, args.months)
    info = input_info(files)
    out = ensure_new_output(args.output)
    workers = [] if args.allow_local_test else check_spark_workers(args.master_ui)
    # These settings control the data pipeline, not cluster/network creation.
    spark = (SparkSession.builder.appName("A3 Yellow Taxi - Spark")
             .config("spark.sql.session.timeZone", "UTC")
             .config("spark.sql.shuffle.partitions", args.partitions)
             .config("spark.sql.adaptive.enabled", "false")
             .config("spark.sql.constraintPropagation.enabled", "false")
             .config("spark.sql.autoBroadcastJoinThreshold", -1)
             .config("spark.sql.parquet.outputTimestampType", "TIMESTAMP_MICROS")
             .config("spark.sql.execution.pythonUDF.arrow.enabled", "false")
             .getOrCreate())
    try:
        from pathlib import Path
        spark.sparkContext.addPyFile(str(Path(__file__).with_name("common.py")))
        if not args.allow_local_test and not spark.sparkContext.master.startswith("spark://"):
            raise RuntimeError("Use a standalone spark:// master, not local[*]")
        spark.sparkContext.setLogLevel("WARN")
        start_epoch = time.time()
        started = time.perf_counter()
        final = pipeline(spark, files, args.zones, args.months)
        plan = final._jdf.queryExecution().executedPlan().toString()
        final.write.mode("errorifexists").option("compression", "snappy").parquet(str(out))
        elapsed = time.perf_counter() - started
        finish_epoch = time.time()
        result = {"framework": "spark", "valid_distributed_run": not args.allow_local_test,
                  "e2e_seconds": elapsed, "pipeline_started_epoch": start_epoch,
                  "pipeline_finished_epoch": finish_epoch, "input": info,
                  "input_bytes": sum(x["bytes"] for x in info),
                  "input_rows": sum(x["rows"] for x in info), "output": parquet_info(out),
                  "output_path": str(out), "months": args.months,
                  "partitions": args.partitions, "rules": RULES,
                  "environment": environment(), "cluster": workers,
                  "application_id": spark.sparkContext.applicationId,
                  "timing_scope": "From constructing reads through completed Parquet export; excludes cluster/driver startup, output inspection and UI hold."}
        write_json(args.metrics, result)
        plan_path = args.metrics.removesuffix(".json") + "_plan.txt"
        from pathlib import Path
        Path(plan_path).write_text(plan)
        print(f"Exported {result['output']['rows']:,} rows. Measurements: {args.metrics}", flush=True)
        if args.hold_ui_seconds:
            print(f"Spark application UI stays alive for {args.hold_ui_seconds}s. Take your port 4040 screenshot now.", flush=True)
            time.sleep(args.hold_ui_seconds)
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
