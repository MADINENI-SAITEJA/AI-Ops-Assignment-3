import json, time
from pyspark.sql import SparkSession, functions as F
from pyspark.sql.types import DoubleType
SRC = "/project/outputs/spark_6months_run1"
P, O, D = "tpep_pickup_datetime", "tpep_dropoff_datetime", "trip_distance"

def speed_mph(d, p, o):
    h = (o - p).total_seconds() / 3600.0
    return d / h

spark = (SparkSession.builder.appName("udf_bench_spark")
         .config("spark.sql.ansi.enabled", "false")
         .config("spark.sql.parquet.inferTimestampNTZ.enabled", "false")
         .config("spark.sql.session.timeZone", "UTC").getOrCreate())
udf = F.udf(speed_mph, DoubleType())
df = spark.read.parquet(SRC).select(D, P, O)
noop = lambda d: d.write.format("noop").mode("overwrite").save()
def t(fn):
    a = time.time(); fn(); return time.time() - a

native = F.col(D) / ((F.col(O).cast("timestamp").cast("long") - F.col(P).cast("timestamp").cast("long")) / 3600.0)
noop(df)  # warm-up
base = t(lambda: noop(df))
tu = t(lambda: noop(df.withColumn("s", udf(D, P, O))))
tn = t(lambda: noop(df.withColumn("s", native)))
out = dict(framework="spark", base_s=base, udf_total_s=tu, native_total_s=tn,
           udf_overhead_s=tu - base, native_overhead_s=tn - base)
print(json.dumps(out, indent=2))
json.dump(out, open("/project/artifacts/udf_spark.json", "w"), indent=2)
spark.stop()
