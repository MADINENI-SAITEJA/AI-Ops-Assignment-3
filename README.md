# DA3408 Assignment 3: Spark vs. Ray (NYC Yellow Taxi, Jan-Jun 2023)

M. Sai Teja, DA24B031. The same data-cleaning pipeline is implemented in Apache Spark and Ray Data, run on a two-worker cluster for each framework, and benchmarked. The benchmark report is submitted separately as a PDF with the UI screenshots.

## Environment (what the reported runs used)

- One 6-core, 7.2 GiB aarch64 Ubuntu VM (UTM/QEMU). Every cluster node is a Docker container built from `Dockerfile` (image `taxi-a3:1`) on one bridge network, `a3-net`. This is a single-host setup, so it does not measure network scaling across machines.
- Spark 3.5.7, Ray 2.49.2, Python 3.11.17, pyarrow 19.0.1, pandas 2.2.3 (see `requirements.txt`).
- **Spark:** master + 2 workers (2 cores, 2 GiB executor memory each, 3 GiB container limit) + a driver container (768 MB driver memory); 4 executor cores in total.
- **Ray:** head (no task CPUs) + 2 workers (2 CPUs each, 1 GiB object store each, 3 GiB container limit); 4 CPUs in total.
- Only one framework's cluster ran at a time (not enough RAM for both).

Worker containers, as run:

```bash
# Spark worker (repeat for spark-worker-2)
sudo docker run -d --name spark-worker-1 --hostname spark-worker-1 --network a3-net \
  --cpus=2 --memory=3g -v "$PWD:/project" taxi-a3:1 \
  spark-class org.apache.spark.deploy.worker.Worker \
  --host spark-worker-1 --cores 2 --memory 2g --webui-port 8081 spark://spark-master:7077

# Ray worker (repeat for ray-worker-2)
sudo docker run -d --name ray-worker-1 --hostname ray-worker-1 --network a3-net \
  --cpus=2 --memory=3g --shm-size=1280m -v "$PWD:/project" taxi-a3:1 \
  bash -lc 'ray start --address=ray-head:6379 --node-ip-address="$(hostname -i)" --num-cpus=2 --memory=1073741824 --object-store-memory=1073741824 --resources="{\"data_worker\":1}" --object-spilling-directory=/project/tmp/ray-worker-1 --block'
```

The coordinator containers (`spark-master`, `spark-driver`, `ray-head`) run on the same network.

## Files

| File | Purpose |
|---|---|
| `spark_clean.py` | Spark pipeline |
| `ray_clean.py` | Ray Data pipeline. This is the final version; it was developed and run as `ray_clean_joinfixed.py` (implementation tag `arrow_clean_explicit_ranges_composite_location_join_v5`). Earlier tuning versions are in `experiments/`. |
| `common.py` | Shared schema, cleaning rules, location lookup and the identical Python feature function |
| `measure_run.py` | Wall-clock timing plus CPU and memory sampling of the containers; writes the run JSON |
| `udf_bench_spark.py`, `udf_bench_ray.py` | Python-UDF micro-benchmark on the finished output |
| `verify_outputs.py` | Exact row-by-row comparison (`EXCEPT ALL` both ways), used on the 1-month outputs |
| `fast_parity.py`, `dup_check.py` | 6-month parity (row count, columns, order-independent row hash) and duplicate check |
| `Dockerfile`, `requirements.txt` | The software environment |
| `artifacts/` | The measured run files (JSON) behind every number in the report |
| `experiments/` | Earlier Ray versions and checks from the tuning process |
| 'DA3408_A3_Report.pdf | Report |

## Pipeline (identical in both frameworks)

Ingest the monthly Parquet files and the taxi-zone lookup; drop rows with nulls in any of the 19 source columns; apply validity filters (passengers 1-8, 0 < distance <= 1000, 0 < duration <= 24 h, non-negative fares, vendor and rate code > 0, flag in Y/N, pickup within the selected months); global DISTINCT over all 19 columns across all files; two INNER joins to the zone lookup (pickup and dropoff); the same Python function computes `avg_speed_mph`, `fare_per_mile`, `fare_per_minute`; export Snappy Parquet (30 columns). The full rule text is in the `rules` field of each run JSON in `artifacts/`.

## How the reported runs were started

```bash
# Spark, 6 months
sudo python3 measure_run.py \
  --metrics artifacts/spark_6months_run2.json \
  --containers spark-master spark-worker-1 spark-worker-2 spark-driver \
  --workers spark-worker-1 spark-worker-2 \
  -- docker exec spark-driver spark-submit \
  --master spark://spark-master:7077 --deploy-mode client \
  --driver-memory 768m --executor-memory 2g --executor-cores 2 --total-executor-cores 4 \
  --conf spark.driver.host=spark-driver --conf spark.driver.bindAddress=0.0.0.0 \
  --conf spark.driver.port=7079 --conf spark.blockManager.port=7080 \
  spark_clean.py --months 6 --partitions 64 \
  --output /project/outputs/spark_6months_run2 \
  --metrics /project/artifacts/spark_6months_run2.json

# Ray, 6 months (the run used the file now named ray_clean.py)
sudo python3 measure_run.py \
  --metrics artifacts/ray_joinfixed_6months_run1.json \
  --containers ray-head ray-worker-1 ray-worker-2 \
  --workers ray-worker-1 ray-worker-2 \
  -- docker exec ray-head python3 ray_clean_joinfixed.py \
  --address ray-head:6379 --months 6 --partitions 64 --shuffle pull \
  --read-blocks-per-file 1 --block-mib 64 \
  --output /project/outputs/ray_joinfixed_6months_run1 \
  --metrics /project/artifacts/ray_joinfixed_6months_run1.json

# Parity and duplicate checks on the two 6-month outputs
sudo docker run --rm --cpus=2 --memory=4g -v "$PWD:/project" taxi-a3:1 python3 fast_parity.py
sudo docker run --rm --cpus=2 --memory=4g -v "$PWD:/project" taxi-a3:1 python3 dup_check.py
```

The data directory and zone-lookup paths are set by the `--data-dir` and `--zones` options in `common.py`. Download the monthly files and `taxi_zone_lookup.csv` from the NYC TLC trip-record page.

## Results (6 months, 19,493,620 input rows, 18,253,326 output rows)

| Metric | Spark run 1 | Spark run 2 | Ray |
|---|---|---|---|
| End-to-end time | 186.6 s | 253.2 s | 780.7 s |
| Peak CPU, cluster (400% = 4 cores) | 487% | 437% | 503% |
| Peak memory, cluster (working set) | 5.83 GiB | 5.81 GiB | 6.67 GiB |

UDF micro-benchmark (18.25M rows): Python UDF overhead 50.2 s (Spark, 1 run) vs 32.1 s (Ray, median of 3 runs, range 31.7 to 46.1 s); native expression 3.7 s (Spark) vs 9.2 s (Ray, median).

Parity: the Spark and Ray outputs have the same rows, columns and dtypes, an identical order-independent row hash, and 0 duplicate rows. The 1-month outputs were also compared row by row (0 differences).

## Limits

- Six of the nine downloaded months. The Ray configurations tried on nine months ran out of memory or disk on this VM.
- One VM hosts every node; absolute times would differ on separate machines.
- Run counts: two Spark end-to-end runs, one Ray end-to-end run; UDF benchmark has one Spark run and three Ray runs.
