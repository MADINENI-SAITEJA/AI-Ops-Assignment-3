# DA3408 Assignment 3: Spark vs Ray

Use `STEP_BY_STEP.md` for the exact commands. All compute runs inside your Ubuntu
UTM VM. The Mac is used to transfer files and open the dashboards.

## Files

| File | Purpose |
|---|---|
| `spark_clean.py`, `ray_clean.py` | Required matching distributed pipelines |
| `common.py` | Shared schema, rules and identical scalar Python feature function |
| `Dockerfile`, `requirements.txt` | One ARM64/AMD64-compatible software environment |
| `validate_data.py` | File/schema/size checks and SHA-256 input manifest |
| `measure_run.py` | Actual wall-clock resource monitoring and raw `top` logs |
| `verify_outputs.py` | Full exact multiset comparison, including duplicates and nulls |
| `prepare_udf_input.py`, `udf_benchmark.py` | Separate experiments using the same real trip rows |
| `summarize_results.py` | CSV and plots from your measured runs only |
| `DA3408_A3_Report.pdf' | Report and summary |

## Assumed environment

Ubuntu 22.04 or 24.04, preferably ARM64 virtualized on your M3 Mac; 4 virtual CPUs,
8 GiB RAM and at least 40 GiB free disk. There are two data-worker containers per
framework, with one CPU each. Both workers share the same mounted directory.
Only one framework runs at a time. Each worker has a 2304 MiB container memory
limit; the driver has 1536 MiB; the coordinator has 1024 MiB.

These are distinct distributed worker processes/nodes on ONE physical VM. In the
report disclose the container topology: this does not measure network scaling
across separate physical hosts. If your TA explicitly requires separate worker
VMs, this container topology must be adapted before benchmarking.

## Input

Keep the nine downloaded files unchanged, named
`yellow_tripdata_2023-01.parquet` through `yellow_tripdata_2023-09.parquet`, in
`data/trips/`. Download the official `taxi_zone_lookup.csv` as shown in the guide.
Source: https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page

The file checker reports actual compressed bytes and row counts. Do not claim
the downloaded files total 2 GB unless the measurement supports it. Parquet disk
size and decoded in-memory size differ.

## Identical logic

1. Normalize all 19 standard 2023 source fields to the same types. Preserve all
   source fields. Reject null/non-finite values and non-integral integer fields.
2. Keep pickups from January 1 through the end of the selected period. Keep
   positive durations up to 24 hours, distances in (0, 1000] miles, passenger
   counts 1-8, nonnegative fare/total, positive vendor/ratecode, and Y/N flags.
   These are explicit experimental cleaning choices, not rules mandated by TLC.
3. Remove exact duplicates globally across every selected input file, using all
   19 normalized columns. Duplicates that differ in source fees are not merged.
4. Perform two native distributed inner joins, on pickup and dropoff LocationID.
   The lookup must have unique IDs. Blank dimension labels become `Unknown`.
   Spark disables automatic broadcast joins and requests sort-merge joins;
   Ray uses native Ray Data hash joins, not a driver-side pandas join.
5. Preserve timestamps as naive local wall-clock values at microsecond precision.
   Compute microsecond duration and pickup hour. This does not infer real UTC
   offsets or resolve ambiguous daylight-saving timestamps.
6. Apply the same `python_features()` function to produce miles/hour, fare/mile
   and fare/minute, rounded to six decimal places. No artificial CPU-burning
   computation is added. Export all 30 columns as Snappy-compressed Parquet.

`verify_outputs.py` checks logical schema and every row in both directions with
`EXCEPT ALL`; it does not depend on file names, partition order or hashes alone.

## Measurement definitions

- End-to-end time: pipeline construction/read planning through completed final
  export. Initial cluster/driver connection, preliminary file inventory, output
  inspection, monitoring startup and screenshot hold time are excluded.
- CPU: observed simultaneous sum across worker cgroups, with 100% representing
  one CPU core. A separate whole-cluster peak includes coordinator and driver.
- Memory: simultaneous working-set sum (`memory.current - inactive_file`). Raw
  charged memory is also saved. One-second sampling can miss shorter spikes.
- UDF experiment: same real, materialized three-column input for both systems;
  three modes (scan/reduce, Python identity/reduce, Python features/reduce).
  Every mode is warmed once and timed three times with rotated ordering.
  Differences estimate overhead; they do not isolate JVM crossings alone.
  Spill/caching differences and task scheduling are part of the limitation.

No results, benchmark values, screenshots or winner are bundled. You must obtain
them from your own runs. The plotting script refuses local-test metrics.

## Validation performed before delivery

Python syntax and installed version-specific API signatures were checked.
The local Spark pipeline completed on edge-case fixtures; Ray's actual batch
cleaning/feature functions with local Arrow reference joins produced the exact
same rows. The Spark UDF experiment and full exact output checker also ran.
Those fixtures and their timings are not included as assignment benchmarks.
Docker is not available in the authoring workspace, and Ray cluster startup is
blocked there by socket restrictions. The full image, ARM64 distributed runs,
resource sampling and real nine-month performance must be checked in your VM.
