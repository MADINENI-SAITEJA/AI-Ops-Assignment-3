"""Full exact output comparison with small, disposable verification partitions.

Hashes only route rows. Every partition uses bidirectional EXCEPT ALL on all
output columns, so collisions cannot hide mismatches or changed multiplicity.
Requires the project's common.py and its existing DuckDB/PyArrow dependencies.
"""
from __future__ import annotations

import argparse
import shutil
import tempfile
import threading
from pathlib import Path

import duckdb
import pyarrow.parquet as pq

from common import OUTPUT_COLUMNS, OUTPUT_SCHEMA, write_json


def literal(value):
    return "'" + str(value).replace("'", "''") + "'"


def identifier(value):
    return '"' + value.replace('"', '""') + '"'


def parquet_source(files):
    paths = '[' + ', '.join(literal(f) for f in files) + ']'
    return f'read_parquet({paths}, hive_partitioning=false)'


def inspect_output(root):
    files = sorted(root.rglob('*.parquet'))
    if not files:
        raise FileNotFoundError(f'No output Parquet files in {root}')
    rows = 0
    for file in files:
        reader = pq.ParquetFile(file)
        schema = reader.schema_arrow
        if schema.names != OUTPUT_COLUMNS:
            raise RuntimeError(f'Unexpected columns/order in {file}: {schema.names}')
        for actual, expected in zip(schema, OUTPUT_SCHEMA):
            if actual.type != expected.type:
                raise RuntimeError(f'Type mismatch in {file}: {actual} != {expected}')
        rows += reader.metadata.num_rows
    return files, rows


def connection(memory, spill):
    spill.mkdir(parents=True, exist_ok=True)
    con = duckdb.connect()
    con.execute('SET memory_limit = ' + literal(memory))
    con.execute('SET threads = 1')
    con.execute('SET preserve_insertion_order = false')
    con.execute('SET temp_directory = ' + literal(spill))
    con.execute("SET max_temp_directory_size = '256MB'")
    con.execute('SET partitioned_write_flush_threshold = 8192')
    con.execute('SET partitioned_write_max_open_files = 8')
    return con


class DiskGuard:
    """Interrupt active queries before verification uses the filesystem reserve."""
    def __init__(self, root, reserve):
        self.root, self.reserve = root, reserve
        self.active = None
        self.stop = threading.Event()
        self.low = False
        self.thread = threading.Thread(target=self.watch, daemon=True)

    def check(self):
        free = shutil.disk_usage(self.root).free
        if free < self.reserve:
            self.low = True
            raise RuntimeError('Verification stopped: less than the configured '
                               'disk reserve remains. Original outputs are unchanged.')

    def watch(self):
        while not self.stop.wait(0.5):
            if shutil.disk_usage(self.root).free < self.reserve:
                self.low = True
                con = self.active
                if con is not None:
                    con.interrupt()


def compare(spark, ray, result, temp_dir, memory='1GB', buckets=128, reserve_gib=1.5):
    roots = {'spark': Path(spark).resolve(), 'ray': Path(ray).resolve()}
    inputs, counts = {}, {}
    for name, root in roots.items():
        inputs[name], counts[name] = inspect_output(root)
    parent = Path(temp_dir).resolve()
    parent.mkdir(parents=True, exist_ok=True)
    # Only this newly-created directory is ever removed. Never touch original data.
    work = Path(tempfile.mkdtemp(prefix='parity-', dir=parent))
    guard = DiskGuard(work, int(reserve_gib * 1024**3))
    cols = ', '.join(identifier(c) for c in OUTPUT_COLUMNS)
    null_condition = ' OR '.join(f'{identifier(c)} IS NULL' for c in OUTPUT_COLUMNS)
    totals = {'spark_only_rows': 0, 'ray_only_rows': 0}
    nulls = {'spark': 0, 'ray': 0}
    duplicates = {'spark': 0, 'ray': 0}
    details = []
    guard.thread.start()
    try:
        guard.check()
        for name in roots:
            print(f'Partitioning {name}: {counts[name]:,} rows into {buckets} buckets...', flush=True)
            con = connection(memory, work / 'spill')
            guard.active = con
            try:
                con.execute(f'COPY (SELECT {cols}, '
                            f'CAST(hash("tpep_pickup_datetime") % {buckets} AS INTEGER) '
                            f'AS _verify_bucket FROM {parquet_source(inputs[name])}) '
                            f'TO {literal(work / name)} '
                            "(FORMAT PARQUET, PARTITION_BY (_verify_bucket), "
                            "COMPRESSION ZSTD, ROW_GROUP_SIZE 16384, FILENAME_PATTERN 'part_{i}')")
            finally:
                guard.active = None
                con.close()
            guard.check()
            copied = sum(pq.ParquetFile(f).metadata.num_rows
                         for f in (work / name).rglob('*.parquet'))
            if copied != counts[name]:
                raise RuntimeError(f'Verification partitioning lost rows: {name} {copied} != {counts[name]}')
        for bucket in range(buckets):
            guard.check()
            con = connection(memory, work / 'spill')
            guard.active = con
            try:
                for name in roots:
                    files = sorted((work / name / f'_verify_bucket={bucket}').glob('*.parquet'))
                    source = parquet_source(files or inputs[name])
                    empty = '' if files else ' LIMIT 0'
                    con.execute(f'CREATE VIEW {name} AS SELECT {cols} FROM {source}{empty}')
                sc, rc = (con.sql(f'SELECT count(*) FROM {name}').fetchone()[0]
                          for name in ('spark', 'ray'))
                left = con.sql('SELECT count(*) FROM (SELECT * FROM spark EXCEPT ALL SELECT * FROM ray)').fetchone()[0]
                right = con.sql('SELECT count(*) FROM (SELECT * FROM ray EXCEPT ALL SELECT * FROM spark)').fetchone()[0]
                totals['spark_only_rows'] += left
                totals['ray_only_rows'] += right
                for name in roots:
                    nulls[name] += con.sql(f'SELECT count(*) FROM {name} WHERE {null_condition}').fetchone()[0]
                    duplicates[name] += con.sql(f'SELECT count(*) FROM (SELECT {cols}, count(*) AS n '
                                               f'FROM {name} GROUP BY ALL HAVING count(*) > 1)').fetchone()[0]
                details.append({'bucket': bucket, 'spark_rows': sc, 'ray_rows': rc,
                                'spark_only_rows': left, 'ray_only_rows': right})
            finally:
                guard.active = None
                con.close()
            # Compared copies can immediately be removed, limiting peak temp usage.
            for name in roots:
                folder = work / name / f'_verify_bucket={bucket}'
                if folder.exists():
                    shutil.rmtree(folder)
            if (bucket + 1) % 8 == 0 or bucket + 1 == buckets:
                print(f'Compared {bucket + 1}/{buckets} buckets; '
                      f'differences {totals["spark_only_rows"]:,}/{totals["ray_only_rows"]:,}', flush=True)
        for name in roots:
            key = name + '_rows'
            if sum(d[key] for d in details) != counts[name]:
                raise RuntimeError(f'Not all {name} rows were compared')
        ok = (counts['spark'] == counts['ray'] > 0 and not any(totals.values())
              and not any(nulls.values()) and not any(duplicates.values()))
        report = {'exact_match': ok, 'method': 'Full bidirectional EXCEPT ALL on all 30 columns in every '
                  'verification bucket; hash only routes rows; no sampling or hash-only comparison',
                  'rows': counts, **totals, 'null_rows': nulls, 'duplicate_groups': duplicates,
                  'spark_path': str(roots['spark']), 'ray_path': str(roots['ray']),
                  'verification_buckets': buckets, 'bucket_results': details,
                  'duckdb_version': duckdb.__version__}
        write_json(result, report)
        print(f'Exact output parity: {"PASS" if ok else "FAIL"}', flush=True)
        print(f'Spark rows: {counts["spark"]:,}; Ray rows: {counts["ray"]:,}; '
              f'differences: {totals["spark_only_rows"]:,}/{totals["ray_only_rows"]:,}', flush=True)
        return report
    except Exception as exc:
        if guard.low:
            raise RuntimeError('Verification stopped to preserve free disk space. '
                               'Its temporary copies will be removed; original outputs remain.') from exc
        raise
    finally:
        guard.active = None
        guard.stop.set()
        guard.thread.join(timeout=2)
        shutil.rmtree(work)


def main():
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument('--spark', required=True)
    p.add_argument('--ray', required=True)
    p.add_argument('--result', default='/project/artifacts/parity_partitioned.json')
    p.add_argument('--temp-dir', default='/project/tmp/duckdb')
    p.add_argument('--memory', default='1GB')
    p.add_argument('--buckets', type=int, default=128)
    p.add_argument('--reserve-gib', type=float, default=1.5)
    args = p.parse_args()
    if not 1 <= args.buckets <= 1024 or args.reserve_gib < 0:
        p.error('buckets must be 1..1024 and reserve-gib nonnegative')
    report = compare(args.spark, args.ray, args.result, args.temp_dir,
                     args.memory, args.buckets, args.reserve_gib)
    if not report['exact_match']:
        raise SystemExit(1)


if __name__ == '__main__':
    main()
