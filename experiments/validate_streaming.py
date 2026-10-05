"""Correctness tests for the multi-file reader; no benchmark timings generated."""
from collections import Counter
from pathlib import Path
import tempfile
from unittest.mock import patch

import pyarrow as pa
import pyarrow.parquet as pq
import ray

import ray_clean_streaming as fixed
import ray_tuning.validate_tuning as previous
from common import BASE_COLUMNS, BASE_SCHEMA


def rows_multiset(table):
    return Counter(tuple(row[name] for name in BASE_COLUMNS) for row in table.to_pylist())


def main():
    # Reuse the independently checked DISTINCT/two-join fixture, exercising
    # the replacement's functions rather than the prior script's functions.
    previous.clean_and_bucket = fixed.clean_and_bucket
    previous.deduplicate_bucket = fixed.deduplicate_bucket
    previous.add_features = fixed.add_features
    previous.configure_data_context = fixed.configure_data_context
    previous.main()
    with tempfile.TemporaryDirectory(prefix='a3-reader-fixture-') as directory:
        files, expected_batches = [], []
        for month in range(1, 10):
            rows = [previous.make_row(i) for i in range(month * 7, month * 7 + 12)]
            rows.append(previous.make_row(0))  # Same trip in all nine files.
            rows.append(dict(previous.make_row(month * 100), passenger_count=2.5))
            if month == 9:
                rows = [dict(row, trip_distance=0.0) for row in rows]  # Empty cleaned file.
            unit = ('s', 'ms', 'us', 'ns')[(month - 1) % 4]
            raw = previous.table(rows, unit, upper_names=month % 2 == 0)
            if month % 3 == 0:
                # A physical integer field differs between monthly files.
                names = raw.column_names
                key = next(name for name in names if name.lower() == 'vendorid')
                index = raw.schema.get_field_index(key)
                raw = raw.set_column(index, key, raw[key].cast(pa.int32()))
            file = Path(directory) / f'yellow_tripdata_2023-{month:02}.parquet'
            pq.write_table(raw, file)
            files.append(file)
            expected_batches.append(previous.baseline_clean(raw, 9))
        source = fixed.NormalizedTaxiParquetDatasource(files, 9, 256, 4)
        source = ray.cloudpickle.loads(ray.cloudpickle.dumps(source))
        tasks = source.get_read_tasks(9)
        assert len(tasks) == 9
        actual_blocks = []
        for task in tasks:
            task = ray.cloudpickle.loads(ray.cloudpickle.dumps(task))
            assert task.metadata.num_rows is None
            for block in task():
                assert block.num_rows <= 4
                assert block.schema == source.output_schema
                actual_blocks.append(block)
        actual = pa.concat_tables(actual_blocks).select(BASE_COLUMNS).cast(BASE_SCHEMA)
        expected = pa.concat_tables(expected_batches).cast(BASE_SCHEMA)
        assert rows_multiset(actual) == rows_multiset(expected)
        actual_distinct = actual.group_by(BASE_COLUMNS, use_threads=False).aggregate([]).cast(BASE_SCHEMA)
        expected_distinct = expected.group_by(BASE_COLUMNS, use_threads=False).aggregate([]).cast(BASE_SCHEMA)
        assert rows_multiset(actual_distinct) == rows_multiset(expected_distinct)
        assert actual_distinct.num_rows < actual.num_rows
        print('PASS: serialized Ray read tasks, nine Parquet files, mixed schemas, bounded batches and cross-file duplicates')

        class Graph:
            def groupby(self, *args, **kwargs): return self
            def map_groups(self, *args, **kwargs): return self
            def join(self, *args, **kwargs): return self
            def map_batches(self, *args, **kwargs): return self
            def union(self, *args, **kwargs): raise AssertionError('Blocking Union reintroduced')
        graph = Graph()
        lookup = Path(directory) / 'zones.csv'
        lookup.write_text('LocationID,Borough,Zone,service_zone\n1,Test,Zone,Test\n')
        with patch.object(ray.data, 'read_datasource', return_value=graph) as read, \
             patch.object(ray.data, 'from_arrow', return_value=graph):
            fixed.pipeline(files, lookup, 9, 64, allow_local=True, read_blocks_per_file=1)
            assert read.call_count == 1
            assert len(read.call_args.args[0].files) == 9
            assert read.call_args.kwargs['override_num_blocks'] == 9
        print('PASS: one read operator for nine files, no Union; global grouping and both joins retained')
    print('ALL STREAMING READER CORRECTNESS CHECKS PASSED. VM runtime still requires measurement.')


if __name__ == '__main__':
    main()
