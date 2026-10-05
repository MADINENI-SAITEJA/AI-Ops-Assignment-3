"""Full exact multiset comparison using spillable DuckDB EXCEPT ALL."""
import argparse
from pathlib import Path

import duckdb
import pyarrow.parquet as pq

from common import OUTPUT_COLUMNS, OUTPUT_SCHEMA, write_json


def sql_string(value):
    return "'" + str(value).replace("'", "''") + "'"


def quoted(name):
    return '"' + name.replace('"', '""') + '"'


def main():
    p = argparse.ArgumentParser()
    p.add_argument("--spark", required=True)
    p.add_argument("--ray", required=True)
    p.add_argument("--result", default="/project/artifacts/parity.json")
    p.add_argument("--temp-dir", default="/project/tmp/duckdb")
    p.add_argument("--memory", default="1GB")
    args = p.parse_args()
    roots = {name: Path(path).resolve() for name, path in (("spark", args.spark), ("ray", args.ray))}
    for name, root in roots.items():
        files = sorted(root.rglob("*.parquet"))
        if not files:
            raise FileNotFoundError(f"No {name} output Parquet files under {root}")
        for file in files:
            schema = pq.ParquetFile(file).schema_arrow
            if schema.names != OUTPUT_COLUMNS:
                raise RuntimeError(f"Unexpected columns/order in {file}: {schema.names}")
            # Nullable flags/metadata are storage details; logical types must match.
            for actual, expected in zip(schema, OUTPUT_SCHEMA):
                if actual.type != expected.type:
                    raise RuntimeError(f"Type mismatch in {file}: {actual} != {expected}")
    temp = Path(args.temp_dir).resolve()
    temp.mkdir(parents=True, exist_ok=True)
    con = duckdb.connect()
    con.execute("SET memory_limit = " + sql_string(args.memory))
    con.execute("SET temp_directory = " + sql_string(temp))
    con.execute("SET threads = 2")
    cols = ", ".join(quoted(c) for c in OUTPUT_COLUMNS)
    for name, root in roots.items():
        con.execute(f"CREATE VIEW {name} AS SELECT {cols} FROM "
                    f"read_parquet({sql_string(root / '**' / '*.parquet')})")
    counts = {name: con.sql(f"SELECT count(*) FROM {name}").fetchone()[0] for name in roots}
    # Counts alone or hashes are insufficient. EXCEPT ALL checks values AND multiplicity.
    left = con.sql("SELECT count(*) FROM (SELECT * FROM spark EXCEPT ALL SELECT * FROM ray)").fetchone()[0]
    right = con.sql("SELECT count(*) FROM (SELECT * FROM ray EXCEPT ALL SELECT * FROM spark)").fetchone()[0]
    null_condition = " OR ".join(f"{quoted(c)} IS NULL" for c in OUTPUT_COLUMNS)
    nulls = {name: con.sql(f"SELECT count(*) FROM {name} WHERE {null_condition}").fetchone()[0]
             for name in roots}
    duplicates = {name: con.sql(f"SELECT count(*) FROM (SELECT {cols}, count(*) AS n "
                                      f"FROM {name} GROUP BY ALL HAVING count(*) > 1)").fetchone()[0]
                  for name in roots}
    ok = (counts["spark"] == counts["ray"] > 0 and left == right == 0
          and not any(nulls.values()) and not any(duplicates.values()))
    result = {"exact_match": ok, "method": "Full bidirectional EXCEPT ALL; no sampling or hash-only comparison",
              "rows": counts, "spark_only_rows": left, "ray_only_rows": right,
              "null_rows": nulls, "duplicate_groups": duplicates,
              "spark_path": str(roots["spark"]), "ray_path": str(roots["ray"])}
    write_json(args.result, result)
    print(f"Exact output parity: {'PASS' if ok else 'FAIL'}")
    print(f"Spark rows: {counts['spark']:,}; Ray rows: {counts['ray']:,}; differences: {left:,}/{right:,}")
    if not ok:
        raise SystemExit(1)


if __name__ == "__main__":
    main()
