# TPC-DS Parquet Dumper

This tool uses Trino itself to copy data from the `tpcds` connector into
Parquet-backed Hive tables on the local filesystem.

It is meant to create real Parquet inputs for Nitro and Trino operator
benchmarks, so later query comparisons do not depend on on-the-fly TPC-DS data
generation.

## Run

```bash
mvn -f tools/tpcds-parquet-dumper/pom.xml package
java --add-modules jdk.incubator.vector \
  --enable-native-access=ALL-UNNAMED \
  -jar tools/tpcds-parquet-dumper/target/tpcds-parquet-dumper-1-SNAPSHOT.jar \
  --output-root /Users/martin/tmp/tpcds-parquet --source-schema sf1
```

Dump only a subset of tables:

```bash
java --add-modules jdk.incubator.vector \
  --enable-native-access=ALL-UNNAMED \
  -jar tools/tpcds-parquet-dumper/target/tpcds-parquet-dumper-1-SNAPSHOT.jar \
  --output-root /Users/martin/tmp/tpcds-parquet --source-schema sf0.01 --tables store_sales,date_dim,item
```

Overwrite existing tables:

```bash
java --add-modules jdk.incubator.vector \
  --enable-native-access=ALL-UNNAMED \
  -jar tools/tpcds-parquet-dumper/target/tpcds-parquet-dumper-1-SNAPSHOT.jar \
  --output-root /Users/martin/tmp/tpcds-parquet --source-schema sf1 --overwrite
```

## Notes

- Source tables come from Trino's `tpcds` connector.
- Target tables are written through Trino's `hive` connector with
  `format = 'PARQUET'`.
- Trino's local Hive writer produces Parquet data files without a `.parquet`
  suffix; they are still standard Parquet files.
- By default, the target schema name is:
  - `tiny` for `sf0.01`
  - otherwise the source schema with `.` replaced by `_`
- The output root contains both the local Hive metastore catalog directory and
  the table data files.
