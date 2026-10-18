# gold_generation v1

Status: seal for one immutable Gold snapshot directory. `schema.json` is the export of `GoldGeneration.model_json_schema()`.

The directory is `gold/snapshots/{storage_key}/` and contains `summary.json` and `manifest.json`. `storage_key` is the SHA-256 hex digest of eight UTF-8 lines, each ending in newline, in this order:

1. `cfi-gold-snapshot-v1`
2. `source_id`
3. `dataset_version`
4. `input_sha256`
5. `contract_version`
6. `transform_version`
7. `gold_contract_version`
8. `aggregate_version`

`pointer_key` is the SHA-256 hex digest of three UTF-8 lines, each ending in newline: `cfi-gold-pointer-v1`, `source_id`, and `dataset_version`.

`run_id` is the Gold publication that created the snapshot. `bronze_run_id` is copied from the Silver generation manifest. Neither value, nor `created_at`, nor the DuckDB reader version, is part of the storage key. A snapshot is not rewritten because the reader version changed.

`summary_sha256` and `summary_size_bytes` seal `summary.json`. The manifest repeats the summary metrics so a reader can cross-check them. The manifest stores no absolute path. `status` is `published`. `parquet_reader` is `duckdb` because DuckDB reads the Silver Parquet files. The aggregate itself is Python decimal arithmetic.
