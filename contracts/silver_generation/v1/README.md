# silver_generation v1

Status: manifest for one published Silver generation directory. `schema.json` is the export of `GenerationManifest.model_json_schema()`.

The directory is `silver/generations/{storage_key}/` and contains `silver.parquet`, `quarantine.parquet`, and `manifest.json`. `storage_key` is the SHA-256 hex digest of six UTF-8 lines, each ending in newline, in this order:

1. `cfi-silver-generation-v1`
2. `source_id`
3. `dataset_version`
4. `input_sha256`
5. `contract_version`
6. `transform_version`

Identifiers are validated before the digest is calculated. The manifest repeats the full identity. `run_id` is the transform that created the generation. `bronze_run_id` is the Bronze manifest `run_id`. Neither value is a path segment.

`parquet_writer`, `parquet_writer_version`, and `parquet_compression` record the writer. They are not part of the storage key. The current writer is DuckDB with ZSTD. A generation is not rewritten because the writer version changed.

Counts on a successful manifest satisfy:

- `input_line_count = valid_record_count + invalid_record_count`
- `valid_record_count = silver_record_count + identical_duplicate_extra_count + conflict_rejection_count`
- `quarantine_row_count = invalid_record_count + conflict_rejection_count`

The manifest stores no absolute path and no Gold aggregate. `status` is `published`.
