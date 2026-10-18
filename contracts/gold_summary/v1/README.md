# gold_summary v1

Status: logical metrics for one Gold snapshot. `schema.json` is the export of `GoldSummary.model_json_schema()`.

The document is `summary.json` inside `gold/snapshots/{storage_key}/`. It has no `created_at` and no `run_id`. Those values live only in the generation manifest.

`total_distance_km` and `average_duration_seconds` are decimal strings, not JSON numbers. Distance is scale 3. The average is seconds with six fractional digits, or JSON null when `trip_count` is 0. `rejected_count` is the Silver `quarantine_row_count`. It does not include identical-duplicate extras.

`duplicate_count` on a Silver row is not a weight. `trip_count` is the number of Silver rows.
