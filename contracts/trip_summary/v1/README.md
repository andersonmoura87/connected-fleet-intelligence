# trip_summary v1

Status: HTTP response for one intact Gold publication. `schema.json` is the export of `TripSummary.model_json_schema()`.

`GET /v1/trips/summary` requires query parameters `source_id` and `dataset_version`. The handler reads `gold/published/{pointer_key}/current.json` once, then `summary.json` and `manifest.json` in the snapshot that pointer names. It returns this document only after those three files validate. It does not query DuckDB and it does not read Bronze or Silver.

`total_distance_km` and `average_duration_seconds` are decimal strings, not JSON numbers. The average is JSON null when `trip_count` is 0. The document has no `generated_at` and no `total_distance` field.

A promotion that replaces `current.json` after the pointer bytes were captured does not invalidate a response built from the captured snapshot. The handler does not read the pointer a second time.

Error responses use `{"code": "<code>"}` and do not include a path or a traceback.

| Status | Code | Condition |
|---|---|---|
| 200 | | Intact publication, including zero trips. |
| 422 | `missing_parameter` | `source_id` or `dataset_version` is absent. |
| 422 | `invalid_identifier` | An identifier fails the storage rule, including an empty value. |
| 404 | `not_published` | `current.json` is absent for the requested origin. |
| 503 | `pointer_invalid` | The pointer is not valid JSON, fails its schema, or names a different origin. |
| 503 | `snapshot_absent` | The snapshot named by the captured pointer is absent. |
| 503 | `integrity_mismatch` | Hash, size, identity, or metrics do not agree. |
| 503 | `schema_mismatch` | The summary or the manifest fails its Gold schema. |
| 500 | `path_limit` | The absolute path exceeds the reader limit before any read. |
| 500 | `reparse_point` | A symlink, junction, or other reparse point is on the lexical path. |
| 500 | `path_escape` | The lexical path leaves the data root. |
| 500 | `storage_error` | The filesystem read fails for another operational reason. |

`GET /health/live` is a separate document, `{"status": "live"}`. It does not claim that a snapshot exists or that the data is intact. `GET /health`, `GET /health/ready`, and `GET /metrics` are not implemented.
