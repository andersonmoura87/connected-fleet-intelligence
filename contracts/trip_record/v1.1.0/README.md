# trip_record v1.1.0

Status: executable contract. `schema.json` is the export of `TripRecord.model_json_schema()`. `contract_version` is `1.1.0`.

Version 1.0.0 remains the frozen snapshot in `../v1.0.0/` and `../v1/`. Its schema is not regenerated from this model. Silver transformation validates rows with `1.1.0`.

The distance unit is kilometers. This row contract does not include batch provenance. Provenance belongs to the Bronze manifest and the Silver generation manifest.

These fixtures are synthetic. They are not a NYC TLC sample and they are not observed fleet telemetry.

## Timestamp precision

A timestamp literal is rejected when it contains seven or more fractional digits, including when the extra digits are zero. The code is `timestamp_precision_exceeded`. The check runs on the string before datetime parsing. There is no truncation and no rounding.

Zero to six fractional digits keep the previous behavior. One to five digits are padded with trailing zeros to microseconds. Six digits are preserved. UTC normalization runs only after this check. `end_before_start` compares only instants that passed it.

| Literal | Result |
|---|---|
| `2024-06-01T12:00:00.123456Z` | accepted, microsecond 123456 |
| `2024-06-01T12:00:00.1Z` | accepted, microsecond 100000 |
| `2024-06-01T12:00:00.1234560Z` | rejected |
| `2024-06-01T12:00:00.1234561Z` | rejected |

`2024-06-01T07:00:00.123456-05:00` and `2024-06-01T12:00:00.123456Z` are the same UTC instant.

## Rejection codes

| Code | When |
|---|---|
| `schema_violation` | Missing or unknown field, wrong JSON type, non-canonical distance syntax. |
| `blank_trip_id` | Empty or whitespace-only `trip_id`. |
| `invalid_timestamp` | Timezone-bearing string that is not a valid instant. |
| `timestamp_missing_timezone` | Parsed instant with no offset. |
| `timestamp_precision_exceeded` | More than six fractional digits in the timestamp literal. |
| `end_before_start` | `dropoff_at` earlier than `pickup_at` after UTC normalization. |
| `nonfinite_distance` | Non-finite distance token or value. |
| `distance_scale_exceeded` | More than three fractional digits in the supplied literal. |
| `negative_distance` | Finite distance below zero at an accepted scale. |

When several fields fail, the codes are returned together, deduplicated, in the order of this table. `validate_trip_record` does not deduplicate rows. The Silver transform applies `TripRecord` equality to rows this contract has accepted.
