# trip_record v1.0.0 snapshot

Status: frozen historical copy of contract_version `1.0.0`. The file `schema.json` in this directory is not regenerated. The same bytes are stored at `../v1.0.0/schema.json`.

The executable contract is [trip_record v1.1.0](../v1.1.0/README.md). Silver transformation uses `contract_version` `1.1.0`.

`contract_version` of this snapshot is `1.0.0`. The distance unit is kilometers. This row contract does not include batch provenance (`source_id`, `dataset_version`, `input_sha256`, `run_id`). Those values belong to a later manifest. Placing them on the row is a schema violation.

These fixtures are synthetic. They are not a NYC TLC sample and they are not observed fleet telemetry.

## Fields

| Field | Required | Rule |
|---|---|---|
| `trip_id` | yes | String. Rejected when empty or only spaces. Every other character is preserved. |
| `pickup_at` | yes | ISO-8601 string or `datetime` with an explicit timezone, then stored in UTC. |
| `dropoff_at` | yes | Same timestamp rule. An instant earlier than `pickup_at` is rejected. Equal instants are accepted. |
| `trip_distance` | yes | Kilometers. See the decimal policy. |
| `source_zone` | no | String, or absence. Omission and JSON `null` both become absence. |
| `destination_zone` | no | Same as `source_zone`. |

Unknown fields are rejected. Values are not silently dropped.

Wire distances in fixtures are canonical strings, so a JSON parser cannot turn them into binary floats. The executable model also accepts an `int` or a `decimal.Decimal`. It rejects `float` and `bool`. The generated schema types the field as an integer or a string, not as a generic JSON number. A fractional JSON number becomes a binary float in the default parser and is rejected.

## Decimal policy

The maximum fractional scale is 3. Scale is the fractional length of the supplied literal, and trailing zeros count.

| Literal | Result |
|---|---|
| `1.234` | accepted |
| `1.230` | accepted and preserved as `1.230` |
| `1.2340` | rejected, including when the extra digit is zero |
| `1.2345` | rejected |
| `-1.2345` | rejected for scale, before the sign is considered |

The model does not quantize, pad, or round to three places. Construction from a string uses an exact decimal context: an inexact or rounded conversion is rejected. `float` is rejected so `Decimal(float_value)` cannot become the conversion path.

Inside one distance value the first matching rule wins:

1. Unsupported type or non-canonical syntax: `schema_violation`.
2. `NaN`, `Infinity`, `-Infinity`, or a non-finite `Decimal`: `nonfinite_distance`.
3. Fractional scale greater than 3: `distance_scale_exceeded`.
4. Value below zero: `negative_distance`.

Signed zero with scale 3 or less is zero. Examples of rejected syntax, not distance errors: `1e-3`, `+1.0`, `01.20`, and surrounding spaces.

## Timestamps

A string without an offset is `timestamp_missing_timezone`. An unparseable string is `invalid_timestamp`. A number or boolean is `schema_violation`. Comparison uses the UTC instants, so a negative offset can make a later wall-clock string the earlier instant.

Duration of one accepted row, used only to explain fixtures, is exact decimal seconds:

`days * 86400 + seconds + microseconds / 1000000`

from the normalized timedelta. This function is not a Gold aggregate.

## Rejection codes

| Code | When |
|---|---|
| `schema_violation` | Missing or unknown field, wrong JSON type, non-canonical distance syntax. |
| `blank_trip_id` | Empty or whitespace-only `trip_id`. |
| `invalid_timestamp` | Timezone-bearing string that is not a valid instant. |
| `timestamp_missing_timezone` | Parsed instant with no offset. |
| `end_before_start` | `dropoff_at` earlier than `pickup_at` after UTC normalization. |
| `nonfinite_distance` | Non-finite distance token or value. |
| `distance_scale_exceeded` | More than three fractional digits in the supplied literal. |
| `negative_distance` | Finite distance below zero at an accepted scale. |

When several fields fail, the codes are returned together, deduplicated, in the order of this table. The row validator does not emit a duplicate code.

## Duplicate policy

Specified for Silver. Not implemented in this contract.

- Identical duplicate: the same `trip_id` and the same validated payload are one logical trip. Numeric decimal equality applies, so `1.5` and `1.500` are the same distance. Omitted and null zones are both absence.
- Conflicting duplicate: the same `trip_id` with any difference in the validated payload rejects every row of that `trip_id`. No row is selected.
- Arrival order and file position do not choose a survivor.
- Each conflicting or identical row can still be valid on its own. Detection belongs to batch processing, not to `validate_trip_record`.

## Fixture expectations

`tests/fixtures/synthetic/trips_valid.jsonl` has two accepted trips.

| Metric | Expected value |
|---|---|
| `trip_count` | 2 |
| `total_distance_km` | 12.750 |
| `average_duration_seconds` | 1500 |

`trip-001` runs for 1200 seconds and 4.500 km. `trip-002` starts at `08:00-05:00`, which is `13:00Z`, ends at `13:30Z`, and is 8.250 km. The average is the decimal mean of those two durations.

`tests/fixtures/synthetic/trips_invalid.jsonl` holds one defect per line: blank id, whitespace id, unparseable timestamp, missing timezone, end before start, UTC order reversal, negative distance, `NaN`, `Infinity`, `-Infinity`, four fractional digits, an unknown field, a missing `trip_id`, and a boolean distance.

`tests/fixtures/synthetic/trips_conflict.jsonl` holds two identical `trip-100` rows and two conflicting `trip-200` rows. Under the policy above, Silver keeps one logical trip of 1.000 km and 600 seconds, and rejects both `trip-200` rows. Swapping the conflicting lines does not change that outcome.
