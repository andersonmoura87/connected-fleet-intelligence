# silver_trip v1

Status: column contract for one logical trip written by transform version `1`.

A Silver row contains the six canonical trip fields, Bronze lineage, the line number of the representative occurrence, and `duplicate_count`. Rows are ordered by `trip_id`. Identical validated duplicates collapse to one row. `duplicate_count` is the number of accepted occurrences in that group. The representative line is the minimum physical line number.

`trip_distance` is `decimal(18,3)`. `pickup_at` and `dropoff_at` are normalized to UTC before they are written. The Parquet physical type is `TIMESTAMP` with microsecond precision and no time zone. `TIMESTAMP WITH TIME ZONE` is not used. A reader must interpret those timezone-naive components as UTC. Optional zones stay null when the validated value is absent. An empty string is not turned into null.

`contract_version` on the row is the trip_record version used to validate the Bronze bytes. The current transform writes `1.1.0`.
