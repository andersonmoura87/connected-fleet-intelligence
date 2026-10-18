# quarantine_record v1

Status: column contract for one rejected Bronze occurrence.

The row stores Bronze identity, the physical line number, the SHA-256 and size of the line bytes without the delimiter, the failure class, the rejection codes, and `trip_id` when a string identifier is available. It does not store the rejected payload.

Failure classes are `parse`, `schema`, `semantic`, and `conflict`. Parse codes are `line_empty`, `invalid_utf8`, `invalid_json`, and `json_not_object`. A validated `trip_id` whose accepted rows disagree uses `conflict` and `duplicate_conflict`. Invalid rows stay individual and do not join that group.

Rows are ordered by `line_number`. An empty quarantine file still has this schema and zero rows.
