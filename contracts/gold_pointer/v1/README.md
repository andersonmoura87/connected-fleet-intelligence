# gold_pointer v1

Status: mutable pointer to one intact Gold snapshot. `schema.json` is the export of `GoldPointer.model_json_schema()`.

The file is `gold/published/{pointer_key}/current.json`. There is one pointer for each `(source_id, dataset_version)`. The pointer does not include `input_sha256`.

`storage_key` names the snapshot directory. `summary_sha256` is the digest of that snapshot's `summary.json`. `status` is `published`.

A missing pointer is created. A pointer that already names the requested intact snapshot is left unchanged. `--promote` replaces an intact pointer that names a different intact snapshot. `--promote` does not repair a corrupt pointer, a missing snapshot, or a hash mismatch.
