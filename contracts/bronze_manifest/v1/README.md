# bronze_manifest v1

Status: executable contract. Schema file `schema.json` is generated from `BronzeManifest.model_json_schema()` and must match that export.

`contract_version` is `1`. This manifest describes one published Bronze directory. It does not describe trips, Silver metrics, or Gold metrics.

## Identity

`source_identity` is `(source_id, dataset_version)`. One published directory exists for that pair.

`artifact_identity` is `(source_id, dataset_version, input_sha256)`. `input_sha256` is the lowercase SHA-256 of the stored original bytes. It is sealed in the manifest. It is not a directory name.

`run_id` records the execution that created the artifact. A later identical replay does not change `run_id` or `created_at`.

## Fields

| Field | Rule |
|---|---|
| `manifest_version` | Only `1`. |
| `source_id` | Identifier. See the identifier rule below. |
| `dataset_version` | Identifier. |
| `input_sha256` | 64 lowercase hexadecimal characters. |
| `input_size_bytes` | Integer, zero or greater. |
| `run_id` | Identifier of the creating execution. |
| `created_at` | UTC timestamp `YYYY-MM-DDTHH:MM:SSZ`. |
| `status` | Only `published`. |

Unknown fields are rejected. The manifest does not store an absolute path. The original file and this manifest sit together in the published directory, so their locations are derived from `source_id` and `dataset_version`.

## Identifier rule

`source_id`, `dataset_version`, and `run_id` match `^[A-Za-z0-9][A-Za-z0-9._-]{0,127}$`. A trailing dot is rejected. Windows device names (`CON`, `PRN`, `AUX`, `NUL`, `COM1`–`COM9`, `LPT1`–`LPT9`) are rejected in any letter case.

## Publication

The published directory contains `original` and `manifest.json`. `original` is the exact input bytes. Ingestion does not validate trip rows.

Replay of the same bytes is a no-op. A different checksum for the same `source_identity` is `identity_conflict` and writes nothing. A new `dataset_version` or a different `source_id` is an independent artifact.

An incomplete directory, a missing file, or a checksum that does not match the stored bytes fails closed. The suspect directory is not repaired and is not deleted.
