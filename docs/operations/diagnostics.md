# Operational diagnostics

`cfi diagnose` inspects one `(source_id, dataset_version)`. It writes nothing: no data root, directory, lock, pointer, or publication. It does not open DuckDB and it does not recompute a Gold aggregate. Exit 0 with stdout `status` `reported` means the inspection finished. It does not mean the artifacts are present or valid. Exit 2 rejects the arguments. Exit 4 means a path or storage failure, and the report is not produced.

The pointer file is read once. Bronze, Silver, and a requested snapshot are separate reads. The command does not promise one transactional view of the tree. A failed read is exit 4. It is not reported as corruption. Run `cfi diagnose` again after the filesystem accepts reads.

Do not hand-edit `current.json`. Do not delete Bronze to force another publication. Do not rebuild an immutable Bronze original, Silver generation, or Gold snapshot by copying or rewriting its files. `--promote` does not replace an invalid pointer.

`input_sha256` is required on stdout only when the operator passed it. Without it, the report has no `silver_state` and no `snapshot_state`. Those directories are named by the checksum.

## Pointer absent, checksum unknown

Symptom. `GET /v1/trips/summary` returns 404 `not_published`, or `cfi diagnose` without `--input-sha256` reports `pointer_state` `absent`.

Evidence. Stdout `status` is `reported`, `pointer_state` is `absent`, and `pointer_sha256` is omitted. `bronze_state` is present. There is no `silver_state`.

Diagnosis. `current.json` was not found for this identity. The command cannot tell an unpublished identity from a durable snapshot whose pointer was not written, because that distinction needs the checksum.

Supported action. If `bronze_state` is `absent`, publish with `cfi ingest` and continue with `cfi transform` and `cfi gold` only after each earlier stage is intact. If `bronze_state` is `intact` and the checksum is known from that manifest, run `cfi diagnose` again with `--input-sha256`.

Verification. A later report names the checksum that was passed. The data root gains files only from the publication commands, not from diagnose.

Stop. Stop when `bronze_state` is `incomplete` or `mismatch`. Do not ingest a second checksum over the same identity, and do not delete the Bronze directory.

## Intact snapshot, pointer absent

Symptom. `cfi diagnose --input-sha256` reports `snapshot_state` `intact`, `silver_state` `intact`, and `pointer_state` `absent`. The summary route still returns 404 `not_published`.

Evidence. `snapshot_storage_key` is present. `pointer_sha256` is omitted. `summary.json` and `manifest.json` already exist under that snapshot key. Exit 0.

Diagnosis. Gold wrote the snapshot and did not replace the pointer. The usual cause is a pointer-lock timeout after the snapshot directory was published. `cfi gold` then exits 5 with `lock_unavailable`, `snapshot_durable` true, and `pointer_replaced` false.

Supported action. Run `cfi gold` again with the same `source_id`, `dataset_version`, `input_sha256`, and a new `run_id`. Do not pass a different checksum. The existing protocol creates `current.json` when the pointer is absent and the sealed snapshot matches.

Verification. `cfi gold` returns `pointer_created` and `published`. The snapshot's `summary.json` and `manifest.json` are byte for byte the same as before the retry. `cfi diagnose` then reports `pointer_state` `intact` and `same_generation` true. The summary route returns 200 for that identity. Another `cfi gold` for the same checksum is a no-op and does not replace the pointer.

Stop. Stop when `silver_state` is not `intact`, or when `snapshot_state` is `incomplete` or `mismatch`. Do not create the pointer by hand. Do not delete the snapshot to publish it again.

## Invalid pointer

Symptom. `cfi diagnose` reports `pointer_state` `pointer_invalid`. The summary route returns 503 `pointer_invalid`.

Evidence. `pointer_sha256` is the checksum of the captured `current.json` bytes when the file was read. `published_storage_key` is omitted. Exit 0 from diagnose. `cfi gold` for that identity exits 3 with `pointer_invalid`, including when `--promote` is set.

Diagnosis. The captured file is not a pointer document for this identity, or it is not a regular file. The publication protocol refuses to replace it.

Supported action. None in this command set. Collect the diagnose report and the `pointer_sha256`.

Verification. A second `cfi diagnose` still reports `pointer_invalid` and the same `pointer_sha256` while the file is unchanged. `cfi gold --promote` still exits 3 and leaves the file unchanged.

Stop. Stop. Do not edit, replace, or delete `current.json` by hand. Do not treat `--promote` as a repair.

## Snapshot absent or divergent

Symptom. The pointer was read and `pointer_state` is `snapshot_absent`, `integrity_mismatch`, or `schema_mismatch`. Or, with a checksum, `snapshot_state` is `incomplete` or `mismatch`.

Evidence. Diagnose exit 0. For a pointer that names a missing directory, `pointer_state` is `snapshot_absent` and the summary route returns 503 `snapshot_absent`. For a hash, size, or document disagreement, the state is `integrity_mismatch` on the pointer or `mismatch` on the requested snapshot. A summary or manifest that fails its schema is `schema_mismatch` on the pointer and `incomplete` on the requested snapshot. A read that raises a filesystem error is exit 4 `storage_error` and is not one of these states.

Diagnosis. `snapshot_absent` means the captured pointer names a snapshot directory that is not there. `integrity_mismatch` and `mismatch` mean the bytes that were read disagree with the sealed document. `incomplete` means a requested snapshot directory exists but is not a sealed pair of documents. These are properties of the captured bytes, not of a concurrent replace that this command did not finish reading.

Supported action. If `silver_state` is `intact` and the requested snapshot directory is `absent`, `cfi gold` with that checksum may publish through the normal protocol. If the pointer itself is `snapshot_absent` or `integrity_mismatch`, do not run `cfi gold` as a repair: an invalid or divergent pointer is not replaced.

Verification. After a normal publication, diagnose reports `snapshot_state` `intact` and, once the pointer names that snapshot, `pointer_state` `intact`. The new snapshot directory is the one `cfi gold` created. An existing divergent directory is unchanged.

Stop. Stop when the snapshot directory exists and is `incomplete`, `mismatch`, `integrity_mismatch`, or `schema_mismatch`. Do not rewrite `summary.json`, `manifest.json`, or the pointer. Do not delete the snapshot directory.

## Silver absent

Symptom. `cfi diagnose --input-sha256` reports `silver_state` `absent`.

Evidence. `silver_storage_key` is omitted. `bronze_state` is reported separately. Exit 0. `cfi transform` and `cfi gold` for that checksum do not create a generation when Bronze is not an intact match.

Diagnosis. The Silver directory for this checksum is not present. Gold cannot attach a pointer to an existing snapshot without that Silver generation: `cfi gold` reports `silver_absent` and does not read the snapshot.

Supported action. Run `cfi transform` only when `bronze_state` is `intact` and the Bronze manifest checksum is the one passed to diagnose. Then run `cfi gold` for that same checksum.

Verification. Diagnose reports `silver_state` `intact` before `cfi gold`. Publication creates a new generation directory rather than changing the Bronze original.

Stop. Stop when Bronze is `absent`, `incomplete`, or `mismatch`. Do not reconstruct Silver from a Gold snapshot, and do not delete Bronze to choose a different checksum.

## Lock unavailable

Symptom. `cfi gold` exits 5 with `code` `lock_unavailable`.

Evidence. Stdout `status` is `partial` when `snapshot_durable` is true and `pointer_replaced` is false. Stdout `status` is `rejected` when the snapshot lock times out before a snapshot exists. Diagnose does not report locks and does not create a lock file.

Diagnosis. Another process holds the advisory lock. The timeout does not take that lock. A partial result means the snapshot directory is already published.

Supported action. Wait until the other process releases the lock, then run the same `cfi gold` command again. Use diagnose first when the result was partial, and continue only if the snapshot is `intact` and Silver is `intact`.

Verification. The retry returns `pointer_created` or `noop`. The sealed snapshot bytes do not change. The lock marker file may remain; deleting it is not part of recovery.

Stop. Stop if the retry returns exit 4 or exit 3. Do not delete the lock file to seize a live lock.

## Read-only storage or permission denied

Symptom. A publication command or `cfi diagnose` exits 4 with `storage_error`.

Evidence. Stdout is `{"code":"storage_error","exit_code":4,"status":"rejected"}` for diagnose, with no `dry_run` field. The event `command_finished` uses `rejected` and `error_code` `storage_error`. The report is absent. The volume's files are unchanged by diagnose. A readable read-only volume still allows diagnose to exit 0 when the reads succeed.

Diagnosis. The process could not read, or a publication could not create a directory. This is not an integrity result. EROFS and EACCES on publication paths are `storage_error`. A file above the read bound uses the same exit and envelope: the inspection stops before a hash comparison, so the file is not reported as corrupt. A lock conflict uses exit 5 instead.

Supported action. Restore read access and run `cfi diagnose` again. Restore write access only before a publication command. Do not retry `cfi gold` on top of exit 4 until diagnose can read the identity.

Verification. Diagnose exits 0 and reports states. A publication is attempted only after that report says the supported action is a publication command.

Stop. Stop while the volume remains read-only or unauthorized. Do not copy the tree to a writable disk and publish it as if it were the same volume.

## Interrupted publication

Symptom. A `cfi ingest`, `cfi transform`, or `cfi gold` process ends without a JSON result. `cfi diagnose` may show a sealed artifact, an absent artifact, or a Gold snapshot with no pointer.

Evidence. The final directory is either absent or complete. A file under `.incoming` may remain. A lock marker under `bronze/locks`, `silver/locks`, or `gold/locks` may remain. The operating-system lock is released when the process exits. `cfi diagnose` does not remove either residue.

Diagnosis. The process stopped before or after the exclusive rename, or after the snapshot rename and before the pointer replace. A leftover temporary directory is not a published dataset and it is not proof that a published directory is corrupt. A successful command, including one that synced a file before renaming it, is not evidence the bytes would survive power loss. Directory sync failure is ignored and is not a durability claim.

ENOSPC, EIO, EACCES, and EROFS while creating a temporary directory, opening a file for write, writing it, or syncing that file are `storage_error`, exit 4. Ingest and transform then report `rejected`, and the final directory is absent. Gold reports `rejected` with `snapshot_durable` false when the snapshot directory was not published. After that directory exists, a failure while writing or replacing the pointer is `partial`, exit 4, with `snapshot_durable` true and `pointer_replaced` false. Run `cfi gold` again with the same checksum to create the pointer. The snapshot bytes stay as they were. This classification does not rebuild a snapshot, replace an invalid pointer, or delete old temporary files.

A temporary directory under `.incoming` may remain when removal fails. The publication error stays the command result when a later removal fails. A path escape or reparse point found while removing a temporary is still reported. DuckDB errors while writing Parquet, read errors while inspecting an existing artifact, and an I/O error while opening a lock marker are outside this classification. None of these results shows that the bytes would survive power loss.

Supported action. Run `cfi diagnose` for the same identity. When the report shows an absent stage and the previous stage is intact, run that stage's command again with the same identity. When a Gold snapshot is intact, Silver is intact, and the pointer is absent, run `cfi gold` again with the same checksum. When the pointer already names that snapshot, the same command is a no-op.

Verification. Published bytes do not change on replay. A new pointer is created only by `cfi gold` when the pointer was absent. The summary route agrees with the pointer. Temporary files and lock markers may still be present.

Stop. Stop when diagnose reports an incomplete or divergent artifact, or when the pointer is invalid. Stop when the command exits 3. Stop on exit 4 until the volume accepts the failed operation. Do not treat `storage_error` as permission to delete `.incoming`, the lock marker, or `current.json`. A traceback instead of a JSON result means the failure was not classified, and that traceback does not prove corruption.
