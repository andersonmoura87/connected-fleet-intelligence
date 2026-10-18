# trip_record v1.0.0

Status: frozen historical contract. `schema.json` is the schema exported when `contract_version` was `1.0.0`. It is not regenerated from the current model.

The executable contract is [trip_record v1.1.0](../v1.1.0/README.md). Version 1.1.0 rejects timestamp literals with more than six fractional digits. Version 1.0.0 did not state that limit, and its parser discarded the extra digits.
