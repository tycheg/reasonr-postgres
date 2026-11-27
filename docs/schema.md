# Schema

At startup `reasonrd` checks that the model is compatible with the stored data (RFC §32) and changes the schema in one transaction under an advisory lock, so instances starting together change it in turn:

- a row per entity or projection row: the state in `state jsonb` (canonical JSON) and columns only for index keys, with types whose SQL order matches the language's (strings `collate "C"`, timestamps as numeric nanoseconds, enumerations as value numbers); tables are `e_<context>__<type>` for entities and actors, `p_…` for projections;
- the system tables `rt_*` (timers, the outbox, idempotency keys, sequences, personal data keys, schema versions, hub payloads, external requests, feature flags, the forget log) and `ev_events`;
- new key columns are filled from `state` (renamed fields through `was`); a unique index over data with repeats is not created, and the error lists the repeated values for a manual migration (`reasonrd migrate --run`);
- personal fields (RFC §20) are stored encrypted with subject keys in the `sealed` column, index keys on them are blind values; when a field becomes or stops being personal, the rows are re-encrypted after the new schema version.

The outbox is read strictly in position order: a position taken by a transaction that has not committed yet stops the reader until the transaction ends.
