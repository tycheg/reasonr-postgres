# Configuration

```yaml
stores:
  main: { type: postgres, dsn_env: REASONR_DSN, tenancy: rls, max_conns: 20 }
  audit: { type: postgres, dsn_env: REASONR_AUDIT_DSN, max_conns: 2 }   # the forget log outside the main backup

state: main            # entities, synchronous projections, timers, the outbox, external requests, idempotency keys
events: main           # the event store (the bus reads it)
notifications:
  hub: { type: postgres }   # LISTEN/NOTIFY on the main store connections; or its own dsn
crypto:
  forget_log: audit
```

| Key | What it sets |
|---|---|
| `dsn`, `dsn_env` | The connection string, or the environment variable that holds it |
| `tenancy` | `column` (default) — every query filters by the `tenant` column; `rls` — Row-Level Security policies show a query only the rows of the tenant in `reasonr.tenant` (the application role must not be a superuser) |
| `max_conns` | The pool size |

A store of this type can serve as the main store, the projection store (`projections:`), the event store (`events:`), the forget log (`crypto.forget_log`), and the entity database of an engine kind that keeps its entities in memory (`port.RowSink`). The full example is `engine.postgres.yaml` in `reasonr-dist`.
