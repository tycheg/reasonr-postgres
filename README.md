# ReasonR PostgreSQL adapters

The ReasonR engine adapters on PostgreSQL (on `pgx`): the main store, asynchronous projections, timers, the outbox, external requests waiting to be sent, idempotency keys, personal data keys, the event store, the forget log, feature flag rollout, effect stream coordination and the link of the notification hub on LISTEN/NOTIFY (it carries the engine's messages between instances as bytes). The adapters see the engine only through `port` and `registry`. Several `reasonrd` instances work on one database.

The standard `reasonrd` distribution ([reasonr-engine](https://github.com/tycheg/reasonr-engine)) includes the adapters. A server of your own imports the package:

```go
import _ "github.com/tycheg/reasonr-postgres/src" // stores: { type: postgres }, notifications.hub: { type: postgres }
```

## Contracts

It requires the contract of plugins alone (`reasonr-engine/port`): imports `port` and `port/registry`; tests: `port/porttest` (the byte-level conformance suites) and the store's own checks. The runs on the example models through the engine are the distribution's (`reasonr-dist`, `integration/postgres`). The stores implement the byte-level store ports (`port.RowStore` and the others of `port/rows.go`): the engine lays the model out, encrypts personal fields and computes the key columns; the adapter writes, reads and searches by them.

## Layout

| Path | What is in it |
|---|---|
| `src/store.go`, `src/layout.go`, `src/ddl.go` | The store, tables by the engine's layout, the system tables |
| `src/commit.go`, `src/entities.go`, `src/drop.go` | A unit of work in one transaction, entity rows in batches, dropping a tenant |
| `src/effects.go`, `src/events.go`, `src/requests.go`, `src/coord.go` | Timers and the outbox, the event store, external requests, effect stream cursors and leases |
| `src/keys.go`, `src/personal.go`, `src/reseal.go`, `src/forgetlog.go` | Personal data keys, encrypted fields, their migrations, the forget log |
| `src/features.go`, `src/transfer.go`, `src/hub.go` | Feature flags, state transfer, the hub link on LISTEN/NOTIFY |
| `src/register.go` | The `stores` and `notifications.hub` factories |
| `docs` | The documentation |

## Documentation

- [Configuration](docs/configuration.md)
- [Schema](docs/schema.md)
- [Development and tests](docs/development.md)
