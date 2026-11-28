# Development and tests

```bash
docker run -d --name reasonr-pg -e POSTGRES_USER=reasonr -e POSTGRES_PASSWORD=reasonr -e POSTGRES_DB=reasonr -p 55432:5432 postgres:19
REASONR_TEST_POSTGRES=postgres://reasonr:reasonr@localhost:55432/reasonr go test ./...
```

Without `REASONR_TEST_POSTGRES` the tests are skipped. Each check works in its own database schema and drops it afterwards. The tests run the byte-level conformance suites of the contract (`port/porttest`) and the store's own checks (concurrent commits, a unique index over repeated keys, outbox gaps, double leases, RLS, large hub messages). The runs on the example models through the engine — scenarios, migrations of schemas and personal data — are the distribution's (`reasonr-dist`, `integration/postgres`).

`make check` runs gofmt, vet, staticcheck and the tests; `make pg-up` starts the PostgreSQL above. A release is a tag `vX.Y.Z`.
