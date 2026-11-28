GO ?= go
# The tests run on a live PostgreSQL when REASONR_TEST_POSTGRES is set,
# otherwise they are skipped: make pg-up, then
# REASONR_TEST_POSTGRES=postgres://reasonr:reasonr@localhost:55432/reasonr make test

.PHONY: all check fmt-check vet lint test test-race pg-up pg-down

all: check

check: fmt-check vet lint test

fmt-check:
	@out=$$(gofmt -l .); if [ -n "$$out" ]; then echo "gofmt needed:"; echo "$$out"; exit 1; fi

vet:
	$(GO) vet ./...

lint:
	staticcheck ./...

test:
	$(GO) test ./...

test-race:
	$(GO) test -race ./...

pg-up:
	docker run -d --name reasonr-pg -e POSTGRES_USER=reasonr -e POSTGRES_PASSWORD=reasonr -e POSTGRES_DB=reasonr -p 55432:5432 postgres:19

pg-down:
	docker rm -f reasonr-pg
