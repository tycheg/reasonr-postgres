package postgres

import (
	"bytes"
	"context"
	"fmt"
	"os"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/tycheg/reasonr-engine/port"
	"github.com/tycheg/reasonr-engine/port/porttest"
)

// Tests on a live PostgreSQL: the address is REASONR_TEST_POSTGRES
// (postgres://user:pass@host:port/db); without it the tests are skipped.
// Each check works in its own database schema, which is dropped afterwards.

var schemaSeq atomic.Int64

func dsn(t testing.TB) string {
	d := os.Getenv("REASONR_TEST_POSTGRES")
	if d == "" {
		t.Skip("REASONR_TEST_POSTGRES is not set")
	}
	return d
}

// appRole is a role without superuser: RLS policies do not apply to a
// superuser.
const appRole = "reasonr_app"

// testPool returns a pool working in a new database schema; app — under a
// role without superuser privileges.
func testPool(t testing.TB, app bool) *pgxpool.Pool {
	ctx := context.Background()
	d := dsn(t)
	admin, err := pgxpool.New(ctx, d)
	if err != nil {
		t.Fatal(err)
	}
	schema := fmt.Sprintf("t_%d_%d", time.Now().UnixNano()%1e9, schemaSeq.Add(1))
	if _, err := admin.Exec(ctx, "create schema "+schema); err != nil {
		t.Fatal(err)
	}
	cfg, err := pgxpool.ParseConfig(d)
	if err != nil {
		t.Fatal(err)
	}
	if app {
		_, err := admin.Exec(ctx, `do $$ begin
			if not exists (select 1 from pg_roles where rolname = '`+appRole+`') then
				create role `+appRole+` login password 'app' nosuperuser nobypassrls;
			end if; end $$`)
		if err == nil {
			_, err = admin.Exec(ctx, "grant usage, create on schema "+schema+" to "+appRole)
		}
		if err != nil {
			t.Fatal(err)
		}
		cfg.ConnConfig.User, cfg.ConnConfig.Password = appRole, "app"
	}
	cfg.ConnConfig.RuntimeParams["search_path"] = schema
	pool, err := pgxpool.NewWithConfig(ctx, cfg)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		pool.Close()
		_, _ = admin.Exec(context.Background(), "drop schema "+schema+" cascade")
		admin.Close()
	})
	return pool
}

// laid is a store in a new schema with the layout of the suites
// (porttest.Layout) for both tenants.
func laid(t *testing.T, opts Options) *Store {
	t.Helper()
	s := New(testPool(t, opts.RLS), opts)
	if err := s.EnsureSchema(context.Background(), porttest.Layout(), []port.Tenant{porttest.T1, porttest.T2}, keep); err != nil {
		t.Fatal(err)
	}
	return s
}

func keep(_ context.Context, _ port.Tenant, _ *port.Table, r port.Row) (port.Row, bool, error) {
	return r, false, nil
}

// order is a row of porttest.Orders with the id n and the code key code.
func order(n byte, version uint64, code string) port.Row {
	keys := make([]port.Scalar, len(porttest.Orders.Keys))
	keys[6] = []byte(code)
	return port.Row{ID: [16]byte{n}, Version: version, State: []byte(fmt.Sprintf(`{"n":%d,"v":%d}`, n, version)), Keys: keys}
}

// write is a unit of work that writes rows of porttest.Orders.
func write(rows ...port.Row) *port.UnitRecord {
	u := &port.UnitRecord{Outbox: []byte(`{}`), Now: porttest.Start}
	for _, r := range rows {
		u.Writes = append(u.Writes, port.RowWrite{Table: porttest.Orders, Row: r})
	}
	return u
}

// TestRLSIsolation: under the application role the policy shows only the
// rows of the tenant from reasonr.tenant, even without a tenant condition
// in the query.
func TestRLSIsolation(t *testing.T) {
	s := laid(t, Options{RLS: true})
	ctx := context.Background()
	for i, tn := range []port.Tenant{porttest.T1, porttest.T2, porttest.T2} {
		if _, err := s.Commit(ctx, tn, write(order(byte(i+1), 0, fmt.Sprint(i)))); err != nil {
			t.Fatal(err)
		}
	}
	count := func(tenant string) int {
		tx, err := s.pool.Begin(ctx)
		if err != nil {
			t.Fatal(err)
		}
		defer func() { _ = tx.Rollback(ctx) }()
		if _, err := tx.Exec(ctx, "select set_config('reasonr.tenant', $1, true)", tenant); err != nil {
			t.Fatal(err)
		}
		var n int
		if err := tx.QueryRow(ctx, "select count(*) from "+tableName(porttest.Orders)).Scan(&n); err != nil {
			t.Fatal(err)
		}
		return n
	}
	if n1, n2, none := count(string(porttest.T1)), count(string(porttest.T2)), count("other"); n1 != 1 || n2 != 2 || none != 0 {
		t.Fatalf("rows visible: t1 %d, t2 %d, a foreign tenant %d", n1, n2, none)
	}
}

// TestHubLarge: a message larger than the NOTIFY limit goes through an
// rt_hub_payloads row, also between two links on one database; the table
// holds only its ciphertext.
func TestHubLarge(t *testing.T) {
	ctx := context.Background()
	pool := testPool(t, false)
	if _, err := New(pool, Options{}).LastSchema(ctx, "base"); err != nil { // the system tables
		t.Fatal(err)
	}
	sender, receiver := NewHub(pool, nil), NewHub(pool, nil)
	defer sender.Close()
	defer receiver.Close()
	got := make(chan []byte, 1)
	if err := receiver.Listen(ctx, func(msg []byte) { got <- bytes.Clone(msg) }); err != nil {
		t.Fatal(err)
	}
	big := []byte(strings.Repeat("é", 10000))
	if err := sender.Send(ctx, big); err != nil {
		t.Fatal(err)
	}
	select {
	case msg := <-got:
		if !bytes.Equal(msg, big) {
			t.Fatalf("got %d bytes", len(msg))
		}
	case <-time.After(5 * time.Second):
		t.Fatal("the large message is not delivered")
	}
	var stored []byte
	if err := pool.QueryRow(ctx, "select payload from rt_hub_payloads order by id desc limit 1").Scan(&stored); err != nil {
		t.Fatal(err)
	}
	if bytes.Contains(stored, []byte("ééé")) {
		t.Error("the large message is stored as plain text")
	}
}
