package postgres

import (
	"context"
	"errors"
	"strings"
	"testing"

	"github.com/tycheg/reasonr-engine/port"
	"github.com/tycheg/reasonr-engine/port/porttest"
)

// TestUniqueDuplicates: a unique index added over rows with repeated keys
// is refused with the repeated values listed, and the schema does not
// change: the transaction rolls back whole.
func TestUniqueDuplicates(t *testing.T) {
	ctx := context.Background()
	s := New(testPool(t, false), Options{})
	l := porttest.Layout()
	for i := range l.Tables {
		l.Tables[i].Indexes = nil
	}
	if err := s.EnsureSchema(ctx, l, []port.Tenant{porttest.T1}, keep); err != nil {
		t.Fatal(err)
	}
	if _, err := s.Commit(ctx, porttest.T1, write(order(1, 0, "SAME"), order(2, 0, "SAME"))); err != nil {
		t.Fatal(err)
	}
	l = porttest.Layout()
	l.Schema = []byte(`{"model":"shop","version":2}`)
	err := s.EnsureSchema(ctx, l, []port.Tenant{porttest.T1}, keep)
	var se *SchemaError
	if !errors.As(err, &se) || !strings.Contains(err.Error(), `t1: \x53414d45 (2 rows)`) {
		t.Fatalf("want an error with the repeats: %v", err)
	}
	var n int
	if err := s.pool.QueryRow(ctx, "select count(*) from rt_schema_versions").Scan(&n); err != nil || n != 1 {
		t.Fatalf("schema versions %d: %v", n, err)
	}
}
