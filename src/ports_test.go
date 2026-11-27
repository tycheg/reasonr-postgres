package postgres

import (
	"context"
	"testing"

	"github.com/tycheg/reasonr-engine/port"
	"github.com/tycheg/reasonr-engine/port/porttest"
)

// The byte-level conformance suites of the contract (port/porttest), in a
// schema of their own for each check.

func TestPorts(t *testing.T) {
	porttest.RunStore(t, func(t *testing.T) any { return New(testPool(t, false), Options{}) })
}

func TestPortsRLS(t *testing.T) {
	porttest.RunStore(t, func(t *testing.T) any { return New(testPool(t, true), Options{RLS: true}) })
}

func TestHubLink(t *testing.T) {
	pool := testPool(t, false)
	if _, err := New(pool, Options{}).LastSchema(context.Background(), "base"); err != nil { // the system tables
		t.Fatal(err)
	}
	porttest.RunHubLink(t, func(t *testing.T) port.HubLink { return NewHub(pool, nil) })
}
