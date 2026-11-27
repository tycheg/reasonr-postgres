package postgres

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/tycheg/reasonr-engine/port"
	"github.com/tycheg/reasonr-engine/port/porttest"
)

// TestOutboxOrder: a position issued to a transaction that commits later
// is not lost — the read waits for it; the position of a rolled back
// transaction is skipped once every transaction that could have taken it
// has finished.
func TestOutboxOrder(t *testing.T) {
	ctx := context.Background()
	s := laid(t, Options{})
	payload := `{"at":"2026-01-01T00:00:00Z"}`
	slow, err := s.pool.Begin(ctx) // takes position 1 and does not commit
	if err != nil {
		t.Fatal(err)
	}
	if _, err := slow.Exec(ctx, "insert into rt_outbox (tenant, txid, payload) values ('t1', pg_current_xact_id(), $1)", payload); err != nil {
		t.Fatal(err)
	}
	if _, err := s.pool.Exec(ctx, "insert into rt_outbox (tenant, txid, payload) values ('t1', pg_current_xact_id(), $1)", payload); err != nil {
		t.Fatal(err)
	}
	read := func(from port.Position) []port.Position {
		t.Helper()
		us, err := s.Read(ctx, from, 0)
		if err != nil {
			t.Fatal(err)
		}
		var out []port.Position
		for _, u := range us {
			out = append(out, u.Position)
		}
		return out
	}
	for range 3 {
		if got := read(0); len(got) != 0 {
			t.Fatalf("read past the uncommitted position 1: %v", got)
		}
	}
	if err := slow.Commit(ctx); err != nil {
		t.Fatal(err)
	}
	if got := read(0); len(got) != 2 || got[0] != 1 || got[1] != 2 {
		t.Fatalf("after the commit: %v", got)
	}

	// A rolled back transaction leaves the gap 3.
	rolled, _ := s.pool.Begin(ctx)
	if _, err := rolled.Exec(ctx, "insert into rt_outbox (tenant, txid, payload) values ('t1', pg_current_xact_id(), $1)", payload); err != nil {
		t.Fatal(err)
	}
	_ = rolled.Rollback(ctx)
	if _, err := s.pool.Exec(ctx, "insert into rt_outbox (tenant, txid, payload) values ('t1', pg_current_xact_id(), $1)", payload); err != nil {
		t.Fatal(err)
	}
	if got := read(2); len(got) != 0 {
		t.Fatalf("the gap is accepted without a check: %v", got)
	}
	// New transactions move xmin past the bound.
	for range 2 {
		if _, err := s.pool.Exec(ctx, "select pg_current_xact_id()"); err != nil {
			t.Fatal(err)
		}
	}
	if got := read(2); len(got) != 1 || got[0] != 4 {
		t.Fatalf("after the transactions finished: %v", got)
	}
}

// TestConcurrentCommits: units of work on one row from many goroutines —
// each commit with a stale version is a conflict, and with retries every
// one is applied once.
func TestConcurrentCommits(t *testing.T) {
	ctx := context.Background()
	s := laid(t, Options{})
	if _, err := s.Commit(ctx, porttest.T1, write(order(1, 0, "a"))); err != nil {
		t.Fatal(err)
	}
	const n = 20
	var wg sync.WaitGroup
	errs := make(chan error, n)
	for range n {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				r, _, err := s.Load(ctx, porttest.T1, porttest.Orders, [16]byte{1})
				if err == nil {
					_, err = s.Commit(ctx, porttest.T1, write(order(1, r.Version, "a")))
				}
				if !errors.Is(err, port.ErrConflict) {
					errs <- err
					return
				}
			}
		}()
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		if err != nil {
			t.Fatal(err)
		}
	}
	if r, _, err := s.Load(ctx, porttest.T1, porttest.Orders, [16]byte{1}); err != nil || r.Version != n+1 {
		t.Fatalf("version %d: %v", r.Version, err)
	}
}

// TestDoubleLease: two instances lease timers at the same time — each
// timer goes to one of them.
func TestDoubleLease(t *testing.T) {
	ctx := context.Background()
	s := laid(t, Options{})
	u := write()
	for i := range 30 {
		u.Timers = append(u.Timers, port.TimerRecord{ID: [16]byte{0xf0, byte(i)}, At: porttest.Start, Entity: porttest.Orders.Type,
			EntityID: []byte(fmt.Sprintf(`"%d"`, i)), Command: "expire", Args: []byte(`{}`)})
	}
	if _, err := s.Commit(ctx, porttest.T1, u); err != nil {
		t.Fatal(err)
	}
	other := New(s.pool, Options{}) // the second instance
	seen := map[[16]byte]int{}
	var mu sync.Mutex
	var wg sync.WaitGroup
	for _, st := range []*Store{s, other, s, other} {
		wg.Add(1)
		go func() {
			defer wg.Done()
			ts, err := st.Lease(ctx, port.LeaseRequest{Now: porttest.Start.Add(time.Minute), Limit: 10, TTL: time.Minute})
			if err != nil {
				t.Error(err)
				return
			}
			mu.Lock()
			for _, lt := range ts {
				seen[lt.ID]++
			}
			mu.Unlock()
		}()
	}
	wg.Wait()
	if len(seen) != 30 {
		t.Fatalf("%d timers of 30 leased", len(seen))
	}
	for id, n := range seen {
		if n != 1 {
			t.Fatalf("timer %x leased %d times", id, n)
		}
	}
}
