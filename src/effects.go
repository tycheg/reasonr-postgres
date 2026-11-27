package postgres

import (
	"context"
	"errors"
	"sort"
	"time"

	"github.com/jackc/pgx/v5"

	"github.com/tycheg/reasonr-engine/port"
)

// Lease selects fired timers and leases them (port.TimerStore): rows held
// by other instances are skipped (skip locked).
func (s *Store) Lease(ctx context.Context, req port.LeaseRequest) ([]port.LeasedTimerRecord, error) {
	now := req.Now.UnixNano()
	limit := req.Limit
	if limit <= 0 {
		limit = 100
	}
	// By the scenario clock (a time offset in a test or a demo) a timer's
	// moment and lease have their offset; otherwise — by the real clock (the index).
	cond, order, set := "fire_at <= $2 and (lease_until is null or lease_until <= $2)", "fire_at, seq", "$4"
	if req.Scenario {
		cond = "fire_at + time_offset <= $2 and (lease_until is null or lease_until + time_offset <= $2)"
		order, set = "fire_at + time_offset, seq", "$4 - t.time_offset"
	}
	rows, err := s.pool.Query(ctx, `with due as (
		select tenant, id from rt_timers
		where ($1 = '' or tenant = $1) and dead is null and `+cond+`
		order by `+order+` limit $3 for update skip locked)
	update rt_timers t set lease_until = `+set+`, attempts = t.attempts + 1
	from due where t.tenant = due.tenant and t.id = due.id
	returning t.tenant, t.id, t.fire_at, t.seq, t.attempts, t.entity_type, t.entity_id, t.command, t.args, t.sealed, t.time_offset`,
		string(req.Tenant), now, limit, req.Now.Add(req.TTL).UnixNano())
	if err != nil {
		return nil, err
	}
	type leased struct {
		port.LeasedTimerRecord
		seq int64
	}
	var out []leased
	for rows.Next() {
		var lt leased
		var tenant string
		var fireAt, offset int64
		if err := rows.Scan(&tenant, &lt.ID, &fireAt, &lt.seq, &lt.Attempts, &lt.Entity, &lt.EntityID, &lt.Command, &lt.Args, &lt.Sealed, &offset); err != nil {
			rows.Close()
			return nil, err
		}
		lt.Tenant, lt.At, lt.Offset = port.Tenant(tenant), time.Unix(0, fireAt).UTC(), time.Duration(offset)
		out = append(out, lt)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	sort.Slice(out, func(i, j int) bool {
		ai, aj := out[i].At.Add(out[i].Offset), out[j].At.Add(out[j].Offset)
		return ai.Before(aj) || ai.Equal(aj) && out[i].seq < out[j].seq
	})
	res := make([]port.LeasedTimerRecord, len(out))
	for i := range out {
		res[i] = out[i].LeasedTimerRecord
	}
	return res, nil
}

// Retry releases the lease after a fault (port.TimerStore).
func (s *Store) Retry(ctx context.Context, tenant port.Tenant, id [16]byte, at time.Time) error {
	_, err := s.pool.Exec(ctx, "update rt_timers set lease_until = $3 where tenant = $1 and id = $2", string(tenant), id, at.UnixNano())
	return err
}

// Dead moves a timer to the dead ones (port.TimerStore).
func (s *Store) Dead(ctx context.Context, tenant port.Tenant, id [16]byte, reason string) error {
	_, err := s.pool.Exec(ctx, "update rt_timers set dead = $3 where tenant = $1 and id = $2", string(tenant), id, reason)
	return err
}

// Revive returns the dead timers of a tenant to work (reasonrd timers
// retry); tenant "" — of all. It returns their number.
func (s *Store) Revive(ctx context.Context, tenant port.Tenant) (int64, error) {
	tag, err := s.pool.Exec(ctx, "update rt_timers set dead = null, attempts = 0, lease_until = null where dead is not null and ($1 = '' or tenant = $1)",
		string(tenant))
	return tag.RowsAffected(), err
}

// Read returns the units of work after the position from (port.Outbox).
// Positions are issued by a sequence before the commit, so a transaction
// with a smaller position may commit later. A gap in positions is either
// such a transaction still running or one that rolled back. Reading stops
// at a gap and remembers the bound — the snapshot's xmax; when the
// snapshot's xmin passes it, every transaction that could have taken the
// skipped positions has finished, and the gap is final. So positions are
// read strictly in increasing order and no committed one is lost.
func (s *Store) Read(ctx context.Context, from port.Position, limit int) ([]port.OutboxRecord, error) {
	if limit <= 0 {
		limit = 1000
	}
	rows, err := s.pool.Query(ctx, "select position, tenant, payload from rt_outbox where position > $1 order by position limit $2", int64(from), limit)
	if err != nil {
		return nil, err
	}
	var found []outboxRow
	for rows.Next() {
		var r outboxRow
		if err := rows.Scan(&r.pos, &r.tenant, &r.payload); err != nil {
			rows.Close()
			return nil, err
		}
		found = append(found, r)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	var xmin, xmax uint64
	if len(found) > 0 && found[0].pos != from+1 || hasGap(found) {
		if err := s.pool.QueryRow(ctx, `select pg_snapshot_xmin(pg_current_snapshot())::text::bigint,
			pg_snapshot_xmax(pg_current_snapshot())::text::bigint`).Scan(&xmin, &xmax); err != nil {
			return nil, err
		}
	}
	var out []port.OutboxRecord
	next := from + 1
	for _, r := range found {
		if r.pos != next && !s.gapClosed(next, xmin, xmax) {
			break
		}
		out = append(out, port.OutboxRecord{Tenant: port.Tenant(r.tenant), Position: r.pos, Payload: r.payload})
		next = r.pos + 1
	}
	s.gapMu.Lock()
	for g := range s.gaps {
		if g < next {
			delete(s.gaps, g) // the gap filled up
		}
	}
	s.gapMu.Unlock()
	return out, nil
}

type outboxRow struct {
	pos     port.Position
	tenant  string
	payload []byte
}

func hasGap(rs []outboxRow) bool {
	for i := 1; i < len(rs); i++ {
		if rs[i].pos != rs[i-1].pos+1 {
			return true
		}
	}
	return false
}

// gapClosed reports whether the gap starting at position g is final.
func (s *Store) gapClosed(g port.Position, xmin, xmax uint64) bool {
	s.gapMu.Lock()
	defer s.gapMu.Unlock()
	fence, ok := s.gaps[g]
	if !ok {
		s.gaps[g] = xmax
		return false
	}
	if xmin < fence {
		return false
	}
	delete(s.gaps, g)
	return true
}

// Wait waits for an outbox entry after the position after
// (port.Outbox): LISTEN reasonr_outbox, with a check every second in
// case a notification is lost.
func (s *Store) Wait(ctx context.Context, after port.Position) error {
	conn, err := s.pool.Acquire(ctx)
	if err != nil {
		return err
	}
	defer conn.Release()
	if _, err := conn.Exec(ctx, "listen reasonr_outbox"); err != nil {
		return err
	}
	defer func() { _, _ = conn.Exec(context.WithoutCancel(ctx), "unlisten reasonr_outbox") }()
	for {
		var last int64
		if err := conn.QueryRow(ctx, "select coalesce(max(position), 0) from rt_outbox").Scan(&last); err != nil {
			return err
		}
		if port.Position(last) > after {
			return nil
		}
		wctx, cancel := context.WithTimeout(ctx, time.Second)
		_, err := conn.Conn().WaitForNotification(wctx)
		cancel()
		if err != nil && ctx.Err() != nil {
			return ctx.Err()
		}
	}
}

// Apply applies the rows of asynchronous projections of the unit at
// position pos (port.AsyncProjections) and erases the data of forgotten
// subjects in their rows: the rows, forget and the position in one
// transaction. A row is applied by its version (applyRow), so positions
// apply in any order and a repeat changes nothing; Applied is the largest
// applied position.
func (s *Store) Apply(ctx context.Context, tenant port.Tenant, pos port.Position, writes []port.RowWrite, deletes []port.RowDelete,
	forget []port.SubjectRef, forgetTables []*port.Table, rewrite port.Rewriter) error {
	err := pgx.BeginFunc(ctx, s.pool, func(tx pgx.Tx) error {
		ctx := s.withTx(ctx, tx)
		if _, err := tx.Exec(ctx, "select set_config('reasonr.tenant', $1, true)", string(tenant)); err != nil {
			return err
		}
		c := &committer{s: s, tx: tx, tenant: tenant, u: &port.UnitRecord{}}
		for i := range writes {
			if err := c.applyRow(ctx, pos, writes[i].Table, writes[i].Key, &writes[i].Row); err != nil {
				return err
			}
		}
		for i := range deletes {
			if err := c.applyRow(ctx, pos, deletes[i].Table, deletes[i].Key, &port.Row{ID: deletes[i].ID, Version: ^uint64(0)}); err != nil {
				return err
			}
		}
		if len(forget) > 0 {
			if err := erase(ctx, tx, tenant, forget, forgetTables, rewrite); err != nil {
				return err
			}
		}
		_, err := tx.Exec(ctx, `insert into rt_applied (tenant, position) values ($1, $2)
			on conflict (tenant) do update set position = greatest(rt_applied.position, excluded.position)`, string(tenant), int64(pos))
		return err
	})
	return conflict(err)
}

// applyRow applies a row of an asynchronous projection at position pos by
// row version (RFC §18): only if pos is greater than the row version and
// the version of its deletion mark (rt_deleted). A write sets the row
// version to pos; a deletion (Version == ^0) leaves a mark with version pos.
func (c *committer) applyRow(ctx context.Context, pos port.Position, t *port.Table, key string, r *port.Row) error {
	version, exists, err := c.lockRow(ctx, t, r.ID)
	if err != nil {
		return err
	}
	var deleted int64
	err = c.tx.QueryRow(ctx, "select version from rt_deleted where tenant = $1 and type = $2 and id = $3 for update",
		string(c.tenant), t.Type, key).Scan(&deleted)
	if err != nil && !errNoRows(err) {
		return err
	}
	err = nil
	if uint64(pos) <= max(version, uint64(deleted)) {
		return nil
	}
	if r.Version == ^uint64(0) {
		if exists {
			if _, err := c.exec(ctx, "delete from "+tableName(t)+" where tenant = $1 and id = $2", string(c.tenant), r.ID); err != nil {
				return err
			}
		}
		_, err := c.exec(ctx, `insert into rt_deleted (tenant, type, id, version) values ($1, $2, $3, $4)
			on conflict (tenant, type, id) do update set version = excluded.version`, string(c.tenant), t.Type, key, int64(pos))
		return err
	}
	if err := c.write(ctx, t, *r, exists, version, uint64(pos)); err != nil {
		return err
	}
	if deleted > 0 {
		_, err = c.exec(ctx, "delete from rt_deleted where tenant = $1 and type = $2 and id = $3", string(c.tenant), t.Type, key)
	}
	return err
}

// Applied returns the last applied position (port.AsyncProjections).
func (s *Store) Applied(ctx context.Context, tenant port.Tenant) (port.Position, error) {
	var pos int64
	err := s.pool.QueryRow(ctx, "select position from rt_applied where tenant = $1", string(tenant)).Scan(&pos)
	if errors.Is(err, pgx.ErrNoRows) {
		return 0, nil
	}
	return port.Position(pos), err
}
