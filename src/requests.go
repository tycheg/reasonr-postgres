package postgres

import (
	"context"
	"sort"
	"time"

	"github.com/jackc/pgx/v5"

	"github.com/tycheg/reasonr-engine/port"
)

// requestsDDL holds the external requests waiting to be sent (RFC §18): a
// row per request is written at commit, the key is the tenant and the
// idempotency key. The dead column and the rt_requests_due index were left
// by the first version (a delivery queue with dead requests).
const requestsDDL = `
create table if not exists rt_requests (
  tenant     text   not null,
  key        uuid   not null,          -- the request's idempotency key
  seq        bigserial,                -- the write order
  position   bigint not null,          -- the unit of work that sent the request
  at         bigint not null,          -- its time, Unix nanoseconds
  next_at    bigint not null,          -- not selected earlier: a lease or a retry pause
  attempts   int    not null default 0,
  last_error text,                     -- the reason of the last failed attempt
  system     text   not null,
  request    text   not null,
  args       jsonb  not null,          -- without personal values
  sealed     jsonb,                    -- their ciphertexts
  timer      uuid,                     -- the deadline timer (the timeout parameter)
  primary key (tenant, key)
);
drop index if exists rt_requests_due;
alter table rt_requests drop column if exists dead;
create index if not exists rt_requests_next on rt_requests (next_at, seq);
`

// insertRequests writes the external requests of a unit of work with the
// position pos in the commit transaction (port.Requests). A request
// with a key that is already waiting to be sent is not written: the key is
// deterministic (the entity, the command, the time, the number), and for
// the recipient it is the same request.
func (c *committer) insertRequests(ctx context.Context, pos port.Position) error {
	if len(c.u.Requests) == 0 {
		return nil
	}
	var b pgx.Batch
	for _, r := range c.u.Requests {
		var timer any
		if r.Timer != nil {
			timer = *r.Timer
		}
		b.Queue(`insert into rt_requests (tenant, key, position, at, next_at, system, request, args, sealed, timer)
			values ($1, $2, $3, $4, 0, $5, $6, $7, $8, $9) on conflict do nothing`,
			string(c.tenant), r.Key, int64(pos), c.u.Now.UnixNano(), r.System, r.Request, r.Args, nullJSON(r.Sealed), timer)
	}
	return c.tx.SendBatch(ctx, &b).Close()
}

// LeaseRequests leases ready requests (port.Requests): rows held by
// other instances are skipped (skip locked); the moment of the deadline
// timer is read by the same query.
func (s *Store) LeaseRequests(ctx context.Context, now time.Time, limit int, ttl time.Duration) ([]port.QueuedRequest, error) {
	if limit <= 0 {
		limit = 100
	}
	rows, err := s.pool.Query(ctx, `with due as (
		select tenant, key from rt_requests
		where next_at <= $1
		order by next_at, seq limit $2 for update skip locked)
	update rt_requests q set next_at = $3, attempts = q.attempts + 1
	from due where q.tenant = due.tenant and q.key = due.key
	returning q.tenant, q.key, q.seq, q.position, q.at, q.attempts, q.system, q.request, q.args, q.sealed, q.timer is not null,
		(select t.fire_at from rt_timers t where t.tenant = q.tenant and t.id = q.timer)`,
		now.UnixNano(), limit, now.Add(ttl).UnixNano())
	if err != nil {
		return nil, err
	}
	type leased struct {
		port.QueuedRequest
		seq int64
	}
	var out []leased
	for rows.Next() {
		var r leased
		var tenant string
		var position, at int64
		var hasTimer bool
		var fireAt *int64
		if err := rows.Scan(&tenant, &r.Request.Key, &r.seq, &position, &at, &r.Attempts, &r.Request.System, &r.Request.Request,
			&r.Request.Args, &r.Request.Sealed, &hasTimer, &fireAt); err != nil {
			rows.Close()
			return nil, err
		}
		r.Tenant, r.Position, r.At = port.Tenant(tenant), port.Position(position), time.Unix(0, at).UTC()
		switch {
		case fireAt != nil:
			r.Deadline = time.Unix(0, *fireAt).UTC()
		case hasTimer:
			r.TimerGone = true
		}
		out = append(out, r)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	sort.Slice(out, func(i, j int) bool { return out[i].seq < out[j].seq })
	res := make([]port.QueuedRequest, len(out))
	for i := range out {
		res[i] = out[i].QueuedRequest
	}
	return res, nil
}

// RequestDone deletes a request (port.Requests).
func (s *Store) RequestDone(ctx context.Context, tenant port.Tenant, key [16]byte) error {
	_, err := s.pool.Exec(ctx, "delete from rt_requests where tenant = $1 and key = $2", string(tenant), key)
	return err
}

// RetryRequest schedules the next attempt (port.Requests).
func (s *Store) RetryRequest(ctx context.Context, tenant port.Tenant, key [16]byte, at time.Time, reason string) error {
	_, err := s.pool.Exec(ctx, "update rt_requests set next_at = $3, last_error = $4 where tenant = $1 and key = $2",
		string(tenant), key, at.UnixNano(), reason)
	return err
}
