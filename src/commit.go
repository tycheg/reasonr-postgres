package postgres

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"

	"github.com/tycheg/reasonr-engine/port"
)

// Commit writes a unit of work in one transaction (port.RowStore): the
// fired timer, entity rows with version checks, synchronous projection
// rows, uniqueness (database unique indexes), timers, the outbox entry,
// the personal data key check, forget (RFC §20 §7.1), the requests and the
// response by idempotency key.
func (s *Store) Commit(ctx context.Context, tenant port.Tenant, u *port.UnitRecord) (port.Position, error) {
	var pos port.Position
	err := pgx.BeginFunc(ctx, s.pool, func(tx pgx.Tx) error {
		c := &committer{s: s, tx: tx, tenant: tenant, u: u}
		var err error
		pos, err = c.run(s.withTx(ctx, tx))
		return err
	})
	if err != nil {
		return 0, conflict(err)
	}
	if pos > 0 {
		s.kick()
	}
	return pos, nil
}

// kick wakes the outbox readers: a separate goroutine sends NOTIFY
// reasonr_outbox at most once in 5 ms. A NOTIFY inside the commit
// transaction would serialize the commits of every instance (the
// notification queue is locked during COMMIT) — the load test lost a third
// of the throughput that way.
func (s *Store) kick() {
	s.notifyOnce.Do(func() {
		s.notifyCh = make(chan struct{}, 1)
		ctx, cancel := context.WithCancel(context.Background())
		s.notifyStop = cancel
		go func() {
			for {
				select {
				case <-ctx.Done():
					return
				case <-s.notifyCh:
				}
				if _, err := s.pool.Exec(ctx, "select pg_notify('reasonr_outbox', '')"); err != nil && ctx.Err() == nil {
					s.log.Warn("postgres: the outbox notification is not sent", "error", err)
				}
				select {
				case <-ctx.Done():
					return
				case <-time.After(5 * time.Millisecond):
				}
			}
		}()
	})
	select {
	case s.notifyCh <- struct{}{}:
	default:
	}
}

// conflict turns serialization and deadlock errors into ErrConflict: the
// kernel repeats the command.
func conflict(err error) error {
	var pe *pgconn.PgError
	if errors.As(err, &pe) && (pe.Code == "40001" || pe.Code == "40P01") {
		return fmt.Errorf("%w: %s", port.ErrConflict, pe.Message)
	}
	return err
}

type committer struct {
	s      *Store
	tx     pgx.Tx
	tenant port.Tenant
	u      *port.UnitRecord
}

func (c *committer) exec(ctx context.Context, sql string, args ...any) (int64, error) {
	tag, err := c.tx.Exec(ctx, sql, args...)
	return tag.RowsAffected(), err
}

func (c *committer) run(ctx context.Context) (port.Position, error) {
	u, tenant := c.u, string(c.tenant)
	var txid uint64
	if err := c.tx.QueryRow(ctx, "select pg_current_xact_id()::text::bigint from set_config('reasonr.tenant', $1, true)", tenant).Scan(&txid); err != nil {
		return 0, err
	}
	if err := c.newKeys(ctx); err != nil {
		return 0, err
	}
	if u.Fired != nil {
		n, err := c.exec(ctx, "delete from rt_timers where tenant = $1 and id = $2", tenant, *u.Fired)
		if err != nil {
			return 0, err
		}
		if n == 0 {
			return 0, port.ErrTimerGone
		}
	}
	var pos port.Position
	if u.Outbox != nil {
		for i := range u.Writes {
			w := &u.Writes[i]
			if w.Projection {
				if err := c.projection(ctx, w); err != nil {
					return 0, err
				}
				continue
			}
			if err := c.write(ctx, w.Table, w.Row, w.Row.Version > 0, w.Row.Version, w.Row.Version+1); err != nil {
				return 0, err
			}
		}
		for _, d := range u.Deletes {
			if _, err := c.exec(ctx, "delete from "+tableName(d.Table)+" where tenant = $1 and id = $2", tenant, d.ID); err != nil {
				return 0, err
			}
		}
		if err := c.timers(ctx); err != nil {
			return 0, err
		}
		var err error
		if pos, err = c.outbox(ctx, txid, u.Outbox); err != nil {
			return 0, err
		}
		if err := c.insertRequests(ctx, pos); err != nil {
			return 0, err
		}
	}
	if u.Idempotency != "" && u.Result != nil {
		result, err := u.Result(pos)
		if err != nil {
			return 0, err
		}
		n, err := c.exec(ctx, `insert into rt_idempotency (tenant, key, result, expires_at) values ($1, $2, $3, $4)
			on conflict (tenant, key) do update set result = excluded.result, expires_at = excluded.expires_at
			where rt_idempotency.expires_at <= $5`, tenant, u.Idempotency, result, u.Expires, u.Now)
		if err != nil {
			return 0, err
		}
		if n == 0 {
			return 0, port.ErrDuplicateKey
		}
	}
	return pos, nil
}

// newKeys writes the subject keys the unit created: version 1 or the next
// one over the tombstone of the previous version; a key another
// transaction wrote first is ErrConflict (the command repeats with it).
func (c *committer) newKeys(ctx context.Context) error {
	for _, k := range c.u.NewKeys {
		n, err := c.exec(ctx, `insert into rt_subject_keys (tenant, subject_type, subject, version, wrapped) values ($1, $2, $3, $4, $5)
			on conflict (tenant, subject_type, subject) do update set version = excluded.version, wrapped = excluded.wrapped
			where rt_subject_keys.wrapped is null and rt_subject_keys.version = excluded.version - 1`,
			string(c.tenant), k.Subject.Type, k.Subject.ID, int64(k.Version), k.Wrapped)
		if err != nil {
			return err
		}
		if n == 0 {
			return fmt.Errorf("%w: the subject key %s %s was created concurrently", port.ErrConflict, k.Subject.Type, k.Subject.ID)
		}
	}
	return nil
}

// write writes a row of the table t with the version next: a new one —
// insert, an existing one — update with a check of the previous version
// version. A unique index violation is *port.UniqueKeyViolation.
func (c *committer) write(ctx context.Context, t *port.Table, r port.Row, existed bool, version, next uint64) error {
	cols := []string{"tenant", "id", "version", "state"}
	args := []any{string(c.tenant), r.ID, int64(next), r.State}
	if t.Personal {
		cols = append(cols, "sealed", "subjects")
		args = append(args, nullJSON(r.Sealed), subjectTexts(r.Subjects))
	}
	for i, k := range t.Keys {
		cols = append(cols, k.Name)
		var v any
		if i < len(r.Keys) {
			v = r.Keys[i]
		}
		args = append(args, v)
	}
	var sql string
	if existed {
		var set []string
		for i, col := range cols[2:] {
			set = append(set, col+" = $"+strconv.Itoa(i+3))
		}
		args = append(args, int64(version))
		sql = fmt.Sprintf("update %s set %s where tenant = $1 and id = $2 and version = $%d", tableName(t), strings.Join(set, ", "), len(args))
	} else {
		vals := make([]string, len(cols))
		for i := range cols {
			vals[i] = "$" + strconv.Itoa(i+1)
		}
		sql = fmt.Sprintf("insert into %s (%s) values (%s) on conflict (tenant, id) do nothing", tableName(t), strings.Join(cols, ", "), strings.Join(vals, ", "))
	}
	n, err := c.exec(ctx, sql, args...)
	if err != nil {
		return unique(err, t)
	}
	if n == 0 {
		return port.ErrConflict
	}
	return nil
}

// nullJSON is a jsonb parameter: empty is NULL.
func nullJSON(b []byte) any {
	if len(b) == 0 {
		return nil
	}
	return b
}

// unique turns a violation of a unique index u_… into UniqueKeyViolation.
func unique(err error, t *port.Table) error {
	var pe *pgconn.PgError
	if !errors.As(err, &pe) || pe.Code != "23505" {
		return err
	}
	for i := range t.Indexes {
		if ix := &t.Indexes[i]; ix.Unique && indexName(t, ix) == pe.ConstraintName {
			return &port.UniqueKeyViolation{Type: t.Type, Index: ix.Name}
		}
	}
	return err
}

// projection writes a synchronous projection row whole with its stored
// version + 1.
func (c *committer) projection(ctx context.Context, w *port.RowWrite) error {
	version, exists, err := c.lockRow(ctx, w.Table, w.Row.ID)
	if err != nil {
		return err
	}
	return c.write(ctx, w.Table, w.Row, exists, version, version+1)
}

// lockRow locks a row and returns its version and existence.
func (c *committer) lockRow(ctx context.Context, t *port.Table, id port.Scalar) (uint64, bool, error) {
	var version int64
	err := c.tx.QueryRow(ctx, "select version from "+tableName(t)+" where tenant = $1 and id = $2 for update", string(c.tenant), id).Scan(&version)
	switch {
	case errNoRows(err):
		return 0, false, nil
	case err != nil:
		return 0, false, err
	}
	return uint64(version), true, nil
}

// timers sets and deletes timers.
func (c *committer) timers(ctx context.Context) error {
	for _, id := range c.u.DeleteTimers {
		if _, err := c.exec(ctx, "delete from rt_timers where tenant = $1 and id = $2", string(c.tenant), id); err != nil {
			return err
		}
	}
	for _, t := range c.u.Timers {
		// fire_at is by the real clock, time_offset is the scenario clock offset (RFC §27.5).
		if _, err := c.exec(ctx, `insert into rt_timers (tenant, id, fire_at, entity_type, entity_id, command, args, sealed, time_offset)
			values ($1, $2, $3, $4, $5, $6, $7, $8, $9)`, string(c.tenant), t.ID, t.At.UnixNano(), t.Entity, t.EntityID, t.Command, t.Args,
			nullJSON(t.Sealed), int64(t.Offset)); err != nil {
			return err
		}
	}
	return nil
}

// forget destroys the keys of forgotten subjects and erases their
// ciphertexts and blind values in every row of the tenant (RFC §20, steps
// 2–3): including those written by this same unit of work.
func (c *committer) forget(ctx context.Context) error {
	if len(c.u.Forget) == 0 {
		return nil
	}
	if err := destroyKeys(ctx, c.tx, c.tenant, c.u.Forget); err != nil {
		return err
	}
	return erase(ctx, c.tx, c.tenant, c.u.Forget, c.u.ForgetTables, c.u.Rewrite)
}

// outbox checks the personal data keys, runs forget (steps 1–3 of RFC §20)
// and writes the outbox entry. Without forget the check and the write go in
// one batch — one round trip to the database.
func (c *committer) outbox(ctx context.Context, txid uint64, payload []byte) (port.Position, error) {
	const insert = "insert into rt_outbox (tenant, txid, payload) values ($1, $2::text::xid8, $3) returning position"
	var pos port.Position
	if len(c.u.Forget) > 0 {
		if err := checkKeys(ctx, c.tx, c.tenant, c.u.Keys); err != nil {
			return 0, err
		}
		if err := c.forget(ctx); err != nil {
			return 0, err
		}
		err := c.tx.QueryRow(ctx, insert, string(c.tenant), strconv.FormatUint(txid, 10), payload).Scan(&pos)
		return pos, err
	}
	sql, args, need := keysCheck(c.tenant, c.u.Keys)
	b := &pgx.Batch{}
	if need != nil {
		b.Queue(sql, args...)
	}
	b.Queue(insert, string(c.tenant), strconv.FormatUint(txid, 10), payload)
	br := c.tx.SendBatch(ctx, b)
	if need != nil {
		rows, err := br.Query()
		if err == nil {
			err = verifyKeys(rows, need)
		}
		if err != nil {
			_ = br.Close()
			return 0, err
		}
	}
	if err := br.QueryRow().Scan(&pos); err != nil {
		_ = br.Close()
		return 0, err
	}
	return pos, br.Close()
}

// Remembered returns the response stored by key (port.RowStore).
func (s *Store) Remembered(ctx context.Context, tenant port.Tenant, key string, now time.Time) ([]byte, error) {
	var data []byte
	err := s.pool.QueryRow(ctx, "select result from rt_idempotency where tenant = $1 and key = $2 and expires_at > $3",
		string(tenant), key, now).Scan(&data)
	if errNoRows(err) {
		return nil, nil
	}
	return data, err
}
