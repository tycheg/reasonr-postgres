package postgres

import (
	"context"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"

	"github.com/tycheg/reasonr-engine/port"
)

var _ port.ForgetLog = (*Store)(nil)

// Record appends forget log entries to rt_forget_log (port.ForgetLog): a
// repeated entry is skipped. A store without models creates the table too
// — the log is usually in a separate database.
func (s *Store) Record(ctx context.Context, entries []port.Forgotten) error {
	if err := s.runtimeTables(ctx); err != nil {
		return err
	}
	b := &pgx.Batch{}
	for _, e := range entries {
		b.Queue(`insert into rt_forget_log (tenant, position, subject_type, subject_id, at) values ($1, $2, $3, $4, $5)
			on conflict do nothing`, string(e.Tenant), int64(e.Position), e.Subject.Type, e.Subject.ID, e.At)
	}
	return s.pool.SendBatch(ctx, b).Close()
}

// Forgotten returns the forget log entries with a moment not earlier than
// since (port.ForgetLog).
func (s *Store) Forgotten(ctx context.Context, since time.Time) ([]port.Forgotten, error) {
	if err := s.runtimeTables(ctx); err != nil {
		return nil, err
	}
	rows, err := s.pool.Query(ctx, `select tenant, position, subject_type, subject_id, at from rt_forget_log
		where at >= $1 order by position, tenant, subject_type, subject_id`, since)
	if err != nil {
		return nil, err
	}
	return pgx.CollectRows(rows, func(r pgx.CollectableRow) (port.Forgotten, error) {
		var e port.Forgotten
		var tenant string
		var pos int64
		err := r.Scan(&tenant, &pos, &e.Subject.Type, &e.Subject.ID, &e.At)
		e.Tenant, e.Position, e.At = port.Tenant(tenant), port.Position(pos), e.At.UTC()
		return e, err
	})
}

// runtimeTables creates the system tables under the advisory schema change
// lock; after a success it does not create them again.
func (s *Store) runtimeTables(ctx context.Context) error {
	s.ddlMu.Lock()
	defer s.ddlMu.Unlock()
	if s.ddlDone {
		return nil
	}
	err := pgx.BeginFunc(ctx, s.pool, func(tx pgx.Tx) error {
		if _, err := tx.Exec(ctx, "select pg_advisory_xact_lock($1)", int64(ddlLock)); err != nil {
			return err
		}
		_, err := tx.Exec(ctx, runtimeDDL)
		return err
	})
	if err != nil {
		return fmt.Errorf("postgres: system tables: %w", err)
	}
	s.ddlDone = true
	return nil
}
