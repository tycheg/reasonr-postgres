package postgres

import (
	"context"
	"time"

	"github.com/tycheg/reasonr-engine/port"
)

// ExportTimers returns the live timers and the sequence counters of a
// tenant (port.Transfer).
func (s *Store) ExportTimers(ctx context.Context, tenant port.Tenant) (port.TimerExport, error) {
	ex := port.TimerExport{Sequences: map[string]int64{}}
	rows, err := s.pool.Query(ctx, "select type, value from rt_sequences where tenant = $1", string(tenant))
	if err != nil {
		return ex, err
	}
	for rows.Next() {
		var name string
		var v int64
		if err := rows.Scan(&name, &v); err != nil {
			rows.Close()
			return ex, err
		}
		ex.Sequences[name] = v
	}
	rows.Close()
	if err := rows.Err(); err != nil {
		return ex, err
	}
	rows, err = s.pool.Query(ctx, `select id, fire_at, entity_type, entity_id, command, args, sealed, time_offset
		from rt_timers where tenant = $1 and dead is null order by seq`, string(tenant))
	if err != nil {
		return ex, err
	}
	defer rows.Close()
	for rows.Next() {
		var t port.TimerRecord
		var fireAt, offset int64
		if err := rows.Scan(&t.ID, &fireAt, &t.Entity, &t.EntityID, &t.Command, &t.Args, &t.Sealed, &offset); err != nil {
			return ex, err
		}
		t.At, t.Offset = time.Unix(0, fireAt).UTC(), time.Duration(offset)
		ex.Timers = append(ex.Timers, t)
	}
	return ex, rows.Err()
}

// ClearTimers deletes the timers of a tenant (port.Transfer).
func (s *Store) ClearTimers(ctx context.Context, tenant port.Tenant) error {
	_, err := s.pool.Exec(ctx, "delete from rt_timers where tenant = $1", string(tenant))
	return err
}

// ImportSequences raises the counters of a tenant (port.Transfer).
func (s *Store) ImportSequences(ctx context.Context, tenant port.Tenant, seqs map[string]int64) error {
	for name, v := range seqs {
		if _, err := s.pool.Exec(ctx, `insert into rt_sequences (tenant, type, value) values ($1, $2, $3)
			on conflict (tenant, type) do update set value = greatest(rt_sequences.value, excluded.value)`, string(tenant), name, v); err != nil {
			return err
		}
	}
	return nil
}
