package postgres

import (
	"context"
	"errors"
	"time"

	"github.com/jackc/pgx/v5"

	"github.com/tycheg/reasonr-engine/port"
)

var _ port.Coordinator = (*Store)(nil)

// Cursor returns a consumer's position (port.Coordinator).
func (s *Store) Cursor(ctx context.Context, consumer string) (port.Position, error) {
	var pos int64
	err := s.pool.QueryRow(ctx, "select position from rt_cursors where consumer = $1", consumer).Scan(&pos)
	if errors.Is(err, pgx.ErrNoRows) {
		return 0, nil
	}
	return port.Position(pos), err
}

// SetCursor remembers a consumer's position (port.Coordinator).
func (s *Store) SetCursor(ctx context.Context, consumer string, pos port.Position) error {
	_, err := s.pool.Exec(ctx, `insert into rt_cursors (consumer, position) values ($1, $2)
		on conflict (consumer) do update set position = excluded.position`, consumer, int64(pos))
	return err
}

// Acquire takes or extends a lease (port.Coordinator).
func (s *Store) Acquire(ctx context.Context, name, holder string, now time.Time, ttl time.Duration) (bool, error) {
	tag, err := s.pool.Exec(ctx, `insert into rt_leases (name, holder, until) values ($1, $2, $3)
		on conflict (name) do update set holder = excluded.holder, until = excluded.until
		where rt_leases.holder = excluded.holder or rt_leases.until <= $4`,
		name, holder, now.Add(ttl).UnixNano(), now.UnixNano())
	return tag.RowsAffected() == 1, err
}

// Bury writes a dead letter (port.Coordinator).
func (s *Store) Bury(ctx context.Context, d port.DeadLetter) error {
	_, err := s.pool.Exec(ctx, `insert into rt_dead_letters (consumer, position, item, tenant, reason, at)
		values ($1, $2, $3, $4, $5, $6) on conflict do nothing`,
		d.Consumer, int64(d.Position), d.Item, string(d.Tenant), d.Reason, d.At)
	return err
}

// DeadLetters returns the dead letters (port.Coordinator).
func (s *Store) DeadLetters(ctx context.Context, consumer string) ([]port.DeadLetter, error) {
	rows, err := s.pool.Query(ctx, `select consumer, position, item, tenant, reason, at from rt_dead_letters
		where $1 = '' or consumer = $1 order by position, item`, consumer)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []port.DeadLetter
	for rows.Next() {
		var d port.DeadLetter
		var pos int64
		var tenant string
		if err := rows.Scan(&d.Consumer, &pos, &d.Item, &tenant, &d.Reason, &d.At); err != nil {
			return nil, err
		}
		d.Position, d.Tenant = port.Position(pos), port.Tenant(tenant)
		out = append(out, d)
	}
	return out, rows.Err()
}

// Resurrect deletes a dead letter (port.Coordinator).
func (s *Store) Resurrect(ctx context.Context, d port.DeadLetter) error {
	_, err := s.pool.Exec(ctx, "delete from rt_dead_letters where consumer = $1 and position = $2 and item = $3",
		d.Consumer, int64(d.Position), d.Item)
	return err
}
