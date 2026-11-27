package postgres

import (
	"context"
	"strconv"
	"time"

	"github.com/jackc/pgx/v5"

	"github.com/tycheg/reasonr-engine/port"
)

// Append appends the events of units of work to ev_events
// (port.EventLog); a repeated unit — by the key (position, entity, event)
// — is skipped. Personal arguments are encrypted with the subject keys from
// Event.Subjects of the versions they were decrypted with from the outbox
// (CommittedUnit.Keys), and the engine does not decrypt them when reading
// the log (RFC §20).
func (s *Store) Append(ctx context.Context, events []port.EventRecord) error {
	b := &pgx.Batch{}
	for _, e := range events {
		b.Queue(`insert into ev_events (tenant, position, entity, seq, entity_type, entity_id, version, name, args, sealed, at)
			values ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11) on conflict do nothing`,
			string(e.Tenant), int64(e.Position), e.Entity, e.Seq, e.Type, e.ID, int64(e.Version), e.Name, e.Args, nullJSON(e.Sealed), e.At)
	}
	if b.Len() == 0 {
		return nil
	}
	return s.pool.SendBatch(ctx, b).Close()
}

var _ port.EventFeed = (*Store)(nil)

// ReadEvents reads ev_events in primary key order (port.EventFeed): the
// arguments as written, personal ones null. The field order in the argument
// JSON is the jsonb order, not the schema's.
func (s *Store) ReadEvents(ctx context.Context, after, upto port.Position, limit int) ([]port.EventRecord, error) {
	q := `select tenant, position, seq, entity_type, entity_id::text, name, args::text, at from ev_events
		where position > $1 and position <= $2 order by position, entity, seq`
	if limit > 0 {
		q += " limit " + strconv.Itoa(limit)
	}
	rows, err := s.pool.Query(ctx, q, int64(after), int64(upto))
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []port.EventRecord
	for rows.Next() {
		var (
			tenant, typ, id, name, args string
			pos                         int64
			seq                         int
			at                          time.Time
		)
		if err := rows.Scan(&tenant, &pos, &seq, &typ, &id, &name, &args, &at); err != nil {
			return nil, err
		}
		out = append(out, port.EventRecord{Tenant: port.Tenant(tenant), Position: port.Position(pos), Seq: seq,
			Type: typ, Name: name, ID: []byte(id), At: at.UTC(), Args: []byte(args)})
	}
	return out, rows.Err()
}
