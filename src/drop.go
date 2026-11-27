package postgres

import (
	"context"

	"github.com/jackc/pgx/v5"

	"github.com/tycheg/reasonr-engine/port"
)

// DropTenant deletes all the data of a tenant: entity and projection rows,
// timers, idempotency and personal data keys, the log (ephemeral test
// tenants). Outbox entries stay: positions are shared by every tenant. The
// forget log stays too, as in MongoDB: it is an audit record for a replay
// after a restore from a backup (decision 13 of RFC §20).
func (s *Store) DropTenant(ctx context.Context, tenant port.Tenant) error {
	return pgx.BeginFunc(ctx, s.pool, func(tx pgx.Tx) error {
		if _, err := tx.Exec(ctx, "select set_config('reasonr.tenant', $1, true)", string(tenant)); err != nil {
			return err
		}
		rows, err := tx.Query(ctx, `select table_name from information_schema.columns
			where table_schema = current_schema() and column_name = 'tenant' and table_name not in ('rt_outbox', 'rt_forget_log')`)
		if err != nil {
			return err
		}
		var tables []string
		for rows.Next() {
			var t string
			if err := rows.Scan(&t); err != nil {
				return err
			}
			tables = append(tables, t)
		}
		if err := rows.Err(); err != nil {
			return err
		}
		for _, t := range tables {
			if _, err := tx.Exec(ctx, "delete from "+pgx.Identifier{t}.Sanitize()+" where tenant = $1", string(tenant)); err != nil {
				return err
			}
		}
		return nil
	})
}
