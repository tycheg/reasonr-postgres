package postgres

import (
	"context"
	"fmt"
	"strconv"
	"strings"

	"github.com/jackc/pgx/v5"

	"github.com/tycheg/reasonr-engine/port"
)

// PutRows writes entity rows in a batch in one transaction (port.RowSink):
// the entity database of an engine kind that keeps its entities in memory.
// A row is written only if its version is greater than the stored one: a
// repeated batch and a late batch of an earlier writer spoil nothing. The row format is the stateless engine's.
func (s *Store) PutRows(ctx context.Context, tenant port.Tenant, rows []port.RowWrite) error {
	if len(rows) == 0 {
		return nil
	}
	return pgx.BeginFunc(ctx, s.pool, func(tx pgx.Tx) error {
		if _, err := tx.Exec(ctx, "select set_config('reasonr.tenant', $1, true)", string(tenant)); err != nil {
			return err
		}
		batch := &pgx.Batch{}
		for _, w := range rows {
			t, r := w.Table, w.Row
			cols := []string{"tenant", "id", "version", "state"}
			args := []any{string(tenant), r.ID, int64(r.Version), r.State}
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
			vals := make([]string, len(cols))
			set := make([]string, 0, len(cols)-2)
			for i, c := range cols {
				vals[i] = "$" + strconv.Itoa(i+1)
				if i >= 2 {
					set = append(set, c+" = excluded."+c)
				}
			}
			batch.Queue(fmt.Sprintf("insert into %s as t (%s) values (%s) on conflict (tenant, id) do update set %s where t.version < excluded.version",
				tableName(t), strings.Join(cols, ", "), strings.Join(vals, ", "), strings.Join(set, ", ")), args...)
		}
		return tx.SendBatch(ctx, batch).Close()
	})
}
