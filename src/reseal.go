package postgres

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"strconv"

	"github.com/jackc/pgx/v5"

	"github.com/tycheg/reasonr-engine/port"
)

// Personal data migrations (RFC §20). The rows of the tables the engine
// lists (port.Layout.Rewrite), and of those that still hold ciphertexts of
// fields no longer personal, are brought to the model after each new schema
// version: the engine encrypts the values of fields that became personal,
// decrypts those that stopped being personal (the values of forgotten
// subjects are none) and re-encrypts those renamed with was. The resealed
// mark of a version in rt_schema_versions: after it the rows are not
// scanned again. Events in the outbox and the log stay as they were.

// resealBatch is the rows in one re-encryption transaction.
const resealBatch = 500

// resealModel rewrites the tenant rows of the model if the last version of
// its schema is not resealed yet.
func (s *Store) resealModel(ctx context.Context, l *port.Layout, tenants []port.Tenant, rewrite port.Rewriter) error {
	var version int64
	var done bool
	err := s.pool.QueryRow(ctx, "select version, resealed from rt_schema_versions where model = $1 order by version desc limit 1", l.Model).
		Scan(&version, &done)
	if err != nil || done {
		return err
	}
	if len(tenants) > 0 {
		names := make([]string, len(tenants))
		for i, t := range tenants {
			names[i] = string(t)
		}
		for i := range l.Tables {
			t := &l.Tables[i]
			cols, err := columnTypes(ctx, s.pool, tableName(t))
			if err != nil {
				return err
			}
			_, hasSealed := cols["sealed"]
			if !slices.Contains(l.Rewrite, t.Type) && (t.Personal || !hasSealed) {
				continue
			}
			n, err := s.reseal(ctx, t, hasSealed, names, rewrite)
			if err != nil {
				return fmt.Errorf("postgres: re-encrypting %s: %w", tableName(t), err)
			}
			if n > 0 {
				s.log.Info("the personal data is brought to the model", "table", tableName(t), "rows", n)
			}
		}
	}
	_, err = s.pool.Exec(ctx, "update rt_schema_versions set resealed = true where model = $1 and version = $2", l.Model, version)
	return err
}

// reseal scans the rows of the table t of the tenants tenants in batches of
// resealBatch, each in its own transaction; it returns the number rewritten.
func (s *Store) reseal(ctx context.Context, t *port.Table, hasSealed bool, tenants []string, rewrite port.Rewriter) (int, error) {
	total := 0
	var after []any // the tenant and id of the batch's last row
	for {
		n, last, err := s.resealBatch(ctx, t, hasSealed, tenants, after, rewrite)
		total += n
		if err != nil || last == nil {
			return total, err
		}
		after = last
	}
}

// resealBatch rewrites the rows of the batch after after that rewrite
// changes; last == nil — no more rows.
func (s *Store) resealBatch(ctx context.Context, t *port.Table, hasSealed bool, tenants []string, after []any, rewrite port.Rewriter) (int, []any, error) {
	rt := readTable(t, hasSealed)
	q := "select tenant, " + columns(rt) + keyColumns(t) + " from " + tableName(t) + " where tenant = any($1::text[])"
	args := []any{tenants}
	if after != nil {
		q += " and (tenant, id) > ($2, $3)"
		args = append(args, after...)
	}
	q += " order by tenant, id limit " + strconv.Itoa(resealBatch) + " for update"
	var n int
	var last []any
	err := pgx.BeginFunc(ctx, s.pool, func(tx pgx.Tx) error {
		ctx := s.withTx(ctx, tx)
		if _, err := tx.Exec(ctx, "select set_config('reasonr.tenant', '*', true)"); err != nil {
			return err
		}
		rows, err := tx.Query(ctx, q, args...)
		if err != nil {
			return err
		}
		type found struct {
			tenant string
			row    port.Row
		}
		var all []found
		for rows.Next() {
			var f found
			var version int64
			var subjects []string
			dest := []any{&f.tenant, &f.row.ID, &version, &f.row.State}
			if rt.Personal {
				dest = append(dest, &f.row.Sealed, &subjects)
			}
			f.row.Keys = make([]port.Scalar, len(t.Keys))
			for i := range f.row.Keys {
				dest = append(dest, &f.row.Keys[i])
			}
			if err := rows.Scan(dest...); err != nil {
				rows.Close()
				return err
			}
			f.row.Version, f.row.Subjects = uint64(version), subjectRefs(subjects)
			for i, k := range t.Keys {
				f.row.Keys[i] = keyScalar(k.Kind, f.row.Keys[i])
			}
			all = append(all, f)
		}
		if err := rows.Err(); err != nil {
			return err
		}
		if len(all) == 0 {
			return nil
		}
		last = []any{all[len(all)-1].tenant, all[len(all)-1].row.ID}
		for _, f := range all {
			out, changed, err := rewrite(ctx, port.Tenant(f.tenant), rt, f.row)
			if err != nil {
				return err
			}
			if !changed {
				continue
			}
			if err := rewriteRow(ctx, tx, port.Tenant(f.tenant), rt, f.row.ID, out); err != nil {
				return err
			}
			n++
		}
		return nil
	})
	if err != nil {
		if errors.Is(err, port.ErrConflict) {
			return 0, after, nil // the key was destroyed concurrently: the batch starts over
		}
		return 0, nil, err
	}
	return n, last, nil
}
