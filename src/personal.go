package postgres

import (
	"context"
	"strconv"
	"strings"

	"github.com/jackc/pgx/v5"

	"github.com/tycheg/reasonr-engine/port"
)

// Personal data in tables (RFC §20): the engine writes a row's state
// without its personal values, their ciphertexts to sealed, the row's
// subjects to subjects (a GIN index, forget finds rows by it), and blind
// values to the index key columns on personal fields (bytea).

// erase rewrites the tenant's rows of the tables tables that hold values of
// the subjects refs with rewrite (it erases their ciphertexts and blind
// values) and increments their versions: a concurrent command gets a
// conflict and rereads the data.
func erase(ctx context.Context, tx pgx.Tx, tenant port.Tenant, refs []port.SubjectRef, tables []*port.Table, rewrite port.Rewriter) error {
	texts := subjectTexts(refs)
	for _, t := range tables {
		if !t.Personal {
			continue
		}
		rows, err := scanTx(ctx, tx, t, "select "+columns(t)+keyColumns(t)+" from "+tableName(t)+
			" where tenant = $1 and subjects && $2::text[] for update", string(tenant), texts)
		if err != nil {
			return err
		}
		for _, r := range rows {
			out, changed, err := rewrite(ctx, tenant, t, r)
			if err != nil {
				return err
			}
			if changed {
				if err := rewriteRow(ctx, tx, tenant, t, r.ID, out); err != nil {
					return err
				}
			}
		}
	}
	return nil
}

// keyColumns are the key columns after columns(t), with a leading comma.
func keyColumns(t *port.Table) string {
	var b strings.Builder
	for _, k := range t.Keys {
		b.WriteString(", " + k.Name)
	}
	return b.String()
}

// scanTx reads rows with their key columns (columns(t) + keyColumns(t)).
func scanTx(ctx context.Context, q querier, t *port.Table, sql string, args ...any) ([]port.Row, error) {
	rows, err := q.Query(ctx, sql, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []port.Row
	for rows.Next() {
		var r port.Row
		var version int64
		var subjects []string
		dest := []any{&r.ID, &version, &r.State}
		if t.Personal {
			dest = append(dest, &r.Sealed, &subjects)
		}
		r.Keys = make([]port.Scalar, len(t.Keys))
		for i := range r.Keys {
			dest = append(dest, &r.Keys[i])
		}
		if err := rows.Scan(dest...); err != nil {
			return nil, err
		}
		r.Version, r.Subjects = uint64(version), subjectRefs(subjects)
		for i, k := range t.Keys {
			r.Keys[i] = keyScalar(k.Kind, r.Keys[i])
		}
		out = append(out, r)
	}
	return out, rows.Err()
}

// keyScalar converts a key column value read by pgx into its kind: numeric
// comes as pgtype.Numeric — the engine's decimal string.
func keyScalar(kind port.ColumnKind, v any) port.Scalar {
	if v == nil {
		return nil
	}
	switch kind {
	case port.ColNumeric, port.ColNanos:
		if n, ok := v.(interface{ Value() (any, error) }); ok {
			if x, err := n.Value(); err == nil {
				if s, ok := x.(string); ok {
					return s
				}
			}
		}
	}
	return v
}

// rewriteRow writes a rewritten row with the next version.
func rewriteRow(ctx context.Context, tx pgx.Tx, tenant port.Tenant, t *port.Table, id port.Scalar, r port.Row) error {
	set := []string{"version = version + 1", "state = $3"}
	args := []any{string(tenant), id, r.State}
	if t.Personal {
		args = append(args, nullJSON(r.Sealed), subjectTexts(r.Subjects))
		set = append(set, "sealed = $"+strconv.Itoa(len(args)-1), "subjects = $"+strconv.Itoa(len(args)))
	}
	for i, k := range t.Keys {
		var v any
		if i < len(r.Keys) {
			v = r.Keys[i]
		}
		args = append(args, v)
		set = append(set, k.Name+" = $"+strconv.Itoa(len(args)))
	}
	_, err := tx.Exec(ctx, "update "+tableName(t)+" set "+strings.Join(set, ", ")+" where tenant = $1 and id = $2", args...)
	return err
}
