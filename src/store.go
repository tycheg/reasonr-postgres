package postgres

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"strconv"
	"strings"
	"sync"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/tycheg/reasonr-engine/port"
)

// Store is a row store on PostgreSQL (the byte-level store ports of the
// engine): the main store, asynchronous projections, timers, the outbox,
// the event log, the request queue, schemas, the key store. The engine lays
// a model out in rows and records (RFC §31.2); the store writes, reads and
// searches them. Tenant data is separated by the tenant column; with RLS,
// PostgreSQL policies show a query only the rows of the tenant from
// reasonr.tenant.
type Store struct {
	pool *pgxpool.Pool
	rls  bool
	log  *slog.Logger

	gapMu sync.Mutex
	gaps  map[port.Position]uint64 // outbox gaps → the xid bound (effects.go)

	notifyOnce sync.Once
	notifyCh   chan struct{}
	notifyStop context.CancelFunc

	ddlMu   sync.Mutex
	ddlDone bool // the system tables are created (runtimeTables)
}

var (
	_ port.RowStore         = (*Store)(nil)
	_ port.AsyncProjections = (*Store)(nil)
	_ port.TimerStore       = (*Store)(nil)
	_ port.Outbox           = (*Store)(nil)
	_ port.EventLog         = (*Store)(nil)
	_ port.Requests         = (*Store)(nil)
	_ port.Transfer         = (*Store)(nil)
	_ port.RowSink          = (*Store)(nil)
	_ port.Schemas          = (*Store)(nil)
	_ port.KeyStore         = (*Store)(nil)
	_ port.Coordinator      = (*Store)(nil)
)

// Options holds the store settings.
type Options struct {
	// RLS isolates tenants with Row-Level Security policies (tenancy: rls);
	// otherwise — with a condition on the tenant column in every query.
	RLS    bool
	Logger *slog.Logger
}

// New returns a store over a connection pool.
func New(pool *pgxpool.Pool, opts Options) *Store {
	log := opts.Logger
	if log == nil {
		log = slog.New(slog.NewTextHandler(io.Discard, nil))
	}
	return &Store{pool: pool, rls: opts.RLS, log: log, gaps: map[port.Position]uint64{}}
}

// Close closes the connection pool.
func (s *Store) Close() error {
	s.notifyOnce.Do(func() {}) // the notifier will not start anymore
	if s.notifyStop != nil {
		s.notifyStop()
	}
	s.pool.Close()
	return nil
}

// Pool returns the connection pool.
func (s *Store) Pool() *pgxpool.Pool { return s.pool }

// Capabilities: everything except full-text search.
func (s *Store) Capabilities() port.Capabilities {
	return port.Capabilities{Transactions: true, Unique: true, Conds: port.AllConds, SortDesc: true, Pages: true, Scan: true, Personal: true}
}

// querier is a connection, a transaction or a pool.
type querier interface {
	Exec(ctx context.Context, sql string, args ...any) (pgconn.CommandTag, error)
	Query(ctx context.Context, sql string, args ...any) (pgx.Rows, error)
	QueryRow(ctx context.Context, sql string, args ...any) pgx.Row
}

// read runs a read of tenant data: with RLS — in a transaction with
// reasonr.tenant, otherwise — right on the pool.
func (s *Store) read(ctx context.Context, tenant port.Tenant, f func(q querier) error) error {
	if !s.rls {
		return f(s.pool)
	}
	return pgx.BeginTxFunc(ctx, s.pool, pgx.TxOptions{AccessMode: pgx.ReadOnly}, func(tx pgx.Tx) error {
		if _, err := tx.Exec(ctx, "select set_config('reasonr.tenant', $1, true)", string(tenant)); err != nil {
			return err
		}
		return f(tx)
	})
}

// Load returns a row (port.RowReader).
func (s *Store) Load(ctx context.Context, tenant port.Tenant, t *port.Table, id port.Scalar) (port.Row, bool, error) {
	var rows []port.Row
	err := s.read(ctx, tenant, func(q querier) error {
		var err error
		rows, err = s.scan(ctx, q, t, "select "+columns(t)+" from "+tableName(t)+" where tenant = $1 and id = $2", string(tenant), id)
		return err
	})
	if err != nil || len(rows) == 0 {
		return port.Row{}, false, err
	}
	return rows[0], true, nil
}

// NextSequence allocates n sequence numbers (port.RowStore).
func (s *Store) NextSequence(ctx context.Context, tenant port.Tenant, typ string, n int) (int64, error) {
	var last int64
	err := s.pool.QueryRow(ctx, `insert into rt_sequences (tenant, type, value) values ($1, $2, $3)
		on conflict (tenant, type) do update set value = rt_sequences.value + excluded.value returning value`,
		string(tenant), typ, int64(n)).Scan(&last)
	return last - int64(n) + 1, err
}

// Find searches rows by key columns (port.RowReader); a page is keyset
// pagination after Query.After.
func (s *Store) Find(ctx context.Context, tenant port.Tenant, q *port.Query) ([]port.Row, error) {
	sql, args, err := findSQL(tenant, q)
	if err != nil {
		return nil, err
	}
	var out []port.Row
	err = s.read(ctx, tenant, func(qr querier) error {
		out, err = s.scan(ctx, qr, q.Table, sql, args...)
		return err
	})
	return out, err
}

// scan reads rows of columns(t).
func (s *Store) scan(ctx context.Context, qr querier, t *port.Table, sql string, args ...any) ([]port.Row, error) {
	rows, err := qr.Query(ctx, sql, args...)
	if err != nil {
		return nil, fmt.Errorf("postgres: %s: %w", t.Type, err)
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
		if err := rows.Scan(dest...); err != nil {
			return nil, err
		}
		r.Version, r.Subjects = uint64(version), subjectRefs(subjects)
		if r.ID, err = idScalar(t, r.ID); err != nil {
			return nil, err
		}
		out = append(out, r)
	}
	return out, rows.Err()
}

// idScalar converts the id column value read by pgx into the kind of the column.
func idScalar(t *port.Table, raw any) (port.Scalar, error) {
	switch x := raw.(type) {
	case [16]byte, int64, string, []byte:
		return x, nil
	}
	return nil, fmt.Errorf("postgres: id of %s of kind %T", t.Type, raw)
}

// columns are the read columns of a row: with the ciphertexts and subjects
// for tables with personal values.
func columns(t *port.Table) string {
	if t.Personal {
		return "id, version, state, sealed, subjects"
	}
	return "id, version, state"
}

// findSQL builds the search query.
func findSQL(tenant port.Tenant, q *port.Query) (string, []any, error) {
	t := q.Table
	kinds := map[string]port.ColumnKind{"id": t.ID}
	for _, k := range t.Keys {
		kinds[k.Name] = k.Kind
	}
	args := []any{string(tenant)}
	arg := func(v any) string {
		args = append(args, v)
		return "$" + strconv.Itoa(len(args))
	}
	var where []string
	for _, c := range q.Conds {
		kind, ok := kinds[c.Column]
		if !ok {
			return "", nil, fmt.Errorf("postgres: %s has no column %s", t.Type, c.Column)
		}
		where = append(where, condSQL(c.Column, kind, c, arg))
	}
	if q.After != nil {
		// After the cursor: the first i columns are equal, the i-th is further in order.
		var alts []string
		for i, o := range q.Order {
			var conj []string
			for j := range i {
				if p := q.After[j]; p == nil {
					conj = append(conj, q.Order[j].Column+" is null")
				} else {
					conj = append(conj, q.Order[j].Column+" = "+arg(p))
				}
			}
			p := q.After[i]
			switch {
			case p == nil && !o.Desc: // none is the first value: everything not none is after it
				conj = append(conj, o.Column+" is not null")
			case p == nil && o.Desc: // none is the last: nothing is after it
				conj = append(conj, "false")
			case !o.Desc:
				conj = append(conj, o.Column+" > "+arg(p))
			default:
				conj = append(conj, "("+o.Column+" < "+arg(p)+" or "+o.Column+" is null)")
			}
			alts = append(alts, "("+strings.Join(conj, " and ")+")")
		}
		where = append(where, "("+strings.Join(alts, " or ")+")")
	}
	sql := "select " + columns(t) + " from " + tableName(t) + " where tenant = $1"
	for _, w := range where {
		sql += " and " + w
	}
	if len(q.Order) > 0 {
		var ob []string
		for _, o := range q.Order {
			if o.Desc {
				ob = append(ob, o.Column+" desc nulls last")
			} else {
				ob = append(ob, o.Column+" asc nulls first")
			}
		}
		sql += " order by " + strings.Join(ob, ", ")
	}
	if q.Limit >= 0 {
		sql += " limit " + strconv.Itoa(q.Limit)
	}
	return sql, args, nil
}

// arrayTypes is the array type of a parameter for in.
var arrayTypes = [...]string{
	port.ColInt: "bigint[]", port.ColFloat: "double precision[]", port.ColNumeric: "numeric[]", port.ColNanos: "numeric[]",
	port.ColText: "text[]", port.ColBytes: "bytea[]", port.ColBool: "boolean[]", port.ColUUID: "uuid[]", port.ColOpaque: "bytea[]",
	port.ColBlind: "bytea[]",
}

// condSQL turns a condition into SQL with the semantics of the engine's
// matching: none equals only none, an order comparison with none is false.
func condSQL(col string, kind port.ColumnKind, c port.Cond, arg func(any) string) string {
	if c.Op == port.OpIn {
		var ints []int64
		var floats []float64
		var texts []string
		var blobs [][]byte
		var bools []bool
		hasNone := false
		for _, p := range c.Values {
			switch v := p.(type) {
			case nil:
				hasNone = true
			case int64:
				ints = append(ints, v)
			case float64:
				floats = append(floats, v)
			case string:
				texts = append(texts, v)
			case [16]byte:
				texts = append(texts, uuidText(v))
			case []byte:
				blobs = append(blobs, v)
			case bool:
				bools = append(bools, v)
			}
		}
		var list any
		switch kind {
		case port.ColInt:
			list = ints
		case port.ColFloat:
			list = floats
		case port.ColBytes, port.ColOpaque, port.ColBlind:
			list = blobs
		case port.ColBool:
			list = bools
		default:
			list = texts
		}
		cond := col + " = any(" + arg(list) + "::" + arrayTypes[kind] + ")"
		if hasNone {
			cond = "(" + cond + " or " + col + " is null)"
		}
		return cond
	}
	var p port.Scalar
	if len(c.Values) > 0 {
		p = c.Values[0]
	}
	switch c.Op {
	case port.OpEq:
		if p == nil {
			return col + " is null"
		}
		return col + " = " + arg(p)
	case port.OpNe:
		if p == nil {
			return col + " is not null"
		}
		return col + " is distinct from " + arg(p)
	}
	if p == nil {
		return "false"
	}
	op := map[port.CondOp]string{port.OpLt: "<", port.OpLe: "<=", port.OpGt: ">", port.OpGe: ">="}[c.Op]
	return col + " " + op + " " + arg(p)
}

func uuidText(u [16]byte) string {
	const hex = "0123456789abcdef"
	var b strings.Builder
	for i, x := range u {
		if i == 4 || i == 6 || i == 8 || i == 10 {
			b.WriteByte('-')
		}
		b.WriteByte(hex[x>>4])
		b.WriteByte(hex[x&0xf])
	}
	return b.String()
}

// errNoRows reports a missing row.
func errNoRows(err error) bool { return errors.Is(err, pgx.ErrNoRows) }
