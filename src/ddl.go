package postgres

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"slices"
	"strings"

	"github.com/jackc/pgx/v5"

	"github.com/tycheg/reasonr-engine/port"
)

// System tables: timers, the outbox, idempotency keys, sequences, personal
// data keys, schema versions, large hub notifications, external requests
// waiting to be sent.
const runtimeDDL = `
create table if not exists rt_timers (
  tenant      text   not null,
  id          uuid   not null,
  fire_at     bigint not null,          -- Unix nanoseconds
  seq         bigserial,                -- the scheduling order
  lease_until bigint,                   -- the lease of the running instance
  attempts    int    not null default 0,
  dead        text,                     -- the reason; null — alive
  entity_type text   not null,
  entity_id   jsonb  not null,
  command     text   not null,
  args        jsonb  not null,
  primary key (tenant, id)
);
create index if not exists rt_timers_due on rt_timers (fire_at, seq) where dead is null;
alter table rt_timers add column if not exists sealed jsonb; -- ciphertexts of personal arguments
alter table rt_timers add column if not exists time_offset bigint not null default 0; -- the scenario clock offset, ns: the scenario moment is fire_at + time_offset
create table if not exists rt_outbox (
  position bigserial primary key,
  tenant   text  not null,
  txid     xid8  not null,
  payload  jsonb not null
);
create table if not exists rt_idempotency (
  tenant     text        not null,
  key        text        not null,
  result     jsonb       not null,
  expires_at timestamptz not null,
  primary key (tenant, key)
);
create table if not exists rt_sequences (
  tenant text   not null,
  type   text   not null,
  value  bigint not null,
  primary key (tenant, type)
);
create table if not exists rt_tenant_keys (  -- RFC §20
  tenant     text        primary key,
  wrapped    bytea       not null,      -- KMS.Wrap(the tenant key)
  created_at timestamptz not null default now()
);
create table if not exists rt_subject_keys (
  tenant       text   not null,
  subject_type text   not null,
  subject      text   not null,         -- the canonical JSON id
  version      bigint not null,
  wrapped      bytea,                   -- wrapped with the tenant key; null — destroyed by forget
  primary key (tenant, subject_type, subject)
);
create table if not exists rt_schema_versions (
  model       text        not null,
  version     bigserial,
  source_hash text        not null,
  schema      jsonb       not null,
  applied_at  timestamptz not null default now(),
  primary key (model, version)
);
-- the personal data storage of rows is brought to this version (reseal.go)
alter table rt_schema_versions add column if not exists resealed boolean not null default false;
create table if not exists rt_applied (
  tenant   text   primary key,
  position bigint not null
);
create table if not exists rt_deleted (   -- deletion marks of asynchronous projection rows (RFC §18)
  tenant  text   not null,
  type    text   not null,
  id      text   not null,                -- the canonical JSON of the identifier
  version bigint not null,                -- the deletion position
  primary key (tenant, type, id)
);
create table if not exists rt_cursors (
  consumer text   primary key,
  position bigint not null
);
create table if not exists rt_leases (
  name   text   primary key,
  holder text   not null,
  until  bigint not null               -- Unix nanoseconds
);
create table if not exists rt_dead_letters (
  consumer text        not null,
  position bigint      not null,
  item     int         not null,
  tenant   text        not null,
  reason   text        not null,
  at       timestamptz not null,
  primary key (consumer, position, item)
);
create table if not exists ev_events (   -- the event store (port.EventLog)
  tenant      text        not null,
  position    bigint      not null,
  entity      int         not null,     -- the entity number in the unit of work
  seq         int         not null,     -- the event number in the unit of work
  entity_type text        not null,
  entity_id   jsonb       not null,
  version     bigint      not null,
  name        text        not null,
  args        jsonb       not null,
  at          timestamptz not null,
  primary key (position, entity, seq)
);
create index if not exists ev_events_entity on ev_events (tenant, entity_type, entity_id, position, seq);
alter table ev_events add column if not exists sealed jsonb; -- ciphertexts of personal arguments
create table if not exists rt_hub_payloads (
  id         bigserial primary key,
  payload    bytea       not null,
  created_at timestamptz not null default now()
);
create table if not exists rt_forget_log (  -- the forget log (decision 13 of RFC §20)
  tenant       text        not null,
  position     bigint      not null,
  subject_type text        not null,
  subject_id   text        not null,     -- the canonical JSON of the identifier
  at           timestamptz not null,
  primary key (tenant, position, subject_type, subject_id)
);
create index if not exists rt_forget_log_at on rt_forget_log (at);
create table if not exists rt_features (     -- feature flag rollout (RFC §23.4)
  feature text not null,                     -- the full name: Ordering.tips
  tenant  text not null default '',          -- '' — all, or an actor in every tenant
  role    text not null default '',
  actor   text not null default '',          -- Ordering.Customer:<id>
  primary key (feature, tenant, role, actor)
);
create table if not exists rt_feature_log (  -- the rollout change log
  id         bigserial   primary key,
  at         timestamptz not null,
  by         text        not null,
  feature    text        not null,
  on_        boolean     not null,
  tenant     text        not null,
  role       text        not null,
  actor      text        not null,
  everywhere boolean     not null
);
` + requestsDDL

// With RLS the policy lets through the tenant rows from reasonr.tenant; the
// value "*" is set only by the adapter itself in service transactions
// (schema, forget).

// ddlLock is the number of the advisory lock held while the schema changes:
// instances starting at the same time change the schema in turn.
const ddlLock = 0x7265_6173_6f6e_72 // "reasonr"

// LastSchema returns the last written model schema (port.Schemas).
func (s *Store) LastSchema(ctx context.Context, model string) ([]byte, error) {
	if _, err := s.pool.Exec(ctx, runtimeDDL); err != nil {
		return nil, fmt.Errorf("postgres: system tables: %w", err)
	}
	var data []byte
	err := s.pool.QueryRow(ctx, "select schema from rt_schema_versions where model = $1 order by version desc limit 1", model).Scan(&data)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, nil
	}
	return data, err
}

// SchemaError: the schema cannot change without a manual intervention: for
// example, a new unique index has repeated values.
type SchemaError struct {
	Model string
	Msg   string
}

func (e *SchemaError) Error() string { return "postgres: model " + e.Model + ": " + e.Msg }

// EnsureSchema creates the system tables, the type tables, key columns and
// indexes of the layout l, has rewrite fill the new key columns of the
// tenants' rows and writes the schema version (port.Schemas). The change is
// idempotent and runs in one transaction under an advisory lock; then the
// rows of l.Rewrite are brought to the model (reseal.go).
func (s *Store) EnsureSchema(ctx context.Context, l *port.Layout, tenants []port.Tenant, rewrite port.Rewriter) error {
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return err
	}
	defer func() { _ = tx.Rollback(ctx) }()
	if _, err := tx.Exec(ctx, "select pg_advisory_xact_lock($1), set_config('reasonr.tenant', '*', true)", int64(ddlLock)); err != nil {
		return err
	}
	if _, err := tx.Exec(ctx, runtimeDDL); err != nil {
		return fmt.Errorf("postgres: system tables: %w", err)
	}
	tctx := s.withTx(ctx, tx)
	for i := range l.Tables {
		if err := s.ensureTable(tctx, tx, l, &l.Tables[i], tenants, rewrite); err != nil {
			return err
		}
	}
	sum := sha256.Sum256(l.Schema)
	hash := hex.EncodeToString(sum[:])
	var last string
	err = tx.QueryRow(ctx, "select source_hash from rt_schema_versions where model = $1 order by version desc limit 1", l.Model).Scan(&last)
	if errors.Is(err, pgx.ErrNoRows) || err == nil && last != hash {
		if _, err := tx.Exec(ctx, "insert into rt_schema_versions (model, source_hash, schema) values ($1, $2, $3)", l.Model, hash, l.Schema); err != nil {
			return err
		}
	} else if err != nil {
		return err
	}
	if err := tx.Commit(ctx); err != nil {
		return err
	}
	return s.resealModel(ctx, l, tenants, rewrite)
}

// columnTypes returns the existing columns of a table and their types.
func columnTypes(ctx context.Context, tx querier, table string) (map[string]string, error) {
	rows, err := tx.Query(ctx, `select column_name, data_type from information_schema.columns
		where table_schema = current_schema() and table_name = $1`, table)
	if err != nil {
		return nil, err
	}
	out := map[string]string{}
	for rows.Next() {
		var n, t string
		if err := rows.Scan(&n, &t); err != nil {
			return nil, err
		}
		out[n] = t
	}
	return out, rows.Err()
}

func (s *Store) ensureTable(ctx context.Context, tx pgx.Tx, l *port.Layout, t *port.Table, tenants []port.Tenant, rewrite port.Rewriter) error {
	name := tableName(t)
	cols, err := columnTypes(ctx, tx, name)
	if err != nil {
		return err
	}
	if len(cols) == 0 {
		personal := ""
		if t.Personal {
			personal = "  sealed jsonb,\n  subjects text[],\n"
		}
		ddl := fmt.Sprintf("create table %s (\n  tenant text not null,\n  id %s not null,\n  version bigint not null,\n  state jsonb not null,\n%s  primary key (tenant, id)\n)",
			name, colTypes[t.ID], personal)
		if _, err := tx.Exec(ctx, ddl); err != nil {
			return fmt.Errorf("postgres: table %s: %w", name, err)
		}
		if s.rls {
			if _, err := tx.Exec(ctx, fmt.Sprintf(`alter table %[1]s enable row level security;
alter table %[1]s force row level security;
create policy tenant_isolation on %[1]s
  using (tenant = current_setting('reasonr.tenant', true) or current_setting('reasonr.tenant', true) = '*');`, name)); err != nil {
				return fmt.Errorf("postgres: RLS %s: %w", name, err)
			}
		}
	} else if have := cols["id"]; have != infoTypes[t.ID] {
		return &SchemaError{Model: l.Model, Msg: fmt.Sprintf("%s has an id column of type %s, the model requires %s: a manual migration is needed", name, have, infoTypes[t.ID])}
	}
	if t.Personal && len(cols) > 0 {
		if _, err := tx.Exec(ctx, fmt.Sprintf("alter table %[1]s add column if not exists sealed jsonb, add column if not exists subjects text[]", name)); err != nil {
			return fmt.Errorf("postgres: personal data columns of %s: %w", name, err)
		}
	}
	if t.Personal {
		// Rows with a subject's data for forget (RFC §20).
		if _, err := tx.Exec(ctx, fmt.Sprintf("create index if not exists %s on %s using gin (subjects)", subjectsIndex(t), name)); err != nil {
			return fmt.Errorf("postgres: the subject index of %s: %w", name, err)
		}
	}
	added := false
	for _, k := range t.Keys {
		have, ok := cols[k.Name]
		if ok && slices.Contains(l.Replace, t.Type+"."+k.Name) {
			// A field became or stopped being personal (RFC §20): the blind
			// column is replaced with a plain one or the other way round,
			// together with the indexes.
			if _, err := tx.Exec(ctx, fmt.Sprintf("alter table %s drop column %s", name, k.Name)); err != nil {
				return fmt.Errorf("postgres: column %s.%s: %w", name, k.Name, err)
			}
			ok = false
		}
		switch {
		case !ok:
			if _, err := tx.Exec(ctx, fmt.Sprintf("alter table %s add column %s %s", name, k.Name, colTypes[k.Kind])); err != nil {
				return fmt.Errorf("postgres: column %s.%s: %w", name, k.Name, err)
			}
			added = true
		case have == "integer" && k.Kind == port.ColInt: // key columns of previous versions
		case have != infoTypes[k.Kind]:
			return &SchemaError{Model: l.Model, Msg: fmt.Sprintf("column %s.%s has type %s, the model requires %s: a manual migration is needed",
				name, k.Name, have, infoTypes[k.Kind])}
		}
	}
	if added && len(cols) > 0 && len(tenants) > 0 {
		// The new key columns are filled before the indexes on them.
		_, hasSealed := cols["sealed"]
		if err := backfill(ctx, tx, t, hasSealed, tenants, rewrite); err != nil {
			return err
		}
	}
	for i := range t.Indexes {
		if err := ensureIndex(ctx, tx, l.Model, t, &t.Indexes[i]); err != nil {
			return err
		}
	}
	return nil
}

// readTable is t as stored: with the sealed column when it exists (a field
// that stopped being personal still has its ciphertexts).
func readTable(t *port.Table, hasSealed bool) *port.Table {
	if !hasSealed || t.Personal {
		return t
	}
	rt := *t
	rt.Personal = true
	return &rt
}

// backfill fills the new key columns of the tenants' rows: rewrite reads
// the values from the state by the current schema (fields with previous
// names by was), blind ones from the decrypted values.
func backfill(ctx context.Context, tx pgx.Tx, t *port.Table, hasSealed bool, tenants []port.Tenant, rewrite port.Rewriter) error {
	names := make([]string, len(tenants))
	for i, tn := range tenants {
		names[i] = string(tn)
	}
	rt := readTable(t, hasSealed)
	rows, err := tx.Query(ctx, "select tenant, "+columns(rt)+keyColumns(t)+" from "+tableName(t)+" where tenant = any($1::text[])", names)
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
	for _, f := range all {
		out, changed, err := rewrite(ctx, port.Tenant(f.tenant), rt, f.row)
		if err != nil {
			return err
		}
		if !changed {
			continue
		}
		set := []string{}
		args := []any{f.tenant, f.row.ID}
		for i, k := range t.Keys {
			args = append(args, out.Keys[i])
			set = append(set, fmt.Sprintf("%s = $%d", k.Name, len(args)))
		}
		if _, err := tx.Exec(ctx, "update "+tableName(t)+" set "+strings.Join(set, ", ")+" where tenant = $1 and id = $2", args...); err != nil {
			return fmt.Errorf("postgres: filling the keys of %s: %w", tableName(t), err)
		}
	}
	return nil
}

func ensureIndex(ctx context.Context, tx pgx.Tx, model string, t *port.Table, ix *port.Index) error {
	iname := indexName(t, ix)
	want := append([]string{"tenant"}, ix.Columns...)
	if !ix.Unique {
		want = append(want, "id")
	}
	// The columns of an existing index: for a field renamed with was, the key
	// is in a new column, and the index with the same name is recreated.
	var have []string
	err := tx.QueryRow(ctx, `select array_agg(a.attname::text order by k.ord) from pg_class c
		join pg_namespace n on n.oid = c.relnamespace and n.nspname = current_schema()
		join pg_index i on i.indexrelid = c.oid
		cross join unnest(i.indkey) with ordinality k(attnum, ord)
		join pg_attribute a on a.attrelid = i.indrelid and a.attnum = k.attnum
		where c.relname = $1`, iname).Scan(&have)
	if err != nil {
		return err
	}
	if slices.Equal(have, want) {
		return nil
	}
	if have != nil {
		if _, err := tx.Exec(ctx, "drop index "+iname); err != nil {
			return err
		}
	}
	cols := []string{"tenant"}
	for i, c := range ix.Columns {
		if ix.Desc[i] {
			cols = append(cols, c+" desc nulls last")
		} else {
			cols = append(cols, c+" asc nulls first")
		}
	}
	if !ix.Unique {
		cols = append(cols, "id")
		_, err := tx.Exec(ctx, fmt.Sprintf("create index %s on %s (%s)", iname, tableName(t), strings.Join(cols, ", ")))
		return err
	}
	// Before a unique index — a duplicate check: the error lists the repeated
	// values so that a manual migration fixes them.
	var notNull []string
	for _, c := range ix.Columns {
		notNull = append(notNull, c+" is not null")
	}
	group := "tenant, " + strings.Join(ix.Columns, ", ")
	// Without the prepared statement cache: a key column may have changed
	// its type (blind ↔ plain), while the same query text is already prepared.
	rows, err := tx.Query(ctx, fmt.Sprintf("select %s, count(*) from %s where %s group by %s having count(*) > 1 limit 10",
		"tenant::text || ': ' || concat_ws(', ', "+strings.Join(ix.Columns, "::text, ")+"::text)", tableName(t), strings.Join(notNull, " and "), group),
		pgx.QueryExecModeSimpleProtocol)
	if err != nil {
		return err
	}
	var dups []string
	for rows.Next() {
		var key string
		var n int64
		if err := rows.Scan(&key, &n); err != nil {
			return err
		}
		dups = append(dups, fmt.Sprintf("%s (%d rows)", key, n))
	}
	if err := rows.Err(); err != nil {
		return err
	}
	if len(dups) > 0 {
		return &SchemaError{Model: model, Msg: fmt.Sprintf("the unique index %s.%s cannot be created: repeated values %s",
			t.Type, ix.Name, strings.Join(dups, "; "))}
	}
	_, err = tx.Exec(ctx, fmt.Sprintf("create unique index %s on %s (%s)", iname, tableName(t), strings.Join(cols, ", ")))
	return err
}
