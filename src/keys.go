package postgres

import (
	"context"
	"errors"

	"github.com/jackc/pgx/v5"

	"github.com/tycheg/reasonr-engine/port"
)

// Personal data keys (port.KeyStore, RFC §20): the tenant key is in
// rt_tenant_keys, subject keys are in rt_subject_keys. A destroyed key is a
// row with wrapped = null (a version tombstone).
//
// In a commit transaction keys are read and created through it (txKey in
// the context): a second pool connection might not be available while the
// transaction is busy. A key created in a rolled back transaction is gone
// with it, so after a failed commit the keys used are dropped from the
// cache. The tenant key is created before the transaction (Sealer.Ready).

var _ port.KeyStore = (*Store)(nil)

// txKey is the transaction key in the context: each store has its own,
// because an additional store encrypts with the main one's keys (decision
// 47 of RFC §20) and its transaction is in another database.
type txKey struct{ s *Store }

// withTx returns a context in which the key store works through q.
func (s *Store) withTx(ctx context.Context, q querier) context.Context {
	return context.WithValue(ctx, txKey{s}, q)
}

// keysQ returns the transaction from the context or the pool.
func (s *Store) keysQ(ctx context.Context) querier {
	if q, ok := ctx.Value(txKey{s}).(querier); ok {
		return q
	}
	return s.pool
}

// TenantKey returns the wrapped tenant key (port.KeyStore).
func (s *Store) TenantKey(ctx context.Context, tenant port.Tenant) ([]byte, error) {
	var wrapped []byte
	err := s.keysQ(ctx).QueryRow(ctx, "select wrapped from rt_tenant_keys where tenant = $1", string(tenant)).Scan(&wrapped)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, nil
	}
	return wrapped, err
}

// CreateTenantKey stores the tenant key if there is none (port.KeyStore).
func (s *Store) CreateTenantKey(ctx context.Context, tenant port.Tenant, wrapped []byte) ([]byte, error) {
	q := s.keysQ(ctx)
	if _, err := q.Exec(ctx, "insert into rt_tenant_keys (tenant, wrapped) values ($1, $2) on conflict do nothing", string(tenant), wrapped); err != nil {
		return nil, err
	}
	var got []byte
	err := q.QueryRow(ctx, "select wrapped from rt_tenant_keys where tenant = $1", string(tenant)).Scan(&got)
	return got, err
}

// TenantKeys returns the keys of every tenant (port.TenantKeyRotator).
func (s *Store) TenantKeys(ctx context.Context) (map[port.Tenant][]byte, error) {
	rows, err := s.pool.Query(ctx, "select tenant, wrapped from rt_tenant_keys")
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := map[port.Tenant][]byte{}
	for rows.Next() {
		var t string
		var w []byte
		if err := rows.Scan(&t, &w); err != nil {
			return nil, err
		}
		out[port.Tenant(t)] = w
	}
	return out, rows.Err()
}

// ReplaceTenantKey replaces the wrapped tenant key if it is still old
// (port.TenantKeyRotator).
func (s *Store) ReplaceTenantKey(ctx context.Context, tenant port.Tenant, old, wrapped []byte) (bool, error) {
	tag, err := s.pool.Exec(ctx, "update rt_tenant_keys set wrapped = $3 where tenant = $1 and wrapped = $2", string(tenant), old, wrapped)
	return tag.RowsAffected() == 1, err
}

// SubjectKeys returns the keys and tombstones of subjects (port.KeyStore).
func (s *Store) SubjectKeys(ctx context.Context, tenant port.Tenant, refs []port.SubjectRef) ([]port.WrappedKey, error) {
	types, ids := refColumns(refs)
	rows, err := s.keysQ(ctx).Query(ctx, `select k.subject_type, k.subject, k.version, k.wrapped from rt_subject_keys k
		join unnest($2::text[], $3::text[]) u(t, s) on k.subject_type = u.t and k.subject = u.s
		where k.tenant = $1`, string(tenant), types, ids)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []port.WrappedKey
	for rows.Next() {
		var wk port.WrappedKey
		var version int64
		if err := rows.Scan(&wk.Subject.Type, &wk.Subject.ID, &version, &wk.Wrapped); err != nil {
			return nil, err
		}
		wk.Version = uint32(version)
		out = append(out, wk)
	}
	return out, rows.Err()
}

// CreateSubjectKey stores a subject key: version 1, after a tombstone — the
// next one; a valid key (a creation race) is returned as is
// (port.KeyStore).
func (s *Store) CreateSubjectKey(ctx context.Context, tenant port.Tenant, ref port.SubjectRef, wrapped []byte) (port.WrappedKey, error) {
	q := s.keysQ(ctx)
	wk := port.WrappedKey{Subject: ref}
	var version int64
	// The valid key comes from the statement's snapshot; a key created by a
	// concurrent transaction after the snapshot comes from a second query.
	err := q.QueryRow(ctx, `with ins as (insert into rt_subject_keys (tenant, subject_type, subject, version, wrapped) values ($1, $2, $3, 1, $4)
		on conflict (tenant, subject_type, subject) do update set version = rt_subject_keys.version + 1, wrapped = excluded.wrapped
		where rt_subject_keys.wrapped is null
		returning version, wrapped)
		select version, wrapped from ins
		union all select version, wrapped from rt_subject_keys
		where tenant = $1 and subject_type = $2 and subject = $3 and wrapped is not null and not exists (select from ins)`,
		string(tenant), ref.Type, ref.ID, wrapped).Scan(&version, &wk.Wrapped)
	if errors.Is(err, pgx.ErrNoRows) {
		err = q.QueryRow(ctx, "select version, wrapped from rt_subject_keys where tenant = $1 and subject_type = $2 and subject = $3",
			string(tenant), ref.Type, ref.ID).Scan(&version, &wk.Wrapped)
	}
	wk.Version = uint32(version)
	return wk, err
}

func refColumns(refs []port.SubjectRef) (types, ids []string) {
	types, ids = make([]string, len(refs)), make([]string, len(refs))
	for i, r := range refs {
		types[i], ids[i] = r.Type, r.ID
	}
	return types, ids
}

// checkKeys checks in the transaction the versions of the keys the unit of
// work encrypted with against the valid ones and holds their rows for share
// until the commit (decision 8 of RFC §20): a forget that destroys a key
// waits for its end, and a key destroyed earlier is a conflict — the kernel
// repeats the command.
func checkKeys(ctx context.Context, tx pgx.Tx, tenant port.Tenant, used []port.KeyVersion) error {
	sql, args, need := keysCheck(tenant, used)
	if need == nil {
		return nil
	}
	rows, err := tx.Query(ctx, sql, args...)
	if err != nil {
		return err
	}
	return verifyKeys(rows, need)
}

// keysCheck returns the check query and the keys it checks; need == nil —
// nothing to check. Keys created by this transaction are not checked: it
// already holds the key row with the insert lock.
func keysCheck(tenant port.Tenant, used []port.KeyVersion) (string, []any, []port.KeyVersion) {
	var need []port.KeyVersion
	var refs []port.SubjectRef
	for _, kv := range used {
		need = append(need, kv)
		refs = append(refs, kv.Subject)
	}
	if need == nil {
		return "", nil, nil
	}
	types, ids := refColumns(refs)
	return `select k.subject_type, k.subject, k.version from rt_subject_keys k
		join unnest($2::text[], $3::text[]) u(t, s) on k.subject_type = u.t and k.subject = u.s
		where k.tenant = $1 and k.wrapped is not null for share of k`, []any{string(tenant), types, ids}, need
}

// verifyKeys reads the check result: each key in need is valid in its
// version, otherwise — ErrConflict.
func verifyKeys(rows pgx.Rows, need []port.KeyVersion) error {
	live := map[port.SubjectRef]uint32{}
	for rows.Next() {
		var ref port.SubjectRef
		var version int64
		if err := rows.Scan(&ref.Type, &ref.ID, &version); err != nil {
			rows.Close()
			return err
		}
		live[ref] = uint32(version)
	}
	if err := rows.Err(); err != nil {
		return err
	}
	for _, kv := range need {
		if v, ok := live[kv.Subject]; !ok || v != kv.Version {
			return port.ErrConflict
		}
	}
	return nil
}

// destroyKeys destroys subject keys in the forget transaction: the row
// stays as a tombstone with the same version.
func destroyKeys(ctx context.Context, tx pgx.Tx, tenant port.Tenant, refs []port.SubjectRef) error {
	types, ids := refColumns(refs)
	_, err := tx.Exec(ctx, `update rt_subject_keys k set wrapped = null
		from unnest($2::text[], $3::text[]) u(t, s)
		where k.tenant = $1 and k.subject_type = u.t and k.subject = u.s and k.wrapped is not null`, string(tenant), types, ids)
	return err
}
