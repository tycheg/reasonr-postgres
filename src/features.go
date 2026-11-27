package postgres

import (
	"context"

	"github.com/jackc/pgx/v5"

	"github.com/tycheg/reasonr-engine/port"
)

var _ port.FeatureStore = (*Store)(nil)

// FeatureRules returns the feature flag rollout from rt_features (port.FeatureStore).
func (s *Store) FeatureRules(ctx context.Context) ([]port.FeatureRule, error) {
	if err := s.runtimeTables(ctx); err != nil {
		return nil, err
	}
	rows, err := s.pool.Query(ctx, `select feature, tenant, role, actor from rt_features order by feature, tenant, role, actor`)
	if err != nil {
		return nil, err
	}
	return pgx.CollectRows(rows, func(r pgx.CollectableRow) (port.FeatureRule, error) {
		var f port.FeatureRule
		var tenant string
		err := r.Scan(&f.Feature, &tenant, &f.Scope.Role, &f.Scope.Actor)
		f.Scope.Tenant = port.Tenant(tenant)
		return f, err
	})
}

// ChangeFeature changes the rollout and writes the change to
// rt_feature_log in one transaction (port.FeatureStore).
func (s *Store) ChangeFeature(ctx context.Context, c port.FeatureChange) error {
	if err := s.runtimeTables(ctx); err != nil {
		return err
	}
	sc := c.Scope
	return pgx.BeginFunc(ctx, s.pool, func(tx pgx.Tx) error {
		var err error
		switch {
		case c.On:
			_, err = tx.Exec(ctx, `insert into rt_features (feature, tenant, role, actor) values ($1, $2, $3, $4) on conflict do nothing`,
				c.Feature, string(sc.Tenant), sc.Role, sc.Actor)
		case c.Everywhere:
			_, err = tx.Exec(ctx, `delete from rt_features where feature = $1`, c.Feature)
		default:
			_, err = tx.Exec(ctx, `delete from rt_features where feature = $1 and tenant = $2 and role = $3 and actor = $4`,
				c.Feature, string(sc.Tenant), sc.Role, sc.Actor)
		}
		if err != nil {
			return err
		}
		_, err = tx.Exec(ctx, `insert into rt_feature_log (at, by, feature, on_, tenant, role, actor, everywhere)
			values ($1, $2, $3, $4, $5, $6, $7, $8)`, c.At, c.By, c.Feature, c.On, string(sc.Tenant), sc.Role, sc.Actor, c.Everywhere)
		return err
	})
}

// FeatureLog returns the rollout change log (port.FeatureStore).
func (s *Store) FeatureLog(ctx context.Context, feature string) ([]port.FeatureChange, error) {
	if err := s.runtimeTables(ctx); err != nil {
		return nil, err
	}
	rows, err := s.pool.Query(ctx, `select at, by, feature, on_, tenant, role, actor, everywhere from rt_feature_log
		where $1 = '' or feature = $1 order by id`, feature)
	if err != nil {
		return nil, err
	}
	return pgx.CollectRows(rows, func(r pgx.CollectableRow) (port.FeatureChange, error) {
		var c port.FeatureChange
		var tenant string
		err := r.Scan(&c.At, &c.By, &c.Feature, &c.On, &tenant, &c.Scope.Role, &c.Scope.Actor, &c.Everywhere)
		c.Scope.Tenant, c.At = port.Tenant(tenant), c.At.UTC()
		return c, err
	})
}
