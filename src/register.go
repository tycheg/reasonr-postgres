package postgres

import (
	"context"
	"fmt"
	"os"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/tycheg/reasonr-engine/port"
	"github.com/tycheg/reasonr-engine/port/registry"
)

// Registration in the engine registry (engine.yaml):
//
//	stores:
//	  main: { type: postgres, dsn: "postgres://app@db/main", tenancy: rls, max_conns: 20 }
//	notifications:
//	  hub: { type: postgres }   # the main store's connections; or its own dsn
func init() {
	registry.RegisterStore("postgres", func(n *port.Node, d registry.Deps) (any, error) {
		if err := n.Only("type", "dsn", "dsn_env", "tenancy", "max_conns"); err != nil {
			return nil, err
		}
		pool, err := openPool(n)
		if err != nil {
			return nil, err
		}
		tenancy, err := n.String("tenancy", "column")
		if err != nil {
			return nil, err
		}
		if tenancy != "column" && tenancy != "rls" {
			return nil, fmt.Errorf("postgres: tenancy %q is not supported (column, rls); schema and database are not implemented yet", tenancy)
		}
		return New(pool, Options{RLS: tenancy == "rls", Logger: d.Logger}), nil
	}, registry.PersonalData)
	registry.RegisterHub("postgres", func(n *port.Node, d registry.Deps) (port.HubLink, error) {
		if err := n.Only("type", "dsn", "dsn_env", "max_conns"); err != nil {
			return nil, err
		}
		if dsn, _ := n.String("dsn", ""); dsn != "" || n.Get("dsn_env") != nil {
			pool, err := openPool(n)
			if err != nil {
				return nil, err
			}
			return NewHub(pool, d.Logger), nil
		}
		state := d.State
		if w, ok := state.(interface{ Unwrap() any }); ok { // the main store seen as model values
			state = w.Unwrap()
		}
		s, ok := state.(*Store)
		if !ok {
			return nil, fmt.Errorf("postgres: the hub needs a dsn — the main store is not PostgreSQL")
		}
		return NewHub(s.pool, d.Logger), nil
	})
}

// openPool opens a pool by dsn or the environment variable dsn_env.
func openPool(n *port.Node) (*pgxpool.Pool, error) {
	dsn, err := n.String("dsn", "")
	if err != nil {
		return nil, err
	}
	if env, _ := n.String("dsn_env", ""); dsn == "" && env != "" {
		if dsn = os.Getenv(env); dsn == "" {
			return nil, fmt.Errorf("postgres: the environment variable %s is empty", env)
		}
	}
	if dsn == "" {
		return nil, fmt.Errorf("postgres: set dsn or dsn_env")
	}
	cfg, err := pgxpool.ParseConfig(dsn)
	if err != nil {
		return nil, fmt.Errorf("postgres: dsn: %w", err)
	}
	if mc, err := n.Int("max_conns", 0); err != nil {
		return nil, err
	} else if mc > 0 {
		cfg.MaxConns = int32(mc)
	}
	return pgxpool.NewWithConfig(context.Background(), cfg)
}
