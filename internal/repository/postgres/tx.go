package postgres

import (
	"context"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

type txKey struct{}

type TxManager struct {
	db *pgxpool.Pool
}

func NewTxManager(db *pgxpool.Pool) *TxManager {
	return &TxManager{
		db: db,
	}
}

func (m *TxManager) WithinTransaction(
	ctx context.Context,
	fn func(ctx context.Context) error,
) error {
	tx, err := m.db.BeginTx(ctx, pgx.TxOptions{})
	if err != nil {
		return err
	}

	defer tx.Rollback(ctx)

	ctx = context.WithValue(ctx, txKey{}, tx)

	if err := fn(ctx); err != nil {
		return err
	}

	return tx.Commit(ctx)
}
