package repository

import (
	"context"
	"shipment-service/internal/model"
	"shipment-service/internal/repository/postgres"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
)

type Shipment interface {
	Create(ctx context.Context, shipment *model.Shipment) error
	GetByID(ctx context.Context, id int64) (*model.Shipment, error)
	GetByIDForUpdate(ctx context.Context, id int64) (*model.Shipment, error)
	GetByOrderID(ctx context.Context, orderID int64) (*model.Shipment, error)
	Update(ctx context.Context, shipment *model.Shipment) error
	Delete(ctx context.Context, id int64) error
}

type Outbox interface {
	Create(ctx context.Context, event *model.OutboxEvent) error
	GetByID(ctx context.Context, id int64) (*model.OutboxEvent, error)
	Update(ctx context.Context, event *model.OutboxEvent) error
	Delete(ctx context.Context, id int64) error
	GetUnpublished(ctx context.Context, limit int) ([]*model.OutboxEvent, error)
	ClaimUnpublished(ctx context.Context, limit int, lease time.Duration) ([]*model.OutboxEvent, error)
	MarkPublished(ctx context.Context, id int64) error
}

type Repository struct {
	Shipment  Shipment
	Outbox    Outbox
	TxManager TxManager
}

func NewRepository(db *pgxpool.Pool) *Repository {
	return &Repository{
		postgres.NewShipmentRepository(db),
		postgres.NewOutboxRepository(db),
		postgres.NewTxManager(db),
	}
}
