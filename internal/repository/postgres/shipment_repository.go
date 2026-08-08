package postgres

import (
	"context"
	"shipment-service/internal/model"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

type ShipmentRepository struct {
	db *pgxpool.Pool
}

func NewShipmentRepository(db *pgxpool.Pool) *ShipmentRepository {
	return &ShipmentRepository{
		db: db,
	}
}

func (r *ShipmentRepository) querier(ctx context.Context) Querier {
	if tx, ok := ctx.Value(txKey{}).(pgx.Tx); ok {
		return tx
	}

	return r.db
}

func (r *ShipmentRepository) Create(ctx context.Context, shipment *model.Shipment) error {
	q := r.querier(ctx)

	return q.QueryRow(ctx, `
		INSERT INTO shipments
		(order_id, customer_id, status, shipped_at, delivered_at)
		VALUES ($1, $2, $3, $4, $5)
		RETURNING id, created_at, updated_at`,
		shipment.OrderID,
		shipment.CustomerID,
		shipment.Status,
		shipment.ShippedAt,
		shipment.DeliveredAt,
	).Scan(
		&shipment.ID,
		&shipment.CreatedAt,
		&shipment.UpdatedAt,
	)
}

func (r *ShipmentRepository) GetByID(ctx context.Context, id int64) (*model.Shipment, error) {
	q := r.querier(ctx)

	shipment := new(model.Shipment)

	err := q.QueryRow(ctx, `
		SELECT
			id,
			order_id,
			customer_id,
			status,
			created_at,
			updated_at,
			shipped_at,
			delivered_at
		FROM shipments
		WHERE id=$1`,
		id,
	).Scan(
		&shipment.ID,
		&shipment.OrderID,
		&shipment.CustomerID,
		&shipment.Status,
		&shipment.CreatedAt,
		&shipment.UpdatedAt,
		&shipment.ShippedAt,
		&shipment.DeliveredAt,
	)

	if err != nil {
		return nil, err
	}

	return shipment, nil
}

// GetByIDForUpdate is used only from a transaction when a status transition
// must be serialized with other updates to the same shipment.
func (r *ShipmentRepository) GetByIDForUpdate(ctx context.Context, id int64) (*model.Shipment, error) {
	q := r.querier(ctx)

	shipment := new(model.Shipment)

	err := q.QueryRow(ctx, `
		SELECT
			id,
			order_id,
			customer_id,
			status,
			created_at,
			updated_at,
			shipped_at,
			delivered_at
		FROM shipments
		WHERE id=$1
		FOR UPDATE`,
		id,
	).Scan(
		&shipment.ID,
		&shipment.OrderID,
		&shipment.CustomerID,
		&shipment.Status,
		&shipment.CreatedAt,
		&shipment.UpdatedAt,
		&shipment.ShippedAt,
		&shipment.DeliveredAt,
	)
	if err != nil {
		return nil, err
	}

	return shipment, nil
}

func (r *ShipmentRepository) GetByOrderID(ctx context.Context, orderID int64) (*model.Shipment, error) {
	q := r.querier(ctx)

	shipment := new(model.Shipment)

	err := q.QueryRow(ctx, `
		SELECT
			id,
			order_id,
			customer_id,
			status,
			created_at,
			updated_at,
			shipped_at,
			delivered_at
		FROM shipments
		WHERE order_id=$1`,
		orderID,
	).Scan(
		&shipment.ID,
		&shipment.OrderID,
		&shipment.CustomerID,
		&shipment.Status,
		&shipment.CreatedAt,
		&shipment.UpdatedAt,
		&shipment.ShippedAt,
		&shipment.DeliveredAt,
	)

	if err != nil {
		return nil, err
	}

	return shipment, nil
}

func (r *ShipmentRepository) Update(ctx context.Context, shipment *model.Shipment) error {
	q := r.querier(ctx)

	err := q.QueryRow(ctx, `
		UPDATE shipments
		SET
			order_id=$2,
			customer_id=$3,
			status=$4,
			shipped_at=$5,
			delivered_at=$6,
			updated_at=now()
		WHERE id=$1
		RETURNING updated_at`,
		shipment.ID,
		shipment.OrderID,
		shipment.CustomerID,
		shipment.Status,
		shipment.ShippedAt,
		shipment.DeliveredAt,
	).Scan(&shipment.UpdatedAt)

	return err
}

func (r *ShipmentRepository) Delete(ctx context.Context, id int64) error {
	q := r.querier(ctx)

	_, err := q.Exec(ctx,
		`DELETE FROM shipments WHERE id=$1`,
		id,
	)

	return err
}
