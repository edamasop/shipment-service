package postgres

import (
	"context"
	"shipment-service/internal/model"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

type OutboxRepository struct {
	db *pgxpool.Pool
}

func NewOutboxRepository(db *pgxpool.Pool) *OutboxRepository {
	return &OutboxRepository{
		db: db,
	}
}

func (r *OutboxRepository) querier(ctx context.Context) Querier {
	if tx, ok := ctx.Value(txKey{}).(pgx.Tx); ok {
		return tx
	}

	return r.db
}

func (r *OutboxRepository) Create(ctx context.Context, event *model.OutboxEvent) error {
	q := r.querier(ctx)
	err := q.QueryRow(
		ctx,
		`INSERT INTO payment_outbox (order_id, customer_id, event_type, payload)
		VALUES ($1, $2, $3, $4)
		RETURNING id, created_at, processed_at`,
		event.OrderID,
		event.CustomerID,
		event.EventType,
		event.Payload,
	).Scan(&event.ID, &event.CreatedAt, &event.ProcessedAt)

	return err
}

func (r *OutboxRepository) GetByID(ctx context.Context, id int64) (*model.OutboxEvent, error) {
	event := new(model.OutboxEvent)
	q := r.querier(ctx)
	err := q.QueryRow(
		ctx,
		`SELECT id,
		   order_id,
		   customer_id, 
		   event_type, 
		   payload, 
		   created_at, 
		   processed_at, 
		   published 
		FROM payment_outbox 
		WHERE id = $1`,
		id,
	).Scan(
		&event.ID,
		&event.OrderID,
		&event.CustomerID,
		&event.EventType,
		&event.Payload,
		&event.CreatedAt,
		&event.ProcessedAt,
		&event.Published)

	return event, err
}

func (r *OutboxRepository) Update(ctx context.Context, event *model.OutboxEvent) error {
	q := r.querier(ctx)
	_, err := q.Exec(ctx, `
	UPDATE payment_outbox 
	SET order_id = $2, 
	    customer_id = $3, 
	    event_type = $4,
	    payload = $5, 
	    updated_at = now(), 
	    published = $6
	WHERE id = $1`,
		event.ID, event.OrderID, event.CustomerID, event.EventType, event.Payload, event.Published)

	return err
}

func (r *OutboxRepository) Delete(ctx context.Context, id int64) error {
	q := r.querier(ctx)
	_, err := q.Exec(ctx, `
	DELETE FROM payment_outbox
	WHERE id = $1`,
		id)

	return err
}

func (r *OutboxRepository) GetUnpublished(ctx context.Context, limit int) ([]*model.OutboxEvent, error) {
	q := r.querier(ctx)

	rows, err := q.Query(ctx,
		`SELECT id,
		order_id,
		customer_id,
		event_type,
		payload,
		created_at,
		processed_at,
		published FROM payment_outbox WHERE published = FALSE LIMIT $1`, limit,
	)

	if err != nil {
		return nil, err
	}

	defer rows.Close()

	var events []*model.OutboxEvent
	for rows.Next() {
		event := new(model.OutboxEvent)

		err := rows.Scan(
			&event.ID,
			&event.OrderID,
			&event.CustomerID,
			&event.EventType,
			&event.Payload,
			&event.CreatedAt,
			&event.ProcessedAt,
			&event.Published,
		)

		if err != nil {
			return nil, err
		}

		events = append(events, event)
	}

	return events, nil
}

func (r *OutboxRepository) MarkPublished(ctx context.Context, id int64) error {
	q := r.querier(ctx)
	_, err := q.Exec(ctx, `UPDATE payment_outbox SET published = TRUE, processed_at = now() WHERE ID = $1`, id)
	return err
}
