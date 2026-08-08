package postgres

import (
	"context"
	"time"

	"shipment-service/internal/model"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

// outboxColumns is the single source of truth for column order. It must
// match the order in which outboxFields() writes into model.OutboxEvent.
const outboxColumns = `id, order_id, customer_id, event_type, payload, created_at,
	processed_at, published, locked_until, attempts`

// outboxColumnsAliased is the same column list, prefixed for contexts where
// the table has an alias (e.g. the "outbox" alias in ClaimUnpublished's
// UPDATE ... FROM). Keep in sync with outboxColumns by construction, not by
// hand: if you add a column, add it in both constants right next to each
// other, or this guarantee is exactly as broken as no constant at all.
const outboxColumnsAliased = `outbox.id, outbox.order_id, outbox.customer_id, outbox.event_type,
	outbox.payload, outbox.created_at, outbox.processed_at, outbox.published,
	outbox.locked_until, outbox.attempts`

const outboxSelect = `SELECT ` + outboxColumns + ` FROM shipment_outbox`

type OutboxRepository struct {
	db *pgxpool.Pool
}

func NewOutboxRepository(db *pgxpool.Pool) *OutboxRepository {
	return &OutboxRepository{db: db}
}

func (r *OutboxRepository) querier(ctx context.Context) Querier {
	if tx, ok := ctx.Value(txKey{}).(pgx.Tx); ok {
		return tx
	}
	return r.db
}

func (r *OutboxRepository) Create(ctx context.Context, event *model.OutboxEvent) error {
	return r.querier(ctx).QueryRow(ctx, `
		INSERT INTO shipment_outbox (order_id, customer_id, event_type, payload)
		VALUES ($1, $2, $3, $4)
		RETURNING id, created_at, processed_at, published, locked_until, attempts`,
		event.OrderID, event.CustomerID, event.EventType, event.Payload,
	).Scan(&event.ID, &event.CreatedAt, &event.ProcessedAt, &event.Published, &event.LockedUntil, &event.Attempts)
}

func (r *OutboxRepository) GetByID(ctx context.Context, id int64) (*model.OutboxEvent, error) {
	event := new(model.OutboxEvent)
	err := r.querier(ctx).QueryRow(ctx, outboxSelect+` WHERE id = $1`, id).Scan(outboxFields(event)...)
	if err != nil {
		return nil, err
	}
	return event, nil
}

func (r *OutboxRepository) Update(ctx context.Context, event *model.OutboxEvent) error {
	_, err := r.querier(ctx).Exec(ctx, `
		UPDATE shipment_outbox
		SET order_id = $2, customer_id = $3, event_type = $4, payload = $5,
			processed_at = $6, published = $7, locked_until = $8
		WHERE id = $1`,
		event.ID, event.OrderID, event.CustomerID, event.EventType, event.Payload,
		event.ProcessedAt, event.Published, event.LockedUntil,
	)
	return err
}

func (r *OutboxRepository) Delete(ctx context.Context, id int64) error {
	_, err := r.querier(ctx).Exec(ctx, `DELETE FROM shipment_outbox WHERE id = $1`, id)
	return err
}

func (r *OutboxRepository) GetUnpublished(ctx context.Context, limit int) ([]*model.OutboxEvent, error) {
	rows, err := r.querier(ctx).Query(ctx, outboxSelect+`
		WHERE published = FALSE
		ORDER BY created_at, id
		LIMIT $1`, limit)
	if err != nil {
		return nil, err
	}
	return scanOutboxEvents(rows)
}

// ClaimUnpublished leases events using SKIP LOCKED, so concurrent pollers do
// not publish the same event during the lease period. Delivery remains
// at-least-once; consumers must be idempotent.
//
// The RETURNING clause reuses outboxColumnsAliased so the column list here
// stays in lockstep with outboxFields()/outboxSelect instead of being
// hand-copied a third time.
func (r *OutboxRepository) ClaimUnpublished(ctx context.Context, limit int, lease time.Duration) ([]*model.OutboxEvent, error) {
	rows, err := r.db.Query(ctx, `
		WITH candidates AS (
			SELECT id
			FROM shipment_outbox
			WHERE published = FALSE
			  AND (locked_until IS NULL OR locked_until < now())
			ORDER BY created_at, id
			FOR UPDATE SKIP LOCKED
			LIMIT $1
		)
		UPDATE shipment_outbox AS outbox
		SET locked_until = now() + ($2 * interval '1 millisecond'),
			attempts = attempts + 1
		FROM candidates
		WHERE outbox.id = candidates.id
		RETURNING `+outboxColumnsAliased, limit, lease.Milliseconds())
	if err != nil {
		return nil, err
	}
	return scanOutboxEvents(rows)
}

func (r *OutboxRepository) MarkPublished(ctx context.Context, id int64) error {
	_, err := r.querier(ctx).Exec(ctx, `
		UPDATE shipment_outbox
		SET published = TRUE, processed_at = now(), locked_until = NULL
		WHERE id = $1`, id)
	return err
}

func outboxFields(event *model.OutboxEvent) []any {
	return []any{
		&event.ID, &event.OrderID, &event.CustomerID, &event.EventType, &event.Payload,
		&event.CreatedAt, &event.ProcessedAt, &event.Published, &event.LockedUntil, &event.Attempts,
	}
}

func scanOutboxEvents(rows pgx.Rows) ([]*model.OutboxEvent, error) {
	defer rows.Close()

	events := make([]*model.OutboxEvent, 0)
	for rows.Next() {
		event := new(model.OutboxEvent)
		if err := rows.Scan(outboxFields(event)...); err != nil {
			return nil, err
		}
		events = append(events, event)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return events, nil
}
