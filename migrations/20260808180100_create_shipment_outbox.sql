-- +goose Up
CREATE TABLE shipment_outbox (
    id BIGSERIAL PRIMARY KEY,
    order_id BIGINT NOT NULL,
    customer_id BIGINT NOT NULL,
    event_type VARCHAR(100) NOT NULL,
    payload JSONB NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    processed_at TIMESTAMPTZ,
    published BOOLEAN NOT NULL DEFAULT FALSE,
    locked_until TIMESTAMPTZ,
    attempts INTEGER NOT NULL DEFAULT 0
);

CREATE INDEX idx_shipment_outbox_available
    ON shipment_outbox (created_at, id)
    WHERE published = FALSE;

-- +goose Down
DROP TABLE IF EXISTS shipment_outbox;
