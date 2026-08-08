-- +goose Up
CREATE TABLE shipments (
    id BIGSERIAL PRIMARY KEY,
    order_id BIGINT NOT NULL UNIQUE,
    customer_id BIGINT NOT NULL,
    status VARCHAR(20) NOT NULL,
    shipped_at TIMESTAMPTZ,
    delivered_at TIMESTAMPTZ,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    CONSTRAINT shipments_status_check CHECK (status IN ('PENDING', 'SHIPPING', 'DELIVERED', 'FAILED', 'CANCELLED')),
    CONSTRAINT shipments_delivery_time_check CHECK (delivered_at IS NULL OR shipped_at IS NOT NULL)
);

CREATE INDEX idx_shipments_customer_id ON shipments (customer_id);
CREATE INDEX idx_shipments_status ON shipments (status);

-- +goose Down
DROP TABLE IF EXISTS shipments;
