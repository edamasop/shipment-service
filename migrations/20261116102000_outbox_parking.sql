-- +goose Up
-- +goose StatementBegin
-- A poison event used to be retried forever. Parking is a deliberate terminal
-- state: the row is kept for inspection but never claimed again.
ALTER TABLE shipment_outbox
    ADD COLUMN IF NOT EXISTS parked BOOLEAN NOT NULL DEFAULT FALSE,
    ADD COLUMN IF NOT EXISTS last_error TEXT;

DROP INDEX IF EXISTS idx_shipment_outbox_available;
CREATE INDEX IF NOT EXISTS idx_shipment_outbox_available
    ON shipment_outbox (created_at, id)
    WHERE published = FALSE AND parked = FALSE;

-- Retention cleanup deletes old published rows by processed_at.
CREATE INDEX IF NOT EXISTS idx_shipment_outbox_processed
    ON shipment_outbox (processed_at)
    WHERE published = TRUE;
-- +goose StatementEnd

-- +goose Down
-- +goose StatementBegin
DROP INDEX IF EXISTS idx_shipment_outbox_processed;
DROP INDEX IF EXISTS idx_shipment_outbox_available;
CREATE INDEX IF NOT EXISTS idx_shipment_outbox_available
    ON shipment_outbox (created_at, id)
    WHERE published = FALSE;

ALTER TABLE shipment_outbox
    DROP COLUMN IF EXISTS last_error,
    DROP COLUMN IF EXISTS parked;
-- +goose StatementEnd
