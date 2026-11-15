package model

import "time"

// OutboxEvent mirrors a single shipment_outbox row.
//
// The JSON tags are not used for the Kafka message itself (the poller publishes
// Payload verbatim), but they keep logs and ad-hoc JSON dumps readable.
type OutboxEvent struct {
	ID          int64      `json:"id"`
	OrderID     int64      `json:"order_id"`
	CustomerID  int64      `json:"customer_id"`
	EventType   string     `json:"event_type"`
	Payload     []byte     `json:"payload"`
	CreatedAt   time.Time  `json:"created_at"`
	ProcessedAt *time.Time `json:"processed_at,omitempty"`
	Published   bool       `json:"published"`
	LockedUntil *time.Time `json:"locked_until,omitempty"`
	Attempts    int        `json:"attempts"`
	// Parked marks an event that exhausted its publish attempts.
	Parked bool `json:"parked"`
	// LastError keeps the reason an event was parked.
	LastError *string `json:"last_error,omitempty"`
}
