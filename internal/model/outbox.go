package model

import "time"

type OutboxEvent struct {
	ID          int64
	OrderID     int64
	CustomerID  int64
	EventType   string
	Payload     []byte
	CreatedAt   time.Time
	ProcessedAt *time.Time
	Published   bool
	LockedUntil *time.Time
	Attempts    int
}
