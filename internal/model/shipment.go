package model

import (
	"time"
)

type ShipmentStatus string

const (
	ShipmentStatusPending   ShipmentStatus = "PENDING"
	ShipmentStatusShipping  ShipmentStatus = "SHIPPING"
	ShipmentStatusDelivered ShipmentStatus = "DELIVERED"
	ShipmentStatusFailed    ShipmentStatus = "FAILED"
	ShipmentStatusCancelled ShipmentStatus = "CANCELLED"
)

type Shipment struct {
	ID         int64          `json:"id"`
	OrderID    int64          `json:"order_id"`
	CustomerID int64          `json:"customer_id"`
	Status     ShipmentStatus `json:"status"`

	CreatedAt   time.Time  `json:"created_at"`
	UpdatedAt   time.Time  `json:"updated_at"`
	ShippedAt   *time.Time `json:"shipped_at,omitempty"`
	DeliveredAt *time.Time `json:"delivered_at,omitempty"`
}
