package schema

import (
	"time"

	"shipment-service/internal/model"
)

type ShipmentCreate struct {
	OrderID    int64 `json:"order_id"`
	CustomerID int64 `json:"customer_id"`
}

type ShipmentUpdate struct {
	Status model.ShipmentStatus `json:"status"`
}

type ShipmentResponse struct {
	ID          int64                `json:"id"`
	OrderID     int64                `json:"order_id"`
	CustomerID  int64                `json:"customer_id"`
	Status      model.ShipmentStatus `json:"status"`
	CreatedAt   time.Time            `json:"created_at"`
	UpdatedAt   time.Time            `json:"updated_at"`
	ShippedAt   *time.Time           `json:"shipped_at,omitempty"`
	DeliveredAt *time.Time           `json:"delivered_at,omitempty"`
}
