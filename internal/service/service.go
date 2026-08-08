package service

import (
	"context"

	"github.com/sirupsen/logrus"

	"shipment-service/internal/repository"
	"shipment-service/internal/schema"
)

type Shipment interface {
	Create(ctx context.Context, dto *schema.ShipmentCreate) (*schema.ShipmentResponse, error)
	GetByID(ctx context.Context, id int64) (*schema.ShipmentResponse, error)
	GetByOrderID(ctx context.Context, orderID int64) (*schema.ShipmentResponse, error)
	Update(ctx context.Context, id int64, dto *schema.ShipmentUpdate) (*schema.ShipmentResponse, error)
	StartDelivery(ctx context.Context, orderID, customerID int64) error
	Delete(ctx context.Context, id int64) error
}

type Services struct {
	Shipment Shipment
}

func NewServices(repos *repository.Repository, log *logrus.Entry) *Services {
	return &Services{
		Shipment: NewShipmentService(repos.Shipment, repos.Outbox, repos.TxManager, log),
	}
}
