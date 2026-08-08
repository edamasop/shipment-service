package http

import (
	"shipment-service/internal/delivery/http/v1"
	"shipment-service/internal/service"

	"github.com/sirupsen/logrus"
)

type Handlers struct {
	Shipment *v1.ShipmentHandler
}

func NewHandlers(services *service.Services, log *logrus.Entry) *Handlers {
	return &Handlers{Shipment: v1.NewShipmentHandler(services.Shipment, log)}
}
