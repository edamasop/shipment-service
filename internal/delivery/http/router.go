package http

import (
	stdhttp "net/http"

	"shipment-service/internal/delivery/http/v1"
)

func NewRouter(handlers *Handlers) stdhttp.Handler {
	mux := stdhttp.NewServeMux()
	v1.RegisterShipmentRoutes(mux, handlers.Shipment)
	return mux
}
