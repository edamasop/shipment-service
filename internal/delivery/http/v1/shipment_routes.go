package v1

import "net/http"

func RegisterShipmentRoutes(mux *http.ServeMux, handler *ShipmentHandler) {
	mux.HandleFunc("POST /v1/shipments", handler.Create)
	mux.HandleFunc("GET /v1/shipments/{id}", handler.GetByID)
	mux.HandleFunc("GET /v1/shipments/order/{orderID}", handler.GetByOrderID)
	mux.HandleFunc("PATCH /v1/shipments/{id}", handler.Update)
	mux.HandleFunc("DELETE /v1/shipments/{id}", handler.Delete)
}
