package v1

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"strconv"

	"github.com/sirupsen/logrus"

	"shipment-service/internal/schema"
	"shipment-service/internal/service"
)

const maxRequestBodyBytes = 1 << 20

type ShipmentService interface {
	Create(ctx context.Context, dto *schema.ShipmentCreate) (*schema.ShipmentResponse, error)
	GetByID(ctx context.Context, id int64) (*schema.ShipmentResponse, error)
	GetByOrderID(ctx context.Context, orderID int64) (*schema.ShipmentResponse, error)
	Update(ctx context.Context, id int64, dto *schema.ShipmentUpdate) (*schema.ShipmentResponse, error)
	Delete(ctx context.Context, id int64) error
}

type ShipmentHandler struct {
	service ShipmentService
	log     *logrus.Entry
}

func NewShipmentHandler(service ShipmentService, log *logrus.Entry) *ShipmentHandler {
	return &ShipmentHandler{service: service, log: log.WithField("handler", "shipment")}
}

func (h *ShipmentHandler) Create(w http.ResponseWriter, r *http.Request) {
	var dto schema.ShipmentCreate
	if !decodeJSON(w, r, &dto) {
		return
	}

	shipment, err := h.service.Create(r.Context(), &dto)
	if err != nil {
		h.writeServiceError(w, err)
		return
	}

	writeJSON(w, http.StatusCreated, shipment)
}

func (h *ShipmentHandler) GetByID(w http.ResponseWriter, r *http.Request) {
	id, ok := pathID(w, r, "id")
	if !ok {
		return
	}

	shipment, err := h.service.GetByID(r.Context(), id)
	if err != nil {
		h.writeServiceError(w, err)
		return
	}

	writeJSON(w, http.StatusOK, shipment)
}

func (h *ShipmentHandler) GetByOrderID(w http.ResponseWriter, r *http.Request) {
	orderID, ok := pathID(w, r, "orderID")
	if !ok {
		return
	}

	shipment, err := h.service.GetByOrderID(r.Context(), orderID)
	if err != nil {
		h.writeServiceError(w, err)
		return
	}

	writeJSON(w, http.StatusOK, shipment)
}

func (h *ShipmentHandler) Update(w http.ResponseWriter, r *http.Request) {
	id, ok := pathID(w, r, "id")
	if !ok {
		return
	}

	var dto schema.ShipmentUpdate
	if !decodeJSON(w, r, &dto) {
		return
	}

	shipment, err := h.service.Update(r.Context(), id, &dto)
	if err != nil {
		h.writeServiceError(w, err)
		return
	}

	writeJSON(w, http.StatusOK, shipment)
}

func (h *ShipmentHandler) Delete(w http.ResponseWriter, r *http.Request) {
	id, ok := pathID(w, r, "id")
	if !ok {
		return
	}

	if err := h.service.Delete(r.Context(), id); err != nil {
		h.writeServiceError(w, err)
		return
	}

	w.WriteHeader(http.StatusNoContent)
}

func (h *ShipmentHandler) writeServiceError(w http.ResponseWriter, err error) {
	switch {
	case errors.Is(err, service.ErrInvalidArgument):
		writeError(w, http.StatusBadRequest, err.Error())
	case errors.Is(err, service.ErrShipmentAlreadyExists):
		writeError(w, http.StatusConflict, "shipment for this order already exists")
	case errors.Is(err, service.ErrShipmentNotFound):
		writeError(w, http.StatusNotFound, "shipment not found")
	case errors.Is(err, service.ErrInvalidStatusTransition):
		writeError(w, http.StatusConflict, err.Error())
	default:
		h.log.WithError(err).Error("shipment request failed")
		writeError(w, http.StatusInternalServerError, "internal server error")
	}
}

func decodeJSON(w http.ResponseWriter, r *http.Request, dst any) bool {
	r.Body = http.MaxBytesReader(w, r.Body, maxRequestBodyBytes)
	decoder := json.NewDecoder(r.Body)
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(dst); err != nil {
		writeError(w, http.StatusBadRequest, "invalid request body")
		return false
	}
	if err := decoder.Decode(&struct{}{}); !errors.Is(err, io.EOF) {
		writeError(w, http.StatusBadRequest, "request body must contain one JSON object")
		return false
	}
	return true
}

func pathID(w http.ResponseWriter, r *http.Request, name string) (int64, bool) {
	id, err := strconv.ParseInt(r.PathValue(name), 10, 64)
	if err != nil || id <= 0 {
		writeError(w, http.StatusBadRequest, name+" must be a positive integer")
		return 0, false
	}
	return id, true
}

func writeJSON(w http.ResponseWriter, status int, body any) {
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(body)
}

func writeError(w http.ResponseWriter, status int, message string) {
	writeJSON(w, status, map[string]string{"error": message})
}
