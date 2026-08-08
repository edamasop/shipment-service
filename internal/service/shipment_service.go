package service

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/edamasop/events"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/sirupsen/logrus"

	"shipment-service/internal/model"
	"shipment-service/internal/repository"
	"shipment-service/internal/schema"
)

type ShipmentService struct {
	shipmentRepository repository.Shipment
	outboxRepository   repository.Outbox
	txManager          repository.TxManager
	log                *logrus.Entry
	now                func() time.Time
}

func NewShipmentService(
	shipmentRepository repository.Shipment,
	outboxRepository repository.Outbox,
	txManager repository.TxManager,
	log *logrus.Entry,
) *ShipmentService {
	return &ShipmentService{
		shipmentRepository: shipmentRepository,
		outboxRepository:   outboxRepository,
		txManager:          txManager,
		log:                log.WithField("service", "shipment_service"),
		now:                time.Now,
	}
}

func (s *ShipmentService) Create(ctx context.Context, dto *schema.ShipmentCreate) (*schema.ShipmentResponse, error) {
	if dto == nil || dto.OrderID <= 0 || dto.CustomerID <= 0 {
		return nil, fmt.Errorf("%w: order_id and customer_id must be positive", ErrInvalidArgument)
	}

	shipment := &model.Shipment{
		OrderID:    dto.OrderID,
		CustomerID: dto.CustomerID,
		Status:     model.ShipmentStatusPending,
	}

	if err := s.txManager.WithinTransaction(ctx, func(txCtx context.Context) error {
		if err := s.shipmentRepository.Create(txCtx, shipment); err != nil {
			if isUniqueViolation(err) {
				return ErrShipmentAlreadyExists
			}
			s.log.WithError(err).WithField("order_id", shipment.OrderID).Error("failed to create shipment")
			return fmt.Errorf("create shipment: %w", err)
		}

		if err := s.createOutboxEvent(txCtx, shipment, events.ShipmentCreated); err != nil {
			s.log.WithError(err).WithField("shipment_id", shipment.ID).Error("failed to create shipment outbox event")
			return err
		}

		return nil
	}); err != nil {
		return nil, err
	}

	s.log.WithFields(logrus.Fields{"shipment_id": shipment.ID, "order_id": shipment.OrderID}).Info("shipment created")
	return shipmentResponse(shipment), nil
}

func (s *ShipmentService) GetByID(ctx context.Context, id int64) (*schema.ShipmentResponse, error) {
	if id <= 0 {
		return nil, fmt.Errorf("%w: shipment id must be positive", ErrInvalidArgument)
	}

	shipment, err := s.shipmentRepository.GetByID(ctx, id)
	if err != nil {
		return nil, s.getError(err, "shipment_id", id)
	}

	return shipmentResponse(shipment), nil
}

func (s *ShipmentService) GetByOrderID(ctx context.Context, orderID int64) (*schema.ShipmentResponse, error) {
	if orderID <= 0 {
		return nil, fmt.Errorf("%w: order id must be positive", ErrInvalidArgument)
	}

	shipment, err := s.shipmentRepository.GetByOrderID(ctx, orderID)
	if err != nil {
		return nil, s.getError(err, "order_id", orderID)
	}

	return shipmentResponse(shipment), nil
}

func (s *ShipmentService) Update(ctx context.Context, id int64, dto *schema.ShipmentUpdate) (*schema.ShipmentResponse, error) {
	if id <= 0 || dto == nil {
		return nil, fmt.Errorf("%w: shipment id and request body are required", ErrInvalidArgument)
	}

	var shipment *model.Shipment
	if err := s.txManager.WithinTransaction(ctx, func(txCtx context.Context) error {
		current, err := s.shipmentRepository.GetByIDForUpdate(txCtx, id)
		if err != nil {
			return s.getError(err, "shipment_id", id)
		}

		if !canTransition(current.Status, dto.Status) {
			return fmt.Errorf("%w: %s -> %s", ErrInvalidStatusTransition, current.Status, dto.Status)
		}

		now := s.now().UTC()
		current.Status = dto.Status
		switch dto.Status {
		case model.ShipmentStatusShipping:
			current.ShippedAt = &now
		case model.ShipmentStatusDelivered:
			current.DeliveredAt = &now
		}

		if err := s.shipmentRepository.Update(txCtx, current); err != nil {
			s.log.WithError(err).WithField("shipment_id", id).Error("failed to update shipment")
			return fmt.Errorf("update shipment: %w", err)
		}

		if err := s.createOutboxEvent(txCtx, current, events.ShipmentUpdated); err != nil {
			s.log.WithError(err).WithField("shipment_id", id).Error("failed to create shipment outbox event")
			return err
		}

		shipment = current
		return nil
	}); err != nil {
		return nil, err
	}

	s.log.WithFields(logrus.Fields{"shipment_id": shipment.ID, "status": shipment.Status}).Info("shipment updated")
	return shipmentResponse(shipment), nil
}

// StartDelivery is idempotent because Kafka can deliver payment events more
// than once. Only a pending shipment produces a state change and outbox event.
func (s *ShipmentService) StartDelivery(ctx context.Context, orderID, customerID int64) error {
	if orderID <= 0 || customerID <= 0 {
		return fmt.Errorf(
			"%w: order_id and customer_id must be positive",
			ErrInvalidArgument,
		)
	}

	return s.txManager.WithinTransaction(ctx, func(txCtx context.Context) error {
		shipment := &model.Shipment{
			OrderID:    orderID,
			CustomerID: customerID,
			Status:     model.ShipmentStatusShipping,
		}

		now := s.now().UTC()
		shipment.ShippedAt = &now

		if err := s.shipmentRepository.Create(txCtx, shipment); err != nil {
			if isUniqueViolation(err) {
				existing, err := s.shipmentRepository.GetByOrderID(txCtx, orderID)
				if err != nil {
					return s.getError(err, "order_id", orderID)
				}

				if existing.CustomerID != customerID {
					return fmt.Errorf(
						"%w: payment customer does not match shipment customer",
						ErrInvalidArgument,
					)
				}

				return nil
			}

			s.log.WithError(err).
				WithField("order_id", orderID).
				Error("failed to create shipment")

			return fmt.Errorf("create shipment: %w", err)
		}

		if err := s.createOutboxEvent(
			txCtx,
			shipment,
			events.ShipmentUpdated,
		); err != nil {
			s.log.WithError(err).
				WithField("shipment_id", shipment.ID).
				Error("failed to create shipment outbox event")

			return err
		}

		s.log.WithFields(logrus.Fields{
			"shipment_id": shipment.ID,
			"order_id":    orderID,
		}).Info("delivery started")

		return nil
	})
}

func (s *ShipmentService) Delete(ctx context.Context, id int64) error {
	if id <= 0 {
		return fmt.Errorf("%w: shipment id must be positive", ErrInvalidArgument)
	}

	return s.txManager.WithinTransaction(ctx, func(txCtx context.Context) error {
		shipment, err := s.shipmentRepository.GetByIDForUpdate(txCtx, id)
		if err != nil {
			return s.getError(err, "shipment_id", id)
		}

		if err := s.shipmentRepository.Delete(txCtx, id); err != nil {
			s.log.WithError(err).WithField("shipment_id", id).Error("failed to delete shipment")
			return fmt.Errorf("delete shipment: %w", err)
		}

		if err := s.createOutboxEvent(txCtx, shipment, events.ShipmentDeleted); err != nil {
			s.log.WithError(err).WithField("shipment_id", id).Error("failed to create shipment outbox event")
			return err
		}

		s.log.WithField("shipment_id", id).Info("shipment deleted")
		return nil
	})
}

func (s *ShipmentService) getError(err error, field string, value int64) error {
	if errors.Is(err, pgx.ErrNoRows) {
		return ErrShipmentNotFound
	}

	s.log.WithError(err).WithField(field, value).Error("failed to get shipment")
	return fmt.Errorf("get shipment: %w", err)
}

func (s *ShipmentService) createOutboxEvent(ctx context.Context, shipment *model.Shipment, eventType events.EventType) error {
	payload, err := json.Marshal(events.ShipmentPayload{
		ID:          shipment.ID,
		OrderID:     shipment.OrderID,
		CustomerID:  shipment.CustomerID,
		Status:      string(shipment.Status),
		ShippedAt:   shipment.ShippedAt,
		DeliveredAt: shipment.DeliveredAt,
	})
	if err != nil {
		return fmt.Errorf("marshal shipment event payload: %w", err)
	}

	if err := s.outboxRepository.Create(ctx, &model.OutboxEvent{
		OrderID:    shipment.OrderID,
		CustomerID: shipment.CustomerID,
		EventType:  string(eventType),
		Payload:    payload,
	}); err != nil {
		return fmt.Errorf("create shipment outbox event: %w", err)
	}

	return nil
}

func shipmentResponse(shipment *model.Shipment) *schema.ShipmentResponse {
	return &schema.ShipmentResponse{
		ID:          shipment.ID,
		OrderID:     shipment.OrderID,
		CustomerID:  shipment.CustomerID,
		Status:      shipment.Status,
		CreatedAt:   shipment.CreatedAt,
		UpdatedAt:   shipment.UpdatedAt,
		ShippedAt:   shipment.ShippedAt,
		DeliveredAt: shipment.DeliveredAt,
	}
}

func canTransition(from, to model.ShipmentStatus) bool {
	switch from {
	case model.ShipmentStatusPending:
		return to == model.ShipmentStatusShipping || to == model.ShipmentStatusFailed || to == model.ShipmentStatusCancelled
	case model.ShipmentStatusShipping:
		return to == model.ShipmentStatusDelivered || to == model.ShipmentStatusFailed
	default:
		return false
	}
}

func isUniqueViolation(err error) bool {
	var pgErr *pgconn.PgError
	return errors.As(err, &pgErr) && pgErr.Code == "23505"
}
