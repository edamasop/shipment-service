package service

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/edamasop/events"
	"github.com/sirupsen/logrus"
)

const paymentEventTimeout = 15 * time.Second

// PaymentConsumer starts shipment delivery after a successful payment.
type PaymentConsumer struct {
	shipments Shipment
	log       *logrus.Entry
}

func NewPaymentConsumer(shipments Shipment, log *logrus.Entry) *PaymentConsumer {
	return &PaymentConsumer{shipments: shipments, log: log.WithField("consumer", "payment")}
}

// HandlePaymentSuccessful returns whether Kafka should retry the message.
func (c *PaymentConsumer) HandlePaymentSuccessful(data json.RawMessage) (bool, error) {
	var payment events.PaymentPayload
	if err := json.Unmarshal(data, &payment); err != nil {
		c.log.WithError(err).Warn("invalid payment.successful payload")
		return false, fmt.Errorf("decode payment.successful payload: %w", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), paymentEventTimeout)
	defer cancel()
	if err := c.shipments.StartDelivery(ctx, payment.OrderID, payment.CustomerID); err != nil {
		if errors.Is(err, ErrInvalidArgument) || errors.Is(err, ErrInvalidStatusTransition) {
			c.log.WithError(err).WithField("order_id", payment.OrderID).Error("payment event cannot start delivery")
			return false, err
		}

		c.log.WithError(err).WithField("order_id", payment.OrderID).Warn("could not start delivery")
		return true, err
	}

	c.log.WithField("order_id", payment.OrderID).Info("payment accepted; delivery started")
	return false, nil
}
