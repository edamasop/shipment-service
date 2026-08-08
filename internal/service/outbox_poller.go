package service

import (
	"context"
	"strconv"
	"sync"
	"time"

	"github.com/edamasop/messaging"
	"github.com/sirupsen/logrus"

	"shipment-service/internal/repository"
)

const (
	defaultOutboxBatchSize  = 50
	defaultOutboxPollPeriod = time.Second
	defaultOutboxLease      = 30 * time.Second
	defaultPublishTimeout   = 10 * time.Second
)

// OutboxPoller publishes committed outbox records with at-least-once delivery.
// ClaimUnpublished uses a lease, allowing multiple service instances to run
// without concurrently publishing the same record.
type OutboxPoller struct {
	repo           repository.Outbox
	producer       messaging.Producer
	log            *logrus.Entry
	pollPeriod     time.Duration
	lease          time.Duration
	publishTimeout time.Duration
	batchSize      int

	wg sync.WaitGroup
}

func NewOutboxPoller(repo repository.Outbox, producer messaging.Producer, log *logrus.Entry) *OutboxPoller {
	return &OutboxPoller{
		repo:           repo,
		producer:       producer,
		log:            log.WithField("service", "shipment_outbox_poller"),
		pollPeriod:     defaultOutboxPollPeriod,
		lease:          defaultOutboxLease,
		publishTimeout: defaultPublishTimeout,
		batchSize:      defaultOutboxBatchSize,
	}
}

func (p *OutboxPoller) Start(ctx context.Context) {
	p.wg.Add(1)
	go func() {
		defer p.wg.Done()
		p.run(ctx)
	}()
}

func (p *OutboxPoller) Wait() {
	p.wg.Wait()
}

func (p *OutboxPoller) run(ctx context.Context) {
	ticker := time.NewTicker(p.pollPeriod)
	defer ticker.Stop()

	p.log.Info("outbox poller started")
	for {
		if err := p.publishBatch(ctx); err != nil && ctx.Err() == nil {
			p.log.WithError(err).Warn("outbox poll failed")
		}

		select {
		case <-ctx.Done():
			p.log.Info("outbox poller stopped")
			return
		case <-ticker.C:
		}
	}
}

func (p *OutboxPoller) publishBatch(ctx context.Context) error {
	events, err := p.repo.ClaimUnpublished(ctx, p.batchSize, p.lease)
	if err != nil {
		return err
	}

	for _, event := range events {
		publishCtx, cancel := context.WithTimeout(context.Background(), p.publishTimeout)
		err := p.producer.ProduceKey(publishCtx, strconv.FormatInt(event.OrderID, 10), event.EventType, event)
		cancel()
		if err != nil {
			p.log.WithError(err).WithFields(logrus.Fields{
				"event_id": event.ID,
				"order_id": event.OrderID,
				"attempt":  event.Attempts,
			}).Warn("failed to publish outbox event; it will be retried after its lease expires")
			continue
		}

		if err := p.repo.MarkPublished(ctx, event.ID); err != nil {
			p.log.WithError(err).WithField("event_id", event.ID).Error("event was published but could not be marked as published")
			continue
		}

		p.log.WithFields(logrus.Fields{
			"event_id": event.ID,
			"order_id": event.OrderID,
			"type":     event.EventType,
		}).Debug("outbox event published")
	}

	return nil
}
