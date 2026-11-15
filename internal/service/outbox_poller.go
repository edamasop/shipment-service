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

	defaultMaxAttempts   = 10
	defaultRetention     = 24 * time.Hour
	defaultCleanupPeriod = time.Hour
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
	maxAttempts    int
	retention      time.Duration
	cleanupPeriod  time.Duration

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
		maxAttempts:    defaultMaxAttempts,
		retention:      defaultRetention,
		cleanupPeriod:  defaultCleanupPeriod,
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

	lastCleanup := time.Now()

	p.log.Info("outbox poller started")
	for {
		if err := p.publishBatch(ctx); err != nil && ctx.Err() == nil {
			p.log.WithError(err).Warn("outbox poll failed")
		}

		// Retention runs on the never-ending poll loop instead of its own
		// goroutine: one timer, one owner, nothing to synchronize.
		if time.Since(lastCleanup) >= p.cleanupPeriod {
			lastCleanup = time.Now()
			p.cleanup(ctx)
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
		// Publish the stored JSON payload, not the outbox row: the row would be
		// encoded with Go field names (and the real payload base64-encoded).
		// Keying by order id keeps all events of one order on one partition.
		err := p.producer.ProduceKey(
			publishCtx,
			strconv.FormatInt(event.OrderID, 10),
			event.EventType,
			event.Payload,
		)
		cancel()
		if err != nil {
			if event.Attempts >= p.maxAttempts {
				if parkErr := p.repo.Park(ctx, event.ID, err.Error()); parkErr != nil {
					p.log.WithError(parkErr).WithField("event_id", event.ID).Error("failed to park undeliverable outbox event")
				}

				p.log.WithError(err).WithFields(logrus.Fields{
					"event_id": event.ID,
					"order_id": event.OrderID,
					"attempts": event.Attempts,
				}).Error("outbox event exhausted its attempts and was parked")

				continue
			}

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

// cleanup enforces the retention policy on published rows.
func (p *OutboxPoller) cleanup(ctx context.Context) {
	cutoff := time.Now().Add(-p.retention)

	deleted, err := p.repo.DeletePublishedBefore(ctx, cutoff)
	if err != nil {
		if ctx.Err() == nil {
			p.log.WithError(err).Warn("outbox cleanup failed")
		}
		return
	}

	if deleted > 0 {
		p.log.WithFields(logrus.Fields{
			"deleted":   deleted,
			"cutoff":    cutoff.Format(time.RFC3339),
			"retention": p.retention.String(),
		}).Info("purged published outbox rows")
	}
}

// Snapshot exposes the backlog for the metrics endpoint.
func (p *OutboxPoller) Snapshot(ctx context.Context) (pending, parked int64, err error) {
	return p.repo.CountPending(ctx)
}
