package app

import (
	"context"
	"errors"
	"net/http"
	"os"
	"os/signal"
	"shipment-service/internal/config"
	delivery "shipment-service/internal/delivery/http"
	"shipment-service/internal/repository"
	"shipment-service/internal/server"
	"shipment-service/internal/service"
	"strings"
	"syscall"
	"time"

	"github.com/edamasop/events"
	"github.com/edamasop/messaging"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/sirupsen/logrus"
)

func Run() {
	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer cancel()

	cfg := config.Load()
	log := logrus.New()
	log.SetOutput(os.Stdout)
	level, err := logrus.ParseLevel(cfg.Loglevel)
	if err != nil {
		log.WithError(err).Warn("invalid LOGLEVEL; using info")
		level = logrus.InfoLevel
	}
	log.SetLevel(level)
	entry := logrus.NewEntry(log)

	dbPool, err := pgxpool.New(ctx, cfg.PostgresDSN)
	if err != nil {
		log.Fatalf("Unable to connect to PostgreSQL: %v", err)
	}
	defer dbPool.Close()

	if err := dbPool.Ping(ctx); err != nil {
		log.Fatalf("PostgreSQL ping failed: %v", err)
	}
	log.Info("connected to PostgreSQL")

	producerCfg := messaging.ProducerConfig{
		BootstrapServers:       strings.Join(cfg.KafkaBrokers, ","),
		Topic:                  cfg.KafkaProducerTopic,
		EnableLogging:          true,
		LogOutput:              os.Stdout,
		ErrOutput:              os.Stderr,
		MaxAttempts:            3,
		AllowAutoTopicCreation: true,
	}

	producer, err := messaging.NewKafkaProducer(producerCfg)
	if err != nil {
		log.Fatalf("Failed to initialize Kafka producer: %v", err)
	}
	log.Info("connected to Kafka producer")

	repos := repository.NewRepository(dbPool)
	services := service.NewServices(repos, entry)
	paymentConsumer := service.NewPaymentConsumer(services.Shipment, entry)
	consumer, err := messaging.NewKafkaConsumer(messaging.ConsumerConfig{
		BootstrapServers: strings.Join(cfg.KafkaBrokers, ","),
		GroupID:          cfg.KafkaGroupID,
		Topics:           []string{cfg.KafkaConsumerTopic},
		EnableLogging:    true,
		LogOutput:        os.Stdout,
		ErrOutput:        os.Stderr,
		ManualCommit:     true,
		MaxRetries:       3,
		ErrorBackoff:     2 * time.Second,
		ReadTimeout:      5 * time.Second,
		EnableDLQ:        true,
		DLQTopic:         cfg.KafkaConsumerTopic + ".dlq",
	})
	
	if err != nil {
		log.Fatalf("create Kafka consumer: %v", err)
	}

	consumer.RegisterHandler(string(events.PaymentStatusSuccessful), paymentConsumer.HandlePaymentSuccessful)
	go consumer.Start(ctx)
	log.WithField("topic", cfg.KafkaConsumerTopic).Info("Kafka payment consumer started")

	handlers := delivery.NewHandlers(services, entry)
	svr, err := server.NewServer(cfg, delivery.NewRouter(handlers))
	if err != nil {
		log.Fatalf("create HTTP server: %v", err)
	}

	poller := service.NewOutboxPoller(repos.Outbox, producer, entry)
	poller.Start(ctx)

	serverErr := make(chan error, 1)
	go func() {
		log.WithField("port", cfg.Port).Info("HTTP server started")
		serverErr <- svr.Run()
	}()

	select {
	case <-ctx.Done():
		log.Info("shutdown signal received")
	case err := <-serverErr:
		if err != nil && !errors.Is(err, http.ErrServerClosed) {
			log.WithError(err).Error("HTTP server stopped unexpectedly")
		}
	}

	cancel()
	shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer shutdownCancel()
	if err := svr.Shutdown(shutdownCtx); err != nil {
		log.WithError(err).Error("graceful HTTP shutdown failed")
	}
	poller.Wait()
	if err := consumer.Close(); err != nil {
		log.WithError(err).Error("close Kafka consumer")
	}
	if err := producer.Close(); err != nil {
		log.WithError(err).Error("close Kafka producer")
	}
}
