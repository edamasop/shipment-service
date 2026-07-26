package app

import (
	"context"
	//delivery "payment-service/internal/delivery/http"
	"shipment-service/internal/config"
	//"shipment-service/internal/repository"

	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

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

	// 1. Initialize PostgreSQL
	dbPool, err := pgxpool.New(ctx, cfg.PostgresDSN)
	if err != nil {
		log.Fatalf("Unable to connect to PostgreSQL: %v", err)
	}
	defer dbPool.Close()

	if err := dbPool.Ping(ctx); err != nil {
		log.Fatalf("PostgreSQL ping failed: %v", err)
	}
	log.Info("Successfully connected to PostgreSQL")

	consumerCfg := messaging.ConsumerConfig{

		BootstrapServers: strings.Join(cfg.KafkaBrokers, ","),
		GroupID:          cfg.KafkaGroupID,
		Topics:           []string{cfg.KafkaConsumerTopic},
		EnableLogging:    false,
		LogOutput:        os.Stdout,
		ManualCommit:     true,
		MaxRetries:       3,
		ErrorBackoff:     2 * time.Second,
		ReadTimeout:      5 * time.Second,

		EnableDLQ: true,
		DLQTopic:  cfg.KafkaConsumerTopic + ".dlq",

		HealthCheckInterval:       15 * time.Second,
		UnhealthyFailureThreshold: 5,
		HealthFailureWindow:       1 * time.Minute,
	}

	consumer, err := messaging.NewKafkaConsumer(consumerCfg)
	if err != nil {
		log.Fatalf("Failed to initialize Kafka consumer: %v", err)
	}

	go func() {
		log.Info("Starting message consumption engine...")
		consumer.Start(ctx)
	}()

	defer func() {
		log.Info("Cleaning up resources and shutting down messaging nodes...")
		if err := consumer.Close(); err != nil {
			log.Errorf("Error while stopping consumer gracefully: %v", err)
		}
	}()

	producerCfg := messaging.ProducerConfig{
		BootstrapServers:       strings.Join(cfg.KafkaBrokers, ","),
		Topic:                  cfg.KafkaProducerTopic,
		EnableLogging:          false,
		LogOutput:              os.Stdout,
		ErrOutput:              os.Stderr,
		MaxAttempts:            3,
		AllowAutoTopicCreation: true,
	}

	producer, err := messaging.NewKafkaProducer(producerCfg)
	if err != nil {
		log.Fatalf("Failed to initialize Kafka producer: %v", err)
	}
	defer func() {
		if err := producer.Close(); err != nil {
			log.Errorf("Error while stopping producer gracefully: %v", err)
		}
	}()

	producer.Health()
	log.Info("Successfully connected to Kafka producer")

	//repositories := repository.NewRepositories(dbPool)
	//services := service.NewServices(cfg, repositories, log)
	//handlers := delivery.NewHandlers(services)
	//outboxPoller := service.NewOutboxPoller(repositories.Outbox, producer, logrus.NewEntry(log))
	//outboxPoller.Start(ctx)

	//router := delivery.NewRouter(handlers)
	//svr, err := server.NewServer(cfg, router)
	//if err != nil {
	//	log.Fatalf("Unable to init svr: %v", err)
	//}
	//
	//go func() {
	//	log.Info("Successfully started server on port: ", cfg.Port)
	//	err = svr.Run()
	//	if err != nil && err != http.ErrServerClosed {
	//		log.Fatalf("Unable to start svr: %v", err)
	//	}
	//}()
	//
	//<-ctx.Done()
	//log.Info("Shutting down signal received...")
	//defer svr.Shutdown(ctx)
}
