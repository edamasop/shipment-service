package config

import (
	"os"
	"strings"

	"github.com/joho/godotenv"
	log "github.com/sirupsen/logrus"
)

// Config holds all environment-based configuration for the application
type Config struct {
	Port           string
	Loglevel       string // info, debug, warn, error, fatal, trace
	Environment    string
	AllowedOrigins []string

	//RedisAddr string
	//RedisPass string

	KafkaBrokers       []string // Parsed from comma-separated string
	KafkaProducerTopic string
	KafkaConsumerTopic string
	KafkaGroupID       string

	// PostgreSQL Configuration
	PostgresDSN string // Full connection string: postgres://user:pass@host:5432/dbname
}

// getEnv checks if an environment variable exists, otherwise returns a default value
func getEnv(key, defaultValue string) string {
	if value := os.Getenv(key); value != "" {
		return value
	}
	return defaultValue
}

// getEnvAsSlice reads an environment variable and splits it into a slice based on a separator
func getEnvAsSlice(key string, defaultValue []string, sep string) []string {
	valStr := getEnv(key, "")
	if valStr == "" {
		return defaultValue
	}

	// Split the string by the separator (e.g., ",")
	items := strings.Split(valStr, sep)

	// Clean up whitespace for each element
	for i := range items {
		items[i] = strings.TrimSpace(items[i])
	}

	return items
}

// Load initializes the configuration by loading .env and mapping environment variables
func Load() *Config {
	// Attempt to load .env file; if it fails, assume we are using system env variables
	if err := godotenv.Load(); err != nil {
		log.Debug("No .env file found, running from OS environment")
	} else {
		log.Debug(".env file successfully loaded")
	}

	return &Config{
		// Basic Server Settings
		Environment:    getEnv("ENVIRONMENT", "dev.default"),
		Port:           getEnv("PORT", "8080"),
		Loglevel:       getEnv("LOGLEVEL", "info"),
		AllowedOrigins: getEnvAsSlice("ALLOWED_ORIGINS", []string{"*"}, ","),

		// Redis Configuration
		//RedisAddr: getEnv("REDIS_ADDR", "localhost:6379"),
		//RedisPass: getEnv("REDIS_PASS", ""),

		// Kafka Configuration
		KafkaBrokers:       getEnvAsSlice("KAFKA_BROKERS", []string{"localhost:9092"}, ","),
		KafkaProducerTopic: getEnv("KAFKA_PRODUCER_TOPIC", "file_service_topic"),
		KafkaConsumerTopic: getEnv("KAFKA_CONSUMER_TOPIC", "file_service_topic"),
		KafkaGroupID:       getEnv("KAFKA_GROUP_ID", "file_service_group"),

		// PostgreSQL Configuration
		// Format: host=localhost user=user password=pass dbname=file_db port=5432 sslmode=disable
		PostgresDSN: getEnv("POSTGRES_DSN", "host=localhost user=postgres password=postgres dbname=file_db port=5432 sslmode=disable"),
	}
}
