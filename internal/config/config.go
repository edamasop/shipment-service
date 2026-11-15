package config

import (
	"fmt"
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
	// KafkaConsumerTopics lists every topic this service subscribes to.
	KafkaConsumerTopics []string
	KafkaGroupID        string

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

// consumerTopics supports both the plural key (a list of topics) and the
// legacy singular one, so existing .env files keep working.
func consumerTopics() []string {
	if topics := getEnvAsSlice("KAFKA_CONSUMER_TOPICS", nil, ","); len(topics) > 0 {
		return topics
	}

	return getEnvAsSlice("KAFKA_CONSUMER_TOPIC", []string{"payment-service"}, ",")
}

// Validate fails fast on a configuration that cannot possibly work, instead of
// failing on the first request or the first Kafka poll.
func (c *Config) Validate() error {
	var problems []string

	if strings.TrimSpace(c.PostgresDSN) == "" {
		problems = append(problems, "POSTGRES_DSN is required")
	}
	if len(c.KafkaBrokers) == 0 {
		problems = append(problems, "KAFKA_BROKERS is required")
	}
	if c.KafkaProducerTopic == "" {
		problems = append(problems, "KAFKA_PRODUCER_TOPIC is required")
	}
	if len(c.KafkaConsumerTopics) == 0 {
		problems = append(problems, "KAFKA_CONSUMER_TOPICS is required")
	}
	if c.KafkaGroupID == "" {
		problems = append(problems, "KAFKA_GROUP_ID is required")
	}
	if c.Port == "" {
		problems = append(problems, "PORT is required")
	}

	if len(problems) > 0 {
		return fmt.Errorf("invalid configuration: %s", strings.Join(problems, "; "))
	}

	return nil
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
		KafkaBrokers:        getEnvAsSlice("KAFKA_BROKERS", []string{"localhost:9092"}, ","),
		KafkaProducerTopic:  getEnv("KAFKA_PRODUCER_TOPIC", "shipment-service"),
		KafkaConsumerTopics: consumerTopics(),
		KafkaGroupID:        getEnv("KAFKA_GROUP_ID", "shipment-service"),

		// PostgreSQL Configuration
		// Format: host=localhost user=user password=pass dbname=file_db port=5432 sslmode=disable
		PostgresDSN: getEnv("POSTGRES_DSN", "host=localhost user=postgres password=postgres dbname=file_db port=5432 sslmode=disable"),
	}
}
