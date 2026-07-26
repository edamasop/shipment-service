# Load environment variables from .env file
include .env
export

# Directory where your migration files are stored
MIGRATIONS_DIR=./migrations
# Database driver
DB_DRIVER=postgres

# Create a new migration file. Usage: make migrate-new NAME=create_users_table
migrate-new:
	goose -dir $(MIGRATIONS_DIR) create $(NAME) sql

# Apply all available migrations
migrate-up:
	goose -dir $(MIGRATIONS_DIR) $(DB_DRIVER) "$(POSTGRES_DSN)" up

# Roll back the last migration
migrate-down:
	goose -dir $(MIGRATIONS_DIR) $(DB_DRIVER) "$(POSTGRES_DSN)" down

# Check the status of migrations
migrate-status:
	goose -dir $(MIGRATIONS_DIR) $(DB_DRIVER) "$(POSTGRES_DSN)" status

# Roll back all migrations
migrate-reset:
	goose -dir $(MIGRATIONS_DIR) $(DB_DRIVER) "$(POSTGRES_DSN)" reset


.PHONY: migrate-new migrate-up migrate-down migrate-status migrate-reset run build