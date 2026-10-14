COMPOSE ?= docker compose

.PHONY: up down logs test lint format migrate ps

up:  ## Build and start the whole stack
	$(COMPOSE) up -d --build

down:  ## Stop the stack and remove volumes
	$(COMPOSE) down -v

logs:
	$(COMPOSE) logs -f api outbox-relay consumer

ps:
	$(COMPOSE) ps

migrate:
	$(COMPOSE) run --rm migrations

test:  ## Run the test suite inside Docker (needs postgres and rabbitmq)
	$(COMPOSE) --profile test build tests
	$(COMPOSE) up -d postgres rabbitmq
	$(COMPOSE) run --rm tests

lint:
	$(COMPOSE) --profile test run --rm --no-deps tests sh -c "ruff check --no-cache . && ruff format --no-cache --check ."
