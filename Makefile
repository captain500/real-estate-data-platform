.PHONY: install lint format test check infra down

## ── Dependencies ────────────────────────────────────────────────
install:                  ## Install all dependencies (dev + test)
	poetry install --no-interaction

## ── Code quality ────────────────────────────────────────────────
lint:                     ## Run ruff linter
	poetry run ruff check src/ tests/

format:                   ## Auto-format code with ruff
	poetry run ruff format src/ tests/

format-check:             ## Check formatting without modifying files
	poetry run ruff format --check src/ tests/

## ── Tests ───────────────────────────────────────────────────────
test:                     ## Run all tests
	poetry run pytest tests/ -v --tb=short

## ── CI (same checks as GitHub Actions) ──────────────────────────
check: lint format-check test   ## Run full CI pipeline locally

## ── Infrastructure ──────────────────────────────────────────────
infra:                    ## Start MinIO + Postgres (detached)
	docker-compose up -d

down:                     ## Stop infrastructure containers
	docker-compose down

## ── Help ────────────────────────────────────────────────────────
help:                     ## Show available targets
	@grep -E '^[a-zA-Z_-]+:.*##' $(MAKEFILE_LIST) | awk 'BEGIN {FS = ":.*## "}; {printf "  \033[36m%-16s\033[0m %s\n", $$1, $$2}'

.DEFAULT_GOAL := help
