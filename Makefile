.PHONY: help install install-dev format lint type-check test test-unit test-integration coverage clean build run

# Default target
help: ## Display this help message
	@echo "Available targets:"
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | awk 'BEGIN {FS = ":.*?## "}; {printf "  \033[36m%-20s\033[0m %s\n", $$1, $$2}'

# Setup & Installation
install: ## Install project dependencies
	uv sync

install-dev: ## Install project with dev dependencies
	uv sync --all-extras --group dev

pre-commit-install: ## Install pre-commit hooks
	uv run pre-commit install

pre-commit-run: ## Run all pre-commit hooks manually
	uv run pre-commit run --all-files

# Code Quality
format: ## Format code with ruff
	uv run ruff format .

lint: ## Lint code with ruff
	uv run ruff check .

lint-fix: ## Lint and fix code with ruff
	uv run ruff check --fix .

type-check: ## Type check with pyright
	uv run pyright src/

# Testing
test: ## Run all tests
	uv run pytest

test-unit: ## Run unit tests only
	uv run pytest tests/unit/

test-integration: ## Run integration tests only
	uv run pytest tests/integration/

coverage: ## Run tests with coverage report
	uv run pytest --cov=src --cov-report=term-missing --cov-report=html

# Development
clean: ## Remove build artifacts and cache files
	rm -rf build/ dist/ *.egg-info .pytest_cache/ .coverage htmlcov/ .ruff_cache/
	find . -type d -name __pycache__ -exec rm -rf {} +
	find . -type f -name "*.pyc" -delete

build: ## Build the package
	uv build

# Convenience targets
check: lint type-check test ## Run all checks (lint, type-check, test)

all: install-dev format check ## Install, format, and run all checks
