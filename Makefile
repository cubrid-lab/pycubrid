.PHONY: help install lint format test integration docker-up docker-down clean

PYTEST = python3 -m pytest
RUFF = ruff

help: ## Show this help message
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | sort | \
		awk 'BEGIN {FS = ":.*?## "}; {printf "\033[36m%-20s\033[0m %s\n", $$1, $$2}'

install: ## Install in development mode with all dependencies
	pip install -e ".[dev]"

lint: ## Run linter and format checks
	$(RUFF) check pycubrid/ tests/
	$(RUFF) format --check pycubrid/ tests/

format: ## Auto-fix lint issues and format code
	$(RUFF) check --fix pycubrid/ tests/
	$(RUFF) format pycubrid/ tests/

test: ## Run offline tests with coverage (no DB required)
	$(PYTEST) tests/ -v \
		--ignore=tests/test_integration.py \
		--cov=pycubrid \
		--cov-report=term-missing \
		--cov-fail-under=95

integration: docker-up ## Run integration tests against CUBRID Docker
	@echo "Waiting for CUBRID to be ready..."
	@sleep 10
	CUBRID_TEST_URL="cubrid://dba@localhost:33000/testdb" \
		$(PYTEST) tests/test_integration.py -v
	$(MAKE) docker-down

docker-up: ## Start CUBRID Docker container
	docker compose up -d
	@echo "CUBRID container starting..."

docker-down: ## Stop and remove CUBRID Docker container
	docker compose down -v

clean: ## Remove build artifacts and caches
	rm -rf build/ dist/ *.egg-info .pytest_cache/ .coverage .ruff_cache/ __pycache__/
	find . -type d -name __pycache__ -exec rm -rf {} + 2>/dev/null || true
	find . -type f -name '*.pyc' -delete 2>/dev/null || true
