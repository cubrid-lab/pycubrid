.PHONY: help install tooling-check lint format typecheck security docs-reason-check check check-all test mutation integration integration-local integration-tls docker-up docker-down changelog clean clean-all doctor release-check

PYTHON = python3
PYTEST = python3 -m pytest
RUFF = $(PYTHON) -m ruff
MYPY = $(PYTHON) -m mypy
BANDIT = bandit
SRC = pycubrid
TESTS = tests
LINT_PATHS = pycubrid tests scripts demos examples

help: ## Show this help message
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | sort | \
		awk 'BEGIN {FS = ":.*?## "}; {printf "\033[36m%-20s\033[0m %s\n", $$1, $$2}'

install: ## Install in development mode with all dependencies
	pip install -e ".[dev]"
	pre-commit install

tooling-check: ## Verify declared, hook, installed-tool, and quality-scope consistency
	$(PYTHON) scripts/check_quality_tools.py

lint: tooling-check ## Run linter and format checks for maintained Python files
	$(RUFF) check $(LINT_PATHS)
	$(RUFF) format --check $(LINT_PATHS)

format: tooling-check ## Auto-fix lint issues and format maintained Python files
	$(RUFF) check --fix $(LINT_PATHS)
	$(RUFF) format $(LINT_PATHS)

typecheck: tooling-check ## Run mypy type checking
	$(MYPY) $(SRC)/ --config-file=pyproject.toml

security: ## Run security scans (bandit)
	$(BANDIT) -r $(SRC)/ -c pyproject.toml

check: lint typecheck ## Run lint + typecheck

docs-reason-check: ## Verify docs exemption examples and real event JSON regressions
	$(PYTHON) -m doctest scripts/check_docs_reason.py
	$(PYTHON) -m unittest discover -s tests -p test_docs_reason.py

check-all: check security docs-reason-check ## Run lint + typecheck + security + docs-reason checks

test: ## Run offline tests with coverage (no DB required)
	$(PYTEST) $(TESTS)/ -v \
		-m "not integration" \
		--cov=$(SRC) \
		--cov-report=term-missing \
		--cov-fail-under=95

mutation: ## Run mutation testing on the driver core (pip install -e ".[dev,mutation]")
	mutmut run
	mutmut results

# Docker integration endpoint. The compose service publishes the broker on
# CUBRID_TEST_PORT (default 33000); `make integration CUBRID_TEST_PORT=33522`
# avoids a port another container already uses (an exported CUBRID_TEST_PORT is
# honored too). Host, database, user and password are pinned to the compose
# service so a stray CUBRID_TEST_* export cannot redirect the run elsewhere.
CUBRID_TEST_PORT ?= 33000
INTEGRATION_RESULTS ?= integration-results.xml
INTEGRATION_ENV = CUBRID_TEST_URL="cubrid://dba@localhost:$(CUBRID_TEST_PORT)/testdb" \
	CUBRID_TEST_HOST=localhost CUBRID_TEST_PORT=$(CUBRID_TEST_PORT) \
	CUBRID_TEST_DB=testdb CUBRID_TEST_USER=dba CUBRID_TEST_PASSWORD=

integration: docker-up ## Run integration tests against a Docker CUBRID (fails if it never becomes ready or every test skips)
	@trap '$(MAKE) docker-down; exit 130' INT TERM; \
	status=0; \
	$(INTEGRATION_ENV) $(PYTHON) scripts/wait_for_cubrid.py 36 5 && \
	$(INTEGRATION_ENV) CUBRID_TEST_DOCKER_CONTAINER="$$(docker compose ps -q cubrid)" \
		$(PYTEST) $(TESTS)/ -m "integration and not tls" -v --junitxml=$(INTEGRATION_RESULTS) && \
	$(PYTHON) scripts/check_integration_lanes.py --results $(INTEGRATION_RESULTS) || status=$$?; \
	$(MAKE) docker-down || { [ $$status -ne 0 ] || status=1; }; \
	exit $$status

integration-local: ## Run integration tests against an already-running CUBRID (set CUBRID_TEST_URL or CUBRID_TEST_HOST/PORT; no Docker)
	@if [ -z "$$CUBRID_TEST_URL" ] && [ -z "$$CUBRID_TEST_HOST" ]; then \
		echo "ERROR: set CUBRID_TEST_URL (e.g. cubrid://dba@127.0.0.1:33000/testdb) or CUBRID_TEST_HOST/CUBRID_TEST_PORT for a running CUBRID"; \
		exit 1; \
	fi
	$(PYTEST) $(TESTS)/ -m integration -v

integration-tls: docker-up ## Run async TLS integration tests (requires SSL=ON broker; see CONTRIBUTING.md)
	@echo "NOTE: requires CUBRID_TLS_TEST_HOST/PORT/CA/DB/USER env vars and a broker with SSL=ON."
	@echo "      For an automated equivalent including SSL=ON flip + cert extraction,"
	@echo "      see the 'integration-tls' job in .github/workflows/integration-full.yml."
	@trap '$(MAKE) docker-down; exit 130' INT TERM; \
	status=0; \
	$(INTEGRATION_ENV) $(PYTHON) scripts/wait_for_cubrid.py 36 5 && \
	$(INTEGRATION_ENV) $(PYTEST) $(TESTS)/test_aio_ssl_integration.py -v --junitxml=$(INTEGRATION_RESULTS) && \
	$(PYTHON) scripts/check_integration_lanes.py --results $(INTEGRATION_RESULTS) || status=$$?; \
	$(MAKE) docker-down || { [ $$status -ne 0 ] || status=1; }; \
	exit $$status

docker-up: ## Start CUBRID Docker container (published on CUBRID_TEST_PORT, default 33000)
	CUBRID_TEST_PORT=$(CUBRID_TEST_PORT) docker compose up -d
	@echo "CUBRID container starting..."

docker-down: ## Stop and remove CUBRID Docker container
	docker compose down -v

changelog: ## Generate changelog with git-cliff
	git-cliff --output CHANGELOG.md

clean: ## Remove build artifacts and caches
	rm -rf build/ dist/ *.egg-info .pytest_cache/ .coverage .ruff_cache/ __pycache__/
	find . -type d -name __pycache__ -exec rm -rf {} + 2>/dev/null || true
	find . -type f -name '*.pyc' -delete 2>/dev/null || true

clean-all: clean ## Remove all artifacts including .mypy_cache and .tox
	rm -rf .mypy_cache/ .tox/ htmlcov/

doctor: ## Check development environment
	@echo "Checking development environment..."
	@python3 --version || echo "ERROR: python3 not found"
	@$(RUFF) --version || echo "ERROR: ruff not found"
	@$(MYPY) --version || echo "ERROR: mypy not found"
	@$(BANDIT) --version || echo "ERROR: bandit not found"
	@pre-commit --version || echo "ERROR: pre-commit not found"
	@echo "All checks passed!"

release-check: ## Read-only release consistency gate (run by prepare-release.yml and release.yml). Usage: make release-check VERSION=x.y.z
	@if [ -z "$(VERSION)" ]; then echo "Usage: make release-check VERSION=x.y.z"; exit 1; fi
	@ACTUAL=$$($(PYTHON) -c 'import ast, pathlib; tree = ast.parse(pathlib.Path("$(SRC)/__init__.py").read_text()); print(next(n.value.value for n in ast.walk(tree) if isinstance(n, ast.Assign) for t in n.targets if isinstance(t, ast.Name) and t.id == "__version__"))') || exit 1; \
		if [ "$$ACTUAL" != "$(VERSION)" ]; then echo "ERROR: $(SRC).__version__ is $$ACTUAL, expected $(VERSION)"; exit 1; fi; \
		echo "OK: $(SRC).__version__ == $(VERSION)"
	$(PYTHON) scripts/lint_changelog.py
	$(PYTHON) scripts/extract_release_notes.py v$(VERSION)
	rm -f RELEASE_NOTES.md
	rm -rf dist
	$(PYTHON) -m build
	$(PYTHON) -m twine check dist/*
