.PHONY: help install doctor tooling-check lint format-check format typecheck security docs-reason-check check check-all verify test mutation integration integration-local integration-tls docker-up docker-down docker-fix-db-host changelog clean clean-all release-check agent-eval-check

# Every tool runs through the same interpreter, and each can be overridden from the
# command line or the environment, e.g. `make check PYTHON=.venv/bin/python`.
PYTHON ?= python3
PIP ?= $(PYTHON) -m pip
PYTEST ?= $(PYTHON) -m pytest
RUFF ?= $(PYTHON) -m ruff
MYPY ?= $(PYTHON) -m mypy
BANDIT ?= $(PYTHON) -m bandit
PRE_COMMIT ?= $(PYTHON) -m pre_commit
SRC = pycubrid
TESTS = tests
LINT_PATHS = pycubrid tests scripts demos examples agent-evals

##@ Setup

help: ## Show this help message, grouped by purpose
	@awk 'BEGIN {FS = ":.*?## "} \
		/^##@ / {printf "\n\033[1m%s\033[0m\n", substr($$0, 5)} \
		/^[a-zA-Z_-]+:.*?## / {printf "  \033[36m%-20s\033[0m %s\n", $$1, $$2}' $(MAKEFILE_LIST)

install: ## Install in development mode with the dev dependency group (pip 25.1+)
	$(PIP) install -e . --group dev
	$(PRE_COMMIT) install

# Every tool is checked before the verdict, so one run lists everything missing.
doctor: ## Check the required local tooling; fails if any tool is missing or broken
	@echo "Checking development environment..."
	@status=0; \
	for tool in "$(PYTHON)" "$(PYTEST)" "$(RUFF)" "$(MYPY)" "$(BANDIT)" "$(PRE_COMMIT)"; do \
		$$tool --version || { echo "ERROR: '$$tool --version' failed" >&2; status=1; }; \
	done; \
	if [ $$status -ne 0 ]; then \
		echo "ERROR: required tooling is missing; run 'make install' in the project environment" >&2; \
		exit $$status; \
	fi; \
	echo "All checks passed!"

##@ Quality (static, no tests)

tooling-check: ## Verify declared, hook, installed-tool, and quality-scope consistency
	$(PYTHON) scripts/check_quality_tools.py

lint: tooling-check ## Run the Ruff linter on maintained Python files (never modifies files)
	$(RUFF) check $(LINT_PATHS)

format-check: tooling-check ## Check Ruff formatting of maintained Python files (never modifies files)
	$(RUFF) format --check $(LINT_PATHS)

format: tooling-check ## Auto-fix lint issues and format maintained Python files (the only mutating target)
	$(RUFF) check --fix $(LINT_PATHS)
	$(RUFF) format $(LINT_PATHS)

typecheck: tooling-check ## Run mypy type checking
	$(MYPY) $(SRC)/ tests/typing_self_contract.py --config-file=pyproject.toml

security: ## Run security scans (bandit)
	$(BANDIT) -r $(SRC)/ -c pyproject.toml

docs-reason-check: ## Verify docs exemption examples and real event JSON regressions
	$(PYTHON) -m doctest scripts/check_docs_reason.py
	$(PYTHON) -m unittest discover -s tests -p test_docs_reason.py

check: lint format-check typecheck security docs-reason-check ## Complete static quality gate: lint, format-check, typecheck, security, docs-reason-check

# Deprecated alias of `check`, kept for one minor release (#1007).
check-all: check ## Deprecated alias of check (removed after the next minor release)
	@echo "WARNING: 'make check-all' is deprecated and will be removed after the next minor release; use 'make check'." >&2

##@ Tests

verify: check test ## Run check, then the offline tests (no Docker, no database)

test: ## Run offline tests with coverage (no DB required)
	$(PYTEST) $(TESTS)/ -v \
		-m "not integration" \
		--cov=$(SRC) \
		--cov-report=term-missing \
		--cov-fail-under=95

agent-eval-check: ## Self-test the agent-evaluation grader and runner against their fixtures (#925)
	$(PYTHON) scripts/agent_eval.py self-test
	$(PYTEST) tests/test_agent_eval.py tests/test_agent_eval_run.py -q

mutation: ## Run mutation testing on the driver core (pip install -e . --group dev --group mutation; Python 3.12+)
	@$(PYTHON) -c 'import sys; sys.exit(0 if sys.version_info >= (3, 12) else "make mutation needs Python 3.12+: mutmut 3.8 fails at stats collection on 3.11 (#812)")'
	$(PYTHON) -m mutmut run
	$(PYTHON) -m mutmut results

##@ Integration (Docker)

# Docker integration endpoint. The compose service publishes the broker on
# CUBRID_TEST_PORT (default 33000); `make integration CUBRID_TEST_PORT=33522`
# avoids a port another container already uses (an exported CUBRID_TEST_PORT is
# honored too). scripts/run_integration.py pins host, database, user and password
# to the compose service, so a stray CUBRID_TEST_* export cannot redirect the run,
# and owns the Compose lifecycle, readiness, cleanup and exit status.
CUBRID_TEST_PORT ?= 33000
INTEGRATION_RESULTS ?= integration-results.xml
RUN_INTEGRATION = $(PYTHON) scripts/run_integration.py --python "$(PYTHON)" --pytest "$(PYTEST)" \
	--port "$(CUBRID_TEST_PORT)" --results "$(INTEGRATION_RESULTS)"

integration: ## Run integration tests against a Docker CUBRID (fails if it never becomes ready or every test skips)
	$(RUN_INTEGRATION) --lane normal

integration-local: ## Run integration tests against an already-running CUBRID (set CUBRID_TEST_URL or CUBRID_TEST_HOST/PORT; no Docker)
	@if [ -z "$$CUBRID_TEST_URL" ] && [ -z "$$CUBRID_TEST_HOST" ]; then \
		echo "ERROR: set CUBRID_TEST_URL (e.g. cubrid://dba@127.0.0.1:33000/testdb) or CUBRID_TEST_HOST/CUBRID_TEST_PORT for a running CUBRID"; \
		exit 1; \
	fi
	$(PYTEST) $(TESTS)/ -m integration -v

integration-tls: ## Run async TLS integration tests (requires SSL=ON broker; see CONTRIBUTING.md)
	$(RUN_INTEGRATION) --lane tls

docker-up: ## Start CUBRID Docker container (published on CUBRID_TEST_PORT, default 33000)
	CUBRID_TEST_PORT=$(CUBRID_TEST_PORT) docker compose up -d
	@echo "CUBRID container starting..."

docker-down: ## DESTRUCTIVE: stop and remove the CUBRID container and its data volumes (deletes the test data)
	docker compose down -v

# A volume created before docker-compose.yml set CUBRID_DB_HOST registers testdb
# for the creating container's ID, which a recreated container cannot resolve
# (-353, #1000). Rewrite only that host field to localhost; the data is kept.
# The file is rewritten in place to keep its owner and mode, after the first
# run saves a copy next to it.
docker-fix-db-host: ## Repair a running container's volume that registers testdb for a stale host (keeps the data)
	docker compose exec -T -u root cubrid sh -c 'set -e; \
		f="$$CUBRID_DATABASES/databases.txt"; t="$$(mktemp)"; \
		[ -e "$$f.before-fix-db-host" ] || cp -p "$$f" "$$f.before-fix-db-host"; \
		awk '"'"'BEGIN { OFS = "\t" } $$1 == "testdb" { $$3 = "localhost" } { print }'"'"' "$$f" > "$$t"; \
		cat "$$t" > "$$f"; rm -f "$$t"; \
		echo "Saved the previous file as $$f.before-fix-db-host"; \
		grep "^testdb[[:space:]]" "$$f"'

##@ Maintenance and release

changelog: ## Generate changelog with git-cliff
	git-cliff --output CHANGELOG.md

clean: ## Remove build artifacts and caches
	rm -rf build/ dist/ *.egg-info .pytest_cache/ .coverage .ruff_cache/ __pycache__/ mutants/
	find . -type d -name __pycache__ -exec rm -rf {} + 2>/dev/null || true
	find . -type f -name '*.pyc' -delete 2>/dev/null || true

clean-all: clean ## Remove all artifacts including .mypy_cache and .tox
	rm -rf .mypy_cache/ .tox/ htmlcov/

release-check: ## Read-only release consistency gate (run by release-please.yml and publish-pypi.yml). Usage: make release-check VERSION=x.y.z
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
