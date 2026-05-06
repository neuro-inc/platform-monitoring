LOKI_HELM_VERSION ?= 6.55.0
ALLOY_HELM_VERSION ?= 1.8.1
GRAFANA_HELM_VERSION ?= 10.1.5
LOKI_CHART ?= grafana/loki
ALLOY_CHART ?= grafana/alloy
GRAFANA_CHART ?= grafana/grafana
GRAFANA_HELM_REPO_URL ?= https://grafana.github.io/helm-charts
HELM_WAIT_TIMEOUT ?= 10m
LOKI_VALUES ?= tests/k8s/loki-values.yml
ALLOY_VALUES ?= tests/k8s/alloy-values.yml
GRAFANA_VALUES ?= tests/k8s/grafana-values.yml

IMAGE_NAME ?= platformmonitoringapi

MYPY_TARGETS ?= platform_monitoring tests
UNIT_TEST_PATH ?= tests/unit
INT_TEST_PATH ?= tests/integration
UNIT_COVERAGE_FILE ?= .coverage-unit.xml
INT_COVERAGE_FILE ?= .coverage-integration.xml
COVERAGE_FILE ?= $(INT_COVERAGE_FILE)

PYTEST_DURATIONS ?= 10
PYTEST_MAXFAIL ?= 3
PYTEST_LOG_LEVEL ?= INFO
PYTEST_RETRIES ?= 3

.PHONY: all test clean
all test clean:

include k8s.mk

.PHONY: venv
venv:
	poetry lock
	poetry install --with dev;

.PHONY: build
build: venv poetry-plugins

.PHONY: poetry-plugins
poetry-plugins:
	poetry self add "poetry-dynamic-versioning[plugin]"; \
    poetry self add "poetry-plugin-export";

.PHONY: setup
setup: venv
	poetry run pre-commit install;

.PHONY: lint
lint: format
	poetry run mypy $(MYPY_TARGETS)

.PHONY: format
format:
ifdef CI
	poetry run pre-commit run --all-files --show-diff-on-failure
else
	poetry run pre-commit run --all-files
endif

.PHONY: test_unit
test_unit:
	poetry run pytest -vv \
		--cov-config=pyproject.toml --cov-report xml:$(UNIT_COVERAGE_FILE) \
		$(UNIT_TEST_PATH)

.PHONY: test_integration
test_integration:
	poetry run pytest -vv \
		--cov-config=pyproject.toml --cov-report xml:$(COVERAGE_FILE) \
		--durations=$(PYTEST_DURATIONS) \
		--maxfail=$(PYTEST_MAXFAIL) \
		--log-level=$(PYTEST_LOG_LEVEL) \
		--retries=$(PYTEST_RETRIES) \
		$(INT_TEST_PATH)

.PHONY: dump_failed_k8s_logs
dump_failed_k8s_logs:
	@echo "=== Pod overview ===" && kubectl get pods -A
	@kubectl get pods -A --no-headers \
		| awk '{ \
			split($$3, r, "/"); not_ready = (r[1] != r[2]); \
			bad_status = ($$4 != "Running" && $$4 != "Completed" && $$4 != "Succeeded"); \
			has_restarts = ($$5+0 > 0); \
			if (bad_status || not_ready || has_restarts) print $$1, $$2, $$5 \
		}' \
		| while read -r ns pod restarts; do \
			echo ""; \
			echo "=== Describe: $$pod (ns=$$ns) ==="; \
			kubectl describe pod "$$pod" -n "$$ns" 2>&1; \
			echo "=== Logs: $$pod (ns=$$ns) ==="; \
			kubectl logs "$$pod" -n "$$ns" --all-containers 2>&1 || true; \
			if [ "$${restarts:-0}" -gt 0 ]; then \
				echo "=== Previous logs: $$pod (ns=$$ns) ==="; \
				kubectl logs "$$pod" -n "$$ns" --all-containers --previous 2>&1 || true; \
			fi; \
		done

.PHONY: clean-dist
clean-dist:
	rm -rf dist

.PHONY: docker_build
docker_build: dist
	docker build \
		--build-arg PY_VERSION=$$(cat .python-version) \
		-t $(IMAGE_NAME):latest .

.python-version:
	@echo "Error: .python-version file is missing!" && exit 1

.PHONY: dist
dist: build
	rm -rf build dist; \
	poetry export -f requirements.txt --without-hashes -o requirements.txt; \
	poetry build -f wheel;

.PHONY: helm_repo_setup
helm_repo_setup:
	helm repo add grafana $(GRAFANA_HELM_REPO_URL) --force-update
	helm repo update

.PHONY: install_helm_loki
install_helm_loki: helm_repo_setup
	helm upgrade loki $(LOKI_CHART) -f $(LOKI_VALUES) --version $(LOKI_HELM_VERSION) --install --wait --timeout $(HELM_WAIT_TIMEOUT)

.PHONY: install_helm_alloy
install_helm_alloy: helm_repo_setup
	helm upgrade alloy $(ALLOY_CHART) -f $(ALLOY_VALUES) --version $(ALLOY_HELM_VERSION) --install --wait --timeout $(HELM_WAIT_TIMEOUT)

.PHONY: install_helm_grafana
install_helm_grafana: helm_repo_setup
	helm upgrade grafana $(GRAFANA_CHART) -f $(GRAFANA_VALUES) --version $(GRAFANA_HELM_VERSION) --install --wait --timeout $(HELM_WAIT_TIMEOUT)

.PHONY: install_helm_stack
install_helm_stack: install_helm_loki install_helm_alloy install_helm_grafana
