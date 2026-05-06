LOKI_HELM_VERSION ?= 6.55.0
ALLOY_HELM_VERSION ?= 1.8.1
LOKI_CHART ?= grafana/loki
ALLOY_CHART ?= grafana/alloy
LOKI_VALUES ?= tests/k8s/loki-values.yml
ALLOY_VALUES ?= tests/k8s/alloy-values.yml

IMAGE_NAME ?= platformmonitoringapi

MYPY_TARGETS ?= platform_monitoring tests
UNIT_TEST_PATH ?= tests/unit
INT_TEST_PATH ?= tests/integration
UNIT_COVERAGE_FILE ?= .coverage-unit.xml
INT_COVERAGE_FILE ?= .coverage-integration.xml

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
		--cov-config=pyproject.toml --cov-report xml:$(INT_COVERAGE_FILE) \
		--durations=$(PYTEST_DURATIONS) \
		--maxfail=$(PYTEST_MAXFAIL) \
		--log-level=$(PYTEST_LOG_LEVEL) \
		--retries=$(PYTEST_RETRIES) \
		$(INT_TEST_PATH)

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

.PHONY: install_helm_loki
install_helm_loki:
	helm upgrade loki $(LOKI_CHART) -f $(LOKI_VALUES) --version $(LOKI_HELM_VERSION) --install

.PHONY: install_helm_alloy
install_helm_alloy:
	helm upgrade alloy $(ALLOY_CHART) -f $(ALLOY_VALUES) --version $(ALLOY_HELM_VERSION) --install
