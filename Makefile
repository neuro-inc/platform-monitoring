.PHONY: all test clean
all test clean:

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
	poetry run mypy platform_monitoring tests

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
		--cov-config=pyproject.toml --cov-report xml:.coverage-unit.xml \
		tests/unit

.PHONY: test_integration
test_integration:
	poetry run pytest -vv \
		--cov-config=pyproject.toml --cov-report xml:.coverage-integration.xml \
		--durations=10 \
		--maxfail=3 \
		--log-level=INFO \
		--retries=3 \
		tests/integration

.PHONY: clean-dist
clean-dist:
	rm -rf dist

IMAGE_NAME = platformmonitoringapi

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

install_k8s:
	./tests/k8s/cluster.sh install


start_k8s:
	./tests/k8s/cluster.sh start


apply_configuration_k8s:
	./tests/k8s/cluster.sh apply


wait_k8s_pods_ready:
	./tests/k8s/cluster.sh wait


test_k8s:
	./tests/k8s/cluster.sh test


clean_k8s:
	./tests/k8s/cluster.sh stop
	docker stop $$(docker ps -a -q)
	docker rm $$(docker ps -a -q)

install_helm_loki:
	helm upgrade loki grafana/loki -f tests/k8s/loki-values.yml --version 6.28.0 --install

install_helm_alloy:
	helm upgrade alloy grafana/alloy  -f tests/k8s/alloy-values.yml --version 0.12.3 --install
