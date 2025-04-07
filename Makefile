.PHONY: all test clean
all test clean:


venv:
	python -m venv venv
	. venv/bin/activate; \
	python -m pip install --upgrade pip

.PHONY: setup
setup: venv
	. venv/bin/activate; \
	pip install -e .[dev]; \
	pre-commit install


.PHONY: lint
lint:
	. venv/bin/activate; \
	python -m pre_commit run --all-files
	. venv/bin/activate; \
	python -m mypy platform_monitoring tests


.PHONY: test_unit
test_unit:
	. venv/bin/activate; \
	pytest -vv \
		--cov=platform_monitoring --cov-report xml:.coverage-unit.xml \
		tests/unit

.PHONY: test_integration
test_integration:
	. venv/bin/activate; \
	pytest -svv \
		--cov=platform_monitoring --cov-report xml:.coverage-integration.xml \
		--durations=10 \
		--maxfail=0 \
		--log-level=INFO \
    	tests/integration \
    	-k "test_apps_only"


.PHONY: docker_build
docker_build:
	rm -rf build dist
	. venv/bin/activate; \
	pip install -U build; \
	python -m build
	docker build \
		--target service \
		--build-arg PY_VERSION=$$(cat .python-version) \
		-t platformmonitoringapi:latest .


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
