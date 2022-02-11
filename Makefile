PLATFORMAPI_IMAGE = $(shell cat PLATFORMAPI_IMAGE)
PLATFORMADMIN_IMAGE = $(shell cat PLATFORMADMIN_IMAGE)
PLATFORMAUTHAPI_IMAGE = $(shell cat PLATFORMAUTHAPI_IMAGE)
PLATFORMCONFIG_IMAGE = $(shell cat PLATFORMCONFIG_IMAGE)
PLATFORMNOTIFICATIONS_IMAGE = $(shell cat PLATFORMNOTIFICATIONS_IMAGE)
PLATFORMCONTAINERRUNTIME_IMAGE = $(shell cat PLATFORMCONTAINERRUNTIME_IMAGE)

include k8s.mk

setup:
	pip install -U pip
	pip install -e .[dev]
	pre-commit install

lint: format
	mypy platform_monitoring tests

format:
ifdef CI
	pre-commit run --all-files --show-diff-on-failure
else
	pre-commit run --all-files
endif

test_unit:
	pytest -vv --cov=platform_monitoring --cov-report xml:.coverage-unit.xml tests/unit

test_integration:
	pytest -vv --maxfail=1 --cov=platform_monitoring --cov-report xml:.coverage-integration.xml tests/integration -m "not minikube"

test_integration_minikube:
	pytest -vv --log-cli-level=debug tests/integration -m minikube

docker_build:
	rm -rf build dist
	pip install -U build
	python -m build
	docker build -t platformmonitoringapi:latest .

docker_pull_test_images:
	@eval $$(minikube docker-env); \
	    docker pull $(PLATFORMAPI_IMAGE); \
	    docker pull $(PLATFORMADMIN_IMAGE); \
	    docker pull $(PLATFORMAUTHAPI_IMAGE); \
	    docker pull $(PLATFORMCONFIG_IMAGE); \
	    docker pull $(PLATFORMNOTIFICATIONS_IMAGE); \
	    docker pull $(PLATFORMCONTAINERRUNTIME_IMAGE); \
	    docker tag $(PLATFORMAPI_IMAGE) platformapi:latest; \
	    docker tag $(PLATFORMADMIN_IMAGE) platformadmin:latest; \
	    docker tag $(PLATFORMAUTHAPI_IMAGE) platformauthapi:latest; \
	    docker tag $(PLATFORMCONFIG_IMAGE) platformconfig:latest; \
	    docker tag $(PLATFORMNOTIFICATIONS_IMAGE) platformnotifications:latest; \
	    docker tag $(PLATFORMCONTAINERRUNTIME_IMAGE) platformcontainerruntime:latest
