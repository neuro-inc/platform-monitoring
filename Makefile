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
		--maxfail=100 \
		--cov=platform_monitoring --cov-report xml:.coverage-integration.xml \
		--durations=10 \
		tests/integration \
	    -n 12

		# -n 12
		# -k "test_s3_merged_log_reader_restarted" tests/integration/test_jobs_service.py tests/integration/test_api.py tests/integration/test_kube.py
		# --log-cli-level=info \
		# -m "not minikube" \
		# -m "not exclude" \

.PHONY: test_integration_minikube
test_integration_minikube:
	. venv/bin/activate; \
	pytest -vv \
		--log-cli-level=info \
		--durations=10 \
		tests/integration \
		-m minikube


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

PLATFORMAPI_IMAGE = $(shell cat PLATFORMAPI_IMAGE)
PLATFORMADMIN_IMAGE = $(shell cat PLATFORMADMIN_IMAGE)
PLATFORMAUTHAPI_IMAGE = $(shell cat PLATFORMAUTHAPI_IMAGE)
PLATFORMCONFIG_IMAGE = $(shell cat PLATFORMCONFIG_IMAGE)
PLATFORMNOTIFICATIONS_IMAGE = $(shell cat PLATFORMNOTIFICATIONS_IMAGE)
PLATFORMCONTAINERRUNTIME_IMAGE = $(shell cat PLATFORMCONTAINERRUNTIME_IMAGE)

.PHONY: docker_pull_test_images
docker_pull_test_images:
	@eval $$(minikube docker-env); \
		docker pull postgres:12.11; \
		docker pull redis:4; \
		docker pull curlimages/curl:8.4.0; \
		docker pull fluent/fluent-bit:2.1.10; \
		docker pull minio/minio:RELEASE.2023-10-14T05-17-22Z; \
		docker pull lachlanevenson/k8s-kubectl:v1.10.3; \
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

#	@eval $$(minikube -p minikube docker-env --unset); \
#		minikube image load platformapi:latest; \
#		minikube image load platformadmin:latest; \
#		minikube image load platformauthapi:latest; \
#		minikube image load platformconfig:latest; \
#		minikube image load platformnotifications:latest; \
#		minikube image load platformcontainerruntime:latest


include k8s.mk
