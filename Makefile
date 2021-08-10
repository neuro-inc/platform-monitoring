IMAGE_NAME ?= platformmonitoringapi
TAG ?= latest

CLOUD_IMAGE_gke   ?= $(GKE_DOCKER_REGISTRY)/$(GKE_PROJECT_ID)/$(IMAGE_NAME)
CLOUD_IMAGE_aws   ?= $(AWS_ACCOUNT_ID).dkr.ecr.$(AWS_REGION).amazonaws.com/$(IMAGE_NAME)
CLOUD_IMAGE_azure ?= $(AZURE_ACR_NAME).azurecr.io/$(IMAGE_NAME)

CLOUD_IMAGE = ${CLOUD_IMAGE_${CLOUD_PROVIDER}}

PLATFORMAPI_IMAGE = $(shell cat PLATFORMAPI_IMAGE)
PLATFORMAUTHAPI_IMAGE = $(shell cat PLATFORMAUTHAPI_IMAGE)
PLATFORMCONFIG_IMAGE = $(shell cat PLATFORMCONFIG_IMAGE)
PLATFORMNOTIFICATIONS_IMAGE = $(shell cat PLATFORMNOTIFICATIONS_IMAGE)
PLATFORMCONTAINERRUNTIME_IMAGE = $(shell cat PLATFORMCONTAINERRUNTIME_IMAGE)

ARTIFACTORY_DOCKER_REPO ?= neuro-docker-local-public.jfrog.io
ARTIFACTORY_HELM_REPO ?= https://neuro.jfrog.io/artifactory/helm-local-public

ARTIFACTORY_IMAGE = $(ARTIFACTORY_DOCKER_REPO)/$(IMAGE_NAME)

HELM_ENV ?= dev
HELM_CHART = platformmonitoringapi

export PIP_EXTRA_INDEX_URL ?= $(shell python pip_extra_index_url.py)

include k8s.mk

setup:
	@echo "Using extra pip index: $(PIP_EXTRA_INDEX_URL)"
	pip install -U pip
	pip install -r requirements/test.txt
	pip install -e .
	pre-commit install

lint: format
	mypy platform_monitoring tests setup.py

format:
ifdef CI_LINT_RUN
	pre-commit run --all-files --show-diff-on-failure
else
	pre-commit run --all-files
endif

test_unit:
	pytest -vv --cov=platform_monitoring --cov-report xml:.coverage-unit.xml tests/unit

test_integration:
	pytest -vv --maxfail=3 --cov=platform_monitoring --cov-report xml:.coverage-integration.xml tests/integration -m "not minikube"

test_integration_minikube: docker_build_tests
	if [ "$$GITHUB_ACTIONS" = "true" ]; then FLAGS="-i"; else FLAGS="-it"; fi; \
	kubectl run --restart=Never --image-pull-policy=Never $$FLAGS --rm --image=platformmonitoringapi-tests:latest tests -- pytest -vv --log-cli-level=debug -s tests/integration/ -m minikube

docker_build:
	python setup.py sdist
	docker build -f Dockerfile.k8s \
		--build-arg PIP_EXTRA_INDEX_URL \
		--build-arg DIST_FILENAME=`python setup.py --fullname`.tar.gz \
		-t $(IMAGE_NAME):latest .

docker_build_tests:
	@eval $$(minikube docker-env); \
	    make docker_build; \
	    docker build -f tests.Dockerfile -t $(IMAGE_NAME)-tests:latest .

gke_k8s_login:
	sudo chown circleci:circleci -R $$HOME
	@echo $(GKE_ACCT_AUTH) | base64 --decode > $(HOME)//gcloud-service-key.json
	gcloud auth activate-service-account --key-file $(HOME)/gcloud-service-key.json
	gcloud config set project $(GKE_PROJECT_ID)
	gcloud --quiet config set container/cluster $(CLUSTER_NAME)
	gcloud config set $(SET_CLUSTER_ZONE_REGION)
	gcloud version
	docker version
	gcloud auth configure-docker

aws_k8s_login:
	aws eks --region $(AWS_REGION) update-kubeconfig --name $(CLUSTER_NAME)

azure_k8s_login:
	az aks get-credentials --resource-group $(AZURE_RG_NAME) --name $(CLUSTER_NAME)

docker_pull_test_images:
	@eval $$(minikube docker-env); \
	    docker pull $(PLATFORMAPI_IMAGE); \
	    docker pull $(PLATFORMAUTHAPI_IMAGE); \
	    docker pull $(PLATFORMCONFIG_IMAGE); \
	    docker pull $(PLATFORMNOTIFICATIONS_IMAGE); \
	    docker pull $(PLATFORMCONTAINERRUNTIME_IMAGE); \
	    docker tag $(PLATFORMAPI_IMAGE) platformapi:latest; \
	    docker tag $(PLATFORMAUTHAPI_IMAGE) platformauthapi:latest; \
	    docker tag $(PLATFORMCONFIG_IMAGE) platformconfig:latest; \
	    docker tag $(PLATFORMNOTIFICATIONS_IMAGE) platformnotifications:latest; \
	    docker tag $(PLATFORMCONTAINERRUNTIME_IMAGE) platformcontainerruntime:latest

gcr_login:
	@echo $(GKE_ACCT_AUTH) | base64 --decode | docker login -u _json_key --password-stdin https://gcr.io

docker_push: docker_build
	docker tag $(IMAGE_NAME):latest $(CLOUD_IMAGE):$(TAG)
	docker push $(CLOUD_IMAGE):$(TAG)

	docker tag $(IMAGE_NAME):latest $(CLOUD_IMAGE):latest
	docker push $(CLOUD_IMAGE):latest

artifactory_docker_push: docker_build
	docker tag $(IMAGE_NAME):latest $(ARTIFACTORY_IMAGE):$(TAG)
	docker push $(ARTIFACTORY_IMAGE):$(TAG)

helm_install:
	curl https://raw.githubusercontent.com/kubernetes/helm/master/scripts/get | bash -s -- -v $(HELM_VERSION)
	helm init --client-only
	helm plugin install https://github.com/belitre/helm-push-artifactory-plugin

_helm_fetch:
	rm -rf temp_deploy/$(HELM_CHART)
	mkdir -p temp_deploy/$(HELM_CHART)
	cp -Rf deploy/$(HELM_CHART) temp_deploy/
	find temp_deploy/$(HELM_CHART) -type f -name 'values*' -delete

_helm_expand_vars:
	export IMAGE_REPO=$(ARTIFACTORY_IMAGE); \
	export IMAGE_TAG=$(TAG); \
	export DOCKER_SERVER=$(ARTIFACTORY_DOCKER_REPO); \
	cat deploy/$(HELM_CHART)/values-template.yaml | envsubst > temp_deploy/$(HELM_CHART)/values.yaml

helm_deploy: _helm_fetch _helm_expand_vars
	helm upgrade platformmonitoringapi temp_deploy/$(HELM_CHART) \
		--namespace platform \
		-f deploy/$(HELM_CHART)/values-$(HELM_ENV)-$(CLOUD_PROVIDER).yaml \
		--set "image.repository=$(CLOUD_IMAGE)" \
		--install --wait --timeout 600

artifactory_helm_push: _helm_fetch _helm_expand_vars
	helm package --app-version=$(TAG) --version=$(TAG) temp_deploy/$(HELM_CHART)
	helm push-artifactory $(HELM_CHART)-$(TAG).tgz $(ARTIFACTORY_HELM_REPO) \
		--username $(ARTIFACTORY_USERNAME) \
		--password $(ARTIFACTORY_PASSWORD)
	rm $(HELM_CHART)-$(TAG).tgz
