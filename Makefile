IMAGE_NAME ?= platformmonitoringapi
IMAGE_TAG ?= $(GITHUB_SHA)
IMAGE ?= $(GKE_DOCKER_REGISTRY)/$(GKE_PROJECT_ID)/$(IMAGE_NAME)

CLOUD_IMAGE_gke   ?= $(GKE_DOCKER_REGISTRY)/$(GKE_PROJECT_ID)/$(IMAGE_NAME)
CLOUD_IMAGE_aws   ?= $(AWS_ACCOUNT_ID).dkr.ecr.$(AWS_REGION).amazonaws.com/$(IMAGE_NAME)
CLOUD_IMAGE_azure ?= $(AZURE_ACR_NAME).azurecr.io/$(IMAGE_NAME)

CLOUD_IMAGE  = ${CLOUD_IMAGE_${CLOUD_PROVIDER}}

PLATFORMAPI_IMAGE = $(shell cat PLATFORMAPI_IMAGE)
PLATFORMAUTHAPI_IMAGE = $(shell cat PLATFORMAUTHAPI_IMAGE)
PLATFORMCONFIG_IMAGE = $(shell cat PLATFORMCONFIG_IMAGE)

ARTIFACTORY_DOCKER_REPO ?= neuro-docker-local-public.jfrog.io
ARTIFACTORY_HELM_REPO ?= https://neuro.jfrog.io/artifactory/helm-local-public

export PIP_EXTRA_INDEX_URL ?= $(shell python pip_extra_index_url.py)

include k8s.mk

setup:
	@echo "Using extra pip index: $(PIP_EXTRA_INDEX_URL)"
	pip install -r requirements/test.txt
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
	docker build -f Dockerfile.k8s -t $(IMAGE_NAME):latest --build-arg PIP_EXTRA_INDEX_URL .

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
	    docker tag $(PLATFORMAPI_IMAGE) platformapi:latest; \
	    docker tag $(PLATFORMAUTHAPI_IMAGE) platformauthapi:latest; \
	    docker tag $(PLATFORMCONFIG_IMAGE) platformconfig:latest

gcr_login:
	@echo $(GKE_ACCT_AUTH) | base64 --decode | docker login -u _json_key --password-stdin https://gcr.io

ecr_login:
	$$(aws ecr get-login --no-include-email --region $(AWS_REGION))

docker_push: docker_build
	docker tag $(IMAGE_NAME):latest $(CLOUD_IMAGE):latest
	docker tag $(IMAGE_NAME):latest $(CLOUD_IMAGE):$(IMAGE_TAG)
	docker push $(CLOUD_IMAGE):latest
	docker push $(CLOUD_IMAGE):$(IMAGE_TAG)

helm_install:
	curl https://raw.githubusercontent.com/kubernetes/helm/master/scripts/get | bash -s -- -v $(HELM_VERSION)
	helm init --client-only

helm_deploy:
	helm -f deploy/platformmonitoringapi/values-$(HELM_ENV)-$(CLOUD_PROVIDER).yaml --set "IMAGE=$(CLOUD_IMAGE):$(IMAGE_TAG)" upgrade --install platformmonitoringapi deploy/platformmonitoringapi/ --namespace platform --wait --timeout 600

artifactory_docker_login:
	@docker login $(ARTIFACTORY_DOCKER_REPO) \
		--username=$(ARTIFACTORY_USERNAME) \
		--password=$(ARTIFACTORY_PASSWORD)

artifactory_docker_push: docker_build
ifeq ($(ARTIFACTORY_TAG),)
	$(error ARTIFACTORY_TAG is not set)
endif
	docker tag $(IMAGE_NAME):latest $(ARTIFACTORY_DOCKER_REPO)/$(IMAGE_NAME):$(ARTIFACTORY_TAG)
	docker push $(ARTIFACTORY_DOCKER_REPO)/$(IMAGE_NAME):$(ARTIFACTORY_TAG)

artifactory_helm_plugin_install:
	helm plugin install https://github.com/belitre/helm-push-artifactory-plugin

artifactory_helm_repo_add:
ifeq ($(ARTIFACTORY_USERNAME),)
	$(error ARTIFACTORY_USERNAME is not set)
endif
ifeq ($(ARTIFACTORY_PASSWORD),)
	$(error ARTIFACTORY_PASSWORD is not set)
endif
	helm init --client-only
	helm repo add neuro-local-public \
		$(ARTIFACTORY_HELM_REPO) \
		--username ${ARTIFACTORY_USERNAME} \
		--password ${ARTIFACTORY_PASSWORD}

artifactory_helm_push:
ifeq ($(ARTIFACTORY_TAG),)
	$(error ARTIFACTORY_TAG is not set)
endif
	mkdir -p temp_deploy/platformmonitoringapi
	cp -Rf deploy/platformmonitoringapi/. temp_deploy/platformmonitoringapi
	cp temp_deploy/platformmonitoringapi/values-template.yaml temp_deploy/platformmonitoringapi/values.yaml
	sed -i "s/IMAGE_TAG/$(ARTIFACTORY_TAG)/g" temp_deploy/platformmonitoringapi/values.yaml
	find temp_deploy/platformmonitoringapi -type f -name 'values-*' -delete
	helm package --app-version=$(ARTIFACTORY_TAG) --version=$(ARTIFACTORY_TAG) temp_deploy/platformmonitoringapi/
	helm push-artifactory $(IMAGE_NAME)-$(ARTIFACTORY_TAG).tgz neuro-local-public
	rm -rf temp_deploy
	rm $(IMAGE_NAME)-$(ARTIFACTORY_TAG).tgz
