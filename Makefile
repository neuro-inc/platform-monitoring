IMAGE_NAME ?= platformmonitoringapi
IMAGE_TAG ?= $(GITHUB_SHA)
IMAGE ?= $(GKE_DOCKER_REGISTRY)/$(GKE_PROJECT_ID)/$(IMAGE_NAME)
IMAGE_AWS ?= $(AWS_ACCOUNT_ID).dkr.ecr.$(AWS_REGION).amazonaws.com/$(IMAGE_NAME)

PLATFORMAPI_IMAGE = $(shell cat PLATFORMAPI_IMAGE)
PLATFORMAUTHAPI_IMAGE = $(shell cat PLATFORMAUTHAPI_IMAGE)
PLATFORMCONFIG_IMAGE = $(shell cat PLATFORMCONFIG_IMAGE)
PLATFORMCONFIGMIGRATIONS_IMAGE = $(shell cat PLATFORMCONFIGMIGRATIONS_IMAGE)

ARTIFACTORY_DOCKER_REPO ?= neuro-docker-local-public.jfrog.io
ARTIFACTORY_HELM_REPO ?= https://neuro.jfrog.io/artifactory/helm-local-public

export PIP_EXTRA_INDEX_URL ?= $(shell python pip_extra_index_url.py)

include k8s.mk

setup:
	@echo "Using extra pip index: $(PIP_EXTRA_INDEX_URL)"
	pip install -r requirements/test.txt

lint:
	black --check platform_monitoring tests setup.py
	flake8 platform_monitoring tests setup.py
	mypy platform_monitoring tests setup.py

format:
	isort -rc platform_monitoring tests setup.py
	black platform_monitoring tests setup.py

test_unit:
	pytest -vv --cov=platform_monitoring --cov-report xml:.coverage-unit.xml tests/unit

test_integration:
	pytest -vv --maxfail=3 --cov=platform_monitoring --cov-report xml:.coverage-integration.xml tests/integration -m "not minikube"

test_integration_minikube: docker_build_tests
	kubectl run --restart=Never --image-pull-policy=Never -it --rm --image=platformmonitoringapi-tests:latest tests -- pytest -vv --log-cli-level=debug -s tests/integration/ -m minikube

docker_build:
	docker build -f Dockerfile.k8s -t $(IMAGE_NAME):latest --build-arg PIP_EXTRA_INDEX_URL .

docker_build_tests:
	@eval $$(minikube docker-env); \
	    make docker_build; \
	    docker build -f tests.Dockerfile -t $(IMAGE_NAME)-tests:latest .

gke_login:
	sudo chown circleci:circleci -R $$HOME
	@echo $(GKE_ACCT_AUTH) | base64 --decode > $(HOME)//gcloud-service-key.json
	gcloud auth activate-service-account --key-file $(HOME)/gcloud-service-key.json
	gcloud config set project $(GKE_PROJECT_ID)
	gcloud --quiet config set container/cluster $(GKE_CLUSTER_NAME)
	gcloud config set $(SET_CLUSTER_ZONE_REGION)
	gcloud version
	docker version
	gcloud auth configure-docker

eks_login:
	aws eks --region $(AWS_REGION) update-kubeconfig --name $(AWS_CLUSTER_NAME)

docker_pull_test_images:
	@eval $$(minikube docker-env); \
	    docker pull $(PLATFORMAPI_IMAGE); \
	    docker pull $(PLATFORMAUTHAPI_IMAGE); \
	    docker pull $(PLATFORMCONFIG_IMAGE); \
	    docker pull $(PLATFORMCONFIGMIGRATIONS_IMAGE); \
	    docker tag $(PLATFORMAPI_IMAGE) platformapi:latest; \
	    docker tag $(PLATFORMAUTHAPI_IMAGE) platformauthapi:latest; \
	    docker tag $(PLATFORMCONFIG_IMAGE) platformconfig:latest; \
	    docker tag $(PLATFORMCONFIGMIGRATIONS_IMAGE) platformconfig-migrations:latest

gcr_login:
	@echo $(GKE_ACCT_AUTH) | base64 --decode | docker login -u _json_key --password-stdin https://gcr.io

ecr_login:
	$$(aws ecr get-login --no-include-email --region $(AWS_REGION))

docker_push: docker_build
	docker tag $(IMAGE_NAME):latest $(IMAGE_AWS):latest
	docker tag $(IMAGE_NAME):latest $(IMAGE_AWS):$(IMAGE_TAG)
	docker push $(IMAGE_AWS):latest
	docker push $(IMAGE_AWS):$(IMAGE_TAG)

helm_install:
	curl https://raw.githubusercontent.com/kubernetes/helm/master/scripts/get | bash -s -- -v $(HELM_VERSION)
	helm init --client-only

helm_deploy:
	helm -f deploy/platformmonitoringapi/values-$(HELM_ENV)-aws.yaml --set "IMAGE=$(IMAGE_AWS):$(IMAGE_TAG)" upgrade --install platformmonitoringapi deploy/platformmonitoringapi/ --namespace platform --wait --timeout 600

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
