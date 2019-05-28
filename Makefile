IMAGE_NAME ?= platformmonitoringapi
IMAGE_TAG ?= latest
IMAGE ?= $(GKE_DOCKER_REGISTRY)/$(GKE_PROJECT_ID)/$(IMAGE_NAME)

ifdef CIRCLECI
    PIP_EXTRA_INDEX_URL ?= https://$(DEVPI_USER):$(DEVPI_PASS)@$(DEVPI_HOST)/$(DEVPI_USER)/$(DEVPI_INDEX)
else
    PIP_EXTRA_INDEX_URL ?= $(shell python pip_extra_index_url.py)
endif
export PIP_EXTRA_INDEX_URL

include k8s.mk

setup:
	pip install --no-use-pep517 -r requirements/test.txt

lint:
	black --check platform_monitoring tests setup.py
	flake8 platform_monitoring tests setup.py
	mypy platform_monitoring tests setup.py

format:
	isort -rc platform_monitoring tests setup.py
	black --check platform_monitoring tests setup.py

test_unit:
	pytest --cov=platform_monitoring --cov-report xml:.coverage.xml tests/unit

test_integration:
	pytest --maxfail=3 --cov=platform_monitoring --cov-report xml:.coverage.xml tests/integration

build:
	@docker build -f Dockerfile.k8s -t $(IMAGE_NAME):$(IMAGE_TAG) .

gke_login:
	sudo chown circleci:circleci -R $$HOME
	@echo $(GKE_ACCT_AUTH) | base64 --decode > $(HOME)//gcloud-service-key.json
	gcloud auth activate-service-account --key-file $(HOME)/gcloud-service-key.json
	gcloud config set project $(GKE_PROJECT_ID)
	gcloud --quiet config set container/cluster $(GKE_CLUSTER_NAME)
	gcloud config set $(SET_CLUSTER_ZONE_REGION)
	gcloud auth --help
	gcloud version
	docker version
	gcloud auth configure-docker

gke_docker_push: build
	docker tag $(IMAGE_NAME):$(IMAGE_TAG) $(IMAGE):latest
	docker tag $(IMAGE_NAME):$(IMAGE_TAG) $(IMAGE):$(CIRCLE_SHA1)
	docker push $(IMAGE)

gke_deploy:
	gcloud --quiet container clusters get-credentials $(GKE_CLUSTER_NAME) $(CLUSTER_ZONE_REGION)
	#helm \
	#	--set "global.env=$(HELM_ENV)" \
	#	--set "IMAGE.$(HELM_ENV)=$(IMAGE):$(CIRCLE_SHA1)" \
	#	upgrade --install platformmonitoring deploy/platformmonitoring/ --wait --timeout 600
