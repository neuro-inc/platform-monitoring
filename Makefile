IMAGE_NAME ?= platformmonitoringapi
IMAGE_TAG ?= latest
IMAGE_NAME_K8S ?= $(IMAGE_NAME)-k8s
IMAGE_K8S ?= $(GKE_DOCKER_REGISTRY)/$(GKE_PROJECT_ID)/$(IMAGE_NAME_K8S)

setup:
	pip install --no-use-pep517 -r requirements/test.txt

test_unit:
	pytest -vv platform_monitoring tests/unit

build_mon_k8s:
	@docker build -f Dockerfile.k8s -t $(IMAGE_NAME_K8S):$(IMAGE_TAG) .

gke_login:
	sudo /opt/google-cloud-sdk/bin/gcloud --quiet components update --version 204.0.0
	sudo /opt/google-cloud-sdk/bin/gcloud --quiet components update --version 204.0.0 kubectl
	sudo chown circleci:circleci -R $$HOME
	@echo $(GKE_ACCT_AUTH) | base64 --decode > $(HOME)//gcloud-service-key.json
	gcloud auth activate-service-account --key-file $(HOME)/gcloud-service-key.json
	gcloud config set project $(GKE_PROJECT_ID)
	gcloud --quiet config set container/cluster $(GKE_CLUSTER_NAME)
	gcloud config set $(SET_CLUSTER_ZONE_REGION)
	gcloud auth configure-docker

gke_docker_push: build_mon_k8s
	docker tag $(IMAGE_NAME_K8S):$(IMAGE_TAG) $(IMAGE_K8S):latest
	docker tag $(IMAGE_NAME_K8S):$(IMAGE_TAG) $(IMAGE_K8S):$(CIRCLE_SHA1)
	docker push $(IMAGE_K8S)

gke_k8s_deploy:
	gcloud --quiet container clusters get-credentials $(GKE_CLUSTER_NAME) $(CLUSTER_ZONE_REGION)
	#helm \
	#	--set "global.env=$(HELM_ENV)" \
	#	--set "IMAGE.$(HELM_ENV)=$(IMAGE_K8S):$(CIRCLE_SHA1)" \
	#	--set "INGRESS_FALLBACK_IMAGE.$(HELM_ENV)=$(INGRESS_FALLBACK_IMAGE_K8S):$(CIRCLE_SHA1)" \
	#	upgrade --install platformmonitoring deploy/platformmonitoring/ --wait --timeout 600
