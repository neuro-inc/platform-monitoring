IMAGE_NAME ?= platformmonitoringapi
IMAGE_TAG ?= latest
IMAGE_NAME_K8S ?= $(IMAGE_NAME)-k8s
IMAGE_K8S ?= $(GKE_DOCKER_REGISTRY)/$(GKE_PROJECT_ID)/$(IMAGE_NAME_K8S)


ifdef CIRCLECI
    PIP_INDEX_URL ?= https://$(DEVPI_USER):$(DEVPI_PASS)@$(DEVPI_HOST)/$(DEVPI_USER)/$(DEVPI_INDEX)
else
    PIP_INDEX_URL ?= $(shell python pip_extra_index_url.py)
endif
export PIP_INDEX_URL

setup:
	pip install --no-use-pep517 --no-binary cryptography -r requirements/test.txt

test_integration:
	pytest -vv --maxfail=3 --cov-config=setup.cfg --cov platform_monitoring tests/integration

build_mon_k8s:
	@docker build --build-arg PIP_INDEX_URL="$(PIP_INDEX_URL)" \
	    -f Dockerfile.k8s -t $(IMAGE_NAME_K8S):$(IMAGE_TAG) .

run_mon_k8s:
	NP_STORAGE_HOST1_MOUNT_PATH=/tmp \
	NP_K8S_MON_URL=https://$$(minikube ip):8443 \
	NP_K8S_CA_PATH=$$HOME/.minikube/ca.crt \
	NP_K8S_AUTH_CERT_PATH=$$HOME/.minikube/client.crt \
	NP_K8S_AUTH_CERT_KEY_PATH=$$HOME/.minikube/client.key \
	platform-monitoring

run_mon_k8s_container:
	docker run --rm -it --name platformmonitoring \
	    -p 8080:8080 \
	    -v $$HOME/.minikube:$$HOME/.minikube \
	    -e NP_STORAGE_HOST_MOUNT_PATH=/tmp \
	    -e NP_K8S_MON_URL=https://$$(minikube ip):8443 \
	    -e NP_K8S_CA_PATH=$$HOME/.minikube/ca.crt \
	    -e NP_K8S_AUTH_CERT_PATH=$$HOME/.minikube/client.crt \
	    -e NP_K8S_AUTH_CERT_KEY_PATH=$$HOME/.minikube/client.key \
	    $(IMAGE_K8S):latest

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
	helm \
		--set "global.env=$(HELM_ENV)" \
		--set "IMAGE.$(HELM_ENV)=$(IMAGE_K8S):$(CIRCLE_SHA1)" \
		--set "INGRESS_FALLBACK_IMAGE.$(HELM_ENV)=$(INGRESS_FALLBACK_IMAGE_K8S):$(CIRCLE_SHA1)" \
		upgrade --install platformmonitoring deploy/platformmonitoring/ --wait --timeout 600
