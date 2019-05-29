#!/usr/bin/env bash
set -o verbose

GKE_DOCKER_REGISTRY=gcr.io
GKE_PROJECT_ID=light-reality-205619
GKE_PREFIX=$(GKE_DOCKER_REGISTRY)/$(GKE_PROJECT_ID)


function minikube::install {
    mkdir -p ~/.minikube/files/files
    cp tests/k8s/fluentd/kubernetes.conf ~/.minikube/files/files/fluentd-kubernetes.conf
    minikube start --kubernetes-version=v1.10.0
    kubectl config use-context minikube
}

function minikube::pull_save_k8s_image {
    local image=$1
    local k8s_image=$GKE_PREFIX/$image

    docker pull $k8s_image:latest
    docker tag $k8s_image:latest $image:latest
    docker save -o /tmp/${image}.image $image:latest
}

function minikube::load_k8s_image {
    local image=$1
    docker load -i /tmp/${image}.image
}

function minikube::activate_docker_env {
    eval $(minikube docker-env)
}

function minikube::load_images {
    minikube::pull_save_k8s_image platformauthapi
    minikube::pull_save_k8s_image platformapi
    minikube::pull_save_k8s_image platformconfig

    minikube::activate_docker_env

    minikube::load_k8s_image platformauthapi
    minikube::load_k8s_image platformconfig
    minikube::load_k8s_image platformapi
}

function minikube::apply_all_configurations {
    kubectl create -f deploy/platformapi/templates/rb.default.gke.yml
    kubectl create -f tests/k8s/platformconfig.yml
    kubectl create -f tests/k8s/platformapi.yml
}

function minikube::delete_all_configurations {
    kubectl delete -f deploy/platformapi/templates/rb.default.gke.yml
    kubectl delete -f tests/k8s/platformconfig.yml
    kubectl delete -f tests/k8s/platformapi.yml
}

function check_service() { # attempt, max_attempt, service
    local attempt=1
    local max_attempts=$1
    local service=$2
    until minikube service $service --url; do
	if [ $attempt == $max_attempts ]; then
	    echo "Can't connect to the container"
            exit 1
	fi
	sleep 1
	((attempt++))
    done    
}

function minikube::check {
    max_attempts=30
    check_service $max_attempts platformapi
    check_service $max_attempts platformauthapi
}


minikube::install
minikube::delete_all_configurations
minikube::load_images
minikube::apply_all_configurations

# wait till our services are up to prevent flakes
sleep 10

minikube::check

export PLATFORM_API_URL=$(minikube service platformapi --url)/api/v1
export AUTH_API_URL=$(minikube service platformauthapi --url)
