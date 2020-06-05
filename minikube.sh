#!/usr/bin/env bash
set -o verbose

GKE_DOCKER_REGISTRY=gcr.io
GKE_PROJECT_ID=light-reality-205619

GKE_PREFIX=$GKE_DOCKER_REGISTRY/$GKE_PROJECT_ID


function minikube::start {
    echo "Starting minikube..."
    mkdir -p ~/.minikube/files/files
    cp tests/k8s/fluentd/kubernetes.conf ~/.minikube/files/files/fluentd-kubernetes.conf
    minikube config set WantUpdateNotification false
    minikube start --kubernetes-version=v1.13.0
    minikube addons enable registry
    kubectl config use-context minikube
    minikube tunnel &
}

function save_k8s_image {
    local image=$1
    echo "Saving ${image}"
    docker save -o /tmp/${image}.image $image:latest
}

function load_k8s_image {
    local image=$1
    echo "Loading ${image}"
    docker load -i /tmp/${image}.image
}

function minikube::load_images {
    echo "Loading images to minikube..."
    save_k8s_image platformauthapi
    save_k8s_image platformapi
    save_k8s_image platformconfig
    save_k8s_image platformconfig-migrations

    eval $(minikube docker-env)

    load_k8s_image platformauthapi
    load_k8s_image platformapi
    load_k8s_image platformconfig
    load_k8s_image platformconfig-migrations
}

function minikube::apply_all_configurations {
    echo "Applying configurations..."
    kubectl config use-context minikube
    kubectl apply -f tests/k8s/dockerengineapi.yml
    kubectl apply -f tests/k8s/rb.default.gke.yml
    kubectl apply -f tests/k8s/logging.yml
    kubectl apply -f tests/k8s/platformconfig.yml
    kubectl apply -f tests/k8s/platformapi.yml
}

function minikube::clean {
    echo "Cleaning up..."
    kubectl config use-context minikube
    kubectl delete -f tests/k8s/dockerengineapi.yml
    kubectl delete -f tests/k8s/rb.default.gke.yml
    kubectl delete -f tests/k8s/logging.yml
    kubectl delete -f tests/k8s/platformconfig.yml
    kubectl delete -f tests/k8s/platformapi.yml
}

function minikube::stop {
    echo "Stopping minikube..."
    kubectl config use-context minikube
    minikube::clean
    minikube stop
}

function check_service() { # attempt, max_attempt, service
    local attempt=1
    local max_attempts=$1
    local service=$2
    echo "Checking service $service..."
    until minikube service $service --url; do
	if [ $attempt == $max_attempts ]; then
	    echo "Can't connect to the container"
            exit 1
	fi
	sleep 1
	((attempt++))
    done    
}

function minikube::apply {
    minikube status
    minikube::apply_all_configurations

    max_attempts=30
    check_service $max_attempts platformapi
    check_service $max_attempts platformauthapi
}


case "${1:-}" in
    start)
        minikube::start
        ;;
    load-images)
        minikube::load_images
        ;;
    apply)
        minikube::apply
        ;;
    clean)
        minikube::clean
        ;;
    stop)
        minikube::stop
        ;;
esac
