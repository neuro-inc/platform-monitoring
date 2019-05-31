#!/usr/bin/env bash
set -o verbose

GKE_DOCKER_REGISTRY=gcr.io
GKE_PROJECT_ID=light-reality-205619

GKE_PREFIX=$GKE_DOCKER_REGISTRY/$GKE_PROJECT_ID


function minikube::start {
    echo "Starting minikube..."
    mkdir -p ~/.minikube/files/files
    cp tests/k8s/fluentd/kubernetes.conf ~/.minikube/files/files/fluentd-kubernetes.conf
    minikube start --kubernetes-version=v1.10.0
    kubectl config use-context minikube
}

function save_k8s_image {
    local image=$1
    docker save -o /tmp/${image}.image $image:latest
}

function load_k8s_image {
    local image=$1
    docker load -i /tmp/${image}.image
}

function minikube::load_images {
    echo "Loading images to minikube..."
    save_k8s_image platformauthapi
    save_k8s_image platformapi
    save_k8s_image platformconfig

    eval $(minikube docker-env)

    load_k8s_image platformauthapi
    load_k8s_image platformconfig
    load_k8s_image platformapi
}

function minikube::apply_all_configurations {
    echo "Applying configurations..."
    kubectl config use-context minikube
    kubectl apply -f tests/k8s/rb.default.gke.yml
    kubectl apply -f tests/k8s/logging.yml
    kubectl apply -f tests/k8s/platformconfig.yml
    kubectl apply -f tests/k8s/platformapi.yml
}

function minikube::delete_all_configurations {
    echo "Cleaning up..."
    kubectl config use-context minikube
    kubectl delete -f tests/k8s/rb.default.gke.yml
    kubectl delete -f tests/k8s/logging.yml
    kubectl delete -f tests/k8s/platformconfig.yml
    kubectl delete -f tests/k8s/platformapi.yml
}

function minikube::stop {
    echo "Stopping minikube..."
    kubectl config use-context minikube
    minikube::delete_all_configurations
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

    n_seconds=10
    echo "Waiting $n_seconds seconds so that all services are up"
    sleep $n_seconds

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
        minikube::delete_all_configurations
        ;;
    stop)
        minikube::stop
        ;;
esac