#!/usr/bin/env bash
set -o verbose

export GKE_DOCKER_REGISTRY=gcr.io
export GKE_PROJECT_ID=light-reality-205619


function minikube::start {
    echo "Starting minikube..."
    mkdir -p ~/.minikube/files/files
    cp tests/k8s/files/* ~/.minikube/files/files/
    minikube config set WantUpdateNotification false
    minikube start \
        --kubernetes-version=v1.14.10 \
        --install-addons=true \
        --addons=registry \
        --wait=all \
        --wait-timeout=5m
    kubectl config use-context minikube
    kubectl get nodes -o name | xargs -I {} kubectl label {} --overwrite \
        platform.neuromation.io/job=true \
        platform.neuromation.io/nodepool=minikube
}

function minikube::load_images {
    echo "Loading images to minikube..."
    make gke_docker_pull_test_images
}

function minikube::apply_all_configurations {
    echo "Applying configurations..."
    kubectl config use-context minikube
    kubectl apply -f tests/k8s/dockerengineapi.yml
    kubectl apply -f tests/k8s/rbac.yml
    kubectl apply -f tests/k8s/logging.yml
    kubectl apply -f tests/k8s/platformconfig.yml
    kubectl apply -f tests/k8s/platformapi.yml
    kubectl apply -f tests/k8s/platformnotifications.yml
    kubectl apply -f tests/k8s/platformcontainerruntime.yml
}

function minikube::clean {
    echo "Cleaning up..."
    kubectl config use-context minikube
    kubectl delete -f tests/k8s/dockerengineapi.yml
    kubectl delete -f tests/k8s/rbac.yml
    kubectl delete -f tests/k8s/logging.yml
    kubectl delete -f tests/k8s/platformconfig.yml
    kubectl delete -f tests/k8s/platformapi.yml
    kubectl delete -f tests/k8s/platformnotifications.yml
    kubectl delete -f tests/k8s/platformcontainerruntime.yml
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
