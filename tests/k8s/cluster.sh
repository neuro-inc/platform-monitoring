#!/usr/bin/env bash

# based on
# https://github.com/kubernetes/minikube#linux-continuous-integration-without-vm-support

function k8s::install_kubectl {
    local kubectl_version=$(curl -s https://storage.googleapis.com/kubernetes-release/release/stable.txt)
    curl -Lo kubectl https://storage.googleapis.com/kubernetes-release/release/${kubectl_version}/bin/linux/amd64/kubectl
    chmod +x kubectl
    sudo mv kubectl /usr/local/bin/
}

function k8s::install_minikube {
    # we have to pin this version in order to run minikube on CircleCI
    # Ubuntu 14 VMs. The newer versions depend on systemd.
    local minikube_version="v0.25.2"
    curl -Lo minikube https://storage.googleapis.com/minikube/releases/${minikube_version}/minikube-linux-amd64
    chmod +x minikube
    sudo mv minikube /usr/local/bin/
    sudo -E minikube config set WantReportErrorPrompt false
}

function k8s::install {
    k8s::install_kubectl
    k8s::install_minikube
}

function k8s::start {
    export KUBECONFIG=$HOME/.kube/config
    mkdir -p $(dirname $KUBECONFIG)
    touch $KUBECONFIG

    export MINIKUBE_WANTUPDATENOTIFICATION=false
    export MINIKUBE_WANTREPORTERRORPROMPT=false
    export MINIKUBE_HOME=$HOME
    export CHANGE_MINIKUBE_NONE_USER=true

    sudo -E mkdir -p ~/.minikube/files/files
    sudo -E cp tests/k8s/fluentd/kubernetes.conf ~/.minikube/files/files/fluentd-kubernetes.conf

    sudo -E minikube config set WantReportErrorPrompt false
    sudo -E minikube start --vm-driver=none --kubernetes-version=v1.10.0
}

function k8s::stop {
    sudo -E minikube stop || :
    sudo -E minikube delete || :
    sudo -E rm -rf ~/.minikube
    sudo rm -rf /root/.minikube
}

function k8s::setup_logging {
    kubectl apply -f tests/k8s/logging.yml
}

function k8s::test {
    kubectl delete jobs testjob1 || :
    kubectl create -f tests/k8s/pod.yml
    for i in {1..300}; do
        if [ "$(kubectl get job testjob1 --template {{.status.succeeded}})" == "1" ]; then
            exit 0
        fi
        if [ "$(kubectl get job testjob1 --template {{.status.failed}})" == "1" ]; then
            exit 1
        fi
        sleep 1
    done
    echo "Could not complete test job"
    kubectl describe job testjob1
    exit 1
}

case "${1:-}" in
    install)
        k8s::install
        ;;
    up)
        k8s::start
        ;;
    down)
        k8s::stop
        ;;
    clean)
        k8s::stop
        ;;
    test)
        k8s::test
        ;;
esac
