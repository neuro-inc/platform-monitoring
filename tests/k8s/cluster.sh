#!/usr/bin/env bash
set -o errexit

# based on
# https://github.com/kubernetes/minikube#linux-continuous-integration-without-vm-support

function k8s::install_minikube {
    local minikube_version="v1.34.0"
    sudo apt-get update
    sudo apt-get install -y conntrack
    curl -Lo minikube https://storage.googleapis.com/minikube/releases/${minikube_version}/minikube-linux-amd64
    chmod +x minikube
    sudo mv minikube /usr/local/bin/
}

function k8s::start {
    export KUBECONFIG=$HOME/.kube/config
    mkdir -p $(dirname $KUBECONFIG)
    touch $KUBECONFIG

    minikube start \
        --vm-driver=docker \
        --container-runtime=containerd \
        --wait=all \
        --wait-timeout=5m \
        --addons=registry \
        --install-addons=true
    kubectl config use-context minikube
}

function k8s::apply_all_configurations {
    echo "Applying configurations..."
    kubectl config use-context minikube
    kubectl get nodes -o name | xargs -I {} \
        kubectl label {} --overwrite platform.neuromation.io/nodepool=minikube
    kubectl apply -f tests/k8s/rbac.yml
    kubectl apply -f tests/k8s/logging.yml
    kubectl apply -f tests/k8s/platformauth.yml
    kubectl apply -f tests/k8s/platformconfig.yml
    kubectl apply -f tests/k8s/platformadmin.yml
    kubectl apply -f tests/k8s/platformapi.yml
    kubectl apply -f tests/k8s/platformnotifications.yml
    kubectl apply -f tests/k8s/platformcontainerruntime.yml

    # build newest platform monitoring image so we can use it in tests
    docker build -t platformmonitoring:tests .
    # load monitoring image into a minikube
    docker image save -o platformmonitoring.tests.tar platformmonitoring:tests
    minikube image load platformmonitoring.tests.tar
    kubectl apply -f tests/k8s/platformmonitoring.yml
    kubectl apply -f tests/k8s/extra-entities.yml

    # for local development you need to run also
    # kubectl create secret docker-registry ghcr-secret --docker-server=ghcr.io
    # --docker-username=<your_github_username> --docker-password=<your_github_token_with_ghcr_access>
}


function k8s::dump_failed_pods {
    echo ""
    echo "=== Pod overview ==="
    kubectl get pods -A

    kubectl get pods -A --no-headers | while read -r ns pod ready status restarts _age; do
        current="${ready%%/*}"
        total="${ready##*/}"
        restart_count="${restarts%% *}"

        bad_status=false
        not_ready=false
        has_restarts=false

        [[ "$status" != "Running" && "$status" != "Completed" && "$status" != "Succeeded" ]] && bad_status=true
        [[ "$current" != "$total" ]] && not_ready=true
        [[ "$restart_count" =~ ^[0-9]+$ && "$restart_count" -gt 0 ]] && has_restarts=true

        if $bad_status || $not_ready || $has_restarts; then
            echo ""
            echo "########## $pod (ns=$ns) ##########"
            echo "--- Events ---"
            kubectl describe pod "$pod" -n "$ns" 2>&1
            echo "--- Logs ---"
            kubectl logs "$pod" -n "$ns" --all-containers 2>&1 || true
            echo "--- Previous logs ---"
            kubectl logs "$pod" -n "$ns" --all-containers --previous 2>&1 || true
        fi
    done
}

function k8s::wait_for_all_pods_ready {
    local timeout=300s

    echo "Waiting for deployments (default)..."
    kubectl rollout status deployment --all --timeout="$timeout" \
        || { k8s::dump_failed_pods; return 1; }

    echo "Waiting for statefulsets (default)..."
    kubectl rollout status statefulset --all --timeout="$timeout" \
        || { k8s::dump_failed_pods; return 1; }

    echo "Waiting for daemonsets (default)..."
    kubectl rollout status daemonset --all --timeout="$timeout" \
        || { k8s::dump_failed_pods; return 1; }

}


function k8s::stop {
    sudo -E minikube stop || :
    sudo -E minikube delete || :
    sudo -E rm -rf ~/.minikube
    sudo rm -rf /root/.minikube
}


function k8s::test {
    kubectl delete jobs testjob1 2>/dev/null || :
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
        k8s::install_minikube
        ;;
    start)
        k8s::start
        ;;
    apply)
        k8s::apply_all_configurations
        ;;
    stop)
        k8s::stop
        ;;
    test)
        k8s::test
        ;;
    wait)
        k8s::wait_for_all_pods_ready
        ;;
esac
