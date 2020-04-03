# platform-monitoring

## Local Development
0. Set env vars:
```shell
   export GKE_DOCKER_REGISTRY=gcr.io
   export GKE_PROJECT_ID=light-reality-205619
```

1. Install minikube (https://github.com/kubernetes/minikube#installation);
2. Authenticate local docker:
```shell
gcloud auth configure-docker  # part of `make gke_login`
```
3. Pull necessary docker images from Neuromation's private repo:
```shell
make gke_docker_pull_test_images
```
4. Launch minikube:
```shell
./minikube.sh start
```
5. Make sure the kubectl tool uses the minikube k8s cluster:
```shell
minikube status
kubectl config use-context minikube
```
6. Load images into minikube's virtual machine:
```shell
./minikube.sh load-images
```
7. Apply minikube configuration and some k8s fixture services:
```shell
./minikube.sh apply
```
6. Create a new virtual environment with Python 3.7:
```shell
python -m venv venv
source venv/bin/activate
```
7. Install testing dependencies:
```shell
make setup
```
8. Run the unit test suite:
```shell
make test_unit
```
9. Run the integration test suite:
```shell
make test_integration
```
10. Cleanup+shutdown minikube:
```shell
./minikube.sh clean
./minikube.sh stop
```
