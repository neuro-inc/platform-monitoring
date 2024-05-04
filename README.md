# platform-monitoring

## Local Development
1. Install minikube (https://github.com/kubernetes/minikube#installation);
1. Authenticate local docker:
    ```shell
    gcloud auth configure-docker  # part of `make gke_login`
    ```
1. Launch minikube:
    ```shell
    ./minikube.sh start
    ```
1. Make sure the kubectl tool uses the minikube k8s cluster:
    ```shell
    minikube status
    kubectl config use-context minikube
    ```
1. Load images into minikube's virtual machine:
    ```shell
    ./minikube.sh load-images
    ```
1. Apply minikube configuration and some k8s fixture services:
    ```shell
    ./minikube.sh apply
    ```
1. Create a new virtual environment with Python 3.11:
    ```shell
    python -m venv venv
    source venv/bin/activate
    ```
1. Install testing dependencies:
    ```shell
    make setup
    ```
1. Run the unit test suite:
    ```shell
    make test_unit
    ```
1. Run the integration test suite:
    ```shell
    make test_integration
    ```
1. Cleanup+shutdown minikube:
    ```shell
    ./minikube.sh clean
    ./minikube.sh stop
    ```

## Logs collection
Job container logs are tracked by Fluent Bit and sent to S3 compatible storage using [Amazon S3](https://docs.fluentbit.io/manual/pipeline/outputs/s3) plugin. Files are pushed to S3 every minute for each running job. S3 keys have format `kube.var.log.containers.{pod_name}_{namespace_name}_{container_name}-{container_id}/{date}_{index}.gz`.

## Logs compaction
Logs are periodically compacted (every hour by default). Logs compaction consists of two phases: merging and cleaning. After merging phase certain number of log files are merged into one and pushed to S3. Cleaning of the merged files is done before the next merging phase to avoid situations where log files are being deleted while user is reading them.

S3 log reader is capable of reading both merged and raw log files and combining them into a single job log output.

### Merging
Batches of raw Fluent Bit logs files are merged and pushed to S3 under `data` folder. S3 keys have format `data/{pod_name}/{container_id}/{date}_{index}.gz`. Additionally for each job a separate metadata file is maintained, it has a key format `metadata/{pod_name}.json`. It contains the list of all the merged files and some additional statistics.

### Cleaning
Every time merging is done for a certain job its name is pushed to the cleanup queue. This queue is just a folder in S3 bucket and we need it in order to perform one last cleanup once the job finishes or stops producing logs. After cleanup job is removed from the queue.
