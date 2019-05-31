K8S_CLUSTER_CMD := tests/k8s/cluster.sh

install_k8s:
	$(K8S_CLUSTER_CMD) install

start_k8s:
	$(K8S_CLUSTER_CMD) start

test_k8s:
	$(K8S_CLUSTER_CMD) test

clean_k8s:
    $(K8S_CLUSTER_CMD) stop
	-docker stop $$(docker ps -a -q)
	-docker rm $$(docker ps -a -q)
