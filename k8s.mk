K8S_CLUSTER_CMD := tests/k8s/cluster.sh

install_k8s:
	$(K8S_CLUSTER_CMD) install

start_k8s: $(K8S_CLUSTER_CMD) install_k8s clean_k8s
	$(K8S_CLUSTER_CMD) up

test_k8s:
	$(K8S_CLUSTER_CMD) test

stop_k8s:
	$(K8S_CLUSTER_CMD) down

clean_k8s: stop_k8s
	$(K8S_CLUSTER_CMD) clean
	-docker stop $$(docker ps -a -q)
	-docker rm $$(docker ps -a -q)
