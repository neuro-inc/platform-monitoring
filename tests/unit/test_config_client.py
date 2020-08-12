from platform_monitoring.config_client import Cluster, NodePool


class TestNodePool:
    def test_node_pool(self) -> None:
        node_pool = NodePool(
            {
                "name": "minikube",
                "min_size": 1,
                "max_size": 1,
                "cpu": 1.0,
                "available_cpu": 0.9,
                "memory_mb": 1024,
                "available_memory_mb": 1000,
            }
        )
        assert node_pool.name == "minikube"
        assert node_pool.max_size == 1
        assert node_pool.available_cpu_m == 900
        assert node_pool.available_memory_mb == 1000
        assert node_pool.gpu == 0
        assert node_pool.gpu_model == ""

        node_pool = NodePool({"gpu": 1, "gpu_model": "nvidia-tesla-k80"})
        assert node_pool.gpu == 1
        assert node_pool.gpu_model == "nvidia-tesla-k80"


class TestCluster:
    def test_cluster(self) -> None:
        cluster = Cluster(
            {"cloud_provider": {"zones": ["us-east-1a"], "node_pools": [{}]}}
        )
        assert cluster.zones == ["us-east-1a"]
        assert cluster.zones_count == 1
        assert cluster.node_pools

    def test_cluster_without_zones(self) -> None:
        cluster = Cluster({"cloud_provider": {"node_pools": [{}]}})
        assert cluster.zones_count == 1
        assert cluster.node_pools
