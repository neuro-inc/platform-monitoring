import pytest
from platform_monitoring.config_client import ConfigClient


class TestConfigClient:
    @pytest.mark.minikube
    @pytest.mark.asyncio
    async def test_get_cluster(self, platform_config_client: ConfigClient) -> None:
        cluster = await platform_config_client.get_cluster()
        assert cluster
