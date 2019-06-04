from neuromation.api import Client as PlatformApiClient


class MonitoringService:
    def __init__(self, platform_client: PlatformApiClient) -> None:
        self._platform_api = platform_client
