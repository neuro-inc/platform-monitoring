from dataclasses import dataclass

from yarl import URL


@dataclass(frozen=True)
class ServerConfig:
    host: str = "0.0.0.0"
    port: int = 8080


@dataclass(frozen=True)
class PlatformApiConfig:
    url: URL
    token: str


@dataclass(frozen=True)
class PlatformAuthConfig:
    url: URL
    token: str


@dataclass(frozen=True)
class Config:
    server: ServerConfig
    platform_api: PlatformApiConfig
    platform_auth: PlatformAuthConfig
