from dataclasses import dataclass


@dataclass(frozen=True)
class ServerConfig:
    host: str = "0.0.0.0"
    port: int = 8080


@dataclass(frozen=True)
class Config:
    monitoring_server: ServerConfig
