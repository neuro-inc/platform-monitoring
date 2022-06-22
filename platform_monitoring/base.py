import time
from abc import ABC, abstractmethod
from collections.abc import AsyncIterator
from dataclasses import dataclass, field
from datetime import datetime
from typing import Any, Optional


class LogReader(ABC):
    last_time: Optional[datetime] = None

    def __init__(self, container_runtime: str, timestamps: bool = False) -> None:
        super().__init__()

        self._container_runtime = container_runtime
        self._timestamps = timestamps

    def encode_log(self, time: str, log: str) -> bytes:
        result = log

        if self._timestamps:
            if self._container_runtime == "docker":
                result = f"{time} {log}"
            else:
                result = f"{time} {log}\n"
        else:
            if self._container_runtime != "docker":
                result = f"{log}\n"

        return result.encode()

    @abstractmethod
    async def __aenter__(self) -> AsyncIterator[bytes]:
        pass

    async def __aexit__(self, *args: Any) -> None:
        pass


@dataclass(frozen=True)
class JobStats:
    cpu: float
    memory: float

    gpu_utilization: Optional[int] = None
    gpu_memory_used: Optional[int] = None

    timestamp: float = field(default_factory=time.time)


class Telemetry(ABC):
    async def __aenter__(self) -> "Telemetry":
        return self

    async def __aexit__(self, *args: Any) -> None:
        pass

    @abstractmethod
    async def get_latest_stats(self) -> Optional[JobStats]:
        pass
