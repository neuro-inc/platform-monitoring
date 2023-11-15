import time
from abc import ABC, abstractmethod
from collections.abc import AsyncIterator
from dataclasses import dataclass, field
from datetime import datetime
from typing import Any, Optional


class LogReader(ABC):
    last_time: Optional[datetime] = None

    def __init__(self, timestamps: bool = False) -> None:
        super().__init__()

        self._timestamps = timestamps

    def encode_log(self, time: str, log: str) -> bytes:
        result = log

        if result and result[-1] != "\n":
            result = f"{result}\n"
        if self._timestamps:
            result = f"{time} {result}"

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
