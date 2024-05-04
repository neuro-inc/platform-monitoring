import time
from abc import ABC, abstractmethod
from collections.abc import AsyncIterator
from dataclasses import dataclass, field
from datetime import datetime
from types import TracebackType


class LogReader(ABC):
    last_time: datetime | None = None

    def __init__(self, *, timestamps: bool = False) -> None:
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

    async def __aexit__(
        self,
        exc_type: type[BaseException] | None,
        exc_value: BaseException | None,
        traceback: TracebackType | None,
    ) -> None:
        return None


@dataclass(frozen=True)
class JobStats:
    cpu: float
    memory: float

    gpu_utilization: int | None = None
    gpu_memory_used: int | None = None

    timestamp: float = field(default_factory=time.time)


class Telemetry(ABC):
    async def __aenter__(self) -> "Telemetry":
        return self

    async def __aexit__(self, *args: object) -> None:
        return None

    @abstractmethod
    async def get_latest_stats(self) -> JobStats | None:
        pass
