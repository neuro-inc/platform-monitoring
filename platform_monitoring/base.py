import time
from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from typing import Any, AsyncIterator, Optional


class LogReader(ABC):
    @abstractmethod
    async def __aenter__(self) -> AsyncIterator[bytes]:
        pass

    async def __aexit__(self, *args: Any) -> None:
        pass


@dataclass(frozen=True)
class JobStats:
    cpu: float
    memory: float

    gpu_duty_cycle: Optional[int] = None
    gpu_memory: Optional[float] = None

    timestamp: float = field(default_factory=time.time)


class Telemetry(ABC):
    async def __aenter__(self) -> "Telemetry":
        return self

    async def __aexit__(self, *args: Any) -> None:
        pass

    @abstractmethod
    async def get_latest_stats(self) -> Optional[JobStats]:
        pass
