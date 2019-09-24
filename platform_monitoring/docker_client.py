from dataclasses import dataclass
from typing import Any

from aiodocker import Docker as AioDocker
from aiodocker.images import DockerImages as AioDockerImages
from docker_image.reference import (
    InvalidReference as _InvalidImageReference,
    Reference as _ImageReference,
)


class DockerImages(AioDockerImages):
    async def commit(self, container: str, repo: str, tag: str) -> None:
        params = dict(container=container, repo=repo, tag=tag)
        await self.docker._query_json(
            "commit",
            method="POST",
            params=params,
            # TODO: a bug in aiodocker. had to explicitly pass the content
            # type, although there is a logic for this
            headers={"content-type": "application/json"},
        )


class Docker(AioDocker):
    def __init__(self, *args: Any, **kwargs: Any) -> None:
        super().__init__(*args, **kwargs)

        self.images = DockerImages(self)

    async def ping(self) -> None:
        async with self._query("_ping", method="GET") as resp:
            resp.raise_for_status()


class ImageReferenceError(ValueError):
    pass


@dataclass(frozen=True)
class ImageReference:
    """
    https://github.com/docker/distribution/blob/master/reference/reference.go
    """

    domain: str = ""
    path: str = ""
    tag: str = ""

    def __post_init__(self) -> None:
        if not self.path:
            raise ImageReferenceError("blank reference path")

    @property
    def repository(self) -> str:
        if self.domain:
            return f"{self.domain}/{self.path}"
        return self.path

    def __str__(self) -> str:
        if self.tag:
            return f"{self.repository}:{self.tag}"
        return self.repository

    @classmethod
    def parse(cls, ref_str: str) -> "ImageReference":
        try:
            ref = _ImageReference.parse(ref_str)
        except _InvalidImageReference as exc:
            raise ImageReferenceError(str(exc))
        domain, path = ref.split_hostname()
        return cls(domain=domain or "", path=path, tag=ref["tag"] or "")
