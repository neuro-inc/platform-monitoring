from typing import Any, Dict, List

from aiodocker import Docker as AioDocker
from aiodocker.exceptions import DockerError
from aiodocker.images import DockerImages as AioDockerImages


class DockerImages(AioDockerImages):
    async def commit(self, container: str, repo: str, tag: str) -> None:
        params = dict(container=container, repo=repo, tag=tag)
        await self.docker._query(
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


def check_docker_push_result(
    repo: str, tag: str, payload: List[Dict[str, Any]]
) -> None:
    for item in payload:
        error = item.get("error")
        if error:
            raise DockerError(
                500, dict(message=f"Failed to push image '{repo}:{tag}': {error}")
            )
