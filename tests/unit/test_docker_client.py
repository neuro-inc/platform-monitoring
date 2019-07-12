from typing import Any, Dict, List

import pytest
from platform_monitoring.docker_client import DockerError, check_docker_push_result


def test_check_docker_push_result() -> None:
    payload: List[Dict[str, Any]] = [
        {"status": "The push refers to repository [registry:80/testuser/alpine]"},
        {
            "errorDetail": {
                "message": (
                    "Get https://registry:80/v2/: dial tcp: "
                    "lookup registry on 10.0.2.3:53: no such host"
                )
            },
            "error": (
                "Get https://registry:80/v2/: dial tcp: "
                "lookup registry on 10.0.2.3:53: no such host"
            ),
        },
    ]
    with pytest.raises(DockerError, match="Failed to push image 'repo:tag'"):
        check_docker_push_result("repo", "tag", payload)
