from typing import Any, Dict, List

import pytest
from platform_monitoring.docker_client import (
    DockerError,
    ImageReference,
    ImageReferenceError,
    check_docker_push_suceeded,
)


def test_check_docker_push_succeeded() -> None:
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
        check_docker_push_suceeded("repo", "tag", payload)


class TestImageReference:
    def test_no_path(self) -> None:
        with pytest.raises(ImageReferenceError, match="blank reference path"):
            ImageReference()

    def test_invalid_path(self) -> None:
        with pytest.raises(ImageReferenceError, match="invalid reference format"):
            ImageReference.parse("_")

    @pytest.mark.parametrize(
        "ref_str, expected_ref, expected_repo",
        (
            ("alpine", ImageReference(path="alpine"), "alpine"),
            ("alpine:latest", ImageReference(path="alpine", tag="latest"), "alpine"),
            (
                "localhost:5000/alpine:latest",
                ImageReference(domain="localhost:5000", path="alpine", tag="latest"),
                "localhost:5000/alpine",
            ),
            (
                "example.com/alpine:latest",
                ImageReference(domain="example.com", path="alpine", tag="latest"),
                "example.com/alpine",
            ),
        ),
    )
    def test_parse(
        self, ref_str: str, expected_ref: ImageReference, expected_repo: str
    ) -> None:
        ref = ImageReference.parse(ref_str)
        assert ref == expected_ref
        assert str(ref) == ref_str
        assert ref.repository == expected_repo
