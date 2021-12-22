from typing import Any

import trafaret as t
from docker_image.reference import InvalidReference, Reference


def create_save_request_payload_validator(expected_image_domain: str) -> t.Trafaret:
    def _validate_image(value: str) -> str:
        try:
            ref = Reference.parse(value)
        except InvalidReference as exc:
            raise t.DataError(str(exc))
        domain, _ = ref.split_hostname()
        if domain != expected_image_domain:
            raise t.DataError("Unknown registry host")
        return value

    return t.Dict({"container": t.Dict({"image": t.String >> t.Call(_validate_image)})})


def create_exec_create_request_payload_validator() -> t.Trafaret:
    return t.Dict(
        {
            t.Key("command"): t.String,
            t.Key("stdin", optional=True, default=False): t.Bool,
            t.Key("stdout", optional=True, default=True): t.Bool,
            t.Key("stderr", optional=True, default=True): t.Bool,
            t.Key("tty", optional=True, default=False): t.Bool,
        }
    )


def _validate_gpu(payload: dict[str, Any]) -> dict[str, Any]:
    if bool(payload["gpu"]) is not bool(payload["gpu_model"]):
        return t.DataError({"gpu": "Invalid number of gpu"}, value=payload["gpu"])
    return payload
