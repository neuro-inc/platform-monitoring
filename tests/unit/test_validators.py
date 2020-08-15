import pytest
import trafaret as t

from platform_monitoring.jobs_service import ImageReference
from platform_monitoring.validators import (
    create_presets_validator,
    create_save_request_payload_validator,
)


class TestSaveRequest:
    def test_no_container(self) -> None:
        validator = create_save_request_payload_validator("")
        with pytest.raises(t.DataError, match="required"):
            validator.check({})

    def test_no_image(self) -> None:
        validator = create_save_request_payload_validator("")
        with pytest.raises(t.DataError, match="required"):
            validator.check({"container": {}})

    def test_invalid_image_reference(self) -> None:
        validator = create_save_request_payload_validator("")
        with pytest.raises(t.DataError, match="invalid reference format"):
            validator.check({"container": {"image": "__"}})

    def test_unknown_registry_host(self) -> None:
        validator = create_save_request_payload_validator("localhost:5000")
        with pytest.raises(t.DataError, match="Unknown registry host"):
            validator.check({"container": {"image": "whatever.com/test"}})

    def test_parsed(self) -> None:
        validator = create_save_request_payload_validator("localhost:5000")
        payload = validator.check({"container": {"image": "localhost:5000/test"}})
        assert payload["container"]["image"] == ImageReference(
            domain="localhost:5000", path="test"
        )


class TestPresetValidator:
    @pytest.fixture
    def validator(self) -> t.Trafaret:
        return create_presets_validator()

    def test_defaults(self, validator: t.Trafaret) -> None:
        assert validator.check({"preset": {"cpu": 1, "memory_mb": 128}}) == {
            "preset": {
                "cpu": 1,
                "gpu": 0,
                "gpu_model": "",
                "is_preemptible": False,
                "memory_mb": 128,
            }
        }

    def test_custom(self, validator: t.Trafaret) -> None:
        payload = {
            "preset": {
                "cpu": 1,
                "memory_mb": 128,
                "gpu": 1,
                "gpu_model": "nvidia-tesla-k80",
                "is_preemptible": True,
            }
        }
        assert validator.check(payload) == payload

    def test_invalid_gpu(self, validator: t.Trafaret) -> None:
        with pytest.raises(t.DataError, match="Invalid number of gpu"):
            validator.check({"preset": {"cpu": 1, "memory_mb": 128, "gpu": 1}})

        with pytest.raises(t.DataError, match="Invalid number of gpu"):
            validator.check(
                {
                    "preset": {
                        "cpu": 1,
                        "memory_mb": 128,
                        "gpu_model": "nvidia-tesla-k80",
                    }
                }
            )
