import pytest
import trafaret as t

from platform_monitoring.validators import create_save_request_payload_validator


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
        assert payload["container"]["image"] == "localhost:5000/test"
