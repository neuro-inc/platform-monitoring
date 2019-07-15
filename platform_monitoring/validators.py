import trafaret as t

from .jobs_service import ImageReference


def create_save_request_payload_validator(expected_image_domain: str) -> t.Trafaret:
    def _validate_image(ref_str: str) -> ImageReference:
        try:
            ref = ImageReference.parse(ref_str)
        except ValueError as err:
            raise t.DataError(str(err))
        if ref.domain != expected_image_domain:
            raise t.DataError("Unknown registry host")
        return ref

    return t.Dict({"container": t.Dict({"image": t.String >> t.Call(_validate_image)})})
