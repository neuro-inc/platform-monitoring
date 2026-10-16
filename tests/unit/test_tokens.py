import json
import stat
from pathlib import Path

import pytest
from jose import jwt

from tests.integration import conftest_auth
from tests.k8s.generate_tokens import generate_tokens


def test_generate_tokens(tmp_path: Path) -> None:
    path = tmp_path / "tokens.json"
    generate_tokens(path)
    manifest = json.loads(path.read_text())
    assert manifest["kind"] == "Secret"
    assert manifest["metadata"]["name"] == "platform-tokens"
    assert stat.S_IMODE(path.stat().st_mode) == 0o600
    data = manifest["stringData"]
    key = data.pop("jwt_secret")
    assert len(key) >= 32
    for identity in ("admin", "cluster", "cluster-admin", "compute", "notification"):
        token = data[f"token_{identity.replace('-', '_')}"]
        payload = jwt.decode(token, key, algorithms=["HS256"])
        expected = {"identity": identity}
        if identity in ("admin", "cluster-admin", "notification"):
            expected["https://platform.neuromation.io/user"] = identity
        assert payload == expected

    original = path.read_bytes()
    generate_tokens(path)
    assert path.read_bytes() == original
    other_path = tmp_path / "other-tokens.json"
    generate_tokens(other_path)
    other_key = json.loads(other_path.read_text())["stringData"]["jwt_secret"]
    assert other_key != key


def test_token_factory_uses_generated_key(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, request: pytest.FixtureRequest
) -> None:
    path = tmp_path / "tokens.json"
    generate_tokens(path)
    monkeypatch.setattr(conftest_auth, "TOKENS_PATH", path)
    token_factory = request.getfixturevalue("token_factory")
    key = json.loads(path.read_text())["stringData"]["jwt_secret"]
    payload = jwt.decode(token_factory("compute"), key, algorithms=["HS256"])
    assert payload == {"identity": "compute"}
