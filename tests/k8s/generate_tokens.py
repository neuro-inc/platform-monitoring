import json
import os
import secrets
from pathlib import Path

from jose import jwt


TOKENS_PATH = Path(__file__).with_name("tokens.json")


def generate_tokens(path: Path = TOKENS_PATH) -> None:
    try:
        fd = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
    except FileExistsError:
        return

    with os.fdopen(fd, "w") as file:
        key = secrets.token_urlsafe(32)
        data = {"jwt_secret": key}
        for identity in (
            "admin",
            "cluster",
            "cluster-admin",
            "compute",
            "notification",
        ):
            payload = {"identity": identity}
            if identity in ("admin", "cluster-admin", "notification"):
                payload["https://platform.neuromation.io/user"] = identity
            data[f"token_{identity.replace('-', '_')}"] = jwt.encode(
                payload, key, algorithm="HS256"
            )
        json.dump(
            {
                "apiVersion": "v1",
                "kind": "Secret",
                "metadata": {"name": "platform-tokens"},
                "type": "Opaque",
                "stringData": data,
            },
            file,
        )
        file.write("\n")


if __name__ == "__main__":
    generate_tokens()
