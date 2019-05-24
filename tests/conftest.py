from uuid import uuid1


def random_str() -> str:
    return str(uuid1())[:8]
