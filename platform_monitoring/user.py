from dataclasses import dataclass, field

from aiohttp.web import HTTPUnauthorized, Request
from aiohttp_security.api import AUTZ_KEY, IDENTITY_KEY


@dataclass(frozen=True)
class User:
    name: str
    token: str = field(repr=False)


async def authorized_user(request: Request) -> User:
    """Request auth-server for authenticated information on the user and
     return the `User` object with all necessary information
    """
    identity = await _get_identity(request)

    autz_policy = request.config_dict.get(AUTZ_KEY)
    autz_user = await autz_policy.authorized_user(identity)
    if autz_user is None:
        raise HTTPUnauthorized()

    return User(name=autz_user.name, token=identity)


async def _get_identity(request: Request) -> str:
    identity_policy = request.config_dict.get(IDENTITY_KEY)
    identity = await identity_policy.identify(request)
    if identity is None:
        raise HTTPUnauthorized()
    return identity
