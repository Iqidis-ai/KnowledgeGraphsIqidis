"""Per-request environment selection.

One backend process can serve multiple iqidis environments (development /
preview / staging / production). Callers declare which environment's
database to use via the `env` query param or the `X-Iqidis-Env` header;
endpoints that already accept `env` in a JSON body pass it through as the
explicit value. When no environment is given, the process-wide APP_ENV
default applies — callers that predate this mechanism keep working
unchanged.
"""
from typing import Optional

from flask import has_request_context, request

ENV_QUERY_PARAM = "env"
ENV_HEADER = "X-Iqidis-Env"


def request_env(explicit: Optional[str] = None) -> Optional[str]:
    """Environment requested by the caller, or None for the APP_ENV default.

    Resolution order: explicit value (e.g. from a JSON body) → `env` query
    param → `X-Iqidis-Env` header → None. Safe to call outside a request
    context (returns the explicit value or None).
    """
    env = explicit
    if not env and has_request_context():
        env = request.args.get(ENV_QUERY_PARAM) or request.headers.get(ENV_HEADER)
    if not env:
        return None
    return env.strip().lower() or None
