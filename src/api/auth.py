"""
Shared-secret auth for the KG HTTP API.

Every /api/* request must carry `Authorization: Bearer <KG_API_SECRET>`.
The only callers are trusted servers (the Next.js proxy, which checks the
user's session and matter access before calling us). Browsers never call
the KG API directly, so there is no per-user token here.

/api/tools/* also accepts KG_TOOLS_TOKEN, the token the agent tool callers
already use.

Fails closed: if KG_API_SECRET is not configured, every protected request
gets 503 instead of being served unauthenticated.
"""
import hmac
import os

from flask import jsonify, request

# Paths that stay open: load-balancer / uptime checks.
_PUBLIC_PATHS = {'/api/health'}


def _bearer_token() -> str:
    header = request.headers.get('Authorization', '')
    return header[7:].strip() if header.startswith('Bearer ') else ''


def _matches(token: str, expected: str) -> bool:
    return bool(token) and bool(expected) and hmac.compare_digest(
        token.encode(), expected.encode()
    )


def require_api_secret():
    """Flask before_request hook. Returns a response to reject, None to allow."""
    if request.method == 'OPTIONS' or request.path in _PUBLIC_PATHS:
        return None

    api_secret = (os.getenv('KG_API_SECRET') or '').strip()
    tools_token = (os.getenv('KG_TOOLS_TOKEN') or '').strip()
    if not api_secret:
        return jsonify({'error': 'auth_not_configured',
                        'message': 'KG_API_SECRET is not set on the KG server'}), 503

    token = _bearer_token()
    if _matches(token, api_secret):
        return None
    if request.path.startswith('/api/tools/') and _matches(token, tools_token):
        return None
    return jsonify({'error': 'unauthorized',
                    'message': 'missing or invalid bearer token'}), 401
