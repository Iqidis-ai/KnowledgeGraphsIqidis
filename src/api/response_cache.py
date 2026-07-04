"""
In-process TTL response cache for read endpoints.

Read endpoints like /graph, /stats, /analytics, /importance, /timeline
recompute from scratch on every hit today — full-graph loads, PageRank,
per-request Postgres round-trips. At 50–100 RPS most of that work is
duplicative. This module caches the JSON response for a short TTL, keyed by
(endpoint, matter_id, sorted query args).

Writes bump a per-matter version counter. The version is part of the cache
key, so a write to matter X atomically invalidates every cached response
for X without needing to enumerate keys.

Scope: in-process only (2 gunicorn workers = 2 independent caches, which is
fine at this scale). Redis is a Phase 3 concern.
"""
import threading
import time
from functools import wraps
from typing import Any, Callable, Iterable, Optional

from cachetools import TTLCache
from flask import Response, request

# Cache sizing: 1024 entries × ~50KB avg JSON = ~50MB per worker cap. TTL
# default is 120s — short enough that stale data windows are small, long
# enough to absorb bursts.
_CACHE_MAX_SIZE = 1024
_DEFAULT_TTL_S = 120

_cache: "TTLCache[tuple, Any]" = TTLCache(maxsize=_CACHE_MAX_SIZE, ttl=_DEFAULT_TTL_S)
_cache_lock = threading.RLock()

# Per-matter version counter. Every write to matter X increments its version;
# reads include the version in the cache key, so a bump invalidates all of
# X's cached responses in O(1).
_matter_versions: dict = {}
_versions_lock = threading.Lock()

# Optional stats for observability (probe via /api/cache/stats if you want).
_stats = {"hit": 0, "miss": 0, "bypass": 0, "invalidate": 0}
_stats_lock = threading.Lock()


def _bump_stat(key: str) -> None:
    with _stats_lock:
        _stats[key] = _stats.get(key, 0) + 1


def get_stats() -> dict:
    with _stats_lock:
        s = dict(_stats)
    s["size"] = len(_cache)
    s["max_size"] = _CACHE_MAX_SIZE
    s["default_ttl_s"] = _DEFAULT_TTL_S
    return s


def _matter_version(matter_id: Optional[str]) -> int:
    if not matter_id:
        return 0
    with _versions_lock:
        return _matter_versions.get(matter_id, 0)


def invalidate_matter(matter_id: Optional[str]) -> None:
    """Invalidate all cached responses for a matter. Call after any write
    that could change the response shape for that matter."""
    if not matter_id:
        return
    with _versions_lock:
        _matter_versions[matter_id] = _matter_versions.get(matter_id, 0) + 1
    _bump_stat("invalidate")


def _cache_key(
    endpoint: str,
    matter_id: Optional[str],
    path_kwargs: Optional[dict] = None,
    extra_args: Optional[Iterable] = None,
) -> tuple:
    args_items = tuple(sorted(request.args.items(multi=True)))
    path_items = tuple(sorted((path_kwargs or {}).items()))
    extra_items = tuple(sorted(extra_args)) if extra_args else ()
    version = _matter_version(matter_id)
    return (endpoint, matter_id, version, args_items, path_items, extra_items)


def invalidates_matter(matter_id_arg: str = "matter_id"):
    """Decorator: after a successful write handler runs, bump the matter's
    cache version so all its cached responses are dropped on next read."""
    def deco(fn: Callable):
        @wraps(fn)
        def wrapper(*args, **kwargs):
            result = fn(*args, **kwargs)
            # Only invalidate on 2xx.
            status = None
            if isinstance(result, tuple) and len(result) >= 2 and isinstance(result[1], int):
                status = result[1]
            else:
                status = getattr(result, "status_code", None)
            if status is None or 200 <= status < 300:
                # Try body first (POST/PUT), then query args, then header.
                mid = None
                if request.method in ("POST", "PUT"):
                    try:
                        body = request.get_json(silent=True) or {}
                        mid = body.get(matter_id_arg)
                    except Exception:
                        mid = None
                if not mid:
                    mid = request.args.get(matter_id_arg) or request.headers.get("X-Matter-Id")
                invalidate_matter(mid)
            return result
        return wrapper
    return deco


def cached_endpoint(
    ttl: int = _DEFAULT_TTL_S,
    matter_id_arg: str = "matter_id",
    extra_key_headers: Optional[Iterable[str]] = None,
):
    """Decorator: cache the JSON response of a GET endpoint for `ttl` seconds.

    Cache key = (endpoint, matter_id, matter_version, sorted(request.args)).
    Bypass with `?nocache=1`. Non-GET requests are never cached.

    matter_id resolution order:
      1. request.args[matter_id_arg]
      2. request.headers["X-Matter-Id"]
    """
    def deco(fn: Callable):
        endpoint_name = fn.__name__

        @wraps(fn)
        def wrapper(*args, **kwargs):
            if request.method != "GET":
                return fn(*args, **kwargs)
            if request.args.get("nocache"):
                _bump_stat("bypass")
                return fn(*args, **kwargs)

            matter_id = request.args.get(matter_id_arg) or request.headers.get("X-Matter-Id")
            header_extras = None
            if extra_key_headers:
                header_extras = [(h, request.headers.get(h, "")) for h in extra_key_headers]

            key = _cache_key(endpoint_name, matter_id, kwargs, header_extras)

            with _cache_lock:
                hit = _cache.get(key)
            if hit is not None:
                _bump_stat("hit")
                # Return a fresh Response each hit so no state on the cached
                # object can leak between requests.
                body, status, content_type = hit
                return Response(body, status=status, mimetype=content_type)

            _bump_stat("miss")
            result = fn(*args, **kwargs)
            _ = ttl  # accepted for forward-compat; single-TTL cache today.

            snapshot = _snapshot_response(result)
            if snapshot is not None:
                with _cache_lock:
                    _cache[key] = snapshot
            return result

        return wrapper

    return deco


def _is_cacheable(result: Any) -> bool:
    """Heuristic: cache Response objects with 2xx status, or bare data (which
    Flask serializes at 200)."""
    # Tuple form: (body, status, [headers])
    if isinstance(result, tuple):
        if len(result) >= 2 and isinstance(result[1], int):
            return 200 <= result[1] < 300
        return True
    # Flask Response
    status = getattr(result, "status_code", None)
    if status is not None:
        return 200 <= status < 300
    return True


def _snapshot_response(result: Any) -> Optional[tuple]:
    """Freeze a Flask handler return into (bytes, status, content_type) so we
    can rebuild a fresh Response on every hit. Returns None if the value
    isn't cacheable."""
    if not _is_cacheable(result):
        return None
    # Flask Response
    if isinstance(result, Response):
        return (result.get_data(), result.status_code, result.mimetype or "application/json")
    # Handlers may return jsonified data directly; if it looks like a
    # Response-like object without being Response, don't try to guess.
    return None


def get_or_none(endpoint: str, matter_id: Optional[str], extra_key: tuple) -> Any:
    """Manual cache lookup for endpoints that can't use the decorator (e.g.
    POST /query where the key is derived from the request body, not args)."""
    version = _matter_version(matter_id)
    key = (endpoint, matter_id, version, (), (), extra_key)
    with _cache_lock:
        hit = _cache.get(key)
    if hit is not None:
        _bump_stat("hit")
    else:
        _bump_stat("miss")
    return hit


def set_manual(endpoint: str, matter_id: Optional[str], extra_key: tuple, value: Any) -> None:
    """Store a value under a manually-constructed key. Pair with `get_or_none`."""
    if not _is_cacheable(value):
        return
    version = _matter_version(matter_id)
    key = (endpoint, matter_id, version, (), (), extra_key)
    with _cache_lock:
        _cache[key] = value


def clear_all() -> None:
    """Nuke the entire cache. Useful in tests and when a global schema change
    invalidates every matter."""
    with _cache_lock:
        _cache.clear()
    with _versions_lock:
        _matter_versions.clear()
