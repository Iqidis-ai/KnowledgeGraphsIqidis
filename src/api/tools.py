"""
Agent tool endpoints (/api/tools/*).

Read-only graph primitives for LLM agents (Workbench via the Next.js MCP
server). Design rules, which every endpoint here follows:

- No LLM calls. Every response is plain SQL / graph traversal, so latency is
  bounded by Postgres (statement timeout 3 s, read-only transaction).
- Provenance on everything. Relations, facts and timeline entries carry
  `source: {document_id, filename, snippet}` where `document_id` is the
  Iqidis document id, so the agent can cite or open the document directly.
- Bounded output. Every list is capped and reports `total` / `truncated`
  so the caller knows when there is more.
- Matter comes from the X-Matter-Id header, set by the trusted caller
  (Next.js resolves it from the chat). The model never supplies it.
- Auth: `Authorization: Bearer <KG_TOOLS_TOKEN>`. If KG_TOOLS_TOKEN is not
  configured the endpoints fail closed (503).
"""
import hmac
import json
import os
import re
import threading
import time
import uuid
from collections import OrderedDict, defaultdict
from datetime import date
from typing import Dict, List, Optional, Tuple

import numpy as np
import psycopg2.errors
import psycopg2.extras
from flask import Blueprint, g, jsonify, request

from ..core.config import get_postgres_url, EnvironmentNotConfiguredError
from ..core.extraction.extraction_pipeline import _is_noise_entity_name
from ..core.storage import db_pool
from .env_context import request_env
from cachetools import TTLCache

from .response_cache import cached_endpoint, _matter_version

tools = Blueprint('tools', __name__, url_prefix='/api/tools')

STATEMENT_TIMEOUT_MS = 3000
SNIPPET_CHARS = 300
TIMELINE_MAX_PAGE = 50
TIMELINE_PAGE_BYTES = 20_000
_timeline_cache: TTLCache = TTLCache(maxsize=256, ttl=120)
_timeline_lock = threading.Lock()
DOCUMENT_LIST_MAX = 100
FACT_CHARS = 500

# Property keys that are extraction bookkeeping, not facts about the entity.
_INTERNAL_PROPS = {'chunk_start', 'chunk_end', 'source', 'aliases'}

_STOPWORDS = {
    'the', 'of', 'and', 'a', 'an', 'in', 'on', 'for', 'to', 'by', 'at', 'or',
    'v', 'vs', 'with', 'is', 'was', 'what', 'who', 'when', 'did', 'does',
}


class ToolError(Exception):
    def __init__(self, status: int, code: str, message: str):
        super().__init__(message)
        self.status, self.code, self.message = status, code, message


# ==================== Request plumbing ====================

@tools.before_request
def _authenticate_and_scope():
    expected = os.getenv('KG_TOOLS_TOKEN')
    if not expected:
        raise ToolError(503, 'tools_disabled', 'KG_TOOLS_TOKEN is not configured on the KG server')
    header = request.headers.get('Authorization', '')
    token = header[7:] if header.startswith('Bearer ') else ''
    if not hmac.compare_digest(token.encode(), expected.encode()):
        raise ToolError(401, 'unauthorized', 'missing or invalid bearer token')

    matter_id = request.headers.get('X-Matter-Id') or request.args.get('matter_id')
    if not matter_id:
        raise ToolError(400, 'bad_request', 'X-Matter-Id header required')
    g.matter_id = _require_uuid(matter_id, 'matter_id')


@tools.errorhandler(ToolError)
def _handle_tool_error(exc: ToolError):
    return jsonify({'error': exc.code, 'message': exc.message}), exc.status


@tools.errorhandler(EnvironmentNotConfiguredError)
def _handle_unconfigured_env(exc):
    return jsonify({'error': 'bad_request', 'message': str(exc)}), 400


@tools.errorhandler(psycopg2.errors.QueryCanceled)
def _handle_timeout(exc):
    return jsonify({'error': 'timeout', 'message': 'graph query exceeded time limit; narrow the request'}), 504


@tools.before_request
def _start_timer():
    g.tools_t0 = time.perf_counter()


@tools.after_request
def _server_timing(resp):
    total = (time.perf_counter() - g.get('tools_t0', time.perf_counter())) * 1000
    resp.headers['Server-Timing'] = (
        f'db;dur={g.get("tools_db_ms", 0.0):.1f}, total;dur={total:.1f}, '
        f'queries;desc="{g.get("tools_db_queries", 0)}"')
    return resp


@tools.teardown_request
def _release(exc):
    conn = g.pop('tools_conn', None)
    if conn is not None:
        try:
            conn.rollback()
        except Exception:
            pass
    try:
        db_pool.release_all()
    except Exception:
        pass


class _TimedCursor(psycopg2.extras.RealDictCursor):
    """Counts round trips and DB time per request (reported in the
    Server-Timing header, so callers can see where latency goes)."""

    def execute(self, query, vars=None):
        t0 = time.perf_counter()
        try:
            return super().execute(query, vars)
        finally:
            g.tools_db_queries = g.get('tools_db_queries', 0) + 1
            g.tools_db_ms = g.get('tools_db_ms', 0.0) + (time.perf_counter() - t0) * 1000


def _cursor():
    """Cursor on a read-only transaction with a short statement timeout."""
    conn = g.get('tools_conn')
    if conn is None:
        conn = db_pool.checkout(get_postgres_url(request_env()))
        conn.rollback()
        g.tools_conn = conn
        conn.cursor(cursor_factory=_TimedCursor).execute(
            f'SET TRANSACTION READ ONLY; SET LOCAL statement_timeout = {STATEMENT_TIMEOUT_MS}')
    return conn.cursor(cursor_factory=_TimedCursor)


def _require_uuid(value: Optional[str], name: str) -> str:
    try:
        return str(uuid.UUID(str(value)))
    except (ValueError, TypeError):
        raise ToolError(400, 'bad_request', f'{name} must be a UUID (got {value!r})')


def _int_arg(name: str, default: int, lo: int, hi: int) -> int:
    raw = request.args.get(name)
    if raw is None or raw == '':
        return default
    try:
        return max(lo, min(hi, int(raw)))
    except ValueError:
        raise ToolError(400, 'bad_request', f'{name} must be an integer')


def _list_arg(name: str) -> List[str]:
    raw = request.args.get(name, '')
    return [p.strip() for p in raw.split(',') if p.strip()]


def _clip(text: Optional[str], n: int) -> Optional[str]:
    if text is None:
        return None
    text = ' '.join(str(text).split())
    return text if len(text) <= n else text[:n - 1] + '…'


def _public_props(props) -> Dict:
    if not isinstance(props, dict):
        return {}
    return {k: v for k, v in props.items() if k not in _INTERNAL_PROPS and v not in ('', None)}


def _ref(row: Dict, prefix: str = '') -> Dict:
    return {'id': str(row[f'{prefix}id']), 'name': row[f'{prefix}name'], 'type': row[f'{prefix}type']}


def _source(doc_row: Optional[Dict], snippet: Optional[str] = None) -> Optional[Dict]:
    """Provenance in the shape every tool returns. `document_id` is the Iqidis
    document id when known (what read_document expects), else the KG id."""
    if not doc_row or not doc_row.get('filename'):
        return None
    out = {
        'document_id': str(doc_row.get('iqidis_doc_id') or doc_row['kg_doc_id']),
        'filename': doc_row['filename'],
    }
    if snippet:
        out['snippet'] = _clip(snippet, SNIPPET_CHARS)
    return out


def _graph_status(cur) -> str:
    cur.execute(
        "SELECT 1 FROM kg_entities WHERE matter_id = %s AND status = 'active' LIMIT 1",
        (g.matter_id,))
    if cur.fetchone():
        return 'ready'
    cur.execute(
        "SELECT state FROM extraction_jobs WHERE matter_id = %s ORDER BY created_at DESC LIMIT 1",
        (g.matter_id,))
    row = cur.fetchone()
    return 'building' if row and row['state'] in ('queued', 'running', 'pending') else 'not_built'


def _require_built(cur):
    status = _graph_status(cur)
    if status != 'ready':
        raise ToolError(409, status, f'the knowledge graph for this matter is {status.replace("_", " ")}')


def _get_entity_row(cur, entity_id: str) -> Dict:
    cur.execute(
        """SELECT id, canonical_name AS name, type, properties, confidence
           FROM kg_entities WHERE id = %s AND matter_id = %s AND status = 'active'""",
        (entity_id, g.matter_id))
    row = cur.fetchone()
    if not row:
        _require_built(cur)
        if _find_document(cur, entity_id):
            raise ToolError(404, 'not_found', f'{entity_id} is a document id, not an entity id; '
                                              'use the document tool to see what the graph holds for it')
        raise ToolError(404, 'not_found', f'entity {entity_id} not found in this matter')
    return row


def _find_document(cur, document_id: str) -> Optional[Dict]:
    """kg_documents row for an Iqidis document id or an internal KG id."""
    cur.execute(
        """SELECT id AS kg_doc_id, doc_id AS iqidis_doc_id, filename FROM kg_documents
           WHERE matter_id = %s AND (id = %s OR doc_id = %s) LIMIT 1""",
        (g.matter_id, document_id, document_id))
    return cur.fetchone()


def _entity_stats(cur, entity_ids: List[str], alias_limit: int = 6) -> Dict[str, Dict]:
    """One round trip: relation count, distinct source documents (edge
    provenance + mentions) and aliases for each entity."""
    if not entity_ids:
        return {}
    cur.execute(
        """SELECT x.id,
                  (SELECT COUNT(*) FROM kg_edges ed WHERE ed.matter_id = %(m)s AND ed.source_entity_id = x.id)
                + (SELECT COUNT(*) FROM kg_edges ed WHERE ed.matter_id = %(m)s AND ed.target_entity_id = x.id) AS relations,
                  (SELECT COUNT(DISTINCT d) FROM (
                       SELECT provenance_doc_id AS d FROM kg_edges ed WHERE ed.matter_id = %(m)s AND ed.source_entity_id = x.id
                       UNION SELECT provenance_doc_id FROM kg_edges ed WHERE ed.matter_id = %(m)s AND ed.target_entity_id = x.id
                       UNION SELECT doc_id FROM kg_mentions WHERE entity_id = x.id) u) AS documents,
                  (SELECT ARRAY(SELECT alias_text FROM kg_aliases WHERE entity_id = x.id ORDER BY alias_text LIMIT %(al)s)) AS aliases
           FROM unnest(%(ids)s::uuid[]) AS x(id)""",
        {'m': g.matter_id, 'ids': entity_ids, 'al': alias_limit})
    return {str(r['id']): r for r in cur.fetchall()}


def _aliases(cur, entity_ids: List[str], per_entity: int) -> Dict[str, List[str]]:
    if not entity_ids:
        return {}
    cur.execute(
        "SELECT entity_id, alias_text FROM kg_aliases WHERE entity_id = ANY(%s::uuid[]) ORDER BY alias_text",
        (entity_ids,))
    out: Dict[str, List[str]] = defaultdict(list)
    for r in cur.fetchall():
        bucket = out[str(r['entity_id'])]
        if len(bucket) < per_entity:
            bucket.append(r['alias_text'])
    return out


def _documents(cur, kg_doc_ids) -> Dict[str, Dict]:
    ids = [str(d) for d in set(kg_doc_ids) if d]
    if not ids:
        return {}
    cur.execute(
        """SELECT id AS kg_doc_id, doc_id AS iqidis_doc_id, filename
           FROM kg_documents WHERE matter_id = %s AND id = ANY(%s::uuid[])""",
        (g.matter_id, ids))
    return {str(r['kg_doc_id']): r for r in cur.fetchall()}


def _snippets(cur, pairs: List[Tuple[str, str]]) -> Dict[Tuple[str, str], str]:
    """(kg_doc_id, entity_id) -> context snippet of one mention of that
    entity in that document. Used as evidence for edges, whose own
    provenance_span is not populated."""
    pairs = [(d, e) for d, e in set(pairs) if d and e]
    if not pairs:
        return {}
    cur.execute(
        """SELECT DISTINCT ON (doc_id, entity_id) doc_id, entity_id, context_snippet
           FROM kg_mentions
           WHERE doc_id = ANY(%s::uuid[]) AND entity_id = ANY(%s::uuid[])
             AND length(context_snippet) > 40
           ORDER BY doc_id, entity_id, span_start""",
        (list({d for d, _ in pairs}), list({e for _, e in pairs})))
    wanted = set(pairs)
    return {(str(r['doc_id']), str(r['entity_id'])): r['context_snippet']
            for r in cur.fetchall() if (str(r['doc_id']), str(r['entity_id'])) in wanted}


def _fact_text(props, name: str) -> str:
    props = props if isinstance(props, dict) else {}
    return _clip(props.get('full_text') or name, FACT_CHARS)


# ==================== Date parsing (timeline) ====================

_MONTHS = {m: i for i, m in enumerate(
    ['january', 'february', 'march', 'april', 'may', 'june', 'july', 'august',
     'september', 'october', 'november', 'december'], 1)}
_MONTHS.update({m[:3]: i for m, i in list(_MONTHS.items())})
_MONTHS['sept'] = 9
_MON = r'(jan(?:uary)?|feb(?:ruary)?|mar(?:ch)?|apr(?:il)?|may|june?|july?|aug(?:ust)?|sept?(?:ember)?|oct(?:ober)?|nov(?:ember)?|dec(?:ember)?)\.?'
_DATE_PATTERNS = [
    (re.compile(_MON + r'\s+(\d{1,2})(?:st|nd|rd|th)?,?\s+(\d{4})', re.I), ('mon', 'd', 'y')),
    (re.compile(r'\b(\d{1,2})(?:st|nd|rd|th)?\s+(?:day\s+of\s+)?' + _MON + r',?\s+(\d{4})', re.I), ('d', 'mon', 'y')),
    (re.compile(r'\b(\d{4})-(\d{1,2})-(\d{1,2})\b'), ('y', 'm', 'd')),
    (re.compile(r'\b(\d{1,2})/(\d{1,2})/(\d{4})\b'), ('m', 'd', 'y')),
]


def _extract_dates(text: str) -> List[Tuple[str, str]]:
    """All explicit calendar dates in `text` as (iso, raw). Year-only and
    month-only mentions are ignored; they are too ambiguous to order."""
    out, seen = [], set()
    for pat, order in _DATE_PATTERNS:
        for m in pat.finditer(text or ''):
            parts = dict(zip(order, m.groups()))
            try:
                month = _MONTHS[parts['mon'].lower().rstrip('.')] if 'mon' in parts else int(parts['m'])
                iso = date(int(parts['y']), month, int(parts['d'])).isoformat()
            except (KeyError, ValueError):
                continue
            if iso not in seen:
                seen.add(iso)
                out.append((iso, m.group(0)))
    return out


# ==================== Endpoints ====================

@tools.route('/overview')
@cached_endpoint()
def tool_overview():
    """Matter-level orientation: build status, counts, the most connected
    entities, and the relation types worth filtering on."""
    cur = _cursor()
    status = _graph_status(cur)
    if status != 'ready':
        return jsonify({'status': status, 'message': f'the knowledge graph for this matter is {status.replace("_", " ")}'})

    m = g.matter_id
    cur.execute("SELECT type, COUNT(*) AS n FROM kg_entities WHERE matter_id = %s AND status = 'active' GROUP BY type ORDER BY n DESC", (m,))
    entity_types = {r['type']: r['n'] for r in cur.fetchall()}
    cur.execute("SELECT relation_type, COUNT(*) AS n FROM kg_edges WHERE matter_id = %s GROUP BY relation_type ORDER BY n DESC", (m,))
    rel_rows = cur.fetchall()
    cur.execute("SELECT COUNT(*) AS n FROM kg_documents WHERE matter_id = %s", (m,))
    doc_total = cur.fetchone()['n']
    cur.execute("SELECT COUNT(DISTINCT provenance_doc_id) AS n FROM kg_edges WHERE matter_id = %s", (m,))
    docs_with_relations = cur.fetchone()['n']

    # Most connected named entities (Facts / Documents / Dates excluded: they
    # are reached through entities, not the other way round).
    # Aggregate degree in one pass over the matter's edges and keep the top
    # 500 before touching kg_entities: joining every connected entity first
    # meant ~28k index lookups on a 144k-edge matter (2-6 s, over the 3 s
    # statement timeout).
    cur.execute(
        """SELECT e.id, e.canonical_name AS name, e.type, x.n AS degree
           FROM (SELECT v.eid, COUNT(*) AS n
                 FROM kg_edges ed
                 CROSS JOIN LATERAL (VALUES (ed.source_entity_id), (ed.target_entity_id)) v(eid)
                 WHERE ed.matter_id = %(m)s
                 GROUP BY v.eid ORDER BY n DESC LIMIT 500) x
           JOIN kg_entities e ON e.id = x.eid
           WHERE e.status = 'active' AND e.type NOT IN ('Fact', 'Document', 'Date')
           ORDER BY x.n DESC LIMIT 60""", {'m': m})
    key_entities = [
        {**_ref(r), 'degree': r['degree']} for r in cur.fetchall()
        if not _is_noise_entity_name(r['name'], r['type'])
    ][:25]

    return jsonify({
        'status': 'ready',
        'counts': {
            'entities': sum(entity_types.values()),
            'relations': sum(r['n'] for r in rel_rows),
            'documents': doc_total,
            'documents_with_relations': docs_with_relations,
        },
        'entity_types': entity_types,
        'top_relation_types': [{'relation_type': r['relation_type'], 'count': r['n']} for r in rel_rows[:30]],
        'relation_type_count': len(rel_rows),
        'key_entities': key_entities,
    })


SEARCH_MODES = ('exact', 'keywords', 'fuzzy', 'semantic')
_CANDIDATE_CAP = 400
_FUZZY_THRESHOLD = 0.3

# Semantic search: per-(env, matter) matrix of unit-normalised entity
# embeddings, decoded from kg_embeddings once and reused for a few minutes.
_VECTOR_TTL_S = 600
VECTOR_LOAD_TIMEOUT_MS = 60000
_VECTOR_CACHE_MAX = 16
_vector_cache: 'OrderedDict[tuple, Dict]' = OrderedDict()
_vector_lock = threading.Lock()
_embedder = None


def _query_embedding(text: str) -> np.ndarray:
    global _embedder
    if _embedder is None:
        from ..core import GEMINI_API_KEY
        from ..core.embeddings.vector_store import EmbeddingGenerator
        _embedder = EmbeddingGenerator(GEMINI_API_KEY)
    vec = _embedder.generate_query_embedding(text)
    norm = float(np.linalg.norm(vec))
    if norm == 0.0:  # the generator returns zeros on API failure
        raise ToolError(503, 'semantic_unavailable', 'could not embed the query; use another search mode')
    return vec / norm


def _matter_vectors(cur) -> Dict:
    key = (request_env(), g.matter_id)
    now = time.time()
    with _vector_lock:
        hit = _vector_cache.get(key)
        if hit and now - hit['loaded_at'] < _VECTOR_TTL_S:
            _vector_cache.move_to_end(key)
            return hit
    # ~3 KB per vector: on big matters the transfer, not the query, dominates,
    # and statement_timeout counts send time. Give this one load more room.
    cur.execute(f'SET LOCAL statement_timeout = {VECTOR_LOAD_TIMEOUT_MS}')
    cur.execute(
        """SELECT k.entity_id, k.vector FROM kg_embeddings k
           JOIN kg_entities e ON e.id = k.entity_id
           WHERE e.matter_id = %s AND e.status = 'active'""", (g.matter_id,))
    ids, vecs = [], []
    for r in cur.fetchall():
        v = np.frombuffer(bytes(r['vector']), dtype=np.float32)
        n = float(np.linalg.norm(v))
        if n > 0:  # zero vectors are failed embeddings
            ids.append(str(r['entity_id']))
            vecs.append(v / n)
    cur.execute(f'SET LOCAL statement_timeout = {STATEMENT_TIMEOUT_MS}')
    dims = {v.shape[0] for v in vecs}
    entry = {'ids': ids, 'matrix': np.vstack(vecs) if vecs and len(dims) == 1 else None, 'loaded_at': now}
    with _vector_lock:
        _vector_cache[key] = entry
        while len(_vector_cache) > _VECTOR_CACHE_MAX:
            _vector_cache.popitem(last=False)
    return entry


def _search_terms(q: str) -> List[str]:
    terms = [t for t in re.findall(r"[\w$.,%/'-]+", q.lower()) if len(t) > 1 and t not in _STOPWORDS]
    return list(dict.fromkeys(t.strip('.,') for t in terms if t.strip('.,'))) or [q.lower()]


def _word_start(term: str) -> str:
    """POSIX regex matching `term` at the start of a word, so 'tax' hits
    'taxes' but 'late' does not hit 'relate'."""
    escaped = re.sub(r'([^\w\s])', r'\\\1', term)
    # \m only anchors before a word character ('$72,500' starts with '$').
    return (r'\m' + escaped) if re.match(r'\w', term) else escaped


def _like(s: str) -> str:
    return '%' + s.replace('\\', '\\\\').replace('%', '\\%').replace('_', '\\_') + '%'


@tools.route('/search')
def tool_search():
    """Find entities and/or facts. The caller picks the matching method;
    there is no blended ranking. Every hit says why it matched.

    mode=exact     phrase (substring) match on name, alias or fact text
    mode=keywords  term match, match=any|all; ordered by number of terms matched
    mode=fuzzy     trigram similarity on names/aliases (typos, name variants)
    mode=semantic  embedding similarity; only entities that have embeddings
    """
    q = (request.args.get('q') or '').strip()
    mode = (request.args.get('mode') or '').strip().lower()
    if len(q) < 2:
        raise ToolError(400, 'bad_request', 'q must be at least 2 characters')
    if mode not in SEARCH_MODES:
        raise ToolError(400, 'bad_request', f'mode is required: one of {", ".join(SEARCH_MODES)}')
    match = (request.args.get('match') or 'any').lower()
    if match not in ('any', 'all'):
        raise ToolError(400, 'bad_request', 'match must be any or all')
    scope = (request.args.get('scope') or 'all').lower()
    if scope not in ('entities', 'facts', 'all'):
        raise ToolError(400, 'bad_request', 'scope must be entities, facts or all')
    types = _list_arg('types')
    limit = _int_arg('limit', 10, 1, 50)
    cur = _cursor()

    scope_clause = {'entities': "AND e.type <> 'Fact'", 'facts': "AND e.type = 'Fact'", 'all': ''}[scope]
    type_clause = 'AND e.type = ANY(%(types)s)' if types else ''
    params = {'m': g.matter_id, 'types': types, 'q': q, 'like': _like(q), 'cap': _CANDIDATE_CAP}
    base = f"""FROM kg_entities e LEFT JOIN kg_aliases a ON a.entity_id = e.id
               WHERE e.matter_id = %(m)s AND e.status = 'active' {scope_clause} {type_clause}"""
    meta: Dict = {}

    if mode == 'exact':
        cur.execute(
            f"""SELECT e.id, e.canonical_name AS name, e.type, e.properties,
                       CASE WHEN BOOL_OR(e.canonical_name ILIKE %(like)s) THEN 'name'
                            WHEN BOOL_OR(a.alias_text ILIKE %(like)s) THEN 'alias' ELSE 'text' END AS field
                {base}
                  AND (e.canonical_name ILIKE %(like)s OR a.alias_text ILIKE %(like)s
                       OR (e.type = 'Fact' AND e.properties->>'full_text' ILIKE %(like)s))
                GROUP BY e.id
                ORDER BY COALESCE(BOOL_OR(lower(e.canonical_name) = lower(%(q)s)
                                          OR lower(a.alias_text) = lower(%(q)s)), false) DESC,
                         length(e.canonical_name)
                LIMIT %(cap)s""", params)
        rows = cur.fetchall()
        for r in rows:
            r['matched'] = {'field': r['field']}

    elif mode == 'keywords':
        terms = _search_terms(q)
        meta = {'terms': terms, 'match': match}
        hit_exprs = []
        for i, t in enumerate(terms):
            params[f't{i}'] = _word_start(t)
            # COALESCE: alias_text is NULL for entities without aliases, and a
            # NULL here would null out the whole match count.
            hit_exprs.append(
                f"COALESCE(BOOL_OR(e.canonical_name ~* %(t{i})s OR a.alias_text ~* %(t{i})s "
                f"OR (e.type = 'Fact' AND e.properties->>'full_text' ~* %(t{i})s)), false)")
        n_hits = ' + '.join(f'({h})::int' for h in hit_exprs)
        need = len(terms) if match == 'all' else 1
        cur.execute(
            f"""SELECT e.id, e.canonical_name AS name, e.type, e.properties,
                       ARRAY[{', '.join(hit_exprs)}] AS hits, {n_hits} AS n_hits
                {base}
                GROUP BY e.id
                HAVING {n_hits} >= {need}
                ORDER BY n_hits DESC, length(e.canonical_name)
                LIMIT %(cap)s""", params)
        rows = cur.fetchall()
        for r in rows:
            r['matched'] = {'terms': [t for t, h in zip(terms, r['hits']) if h],
                            'terms_matched': f"{r['n_hits']}/{len(terms)}"}

    elif mode == 'fuzzy':
        meta = {'note': 'fuzzy compares names and aliases only, not fact text'}
        params['th'] = _FUZZY_THRESHOLD
        cur.execute(
            f"""SELECT e.id, e.canonical_name AS name, e.type, e.properties,
                       MAX(GREATEST(similarity(lower(e.canonical_name), lower(%(q)s)),
                                    COALESCE(similarity(lower(a.alias_text), lower(%(q)s)), 0))) AS score
                {base}
                  AND (similarity(lower(e.canonical_name), lower(%(q)s)) >= %(th)s
                       OR similarity(lower(a.alias_text), lower(%(q)s)) >= %(th)s)
                GROUP BY e.id
                ORDER BY score DESC
                LIMIT %(cap)s""", params)
        rows = cur.fetchall()
        for r in rows:
            r['matched'] = {'score': round(float(r['score']), 3)}

    else:  # semantic
        vecs = _matter_vectors(cur)
        cur.execute(
            f"""SELECT COUNT(*) AS total, COUNT(k.entity_id) AS embedded
                FROM kg_entities e LEFT JOIN kg_embeddings k ON k.entity_id = e.id
                WHERE e.matter_id = %(m)s AND e.status = 'active' {scope_clause} {type_clause}""", params)
        cov = cur.fetchone()
        meta = {'coverage': {
            'searchable': cov['embedded'], 'total_in_scope': cov['total'],
            'note': 'semantic search only covers entities with embeddings: named entities (people, '
                    'organizations, places, dates, money, references, clauses). Facts and uploaded-'
                    'document entities are mostly not embedded; use exact or keywords for those.'}}
        rows = []
        if vecs['matrix'] is not None and cov['embedded']:
            scores = vecs['matrix'] @ _query_embedding(q)
            order = np.argsort(-scores)[:_CANDIDATE_CAP]
            top_ids = [vecs['ids'][i] for i in order]
            cur.execute(
                f"""SELECT e.id, e.canonical_name AS name, e.type, e.properties
                    FROM kg_entities e WHERE e.id = ANY(%(ids)s::uuid[]) AND e.matter_id = %(m)s
                      AND e.status = 'active' {scope_clause} {type_clause}""", {**params, 'ids': top_ids})
            by_id = {str(r['id']): r for r in cur.fetchall()}
            for i in order:
                r = by_id.get(vecs['ids'][i])
                if r is not None:
                    r['matched'] = {'score': round(float(scores[i]), 3)}
                    rows.append(r)

    if not rows:
        _require_built(cur)

    entity_rows = [r for r in rows if r['type'] != 'Fact' and not _is_noise_entity_name(r['name'], r['type'])]
    # The same sentence is often stored as several Fact entities (extraction
    # dedups per batch only). Show it once, keeping every id for sources.
    fact_rows, fact_ids_by_key = [], {}
    for r in rows:
        if r['type'] != 'Fact':
            continue
        key = ' '.join(((r['properties'] or {}).get('full_text') or r['name']).lower().split())
        if key in fact_ids_by_key:
            fact_ids_by_key[key].append(str(r['id']))
        else:
            fact_ids_by_key[key] = [str(r['id'])]
            r['_key'] = key
            fact_rows.append(r)
    top, fact_top = entity_rows[:limit], fact_rows[:limit]

    stats = _entity_stats(cur, [str(r['id']) for r in top])
    entities = []
    for r in top:
        st = stats.get(str(r['id'])) or {}
        entities.append({
            **_ref(r),
            'aliases': list(st.get('aliases') or []),
            'relation_count': st.get('relations') or 0,
            'document_count': st.get('documents') or 0,
            'matched': r['matched'],
        })
    all_fact_ids = [i for r in fact_top for i in fact_ids_by_key[r['_key']]]
    fact_sources = _fact_sources(cur, all_fact_ids)
    facts = []
    for r in fact_top:
        srcs = []
        for fid in fact_ids_by_key[r['_key']]:
            for src in fact_sources.get(fid, []):
                if src not in srcs and len(srcs) < 3:
                    srcs.append(src)
        facts.append({
            'id': str(r['id']),
            'text': _fact_text(r['properties'], r['name']),
            'fact_type': (r['properties'] or {}).get('fact_type'),
            'sources': srcs,
            'matched': r['matched'],
        })

    out = {'query': q, 'mode': mode, 'scope': scope, **meta, 'entities': entities, 'facts': facts}
    if mode == 'semantic':
        # Every embedded entity gets a score, so "total matches" is meaningless;
        # results are simply the top `limit` by similarity.
        out['truncated'] = True
    else:
        out.update({
            'entities_total': len(entity_rows),
            'facts_total': len(fact_rows),
            'truncated': len(entity_rows) > limit or len(fact_rows) > limit or len(rows) >= _CANDIDATE_CAP,
        })
    return jsonify(out)


def _fact_sources(cur, fact_ids: List[str], per_fact: int = 3) -> Dict[str, List[Dict]]:
    """Documents a fact came from: its mentions first, else edge provenance."""
    if not fact_ids:
        return {}
    cur.execute(
        """SELECT entity_id AS fid, doc_id FROM kg_mentions WHERE entity_id = ANY(%(ids)s::uuid[])
           UNION
           SELECT CASE WHEN source_entity_id = ANY(%(ids)s::uuid[]) THEN source_entity_id ELSE target_entity_id END,
                  provenance_doc_id
           FROM kg_edges WHERE matter_id = %(m)s
             AND (source_entity_id = ANY(%(ids)s::uuid[]) OR target_entity_id = ANY(%(ids)s::uuid[]))""",
        {'ids': fact_ids, 'm': g.matter_id})
    pairs = [(str(r['fid']), str(r['doc_id'])) for r in cur.fetchall() if r['doc_id']]
    docs = _documents(cur, [d for _, d in pairs])
    out: Dict[str, List[Dict]] = defaultdict(list)
    for fid, did in pairs:
        src = _source(docs.get(did))
        if src and len(out[fid]) < per_fact and src not in out[fid]:
            out[fid].append(src)
    return out


@tools.route('/entity/<entity_id>')
def tool_entity(entity_id):
    """One entity with its relations (grouped by type, each with source
    document + evidence snippet), linked facts, and the documents that
    mention it."""
    entity_id = _require_uuid(entity_id, 'entity_id')
    relation_filter = _list_arg('relation_type')
    per_type = _int_arg('limit_per_type', 5, 1, 50)
    max_groups = _int_arg('max_groups', 12, 1, 100)
    cur = _cursor()
    ent = _get_entity_row(cur, entity_id)
    m = g.matter_id

    rel_clause = 'AND ed.relation_type = ANY(%(rels)s)' if relation_filter else ''
    # Aggregate in SQL: one row per (relation, direction, other entity), so a
    # hub with thousands of edges ships hundreds of small rows, not thousands.
    cur.execute(
        f"""SELECT ed.relation_type, (ed.source_entity_id = %(id)s) AS outgoing,
                   o.id AS o_id, o.canonical_name AS o_name, o.type AS o_type,
                   MAX(ed.confidence::text) AS confidence,
                   ARRAY_AGG(DISTINCT ed.provenance_doc_id) FILTER (WHERE ed.provenance_doc_id IS NOT NULL) AS docs
            FROM kg_edges ed
            JOIN kg_entities o ON o.id = CASE WHEN ed.source_entity_id = %(id)s
                                              THEN ed.target_entity_id ELSE ed.source_entity_id END
            WHERE ed.matter_id = %(m)s AND (ed.source_entity_id = %(id)s OR ed.target_entity_id = %(id)s)
              AND o.status = 'active' {rel_clause}
            GROUP BY 1, 2, 3, 4, 5""",
        {'id': entity_id, 'm': m, 'rels': relation_filter})
    edges = cur.fetchall()

    groups: Dict[Tuple[str, bool], Dict[str, Dict]] = defaultdict(dict)
    facts: Dict[str, Dict] = {}
    for e in edges:
        oid = str(e['o_id'])
        docs_list = [str(d) for d in (e['docs'] or [])]
        if e['o_type'] == 'Fact':
            f = facts.setdefault(oid, {'id': oid, '_name': e['o_name'], 'relation': e['relation_type'], '_docs': []})
            f['_docs'].extend(docs_list)
            continue
        groups[(e['relation_type'], e['outgoing'])][oid] = {
            'relation': e['relation_type'],
            'direction': 'outgoing' if e['outgoing'] else 'incoming',
            'other': {'id': oid, 'name': e['o_name'], 'type': e['o_type']},
            'confidence': e['confidence'],
            '_docs': docs_list,
        }

    ordered = sorted(groups.items(), key=lambda kv: len(kv[1]), reverse=True)
    shown_groups = ordered[:max_groups]
    shown_items = [it for _, items in shown_groups for it in list(items.values())[:per_type]]
    fact_items = list(facts.values())[:10]
    if fact_items:
        cur.execute("SELECT id, properties FROM kg_entities WHERE id = ANY(%s::uuid[])", ([f['id'] for f in fact_items],))
        fprops = {str(r['id']): r['properties'] or {} for r in cur.fetchall()}
        for f in fact_items:
            props = fprops.get(f['id'], {})
            f['text'] = _fact_text(props, f.pop('_name'))
            f['fact_type'] = props.get('fact_type')

    doc_ids = [d for it in shown_items + fact_items for d in it['_docs'][:3]]
    docs = _documents(cur, doc_ids)
    snips = _snippets(cur, [(str(it['_docs'][0]), it['other']['id']) for it in shown_items if it['_docs'][0]]
                      + [(str(it['_docs'][0]), entity_id) for it in shown_items if it['_docs'][0]])

    def finish(it, other_id):
        srcs = []
        all_docs = list(dict.fromkeys(str(x) for x in it.pop('_docs') if x))
        if len(all_docs) > 2:
            it['source_count'] = len(all_docs)
        for i, d in enumerate(all_docs):
            if len(srcs) == 2:
                break
            snippet = (snips.get((d, other_id)) or snips.get((d, entity_id))) if i == 0 and other_id else None
            src = _source(docs.get(d), snippet)
            if src:
                srcs.append(src)
        it['sources'] = srcs
        return it

    relations = [{
        'relation': rel,
        'direction': 'outgoing' if out else 'incoming',
        'total': len(items),
        'items': [finish(it, it['other']['id']) for it in list(items.values())[:per_type]],
    } for (rel, out), items in shown_groups]

    # Documents that mention the entity (where to read more).
    cur.execute(
        """SELECT m.doc_id, COUNT(*) AS n,
                  (ARRAY_AGG(m.context_snippet ORDER BY length(m.context_snippet) DESC))[1] AS snippet
           FROM kg_mentions m WHERE m.entity_id = %s GROUP BY m.doc_id ORDER BY n DESC""",
        (entity_id,))
    mention_rows = cur.fetchall()
    mdocs = _documents(cur, [r['doc_id'] for r in mention_rows[:5]])
    mentioned_in = []
    for r in mention_rows[:5]:
        src = _source(mdocs.get(str(r['doc_id'])), r['snippet'] if r['snippet'] and len(r['snippet']) > 40 else None)
        if src:
            mentioned_in.append({**src, 'mention_count': r['n']})

    return jsonify({
        'entity': {
            **_ref(ent),
            'aliases': _aliases(cur, [entity_id], 30).get(entity_id, []),
            'properties': _public_props(ent['properties']),
            'confidence': ent['confidence'],
        },
        'relations': relations,
        'relation_groups_total': len(ordered),
        'relations_total': sum(len(v) for v in groups.values()),
        'facts': [finish(f, None) for f in fact_items],
        'facts_total': len(facts),
        'mentioned_in': mentioned_in,
        'mentioned_in_total': len(mention_rows),
        'truncated': len(ordered) > max_groups or any(len(v) > per_type for _, v in shown_groups) or len(facts) > len(fact_items),
    })


@tools.route('/connections')
def tool_connections():
    """How two entities are connected: direct relations plus up to `limit`
    shortest paths (bidirectional BFS, max_hops <= 4)."""
    a = _require_uuid(request.args.get('from'), 'from')
    b = _require_uuid(request.args.get('to'), 'to')
    max_hops = _int_arg('max_hops', 3, 1, 4)
    limit = _int_arg('limit', 5, 1, 10)
    if a == b:
        raise ToolError(400, 'bad_request', 'from and to must differ')
    cur = _cursor()
    ends = {a: _get_entity_row(cur, a), b: _get_entity_row(cur, b)}

    frontier_cap = 3000
    # parent[side][node] = (prev_node, edge_row) ; side 0 grows from a, 1 from b
    parent = [{a: None}, {b: None}]
    frontier = [{a}, {b}]
    meets: List[str] = []
    hops = [0, 0]
    while hops[0] + hops[1] < max_hops and not meets and frontier[0] and frontier[1]:
        side = 0 if len(frontier[0]) <= len(frontier[1]) else 1
        cur.execute(
            """SELECT DISTINCT source_entity_id AS s, target_entity_id AS t
               FROM kg_edges WHERE matter_id = %(m)s
                 AND (source_entity_id = ANY(%(f)s::uuid[]) OR target_entity_id = ANY(%(f)s::uuid[]))""",
            {'m': g.matter_id, 'f': list(frontier[side])})
        nxt = set()
        for e in cur.fetchall():
            s, t = str(e['s']), str(e['t'])
            for here, there in ((s, t), (t, s)):
                if here in frontier[side] and there not in parent[side]:
                    parent[side][there] = (here, (s, t))
                    nxt.add(there)
        hops[side] += 1
        frontier[side] = set(list(nxt)[:frontier_cap])
        # At one hop the only possible meet is the other root, i.e. a direct
        # edge; those are reported separately, so keep searching for paths.
        if hops[0] + hops[1] > 1:
            meets = [n for n in nxt if n in parent[1 - side]]

    def walk(node, side):
        steps = []
        while parent[side][node] is not None:
            prev, e = parent[side][node]
            steps.append(e)
            node = prev
        return steps  # edges from `node` back toward the side's root

    paths_edges = []
    for n in meets[:limit]:
        left = list(reversed(walk(n, 0)))
        right = walk(n, 1)
        paths_edges.append(left + right)

    # Details (relation, provenance) only for the edges on the chosen paths.
    pairs = list({pr for es in paths_edges for pr in es})
    node_ids = list({x for pr in pairs for x in pr}) or [a]
    cur.execute("SELECT id, canonical_name AS name, type FROM kg_entities WHERE id = ANY(%s::uuid[])", (node_ids,))
    nodes = {str(r['id']): _ref(r) for r in cur.fetchall()}
    edge_info: Dict[Tuple[str, str], Dict] = {}
    if pairs:
        cur.execute(
            """SELECT DISTINCT ON (source_entity_id, target_entity_id)
                      source_entity_id AS s, target_entity_id AS t, relation_type, provenance_doc_id
               FROM kg_edges WHERE matter_id = %s
                 AND (source_entity_id, target_entity_id) IN (SELECT * FROM unnest(%s::uuid[], %s::uuid[]))
               ORDER BY source_entity_id, target_entity_id, (relation_type = 'binds')""",
            (g.matter_id, [p[0] for p in pairs], [p[1] for p in pairs]))
        edge_info = {(str(r['s']), str(r['t'])): r for r in cur.fetchall()}
    docs = _documents(cur, [e['provenance_doc_id'] for e in edge_info.values()])

    def step(pr):
        e = edge_info.get(pr) or {}
        return {'from': nodes.get(pr[0]), 'relation': e.get('relation_type'), 'to': nodes.get(pr[1]),
                'source': _source(docs.get(str(e.get('provenance_doc_id'))))}

    # All direct relations, regardless of path search.
    cur.execute(
        """SELECT ed.source_entity_id AS s, ed.target_entity_id AS t, ed.relation_type, ed.provenance_doc_id
           FROM kg_edges ed WHERE ed.matter_id = %(m)s
             AND ((ed.source_entity_id = %(a)s AND ed.target_entity_id = %(b)s)
               OR (ed.source_entity_id = %(b)s AND ed.target_entity_id = %(a)s))""",
        {'m': g.matter_id, 'a': a, 'b': b})
    direct_rows = cur.fetchall()
    ddocs = _documents(cur, [r['provenance_doc_id'] for r in direct_rows])
    names = {k: _ref(v) for k, v in ends.items()}
    merged: Dict[Tuple[str, str, str], Dict] = {}
    for r in direct_rows:
        item = merged.setdefault((str(r['s']), r['relation_type'], str(r['t'])), {
            'from': names[str(r['s'])], 'relation': r['relation_type'], 'to': names[str(r['t'])], 'sources': []})
        src = _source(ddocs.get(str(r['provenance_doc_id'])))
        if src and src not in item['sources'] and len(item['sources']) < 3:
            item['sources'].append(src)
    direct = list(merged.values())[:20]

    return jsonify({
        'from': names[a],
        'to': names[b],
        'direct_relations': direct,
        'direct_relations_total': len(merged),
        'paths': [{'hops': len(es), 'steps': [step(e) for e in es]} for es in paths_edges if len(es) > 1],
        'searched_hops': hops[0] + hops[1],
        'connected': bool(meets) or bool(direct_rows),
    })


@tools.route('/timeline')
def tool_timeline():
    """Dated events, oldest first. Built from Date entities (with what they
    are linked to) and from explicit dates inside Fact text. Optional
    entity_id narrows it to that entity's dates and facts."""
    entity_id = request.args.get('entity_id')
    entity_id = _require_uuid(entity_id, 'entity_id') if entity_id else None
    date_from = request.args.get('from') or '0000-00-00'
    date_to = request.args.get('to') or '9999-99-99'
    limit = _int_arg('limit', 25, 1, TIMELINE_MAX_PAGE)
    offset = _int_arg('offset', 0, 0, 1_000_000)
    cur = _cursor()
    m = g.matter_id
    if entity_id:
        _get_entity_row(cur, entity_id)
    else:
        _require_built(cur)

    # Paging re-requests the same list; build it once and serve later pages
    # from a short-lived cache. The matter's write version is in the key, so
    # any graph edit invalidates it.
    cache_key = (request_env(), m, _matter_version(m), entity_id, date_from, date_to)
    with _timeline_lock:
        deduped = _timeline_cache.get(cache_key)
    if deduped is None:
        scope = ''
        params = {'m': m, 'id': entity_id}
        if entity_id:
            # Dates/facts directly linked to the entity, plus facts that name it.
            cur.execute("SELECT canonical_name FROM kg_entities WHERE id = %s", (entity_id,))
            names = [cur.fetchone()['canonical_name']] + _aliases(cur, [entity_id], 10).get(entity_id, [])
            params['pats'] = [f'%{n}%' for n in names if len(n) >= 4] or ['%\x00%']
            scope = """AND (e.id IN (SELECT CASE WHEN source_entity_id = %(id)s THEN target_entity_id ELSE source_entity_id END
                                     FROM kg_edges WHERE matter_id = %(m)s AND (source_entity_id = %(id)s OR target_entity_id = %(id)s))
                            OR (e.type = 'Fact' AND e.properties->>'full_text' ILIKE ANY(%(pats)s)))"""

        cur.execute(
            # Only the fields the timeline reads, not the whole properties blob.
            f"""SELECT e.id, e.canonical_name AS name, e.type,
                       jsonb_build_object('full_text', e.properties->>'full_text', 'fact_type', e.properties->>'fact_type',
                                          'due_date', e.properties->>'due_date', 'date', e.properties->>'date',
                                          'context', e.properties->>'context') AS properties
                FROM kg_entities e
                WHERE e.matter_id = %(m)s AND e.status = 'active' AND e.type IN ('Date', 'Fact') {scope}""",
            params)
        rows = cur.fetchall()

        entries = []
        date_ids = []
        for r in rows:
            props = r['properties'] if isinstance(r['properties'], dict) else {}
            if r['type'] == 'Date':
                found = _extract_dates(r['name'])
                if found:
                    date_ids.append(str(r['id']))
                    entries.append({'date': found[0][0], 'date_text': r['name'], 'kind': 'date',
                                    'description': props.get('context') or r['name'], '_id': str(r['id'])})
            else:
                text = props.get('full_text') or r['name']
                for iso, raw in _extract_dates(' '.join(str(props.get(k) or '') for k in ('due_date', 'date')) + ' ' + text)[:3]:
                    entries.append({'date': iso, 'date_text': raw, 'kind': props.get('fact_type') or 'fact',
                                    'description': _clip(text, FACT_CHARS), '_id': str(r['id'])})

        entries = [e for e in entries if date_from <= e['date'] <= date_to]
        # Fully deterministic order so offset-based pages never shift.
        entries.sort(key=lambda e: (e['date'], e['kind'] != 'date', e['_id'], e['date_text']))
        seen, deduped = set(), []
        for e in entries:
            key = (e['date'], e['description'].lower()[:120])
            if key not in seen:
                seen.add(key)
                deduped.append(e)
        with _timeline_lock:
            _timeline_cache[cache_key] = deduped
    # Entries are enriched per page below; copy so the cached list stays clean.
    deduped = [dict(e) for e in deduped]
    total = len(deduped)
    shown = deduped[offset:offset + limit]

    # Linked entities + provenance for the shown entries.
    shown_ids = list({e['_id'] for e in shown})
    links: Dict[str, List[Dict]] = defaultdict(list)
    prov: Dict[str, str] = {}
    if shown_ids:
        cur.execute(
            """SELECT CASE WHEN ed.source_entity_id = ANY(%(ids)s::uuid[]) THEN ed.source_entity_id ELSE ed.target_entity_id END AS anchor,
                      o.id, o.canonical_name AS name, o.type, ed.relation_type, ed.provenance_doc_id
               FROM kg_edges ed
               JOIN kg_entities o ON o.id = CASE WHEN ed.source_entity_id = ANY(%(ids)s::uuid[]) THEN ed.target_entity_id ELSE ed.source_entity_id END
               WHERE ed.matter_id = %(m)s AND o.status = 'active'
                 AND (ed.source_entity_id = ANY(%(ids)s::uuid[]) OR ed.target_entity_id = ANY(%(ids)s::uuid[]))""",
            {'ids': shown_ids, 'm': m})
        for r in cur.fetchall():
            anchor = str(r['anchor'])
            prov.setdefault(anchor, str(r['provenance_doc_id']) if r['provenance_doc_id'] else None)
            link = {**_ref(r), 'relation': r['relation_type']}
            if r['type'] not in ('Date', 'Fact') and len(links[anchor]) < 5 and link not in links[anchor]:
                links[anchor].append(link)
        cur.execute("SELECT DISTINCT ON (entity_id) entity_id, doc_id FROM kg_mentions WHERE entity_id = ANY(%s::uuid[])",
                    (shown_ids,))
        for r in cur.fetchall():
            prov[str(r['entity_id'])] = prov.get(str(r['entity_id'])) or str(r['doc_id'])
    docs = _documents(cur, prov.values())

    for e in shown:
        eid = e.pop('_id')
        e['entity_id'] = eid
        e['related'] = links.get(eid, [])
        e['source'] = _source(docs.get(prov.get(eid) or ''))

    # Size backstop: drop entries from the end of the page until it fits;
    # next_offset then resumes exactly where this page stopped.
    while len(shown) > 1 and len(json.dumps(shown)) > TIMELINE_PAGE_BYTES:
        shown.pop()
    end = offset + len(shown)
    return jsonify({
        'entries': shown,
        'offset': offset,
        'total': total,
        'next_offset': end if end < total else None,
    })


@tools.route('/document/<document_id>')
def tool_document(document_id):
    """What the graph extracted from one document: entities mentioned in it,
    facts drawn from it, and relations whose provenance is this document.
    Accepts the Iqidis document id (as returned in `source.document_id`) or
    the internal KG document id."""
    document_id = _require_uuid(document_id, 'document_id')
    limit = _int_arg('limit', 20, 1, DOCUMENT_LIST_MAX)
    types = _list_arg('types')
    relation_filter = _list_arg('relation_type')
    cur = _cursor()
    doc = _find_document(cur, document_id)
    if not doc:
        _require_built(cur)
        raise ToolError(404, 'not_found', f'document {document_id} is not in this matter\'s graph')
    kg_doc = str(doc['kg_doc_id'])

    # Entities mentioned in the document (kg_mentions is indexed on doc_id).
    type_clause = 'AND e.type = ANY(%(types)s)' if types else ''
    cur.execute(
        f"""SELECT e.id, e.canonical_name AS name, e.type, COUNT(*) AS mentions,
                   (ARRAY_AGG(m.context_snippet ORDER BY length(m.context_snippet) DESC))[1] AS snippet
            FROM kg_mentions m JOIN kg_entities e ON e.id = m.entity_id
            WHERE m.doc_id = %(d)s AND e.status = 'active' AND e.type <> 'Fact' {type_clause}
            GROUP BY e.id ORDER BY mentions DESC, e.canonical_name""",
        {'d': kg_doc, 'types': types})
    ent_rows = [r for r in cur.fetchall() if not _is_noise_entity_name(r['name'], r['type'])]

    # Relations provenanced to the document. Edges to Fact entities are the
    # document's facts, reported separately.
    rel_clause = 'AND ed.relation_type = ANY(%(rels)s)' if relation_filter else ''
    cur.execute(
        f"""SELECT ed.relation_type, ed.confidence,
                   s.id AS s_id, s.canonical_name AS s_name, s.type AS s_type, s.properties AS s_props,
                   t.id AS t_id, t.canonical_name AS t_name, t.type AS t_type, t.properties AS t_props
            FROM kg_edges ed
            JOIN kg_entities s ON s.id = ed.source_entity_id AND s.status = 'active'
            JOIN kg_entities t ON t.id = ed.target_entity_id AND t.status = 'active'
            WHERE ed.matter_id = %(m)s AND ed.provenance_doc_id = %(d)s {rel_clause}
            ORDER BY ed.relation_type, s.canonical_name, t.canonical_name""",
        {'m': g.matter_id, 'd': kg_doc, 'rels': relation_filter})
    relations, seen_rel, facts = [], set(), {}
    for r in cur.fetchall():
        if r['s_type'] == 'Fact' or r['t_type'] == 'Fact':
            side = 's' if r['s_type'] == 'Fact' else 't'
            fid = str(r[f'{side}_id'])
            f = facts.setdefault(fid, {'id': fid, 'text': _fact_text(r[f'{side}_props'], r[f'{side}_name']),
                                       'fact_type': (r[f'{side}_props'] or {}).get('fact_type'), 'about': []})
            other = 't' if side == 's' else 's'
            if len(f['about']) < 5:
                f['about'].append(_ref({'id': r[f'{other}_id'], 'name': r[f'{other}_name'], 'type': r[f'{other}_type']}))
            continue
        key = (str(r['s_id']), r['relation_type'], str(r['t_id']))
        if key in seen_rel:
            continue
        seen_rel.add(key)
        relations.append({'from': _ref({'id': r['s_id'], 'name': r['s_name'], 'type': r['s_type']}),
                          'relation': r['relation_type'],
                          'to': _ref({'id': r['t_id'], 'name': r['t_name'], 'type': r['t_type']}),
                          'confidence': r['confidence']})

    fact_list = list(facts.values())
    return jsonify({
        'document': _source(doc),
        'entities': [{**_ref(r), 'mention_count': r['mentions'],
                      **({'snippet': _clip(r['snippet'], SNIPPET_CHARS)} if r['snippet'] and len(r['snippet']) > 40 else {})}
                     for r in ent_rows[:limit]],
        'entities_total': len(ent_rows),
        'facts': fact_list[:limit],
        'facts_total': len(fact_list),
        'relations': relations[:limit],
        'relations_total': len(relations),
        'truncated': len(ent_rows) > limit or len(fact_list) > limit or len(relations) > limit,
    })
