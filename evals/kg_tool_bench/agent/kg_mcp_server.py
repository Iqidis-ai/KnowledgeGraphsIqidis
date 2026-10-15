#!/usr/bin/env python3
"""
Minimal stdio MCP server exposing the /api/tools/* endpoints as agent tools.

Used by run_agent_eval.py: each Claude session gets only these tools. The
server calls the tools blueprint in-process (Flask test client), with the
matter / env / token fixed by environment variables, so the model never
supplies a matter id. Every call is appended to $KG_EVAL_LOG (JSONL) with
latency, DB round trips and payload size.

The tool names and descriptions here are the ones proposed for Workbench.

Env: KG_EVAL_MATTER, KG_EVAL_ENV, KG_EVAL_LOG, KG_EVAL_VECTOR_CACHE (optional).
"""
import json
import os
import re
import sys
import time
import uuid
from pathlib import Path

ROOT = Path(__file__).resolve().parents[3]
# MCP speaks JSON-RPC on stdout; anything else printed (import-time banners,
# library warnings) must go to stderr or it corrupts the stream.
_PROTO_OUT = sys.stdout
sys.stdout = sys.stderr
sys.path.insert(0, str(ROOT))
os.environ.setdefault('KG_TOOLS_TOKEN', f'eval-{uuid.uuid4().hex}')

TOOLS = [
    {
        'name': 'kg_overview',
        'description': (
            "Orientation for this matter's knowledge graph: build status, entity/relation counts, "
            "the most connected entities (with ids) and the most common relation types. Call once at "
            "the start if you don't know the matter's key players."),
        'inputSchema': {'type': 'object', 'properties': {}},
        'path': lambda a: ('/api/tools/overview', {}),
    },
    {
        'name': 'kg_search',
        'description': (
            "Find entities (people, organizations, documents, references, money, dates, locations, clauses) "
            "and extracted facts (sentences, with source documents). You choose how matching works; there "
            "is no blended ranking, and every hit says why it matched.\n"
            "- exact: the query as a phrase (substring) in a name, alias or fact text. For known names, "
            "case numbers, amounts, quoted wording.\n"
            "- keywords: individual words, matched at word starts ('tax' matches 'taxes'). match=any "
            "(default) returns items with at least one word, ordered by how many words matched; match=all "
            "requires every word. Searches names, aliases and fact text.\n"
            "- fuzzy: spelling similarity on names and aliases only (typos, OCR errors, name variants). "
            "Each hit has a 0-1 score.\n"
            "- semantic: meaning similarity via embeddings, for concepts or roles ('the trial judge', "
            "'title company'). Covers named entities only: facts and uploaded-document entities are not "
            "embedded, so use exact/keywords for those. Each hit has a 0-1 score; low scores mean weak matches.\n"
            "The same real person may appear as several entities."),
        'inputSchema': {
            'type': 'object',
            'properties': {
                'query': {'type': 'string', 'description': 'Text to search for (2+ characters).'},
                'mode': {'type': 'string', 'enum': ['exact', 'keywords', 'fuzzy', 'semantic']},
                'match': {'type': 'string', 'enum': ['any', 'all'],
                          'description': 'keywords mode only. Default any.'},
                'scope': {'type': 'string', 'enum': ['entities', 'facts', 'all'],
                          'description': 'Search entities, facts, or both (default all).'},
                'types': {'type': 'array', 'items': {'type': 'string'},
                          'description': 'Optional entity types filter, e.g. ["Person","Organization"].'},
                'limit': {'type': 'integer', 'description': 'Max entities and max facts returned (default 10, max 50).'},
            },
            'required': ['query', 'mode'],
        },
        'path': lambda a: ('/api/tools/search', {'q': a.get('query', ''), 'mode': a.get('mode'),
                                                 'match': a.get('match'), 'scope': a.get('scope'),
                                                 'types': ','.join(a.get('types') or []),
                                                 'limit': a.get('limit')}),
    },
    {
        'name': 'kg_get_entity',
        'description': (
            "Everything the graph holds about one entity: properties, aliases, relations grouped by type "
            "(each with the other entity, source document and an evidence snippet), linked facts, and the "
            "documents that mention it. Large entities are truncated; pass relation_types to focus."),
        'inputSchema': {
            'type': 'object',
            'properties': {
                'entity_id': {'type': 'string', 'description': 'Entity id from kg_search / kg_overview.'},
                'relation_types': {'type': 'array', 'items': {'type': 'string'},
                                   'description': 'Only these relation types, e.g. ["represents","party_to"].'},
                'limit_per_type': {'type': 'integer', 'description': 'Items per relation group (default 5, max 50).'},
            },
            'required': ['entity_id'],
        },
        'path': lambda a: (f"/api/tools/entity/{a.get('entity_id', '')}",
                           {'relation_type': ','.join(a.get('relation_types') or []),
                            'limit_per_type': a.get('limit_per_type')}),
    },
    {
        'name': 'kg_find_connections',
        'description': (
            "How two entities are connected: all direct relations between them plus a few shortest "
            "multi-hop paths, each step with its source document."),
        'inputSchema': {
            'type': 'object',
            'properties': {
                'from_entity_id': {'type': 'string'},
                'to_entity_id': {'type': 'string'},
                'max_hops': {'type': 'integer', 'description': 'Default 3, max 4.'},
            },
            'required': ['from_entity_id', 'to_entity_id'],
        },
        'path': lambda a: ('/api/tools/connections', {'from': a.get('from_entity_id'), 'to': a.get('to_entity_id'),
                                                      'max_hops': a.get('max_hops')}),
    },
    {
        'name': 'kg_timeline',
        'description': (
            "Dated entries in chronological order, from Date entities (with linked entities) and dates "
            "found in extracted facts, each with a source document. Optionally scope to one entity and/or "
            "a date range (YYYY-MM-DD). Paginated: returns `total` and `next_offset`; pass offset=next_offset "
            "for the next page, or narrow the date range instead."),
        'inputSchema': {
            'type': 'object',
            'properties': {
                'entity_id': {'type': 'string', 'description': 'Only dates/facts linked to this entity.'},
                'from_date': {'type': 'string', 'description': 'YYYY-MM-DD inclusive.'},
                'to_date': {'type': 'string', 'description': 'YYYY-MM-DD inclusive.'},
                'limit': {'type': 'integer', 'description': 'Entries per page (default 25, max 50).'},
                'offset': {'type': 'integer', 'description': 'Start position; use next_offset from the previous page.'},
            },
        },
        'path': lambda a: ('/api/tools/timeline', {'entity_id': a.get('entity_id'), 'from': a.get('from_date'),
                                                   'to': a.get('to_date'), 'limit': a.get('limit'),
                                                   'offset': a.get('offset')}),
    },
    {
        'name': 'kg_get_document',
        'description': (
            "What the graph extracted from one document: the entities mentioned in it (with snippets), the "
            "facts drawn from it, and the relations it is the source of. Pass a document_id from any "
            "`source`/`sources` field (or a document id from kg_get_entity's mentioned_in)."),
        'inputSchema': {
            'type': 'object',
            'properties': {
                'document_id': {'type': 'string'},
                'types': {'type': 'array', 'items': {'type': 'string'}, 'description': 'Only these entity types.'},
                'relation_types': {'type': 'array', 'items': {'type': 'string'}, 'description': 'Only these relation types.'},
                'limit': {'type': 'integer', 'description': 'Max items per list (default 20, max 100).'},
            },
            'required': ['document_id'],
        },
        'path': lambda a: (f"/api/tools/document/{a.get('document_id', '')}",
                           {'types': ','.join(a.get('types') or []),
                            'relation_type': ','.join(a.get('relation_types') or []), 'limit': a.get('limit')}),
    },
]
BY_NAME = {t['name']: t for t in TOOLS}

_client = None


def _headers():
    return {'Authorization': f"Bearer {os.environ['KG_TOOLS_TOKEN']}",
            'X-Matter-Id': os.environ['KG_EVAL_MATTER'],
            'X-Iqidis-Env': os.environ.get('KG_EVAL_ENV', 'production')}


def client():
    global _client
    if _client is None:
        from flask import Flask
        from src.api import tools as tools_mod
        app = Flask('kg-tools-mcp')
        app.register_blueprint(tools_mod.tools)
        _client = app.test_client()
        _preload_vectors(tools_mod)
    return _client


def _preload_vectors(tools_mod):
    """Eval-only: seed the semantic-search vector cache from a local .npz so
    every session doesn't pull the matter's embeddings over the WAN (that
    transfer is ~30 s from a laptop, ~0 in-region)."""
    path = os.environ.get('KG_EVAL_VECTOR_CACHE')
    if not path or not os.path.exists(path):
        return
    import numpy as np
    data = np.load(path, allow_pickle=False)
    key = (os.environ.get('KG_EVAL_ENV', 'production'), os.environ['KG_EVAL_MATTER'])
    tools_mod._vector_cache[key] = {'ids': [str(x) for x in data['ids']], 'matrix': data['matrix'],
                                    'loaded_at': time.time() + 10 ** 7}


def build_vector_cache(path):
    """Load the matter's vectors once through the real code path and save them."""
    import numpy as np
    from flask import Flask, g
    from src.api import tools as tools_mod
    app = Flask('kg-vector-cache')
    # Over a laptop's WAN link a big matter's vectors take minutes to arrive.
    tools_mod.VECTOR_LOAD_TIMEOUT_MS = 15 * 60 * 1000
    with app.test_request_context(headers=_headers()):
        g.matter_id = os.environ['KG_EVAL_MATTER']
        entry = tools_mod._matter_vectors(tools_mod._cursor())
        tools_mod._release(None)
    np.savez(path, ids=np.array(entry['ids']), matrix=entry['matrix'])
    print(f'saved {len(entry["ids"])} vectors to {path}', file=sys.stderr)


def call_tool(name, args):
    tool = BY_NAME.get(name)
    if not tool:
        return {'error': f'unknown tool {name}'}, True
    path, params = tool['path'](args or {})
    params = {k: v for k, v in params.items() if v not in (None, '')}
    t = time.perf_counter()
    resp = client().get(path, query_string=params, headers=_headers())
    wall = (time.perf_counter() - t) * 1000
    body = resp.get_data(as_text=True)
    timing = resp.headers.get('Server-Timing', '')
    q = re.search(r'queries;desc="(\d+)"', timing)
    log = os.environ.get('KG_EVAL_LOG')
    if log:
        with open(log, 'a') as f:
            f.write(json.dumps({'ts': time.time(), 'tool': name, 'args': args, 'status': resp.status_code,
                                'wall_ms': round(wall, 1), 'queries': int(q.group(1)) if q else 0,
                                'bytes': len(body)}) + '\n')
    return body, resp.status_code >= 400


def handle(msg):
    method, mid = msg.get('method'), msg.get('id')
    if method == 'initialize':
        return {'jsonrpc': '2.0', 'id': mid, 'result': {
            'protocolVersion': msg.get('params', {}).get('protocolVersion', '2025-06-18'),
            'capabilities': {'tools': {}},
            'serverInfo': {'name': 'kg', 'version': '0.1'}}}
    if method == 'tools/list':
        return {'jsonrpc': '2.0', 'id': mid, 'result': {'tools': [
            {k: t[k] for k in ('name', 'description', 'inputSchema')} for t in TOOLS]}}
    if method == 'tools/call':
        p = msg.get('params', {})
        try:
            text, is_error = call_tool(p.get('name'), p.get('arguments') or {})
        except Exception as e:  # surface to the model, keep the server alive
            text, is_error = json.dumps({'error': 'tool_failed', 'message': str(e)}), True
        return {'jsonrpc': '2.0', 'id': mid, 'result': {
            'content': [{'type': 'text', 'text': text}], 'isError': is_error}}
    if method == 'ping':
        return {'jsonrpc': '2.0', 'id': mid, 'result': {}}
    if mid is None:  # notification
        return None
    return {'jsonrpc': '2.0', 'id': mid, 'error': {'code': -32601, 'message': f'unknown method {method}'}}


def main():
    for line in sys.stdin:
        line = line.strip()
        if not line:
            continue
        try:
            out = handle(json.loads(line))
        except Exception as e:
            out = {'jsonrpc': '2.0', 'id': None, 'error': {'code': -32603, 'message': str(e)}}
        if out is not None:
            _PROTO_OUT.write(json.dumps(out) + '\n')
            _PROTO_OUT.flush()


if __name__ == '__main__':
    if len(sys.argv) == 3 and sys.argv[1] == '--build-vector-cache':
        build_vector_cache(sys.argv[2])
    else:
        main()
