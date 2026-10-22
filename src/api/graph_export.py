"""
Whole-graph export for agents (/api/tools/export).

Instead of navigating the graph through the other /api/tools endpoints, an
agent with a sandbox (Codex) can download the matter's graph as one SQLite
file and query it locally however it likes (sqlite3, FTS5 search, joins,
recursive CTEs). The endpoint builds the file, uploads it to S3 and returns
a short-lived presigned download URL plus a schema/usage note.

- One gzipped SQLite file per (env, matter, graph version). The version is a
  fingerprint of cheap indexed aggregates (entity/edge/document counts and
  last update times), so repeat calls for an unchanged graph reuse the
  uploaded file and only sign a new URL.
- Entity, document and obligation ids are small integers; the entity UUID is
  kept in `entities.uuid` and `documents.document_id` is the Iqidis document
  id (what read_document accepts), like `source.document_id` elsewhere.
- `binds` edges are stored collapsed. Extraction fans each inferred
  obligation out to every pair of entities near it (one 12k-entity matter
  has 384k `binds` rows for ~2k obligation/document pairs), so they are
  stored once per obligation in `obligations` + `obligation_parties`.

Storage: uploads go to s3://$KG_EXPORT_BUCKET/kg-exports/ (default bucket
iqidis-artifact). The URL expires after KG_EXPORT_URL_TTL_S, but that does
not delete the object.
TODO: create an S3 lifecycle rule that expires objects under `kg-exports/`
(e.g. after 1 day, plus noncurrent versions if the bucket is versioned).
Until that rule exists, exports accumulate in the bucket; one file per
graph version per matter.
"""
import gzip
import hashlib
import json
import os
import sqlite3
import threading
import time
from collections import defaultdict
from datetime import datetime, timezone
from typing import Dict, Tuple

import psycopg2.extensions
from flask import g, jsonify

from ..core.s3_fetcher import _get_s3_client
from .env_context import request_env
from .tools import tools, ToolError, _INTERNAL_PROPS, _cursor, _require_built

EXPORT_FORMAT_VERSION = 1
EXPORT_PREFIX = 'kg-exports'
EXPORT_STATEMENT_TIMEOUT_MS = 20000
URL_TTL_S = int(os.getenv('KG_EXPORT_URL_TTL_S', '900'))

# Concurrent requests for the same export wait for one build instead of
# each reading the whole graph.
_build_locks: Dict[Tuple[str, str], threading.Lock] = defaultdict(threading.Lock)
_build_locks_guard = threading.Lock()

SCHEMA_SQL = """
CREATE TABLE meta (key TEXT PRIMARY KEY, value TEXT);
CREATE TABLE documents (
    id INTEGER PRIMARY KEY,
    document_id TEXT,              -- Iqidis document id (read_document accepts it)
    filename TEXT
);
CREATE TABLE entities (
    id INTEGER PRIMARY KEY,
    uuid TEXT NOT NULL,            -- entity id used by the other kg_* tools
    type TEXT NOT NULL,            -- Person, Organization, Fact, Document, Date, Money, ...
    name TEXT NOT NULL,
    text TEXT,                     -- full sentence for Fact entities
    props TEXT,                    -- JSON, extraction properties
    confidence TEXT
);
CREATE TABLE aliases (entity_id INTEGER NOT NULL, alias TEXT NOT NULL);
CREATE TABLE edges (
    id INTEGER PRIMARY KEY,
    src INTEGER NOT NULL,          -- entities.id
    rel TEXT NOT NULL,             -- relation type, e.g. represents, party_to, about
    dst INTEGER NOT NULL,          -- entities.id
    doc INTEGER,                   -- documents.id the relation was extracted from
    props TEXT                     -- JSON
);
CREATE TABLE obligations (
    id INTEGER PRIMARY KEY,
    text TEXT NOT NULL,
    doc INTEGER                    -- documents.id
);
CREATE TABLE obligation_parties (
    obligation_id INTEGER NOT NULL,
    entity_id INTEGER NOT NULL,
    side TEXT NOT NULL             -- 'source' or 'target' of the original binds edges
);
CREATE TABLE mentions (
    entity_id INTEGER NOT NULL,
    doc INTEGER,                   -- documents.id
    surface TEXT,
    snippet TEXT                   -- surrounding text in the document
);
CREATE VIRTUAL TABLE search USING fts5(
    text, kind UNINDEXED, ref UNINDEXED, tokenize = 'porter unicode61');
"""

INDEX_SQL = """
CREATE INDEX idx_entities_type ON entities (type);
CREATE INDEX idx_entities_name ON entities (name COLLATE NOCASE);
CREATE INDEX idx_entities_uuid ON entities (uuid);
CREATE INDEX idx_aliases_entity ON aliases (entity_id);
CREATE INDEX idx_edges_src ON edges (src, rel);
CREATE INDEX idx_edges_dst ON edges (dst, rel);
CREATE INDEX idx_edges_rel ON edges (rel);
CREATE INDEX idx_edges_doc ON edges (doc);
CREATE INDEX idx_obligation_parties_entity ON obligation_parties (entity_id);
CREATE INDEX idx_obligation_parties_obligation ON obligation_parties (obligation_id);
CREATE INDEX idx_mentions_entity ON mentions (entity_id);
CREATE INDEX idx_mentions_doc ON mentions (doc);
CREATE VIEW relations AS
    SELECT e.id, e.src, s.type AS src_type, s.name AS src_name, e.rel,
           e.dst, t.type AS dst_type, t.name AS dst_name, e.doc, d.filename, e.props
    FROM edges e
    JOIN entities s ON s.id = e.src
    JOIN entities t ON t.id = e.dst
    LEFT JOIN documents d ON d.id = e.doc;
"""

README = """\
SQLite database of one matter's extracted graph (entities, relations, facts, mentions), with an FTS5 index.
Tables (integer ids; documents.document_id is the Iqidis document id, entities.uuid the entity id used by other tools):
- entities(id, uuid, type, name, text, props, confidence): type is Person, Organization, Fact, Document, Date, Money, \
Reference, Clause, Location, ...; Fact entities hold an extracted sentence in `text`.
- aliases(entity_id, alias): other names for an entity.
- edges(id, src, rel, dst, doc, props): directed relations between entities, with the source document. \
View `relations` adds entity names/types and the filename.
- obligations(id, text, doc) + obligation_parties(obligation_id, entity_id, side): inferred obligations and the entities they bind.
- mentions(entity_id, doc, surface, snippet): where an entity appears in a document, with surrounding text.
- documents(id, document_id, filename); meta(key, value): export info and counts.
- search: FTS5 over entity names, aliases, fact text, mention snippets and obligations; \
columns text, kind (name|alias|fact|mention|obligation), ref (entities.id, or obligations.id for kind=obligation).
The same real person may appear as several entities.
Examples:
  SELECT kind, ref, snippet(search, 0, '[', ']', '...', 12) FROM search WHERE search MATCH 'guarantee' LIMIT 20;
  SELECT rel, dst_type, dst_name, filename FROM relations WHERE src = 42 ORDER BY rel;
  SELECT e.name, d.filename, m.snippet FROM mentions m JOIN entities e ON e.id = m.entity_id \
JOIN documents d ON d.id = m.doc WHERE e.name LIKE '%Smith%';"""


class _TimedTupleCursor(psycopg2.extensions.cursor):
    """Plain tuple cursor (the export reads hundreds of thousands of rows;
    dict rows cost noticeably more) that reports into Server-Timing like the
    other tools."""

    def execute(self, query, vars=None):
        t0 = time.perf_counter()
        try:
            return super().execute(query, vars)
        finally:
            g.tools_db_queries = g.get('tools_db_queries', 0) + 1
            g.tools_db_ms = g.get('tools_db_ms', 0.0) + (time.perf_counter() - t0) * 1000


def _graph_fingerprint(cur, matter_id: str) -> Tuple[str, Dict]:
    """Version of the matter's graph from indexed aggregates (~tens of ms).
    Any extraction, merge or delete changes at least one of these."""
    cur.execute(
        """SELECT (SELECT COUNT(*) FROM kg_entities WHERE matter_id = %(m)s AND status = 'active'),
                  (SELECT MAX(updated_at) FROM kg_entities WHERE matter_id = %(m)s),
                  (SELECT COUNT(*) FROM kg_edges WHERE matter_id = %(m)s),
                  (SELECT COUNT(*) FROM kg_documents WHERE matter_id = %(m)s),
                  (SELECT MAX(processed_at) FROM kg_documents WHERE matter_id = %(m)s)""",
        {'m': matter_id})
    row = cur.fetchone()
    raw = json.dumps([EXPORT_FORMAT_VERSION, matter_id, *[str(v) for v in row]])
    return hashlib.sha256(raw.encode()).hexdigest()[:16], {'entities': row[0], 'relations': row[2], 'documents': row[3]}


def build_export(cur, matter_id: str) -> Tuple[bytes, Dict]:
    """Read the matter's graph with `cur` (tuple rows) and return the SQLite
    database file as bytes, plus row counts."""
    db = sqlite3.connect(':memory:')
    db.executescript(SCHEMA_SQL)
    search_rows = []

    cur.execute("SELECT id, doc_id, filename FROM kg_documents WHERE matter_id = %s ORDER BY added_at, id",
                (matter_id,))
    doc_ids = {}
    doc_rows = []
    for i, (kg_id, iqidis_id, filename) in enumerate(cur.fetchall(), 1):
        doc_ids[str(kg_id)] = i
        doc_rows.append((i, str(iqidis_id or kg_id), filename))
    db.executemany('INSERT INTO documents VALUES (?, ?, ?)', doc_rows)

    cur.execute(
        """SELECT id, type, canonical_name, properties, confidence FROM kg_entities
           WHERE matter_id = %s AND status = 'active' ORDER BY type, canonical_name""",
        (matter_id,))
    ent_ids = {}
    ent_rows = []
    for i, (uid, etype, name, props, confidence) in enumerate(cur.fetchall(), 1):
        ent_ids[str(uid)] = i
        props = props if isinstance(props, dict) else {}
        text = props.get('full_text') if etype == 'Fact' else None
        public = {k: v for k, v in props.items()
                  if k not in _INTERNAL_PROPS and k != 'full_text' and v not in ('', None)}
        ent_rows.append((i, str(uid), etype, name, text,
                         json.dumps(public, ensure_ascii=False) if public else None, confidence))
        search_rows.append((text or name, 'fact' if etype == 'Fact' else 'name', i))
    db.executemany('INSERT INTO entities VALUES (?, ?, ?, ?, ?, ?, ?)', ent_rows)

    cur.execute(
        """SELECT a.entity_id, a.alias_text FROM kg_aliases a
           JOIN kg_entities e ON e.id = a.entity_id
           WHERE e.matter_id = %s AND e.status = 'active'""",
        (matter_id,))
    alias_rows = [(ent_ids[str(eid)], alias) for eid, alias in cur.fetchall() if str(eid) in ent_ids]
    db.executemany('INSERT INTO aliases VALUES (?, ?)', alias_rows)
    search_rows.extend((alias, 'alias', eid) for eid, alias in alias_rows)

    # Two ranges instead of `relation_type <> 'binds'`: with `<>` the planner
    # can pick a parallel seq scan of the whole kg_edges table for matters
    # with many binds rows (0.9 s vs 0.1 s on a 430k-edge matter).
    cur.execute(
        """SELECT source_entity_id, relation_type, target_entity_id, provenance_doc_id, properties::text
           FROM kg_edges
           WHERE matter_id = %s AND (relation_type < 'binds' OR relation_type > 'binds')""",
        (matter_id,))
    edge_rows = []
    for src, rel, dst, doc, props in cur.fetchall():
        s, t = ent_ids.get(str(src)), ent_ids.get(str(dst))
        if s is None or t is None:
            continue
        edge_rows.append((s, rel, t, doc_ids.get(str(doc)) if doc else None,
                          props if props and props != '{}' else None))
    db.executemany('INSERT INTO edges (src, rel, dst, doc, props) VALUES (?, ?, ?, ?, ?)', edge_rows)

    cur.execute(
        """SELECT properties->>'obligation', provenance_doc_id,
                  ARRAY_AGG(DISTINCT source_entity_id), ARRAY_AGG(DISTINCT target_entity_id)
           FROM kg_edges WHERE matter_id = %s AND relation_type = 'binds'
           GROUP BY 1, 2""",
        (matter_id,))
    obligation_rows, party_rows = [], []
    for i, (text, doc, sources, targets) in enumerate(cur.fetchall(), 1):
        obligation_rows.append((i, text or '', doc_ids.get(str(doc)) if doc else None))
        for side, members in (('source', sources), ('target', targets)):
            party_rows.extend((i, ent_ids[str(e)], side) for e in members or () if str(e) in ent_ids)
        if text:
            search_rows.append((text, 'obligation', i))
    db.executemany('INSERT INTO obligations VALUES (?, ?, ?)', obligation_rows)
    db.executemany('INSERT INTO obligation_parties VALUES (?, ?, ?)', party_rows)

    cur.execute(
        """SELECT m.entity_id, m.doc_id, m.surface_text, m.context_snippet
           FROM kg_mentions m JOIN kg_entities e ON e.id = m.entity_id
           WHERE e.matter_id = %s AND e.status = 'active'
           ORDER BY m.doc_id, m.span_start""",
        (matter_id,))
    mention_rows = [(ent_ids[str(eid)], doc_ids.get(str(doc)) if doc else None, surface, snippet)
                    for eid, doc, surface, snippet in cur.fetchall() if str(eid) in ent_ids]
    db.executemany('INSERT INTO mentions VALUES (?, ?, ?, ?)', mention_rows)
    search_rows.extend((snippet, 'mention', eid) for eid, _, _, snippet in mention_rows if snippet)

    db.executemany('INSERT INTO search (text, kind, ref) VALUES (?, ?, ?)', search_rows)
    db.executescript(INDEX_SQL)

    counts = {'documents': len(doc_rows), 'entities': len(ent_rows), 'aliases': len(alias_rows),
              'relations': len(edge_rows), 'obligations': len(obligation_rows),
              'obligation_parties': len(party_rows), 'mentions': len(mention_rows)}
    meta = {'matter_id': matter_id, 'format_version': str(EXPORT_FORMAT_VERSION),
            'exported_at': datetime.now(timezone.utc).isoformat(timespec='seconds'),
            'readme': README, **{f'count_{k}': str(v) for k, v in counts.items()}}
    db.executemany('INSERT INTO meta VALUES (?, ?)', meta.items())
    db.commit()
    data = db.serialize()
    db.close()
    return data, counts


@tools.route('/export')
def tool_export():
    """Presigned download URL for the matter's whole graph as a gzipped
    SQLite file. Builds and uploads it when this graph version has not been
    exported yet."""
    cur = _cursor()
    _require_built(cur)
    conn = g.tools_conn
    tcur = conn.cursor(cursor_factory=_TimedTupleCursor)
    tcur.execute(f'SET LOCAL statement_timeout = {EXPORT_STATEMENT_TIMEOUT_MS}')

    matter_id = g.matter_id
    env = request_env() or os.getenv('APP_ENV', 'development').strip().lower()
    version, live_counts = _graph_fingerprint(tcur, matter_id)
    bucket = os.getenv('KG_EXPORT_BUCKET', 'iqidis-artifact')
    key = f'{EXPORT_PREFIX}/{env}/{matter_id}/{version}.sqlite.gz'
    s3 = _get_s3_client()

    with _build_locks_guard:
        lock = _build_locks[(env, matter_id)]
    with lock:
        head = _head(s3, bucket, key)
        cached = head is not None
        if cached:
            size = head['ContentLength']
            meta = head.get('Metadata') or {}
            exported_at = meta.get('exported-at')
            counts = json.loads(meta['counts']) if meta.get('counts') else live_counts
            sqlite_bytes = int(meta['sqlite-bytes']) if meta.get('sqlite-bytes') else None
        else:
            data, counts = build_export(tcur, matter_id)
            # Level 1: ~0.3 s vs ~1.1 s at the default level, ~10% larger.
            body = gzip.compress(data, compresslevel=1)
            size, sqlite_bytes = len(body), len(data)
            exported_at = datetime.now(timezone.utc).isoformat(timespec='seconds')
            try:
                s3.put_object(
                    Bucket=bucket, Key=key, Body=body,
                    ContentType='application/gzip',
                    ServerSideEncryption='AES256',
                    Metadata={'exported-at': exported_at, 'counts': json.dumps(counts),
                              'sqlite-bytes': str(sqlite_bytes), 'matter-id': matter_id})
            except Exception as e:
                raise ToolError(502, 'upload_failed', f'could not store the export: {e}')

    url = s3.generate_presigned_url('get_object', Params={'Bucket': bucket, 'Key': key}, ExpiresIn=URL_TTL_S)
    return jsonify({
        'status': 'ready',
        'url': url,
        'expires_in': URL_TTL_S,
        'format': 'sqlite',
        'compression': 'gzip',
        'bytes': size,
        'sqlite_bytes': sqlite_bytes,
        'graph_version': version,
        'exported_at': exported_at,
        'cached': cached,
        'counts': counts,
        'usage': 'curl -sSfL "<url>" | gunzip > graph.sqlite && sqlite3 graph.sqlite '
                 '"SELECT value FROM meta WHERE key = \'readme\'"',
        'schema': README,
    })


def _head(s3, bucket: str, key: str):
    from botocore.exceptions import ClientError
    try:
        return s3.head_object(Bucket=bucket, Key=key)
    except ClientError as e:
        if e.response.get('Error', {}).get('Code') in ('404', 'NoSuchKey', 'NotFound'):
            return None
        raise ToolError(502, 'storage_unavailable', f'could not check the export store: {e}')
