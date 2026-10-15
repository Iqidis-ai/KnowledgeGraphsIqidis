#!/usr/bin/env python3
"""
Latency + error bench for the /api/tools/* agent endpoints.

Starts the real Flask app (tools blueprint) in-process on a local port,
then drives a fixed workload against one matter:

  1. functional pass   every call once, checks status codes / shape
  2. latency pass      every call `--repeat` times, sequential (cold cache:
                       overview is called with nocache=1)
  3. concurrency pass  the whole workload on `--workers` threads at once

Latency is reported two ways:
  - wall:      measured here, includes the WAN round trip to the DB
  - in-region: python/flask time + DB execution time (EXPLAIN ANALYZE of
               every query, bench-only instrumentation) + 1 ms per DB round
               trip. Excludes result transfer, which in-region is negligible
               for these payload sizes.

Usage (from repo root):
    .venv/bin/python evals/kg_tool_bench/tool_latency.py \
        --matter 107fa33c-cb73-4d7a-bb24-76980eed00b5 --env production
"""
import argparse
import json
import os
import re
import statistics
import sys
import threading
import time
import urllib.error
import urllib.parse
import urllib.request
import uuid
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT))

TOKEN = os.environ.setdefault('KG_TOOLS_TOKEN', f'bench-{uuid.uuid4().hex}')
IN_REGION_RTT_MS = 1.0

# Whitfield workload. Names come from the matter profile; a few are
# deliberately misspelled / partial / role words to exercise fuzzy search.
SEARCHES = [
    'Margaret Calloway-Briggs', 'Calloway Brigs', 'Diana Whitfield', 'Marcus',
    'Eleanor Calloway', 'Homer', 'Daniel Briggs', 'Christian Brown', 'Reinhardt',
    'Cosby', 'Calloway Family Revocable Living Trust', 'Fenoglio',
    'Sterling Court Reporting', 'McPhearson', 'Shipp', 'mediation', 'mediator',
    'taxes', 'rescind', 'summary judgment', 'deposition', 'Rule 11',
    'Stagecoach Lane', 'Bonanza Trail', 'Horse Shoe Hills', 'royalty reservation',
    '$72,500', '12,000', 'heart attack', 'Third Amended Petition',
    'dismissal with prejudice', 'Coonrod', 'Van Vleck', 'Tract 25A',
]
SEMANTIC_SEARCHES = ['the presiding judge', 'court reporter', 'opposing counsel law firm',
                     'the property in dispute', 'title company', 'mediator', 'monthly payment amount']
ENTITY_NAMES = ['Margaret Calloway-Briggs', 'Diana Whitfield', 'Christian Brown',
                'Calloway Family Revocable Living Trust', 'Cosby', 'Daniel Briggs',
                'Eleanor Calloway', 'Coonrod', 'Fenoglio']
CONNECTION_PAIRS = [('Diana Whitfield', 'Calloway Family Revocable Living Trust'),
                    ('Christian Brown', 'Margaret Calloway-Briggs'),
                    ('Eleanor Calloway', 'Margaret Calloway-Briggs'),
                    ('Coonrod', 'Diana Whitfield'),
                    ('Cosby', 'Reinhardt')]


def _instrument_db_exec_time():
    """Bench-only: before each SELECT, run it under EXPLAIN ANALYZE to get the
    DB's own execution time. With that we can report in-region latency as
    python/flask time + DB execution + 1 ms per round trip, instead of
    subtracting a noisy WAN RTT."""
    import time as _t
    from flask import g
    from src.api import tools as tools_mod
    real_execute = tools_mod._TimedCursor.execute

    def execute(self, query, vars=None):
        if isinstance(query, str) and query.lstrip().upper().startswith(('SELECT', 'WITH')):
            t0 = _t.perf_counter()
            psycopg2_cursor = super(tools_mod._TimedCursor, self)
            psycopg2_cursor.execute('EXPLAIN (ANALYZE, FORMAT JSON) ' + query, vars)
            plan = psycopg2_cursor.fetchone()
            plan = plan['QUERY PLAN'][0] if isinstance(plan, dict) else plan[0][0]
            g.bench_exec_ms = g.get('bench_exec_ms', 0.0) + plan['Execution Time'] + plan.get('Planning Time', 0)
            g.bench_explain_wall_ms = g.get('bench_explain_wall_ms', 0.0) + (_t.perf_counter() - t0) * 1000
        return real_execute(self, query, vars)

    tools_mod._TimedCursor.execute = execute


def start_server():
    from flask import Flask, g
    from werkzeug.serving import make_server
    from src.api.tools import tools
    _instrument_db_exec_time()
    app = Flask('kg-tools-bench')
    app.register_blueprint(tools)

    @app.after_request
    def _bench_headers(resp):
        resp.headers['X-Bench-Exec-Ms'] = f"{g.get('bench_exec_ms', 0.0):.1f}"
        resp.headers['X-Bench-Explain-Wall-Ms'] = f"{g.get('bench_explain_wall_ms', 0.0):.1f}"
        return resp
    srv = make_server('127.0.0.1', 0, app, threaded=True)
    threading.Thread(target=srv.serve_forever, daemon=True).start()
    return srv, f'http://127.0.0.1:{srv.server_port}'


def measure_rtt(env):
    import psycopg2
    from src.core.config import get_postgres_url
    conn = psycopg2.connect(get_postgres_url(env))
    cur = conn.cursor()
    samples = []
    for _ in range(12):
        t = time.perf_counter()
        cur.execute('SELECT 1')
        cur.fetchall()
        samples.append((time.perf_counter() - t) * 1000)
    conn.close()
    return statistics.median(samples[2:])


class Client:
    def __init__(self, base, matter, env):
        self.base, self.matter, self.env = base, matter, env

    def get(self, path, params=None, token=TOKEN, matter='__default__'):
        url = f'{self.base}{path}'
        if params:
            url += '?' + urllib.parse.urlencode(params)
        headers = {'X-Iqidis-Env': self.env}
        if token:
            headers['Authorization'] = f'Bearer {token}'
        matter = self.matter if matter == '__default__' else matter
        if matter:
            headers['X-Matter-Id'] = matter
        req = urllib.request.Request(url, headers=headers)
        t = time.perf_counter()
        try:
            with urllib.request.urlopen(req, timeout=60) as r:
                status, body, hdr = r.status, r.read(), r.headers
        except urllib.error.HTTPError as e:
            status, body, hdr = e.code, e.read(), e.headers
        timing = hdr.get('Server-Timing', '')
        exec_ms = float(hdr.get('X-Bench-Exec-Ms') or 0)
        explain_wall = float(hdr.get('X-Bench-Explain-Wall-Ms') or 0)
        wall = (time.perf_counter() - t) * 1000
        m = re.search(r'queries;desc="(\d+)"', timing)
        db = re.search(r'db;dur=([\d.]+)', timing)
        tot = re.search(r'total;dur=([\d.]+)', timing)
        try:
            data = json.loads(body)
        except ValueError:
            data = None
        return {'status': status, 'wall_ms': wall, 'queries': int(m.group(1)) if m else 0,
                'db_ms': float(db.group(1)) if db else 0.0,
                'server_ms': float(tot.group(1)) if tot else wall,
                'exec_ms': exec_ms, 'explain_wall_ms': explain_wall, 'bytes': len(body), 'data': data}


def build_workload(c):
    """Resolve names -> ids via the search tool, then build the call list.
    Each item: (tool, label, path, params, expected_status, check)."""
    ids = {}
    for name in set(ENTITY_NAMES) | {n for p in CONNECTION_PAIRS for n in p}:
        r = c.get('/api/tools/search', {'q': name, 'mode': 'keywords', 'match': 'all', 'scope': 'entities', 'limit': 1})
        ents = (r['data'] or {}).get('entities') or []
        if ents:
            ids[name] = ents[0]['id']
    has = lambda key: lambda d: isinstance(d, dict) and key in d  # noqa: E731

    w = [('overview', 'overview', '/api/tools/overview', {'nocache': 1}, 200, has('key_entities'))]
    for q in SEARCHES:
        for mode in ('exact', 'keywords', 'fuzzy'):
            w.append((f'search:{mode}', q, '/api/tools/search', {'q': q, 'mode': mode}, 200, has('entities')))
    w.append(('search:keywords', 'match=all', '/api/tools/search',
              {'q': 'Diana deposition transcript', 'mode': 'keywords', 'match': 'all'}, 200, has('entities')))
    w.append(('search:exact', 'types=Person', '/api/tools/search',
              {'q': 'Whitfield', 'mode': 'exact', 'types': 'Person'}, 200, has('entities')))
    for q in SEMANTIC_SEARCHES:
        w.append(('search:semantic', q, '/api/tools/search', {'q': q, 'mode': 'semantic'}, 200, has('coverage')))
    for name in ENTITY_NAMES:
        if name in ids:
            w.append(('entity', name, f'/api/tools/entity/{ids[name]}', None, 200, has('relations')))
    if 'Margaret Calloway-Briggs' in ids:
        w.append(('entity', 'Margaret relation_type=represents', f'/api/tools/entity/{ids["Margaret Calloway-Briggs"]}',
                  {'relation_type': 'represents,opposes'}, 200, has('relations')))
    for a, b in CONNECTION_PAIRS:
        if a in ids and b in ids:
            w.append(('connections', f'{a} -> {b}', '/api/tools/connections', {'from': ids[a], 'to': ids[b]}, 200, has('paths')))
    w.append(('timeline', 'all', '/api/tools/timeline', None, 200, has('entries')))
    w.append(('timeline', '2024 only', '/api/tools/timeline', {'from': '2024-01-01', 'to': '2024-12-31', 'limit': 100}, 200, has('entries')))
    for name in ('Diana Whitfield', 'Margaret Calloway-Briggs', 'Calloway Family Revocable Living Trust'):
        if name in ids:
            w.append(('timeline', f'entity={name}', '/api/tools/timeline', {'entity_id': ids[name]}, 200, has('entries')))
    return w, ids


def error_cases(c):
    """(label, response, expected_status)."""
    bogus = str(uuid.uuid4())
    return [
        ('no token', c.get('/api/tools/overview', token=None), 401),
        ('wrong token', c.get('/api/tools/overview', token='nope'), 401),
        ('no matter', c.get('/api/tools/overview', matter=None), 400),
        ('matter not uuid', c.get('/api/tools/overview', matter='abc'), 400),
        ('unbuilt matter overview', c.get('/api/tools/overview', matter=bogus), 200),
        ('unbuilt matter search', c.get('/api/tools/search', {'q': 'anything', 'mode': 'exact'}, matter=bogus), 409),
        ('search without mode', c.get('/api/tools/search', {'q': 'Diana'}), 400),
        ('search bad mode', c.get('/api/tools/search', {'q': 'Diana', 'mode': 'hybrid'}), 400),
        ('search bad scope', c.get('/api/tools/search', {'q': 'Diana', 'mode': 'exact', 'scope': 'x'}), 400),
        ('entity id not uuid', c.get('/api/tools/entity/xyz'), 400),
        ('unknown entity', c.get(f'/api/tools/entity/{bogus}'), 404),
        ('search q too short', c.get('/api/tools/search', {'q': 'a', 'mode': 'exact'}), 400),
        ('connections missing to', c.get('/api/tools/connections', {'from': bogus}), 400),
        ('limit not int', c.get('/api/tools/search', {'q': 'Diana', 'mode': 'exact', 'limit': 'ten'}), 400),
        ('sql-ish query', c.get('/api/tools/search', {'q': "'; DROP TABLE kg_entities; --", 'mode': 'keywords'}), 200),
        ('percent wildcard', c.get('/api/tools/search', {'q': '%%', 'mode': 'exact'}), 200),
        ('regex chars', c.get('/api/tools/search', {'q': '(a+)+$ [x', 'mode': 'keywords'}), 200),
    ]


def pct(xs, p):
    xs = sorted(xs)
    return xs[min(len(xs) - 1, int(round(p / 100 * (len(xs) - 1))))] if xs else 0


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument('--matter', default='107fa33c-cb73-4d7a-bb24-76980eed00b5')
    ap.add_argument('--env', default='production')
    ap.add_argument('--repeat', type=int, default=3)
    ap.add_argument('--workers', type=int, default=8)
    ap.add_argument('--skip-concurrency', action='store_true')
    ap.add_argument('--out', default=str(Path(__file__).parent / 'results'))
    args = ap.parse_args()

    rtt = measure_rtt(args.env)
    srv, base = start_server()
    c = Client(base, args.matter, args.env)
    print(f'DB round trip from this machine: {rtt:.0f} ms')

    workload, ids = build_workload(c)
    print(f'workload: {len(workload)} calls ({len(ids)} entity names resolved)')

    failures = []
    # 1. functional
    for tool, label, path, params, expect, check in workload:
        r = c.get(path, params)
        if r['status'] != expect or not check(r['data']):
            failures.append({'pass': 'functional', 'tool': tool, 'label': label, 'status': r['status'],
                             'body': json.dumps(r['data'])[:300]})
    errs = error_cases(c)
    for label, r, expect in errs:
        if r['status'] != expect:
            failures.append({'pass': 'error-cases', 'label': label, 'expected': expect, 'status': r['status'],
                             'body': json.dumps(r['data'])[:300]})

    # 2. latency (sequential)
    samples = []
    for _ in range(args.repeat):
        for tool, label, path, params, expect, _check in workload:
            r = c.get(path, params)
            # python/flask time + DB execution time + an in-region round trip per query.
            app_ms = max(r['server_ms'] - r['explain_wall_ms'] - r['db_ms'], 0)
            r['in_region_ms'] = app_ms + r['exec_ms'] + r['queries'] * IN_REGION_RTT_MS
            r['app_ms'] = app_ms
            samples.append({'tool': tool, 'label': label, **{k: v for k, v in r.items() if k != 'data'}})
            if r['status'] >= 500:
                failures.append({'pass': 'latency', 'tool': tool, 'label': label, 'status': r['status']})

    # 3. concurrency
    t0 = time.perf_counter()
    with ThreadPoolExecutor(args.workers) as ex:
        conc = [] if args.skip_concurrency else list(ex.map(lambda item: (item, c.get(item[2], item[3])), workload * 2))
    conc_wall = (time.perf_counter() - t0) * 1000
    conc_fail = [(it[1], r['status']) for it, r in conc if r['status'] != it[4]]
    for label, status in conc_fail:
        failures.append({'pass': 'concurrency', 'label': label, 'status': status})
    srv.shutdown()

    # Report
    by_tool = {}
    for s in samples:
        by_tool.setdefault(s['tool'], []).append(s)
    lines = [f'# KG tools latency bench: {datetime.now():%Y-%m-%d %H:%M}', '',
             f'- Matter `{args.matter}` ({args.env}); {len(workload)} calls x {args.repeat} repeats; '
             f'concurrency pass {len(conc)} calls on {args.workers} threads.',
             f'- DB round trip from this machine: **{rtt:.0f} ms**. In-region estimate assumes {IN_REGION_RTT_MS:.0f} ms.',
             f'- Failures: **{len(failures)}** (functional + {len(errs)} error cases + 5xx + concurrency).', '',
             '| Tool | Calls | DB round trips (median) | Wall from here p50 / p95 (ms) | In-region p50 / p95 (ms) | DB exec p50 / p95 (ms) | Python p50 (ms) | Payload p50 / max (KB) |',
             '|---|---|---|---|---|---|---|---|']
    for tool, ss in by_tool.items():
        w = [s['wall_ms'] for s in ss]
        ir = [s['in_region_ms'] for s in ss]
        b = [s['bytes'] / 1024 for s in ss]
        q = statistics.median(s['queries'] for s in ss)
        ex = [s['exec_ms'] for s in ss]
        ap_ = [s['app_ms'] for s in ss]
        lines.append(f'| {tool} | {len(ss)} | {q:.0f} | {pct(w, 50):.0f} / {pct(w, 95):.0f} | '
                     f'{pct(ir, 50):.0f} / {pct(ir, 95):.0f} | {pct(ex, 50):.0f} / {pct(ex, 95):.0f} | '
                     f'{pct(ap_, 50):.0f} | {pct(b, 50):.1f} / {max(b):.1f} |')
    lines += ['', f'Concurrency pass: {len(conc)} calls in {conc_wall / 1000:.1f}s, {len(conc_fail)} unexpected statuses.', '',
              '## Slowest calls (in-region estimate)', '']
    for s in sorted(samples, key=lambda s: -s['in_region_ms'])[:8]:
        lines.append(f"- {s['tool']} `{s['label']}`: {s['in_region_ms']:.0f} ms in-region (DB exec {s['exec_ms']:.0f}, python {s['app_ms']:.0f}; {s['wall_ms']:.0f} wall, "
                     f"{s['queries']} queries, {s['bytes'] / 1024:.1f} KB)")
    lines += ['', '## Failures', ''] + ([f'- {json.dumps(f)}' for f in failures] or ['None.'])
    report = '\n'.join(lines)
    print('\n' + report)

    out = Path(args.out)
    out.mkdir(parents=True, exist_ok=True)
    stamp = datetime.now().strftime('%Y%m%d-%H%M%S')
    (out / f'latency-{stamp}.md').write_text(report + '\n')
    (out / f'latency-{stamp}.json').write_text(json.dumps(
        {'rtt_ms': rtt, 'samples': samples, 'failures': failures, 'ids': ids}, indent=1, default=str))
    print(f'\nwrote {out}/latency-{stamp}.md')
    sys.exit(1 if failures else 0)


if __name__ == '__main__':
    main()
