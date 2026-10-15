#!/usr/bin/env python3
"""
Basic accuracy + latency tests for every /api/tools/* tool and every search
mode, on the Whitfield matter. Expectations come from whitfield.profile.md
(ground truth checked against the documents), so a failure means the tool
did not surface something the graph is known to contain.

Each case runs `--repeat` times. Latency is reported as:
  - wall:      measured from this machine (includes WAN round trips)
  - in-region: python/flask time + DB execution time (EXPLAIN ANALYZE) +
               1 ms per DB round trip; semantic search also includes the
               real Gemini query-embedding call.

Usage (repo root):
    .venv/bin/python evals/kg_tool_bench/tool_tests.py
"""
import argparse
import json
import statistics
import sys
import time
from datetime import datetime
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
from tool_latency import IN_REGION_RTT_MS, Client, measure_rtt, start_server  # noqa: E402

MATTER = '107fa33c-cb73-4d7a-bb24-76980eed00b5'  # Whitfield

# Entities the tests need ids for, resolved once by exact name.
ANCHORS = {
    'cosby': 'Don Cosby',
    'diana': 'Diana Whitfield',
    'trust': 'Calloway Family Revocable Living Trust',
    'brown': 'Christian Brown',
    'margaret': 'Defendant Margaret Calloway-Briggs',
    'coonrod': 'Mary Catherine Coonrod',
    'vanvleck': 'O. Van Vleck',
}


# ---- check helpers: each returns (passed, detail) ----

def names(d, key='entities', k=5):
    return [e.get('name', '') for e in (d.get(key) or [])[:k]]


def entity_in_top(substr, k=5):
    def chk(d):
        top = names(d, k=k)
        ok = any(substr.lower() in n.lower() for n in top)
        return ok, f'top{k}={top}'
    return chk


def fact_in_top(substr, k=5):
    def chk(d):
        top = [f.get('text', '') for f in (d.get('facts') or [])[:k]]
        ok = any(substr.lower() in t.lower() for t in top)
        return ok, f'{sum(substr.lower() in t.lower() for t in top)}/{len(top)} top facts contain {substr!r}'
    return chk


def all_of(*checks):
    def chk(d):
        results = [c(d) for c in checks]
        return all(r[0] for r in results), '; '.join(r[1] for r in results)
    return chk


def has_relation(rel, other_substr=None):
    def chk(d):
        for grp in d.get('relations') or []:
            if grp['relation'] == rel:
                if other_substr is None:
                    return True, f'{rel} present ({grp["total"]})'
                if any(other_substr.lower() in it['other']['name'].lower() for it in grp['items']):
                    return True, f'{rel} -> {other_substr} present'
        return False, f'relations={[g["relation"] for g in d.get("relations") or []][:12]}'
    return chk


def related_anywhere(substr):
    def chk(d):
        found = [it['other']['name'] for g in d.get('relations') or [] for it in g['items']
                 if substr.lower() in it['other']['name'].lower()]
        return bool(found), f'{substr!r} among related: {found[:3]}'
    return chk


def sources_present(d):
    items = [it for g in d.get('relations') or [] for it in g['items']]
    with_src = sum(1 for it in items if it.get('sources'))
    return with_src == len(items) and items, f'{with_src}/{len(items)} relation items have sources'


def only_relations(*rels):
    def chk(d):
        got = {g['relation'] for g in d.get('relations') or []}
        return bool(got) and got <= set(rels), f'relations={sorted(got)}'
    return chk


def size_under(kb):
    def chk(d):
        n = len(json.dumps(d)) / 1024
        return n < kb, f'{n:.1f} KB (< {kb})'
    return chk


def field_equals(path, value):
    def chk(d):
        cur = d
        for p in path.split('.'):
            cur = (cur or {}).get(p)
        return cur == value, f'{path}={cur!r}'
    return chk


def timeline_sorted(d):
    dates = [e['date'] for e in d.get('entries') or []]
    return dates == sorted(dates) and bool(dates), f'{len(dates)} entries, first={dates[:1]} last={dates[-1:]}'


def timeline_has(date_prefix, desc_substr=''):
    def chk(d):
        hit = [e for e in d.get('entries') or [] if e['date'].startswith(date_prefix)
               and desc_substr.lower() in (e['description'] + ' ' + e['date_text']).lower()]
        return bool(hit), f'{len(hit)} entries on {date_prefix} matching {desc_substr!r}'
    return chk


def timeline_within(lo, hi):
    def chk(d):
        dates = [e['date'] for e in d.get('entries') or []]
        return bool(dates) and all(lo <= x <= hi for x in dates), f'{len(dates)} entries, range {dates[:1]}..{dates[-1:]}'
    return chk


def overview_ok(d):
    keys = [e['name'] for e in d.get('key_entities') or []]
    ok = d.get('status') == 'ready' and d['counts']['entities'] > 2000 and any('Calloway-Briggs' in k for k in keys[:5])
    return ok, f"status={d.get('status')} entities={d['counts']['entities']} top3={keys[:3]}"


def connected(expect=True):
    def chk(d):
        return d.get('connected') == expect, (f"connected={d.get('connected')} direct={d.get('direct_relations_total')} "
                                              f"paths={[p['hops'] for p in d.get('paths') or []]}")
    return chk


def path_steps_sourced(d):
    steps = [s for p in d.get('paths') or [] for s in p['steps']]
    with_src = sum(1 for s in steps if s.get('source'))
    return bool(steps), f'{len(steps)} path steps, {with_src} with a source document'


def cases(ids):
    S = '/api/tools/search'
    E = lambda key: f'/api/tools/entity/{ids[key]}'  # noqa: E731
    return [
        # tool, mode, label, path, params, check
        ('overview', '-', 'matter overview', '/api/tools/overview', {'nocache': 1}, overview_ok),

        ('search', 'exact', 'judge by surname', S, {'q': 'Cosby', 'mode': 'exact'}, entity_in_top('Don Cosby', 1)),
        ('search', 'exact', 'court reporting firm', S, {'q': 'Sterling Court Reporting', 'mode': 'exact', 'scope': 'entities'},
         entity_in_top('Sterling Court Reporting', 1)),
        ('search', 'exact', 'money amount', S, {'q': '$60,000', 'mode': 'exact', 'types': 'Money'}, entity_in_top('$60,000', 1)),
        ('search', 'exact', 'leaked real name (alias)', S, {'q': 'Fenoglio', 'mode': 'exact', 'scope': 'entities'},
         entity_in_top('Fenoglio', 3)),

        ('search', 'keywords', 'tax facts (any)', S, {'q': 'property taxes paid', 'mode': 'keywords', 'scope': 'facts'},
         fact_in_top('tax', 5)),
        ('search', 'keywords', 'royalty clause (any)', S, {'q': 'royalty minerals', 'mode': 'keywords', 'scope': 'entities'},
         entity_in_top('royalty', 5)),
        ('search', 'keywords', 'deposition docs (all)', S,
         {'q': 'Diana deposition', 'mode': 'keywords', 'match': 'all', 'scope': 'entities'}, entity_in_top('deposition', 5)),
        ('search', 'keywords', 'heart attack fact (all)', S,
         {'q': 'heart attack', 'mode': 'keywords', 'match': 'all', 'scope': 'facts'}, fact_in_top('heart attack', 3)),

        ('search', 'fuzzy', 'misspelled defendant', S, {'q': 'Calloway Brigs', 'mode': 'fuzzy'}, entity_in_top('Calloway-Briggs', 3)),
        ('search', 'fuzzy', 'misspelled reporter', S, {'q': 'McPhearsen', 'mode': 'fuzzy'}, entity_in_top('McPhearson', 3)),
        ('search', 'fuzzy', 'misspelled judge', S, {'q': 'Don Cosbey', 'mode': 'fuzzy'}, entity_in_top('Cosby', 3)),

        ('search', 'semantic', 'role: judge', S, {'q': 'the presiding judge', 'mode': 'semantic'}, entity_in_top('Cosby', 5)),
        ('search', 'semantic', 'role: court reporter', S, {'q': 'court reporter', 'mode': 'semantic'},
         all_of(entity_in_top('Sterling', 5), entity_in_top('McPhearson', 5))),
        ('search', 'semantic', 'concept: family trust', S, {'q': 'family trust that owns the land', 'mode': 'semantic'},
         entity_in_top('Calloway Family Revocable Living Trust', 5)),
        ('search', 'semantic', "role: plaintiffs' lawyer", S,
         {'q': "plaintiffs' attorney", 'mode': 'semantic', 'types': 'Person'}, entity_in_top('Christian Brown', 5)),

        ('entity', '-', 'judge -> court', E('cosby'), None, has_relation('presides_over')),
        ('entity', '-', 'trust -> grantors', E('trust'), None,
         all_of(related_anywhere('Homer'), related_anywhere('Eleanor'), sources_present)),
        ('entity', '-', 'hub entity is bounded', E('margaret'), None, all_of(field_equals('truncated', True), size_under(35))),
        ('entity', '-', 'relation_type filter', E('brown'), {'relation_type': 'represents'}, only_relations('represents')),

        ('connections', '-', 'Diana <-> Trust', '/api/tools/connections', {'from': ids['diana'], 'to': ids['trust']},
         connected(True)),
        ('connections', '-', 'Coonrod <-> Van Vleck', '/api/tools/connections',
         {'from': ids['coonrod'], 'to': ids['vanvleck']}, connected(True)),
        ('connections', '-', 'judge <-> plaintiffs\' counsel (multi-hop)', '/api/tools/connections',
         {'from': ids['cosby'], 'to': ids['brown']}, all_of(connected(True), path_steps_sourced)),

        ('timeline', '-', 'global, sorted, starts 1938', '/api/tools/timeline', {'limit': 200},
         all_of(timeline_sorted, timeline_has('1938-01-29'))),
        ('timeline', '-', 'Diana: date of birth', '/api/tools/timeline', {'entity_id': ids['diana']},
         timeline_has('1968-11-13', 'birth')),
        ('timeline', '-', 'date range 2024', '/api/tools/timeline', {'from': '2024-01-01', 'to': '2024-12-31', 'limit': 200},
         timeline_within('2024-01-01', '2024-12-31')),
    ]


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument('--env', default='production')
    ap.add_argument('--repeat', type=int, default=3)
    ap.add_argument('--out', default=str(Path(__file__).parent / 'results'))
    args = ap.parse_args()

    rtt = measure_rtt(args.env)
    srv, base = start_server()
    c = Client(base, MATTER, args.env)

    ids = {}
    for key, name in ANCHORS.items():
        r = c.get('/api/tools/search', {'q': name, 'mode': 'exact', 'scope': 'entities', 'limit': 10})
        exact = [e for e in (r['data'] or {}).get('entities', []) if e['name'].lower() == name.lower()]
        if not exact:
            sys.exit(f'anchor not found: {name}')
        ids[key] = exact[0]['id']

    # Warm the semantic vector cache once; report the cold load separately.
    cold = c.get('/api/tools/search', {'q': 'warm up', 'mode': 'semantic', 'limit': 1})

    rows = []
    for tool, mode, label, path, params, check in cases(ids):
        runs = [c.get(path, params) for _ in range(args.repeat)]
        last = runs[-1]
        try:
            passed, detail = check(last['data']) if last['status'] == 200 else (False, f"HTTP {last['status']}: {last['data']}")
        except Exception as e:  # a malformed response is a failure, not a crash
            passed, detail = False, f'check error: {e!r}'
        stable = len({json.dumps(r['data'], sort_keys=True) for r in runs}) == 1
        for r in runs:
            r['app_ms'] = max(r['server_ms'] - r['explain_wall_ms'] - r['db_ms'], 0)
            r['in_region_ms'] = r['app_ms'] + r['exec_ms'] + r['queries'] * IN_REGION_RTT_MS
        rows.append({
            'tool': tool, 'mode': mode, 'label': label, 'passed': bool(passed), 'detail': detail,
            'deterministic': stable, 'status': last['status'], 'queries': last['queries'], 'kb': last['bytes'] / 1024,
            'wall_ms': statistics.median(r['wall_ms'] for r in runs),
            'in_region_ms': statistics.median(r['in_region_ms'] for r in runs),
            'db_exec_ms': statistics.median(r['exec_ms'] for r in runs),
            'app_ms': statistics.median(r['app_ms'] for r in runs),
        })
        print(f"{'PASS' if passed else 'FAIL'} {tool:11} {mode:9} {label:42} "
              f"{rows[-1]['in_region_ms']:6.0f} ms in-region  ({detail[:110]})", flush=True)
    srv.shutdown()

    n_pass = sum(r['passed'] for r in rows)
    lines = [f'# KG tools: accuracy + latency tests ({datetime.now():%Y-%m-%d %H:%M})', '',
             f'- Matter: Whitfield `{MATTER}` ({args.env}). {len(rows)} cases, each run {args.repeat}x.',
             f'- **Accuracy: {n_pass}/{len(rows)} passed.** Non-deterministic responses: '
             f'{sum(not r["deterministic"] for r in rows)}.',
             f'- DB round trip from this machine: {rtt:.0f} ms. In-region = python + DB execution (EXPLAIN ANALYZE) '
             f'+ {IN_REGION_RTT_MS:.0f} ms per round trip; semantic includes the live Gemini query-embedding call.',
             f"- Semantic cold start (loading the matter's vectors) from this machine: {cold['wall_ms'] / 1000:.1f} s; "
             f'excluded below (cached per process for 10 min).', '',
             '## Latency by tool / mode (median of cases)', '',
             '| Tool | Mode | Cases | In-region p50 / max (ms) | DB exec p50 (ms) | Python p50 (ms) | Wall from here p50 (ms) | Payload p50 (KB) |',
             '|---|---|---|---|---|---|---|---|']
    groups = {}
    for r in rows:
        groups.setdefault((r['tool'], r['mode']), []).append(r)
    for (tool, mode), rs in groups.items():
        med = lambda k: statistics.median(x[k] for x in rs)  # noqa: E731
        lines.append(f"| {tool} | {mode} | {len(rs)} | {med('in_region_ms'):.0f} / {max(x['in_region_ms'] for x in rs):.0f} | "
                     f"{med('db_exec_ms'):.0f} | {med('app_ms'):.0f} | {med('wall_ms'):.0f} | {med('kb'):.1f} |")
    lines += ['', '## Cases', '', '| Result | Tool | Mode | Case | In-region (ms) | Detail |', '|---|---|---|---|---|---|']
    for r in rows:
        lines.append(f"| {'PASS' if r['passed'] else '**FAIL**'} | {r['tool']} | {r['mode']} | {r['label']} | "
                     f"{r['in_region_ms']:.0f} | {r['detail'].replace('|', '/')[:220]} |")
    report = '\n'.join(lines)
    print('\n' + report)

    out = Path(args.out)
    out.mkdir(parents=True, exist_ok=True)
    stamp = datetime.now().strftime('%Y%m%d-%H%M%S')
    (out / f'tool-tests-{stamp}.md').write_text(report + '\n')
    (out / f'tool-tests-{stamp}.json').write_text(json.dumps({'rtt_ms': rtt, 'ids': ids, 'rows': rows}, indent=1))
    print(f'\nwrote {out}/tool-tests-{stamp}.md')


if __name__ == '__main__':
    main()
