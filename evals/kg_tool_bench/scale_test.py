#!/usr/bin/env python3
"""
Scale test for /api/tools/* on a large matter: latency + filter correctness.

No query set is needed: the workload is derived from the matter's own
overview (biggest hubs, mid-sized entities, most common relation types), so
it works on any matter. Every call runs with EXPLAIN ANALYZE instrumentation
(see tool_latency.py), so in-region latency = python + DB execution + 1 ms
per round trip.

Filter checks verify the response honours what was asked: types, scope,
match=all, limit, relation_type, limit_per_type, date range, max_hops,
score ordering.

Usage (repo root):
    .venv/bin/python evals/kg_tool_bench/scale_test.py --matter 59f4b758-33c0-45ed-8438-c724cd5e01da
"""
import argparse
import json
import re
import statistics
import sys
from datetime import datetime
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
from tool_latency import IN_REGION_RTT_MS, Client, measure_rtt, start_server  # noqa: E402

S = '/api/tools/search'
GENERIC_SEMANTIC = ['the presiding judge', "plaintiff's attorney", 'the bank', 'expert witness',
                    'insurance company', 'date the loan was signed']


def typo(name):
    """Deterministic misspelling: swap two inner letters of the longest word."""
    words = name.split()
    w = max(words, key=len)
    if len(w) < 5:
        return name + 'x'
    i = len(w) // 2
    bad = w[:i] + w[i + 1] + w[i] + w[i + 2:]
    return name.replace(w, bad, 1)


# ---- filter checks: (passed, detail) ----

def ents(d):
    return d.get('entities') or []


def facts(d):
    return d.get('facts') or []


def chk_types(allowed):
    def c(d):
        bad = [e['type'] for e in ents(d) if e['type'] not in allowed]
        return not bad and not facts(d), f'{len(ents(d))} entities, types={sorted({e["type"] for e in ents(d)})}, facts={len(facts(d))}'
    return c


def chk_scope(scope):
    def c(d):
        if scope == 'facts':
            return not ents(d), f'entities={len(ents(d))} facts={len(facts(d))}'
        return not facts(d), f'entities={len(ents(d))} facts={len(facts(d))}'
    return c


def chk_limit(n):
    def c(d):
        return len(ents(d)) <= n and len(facts(d)) <= n, f'entities={len(ents(d))} facts={len(facts(d))} (limit {n})'
    return c


def chk_match_all(d):
    hits = [x['matched']['terms_matched'] for x in ents(d) + facts(d)]
    bad = [h for h in hits if h.split('/')[0] != h.split('/')[1]]
    return not bad, f'{len(hits)} hits, partial matches={bad[:3]}'


def chk_keyword_order(d):
    n = [int(x['matched']['terms_matched'].split('/')[0]) for x in ents(d)]
    return n == sorted(n, reverse=True), f'entity term counts={n[:10]}'


def chk_exact(q):
    def c(d):
        bad = [e['name'] for e in ents(d) if e['matched']['field'] == 'name' and q.lower() not in e['name'].lower()]
        return not bad, f'{len(ents(d))} hits; name-field hits not containing query: {bad[:3]}'
    return c


def chk_scores_desc(minimum=None):
    def c(d):
        s = [e['matched']['score'] for e in ents(d)]
        ok = s == sorted(s, reverse=True) and (minimum is None or all(x >= minimum for x in s))
        return ok, f'scores={s[:6]}'
    return c


def chk_semantic(d):
    cov = d.get('coverage') or {}
    ok, det = chk_scores_desc()(d)
    return ok and 'searchable' in cov, f"{det}; searchable={cov.get('searchable')}/{cov.get('total_in_scope')}; top={[e['name'][:30] for e in ents(d)[:3]]}"


def chk_found(substr):
    def c(d):
        top = [e['name'] for e in ents(d)[:5]]
        return any(substr.lower() in n.lower() for n in top), f'top5={top}'
    return c


def chk_relations_only(rels):
    def c(d):
        got = {g['relation'] for g in d.get('relations') or []}
        return bool(got) and got <= set(rels), f'relations={sorted(got)}'
    return c


def chk_per_type(n, max_groups):
    def c(d):
        groups = d.get('relations') or []
        sizes = [len(g['items']) for g in groups]
        return all(s <= n for s in sizes) and len(groups) <= max_groups, \
            f'{len(groups)} groups (max {max_groups}), items/group max={max(sizes or [0])} (limit {n}), truncated={d.get("truncated")}'
    return c


def chk_entity_basic(d):
    items = [it for g in d.get('relations') or [] for it in g['items']]
    src = sum(1 for it in items if it.get('sources'))
    return 'entity' in d, (f"relations_total={d.get('relations_total')} groups={d.get('relation_groups_total')} "
                           f"items shown={len(items)} with sources={src} facts={d.get('facts_total')} "
                           f"mentioned_in={d.get('mentioned_in_total')} {len(json.dumps(d)) / 1024:.1f}KB")


def chk_connections(max_hops):
    def c(d):
        hops = [p['hops'] for p in d.get('paths') or []]
        return d.get('searched_hops', 0) <= max_hops and all(h <= max_hops for h in hops), \
            f"connected={d.get('connected')} direct={d.get('direct_relations_total')} paths={hops} searched_hops={d.get('searched_hops')}"
    return c


def chk_timeline(lo='0000', hi='9999'):
    def c(d):
        dates = [e['date'] for e in d.get('entries') or []]
        ok = dates == sorted(dates) and all(lo <= x <= hi + '~' for x in dates)
        return ok, f"{len(dates)} shown of {d.get('total')}, range {dates[:1]}..{dates[-1:]}"
    return c


def build_cases(c):
    ov = c.get('/api/tools/overview', {'nocache': 1})
    if ov['status'] != 200 or (ov['data'] or {}).get('status') != 'ready':
        sys.exit(f'overview failed: {ov["status"]} {ov["data"]}')
    o = ov['data']
    keys = [e for e in o['key_entities'] if len(e['name']) >= 4]
    hub1, hub2 = keys[0], keys[1]
    persons = [e for e in keys if e['type'] == 'Person']
    orgs = [e for e in keys if e['type'] == 'Organization']
    mid = keys[min(len(keys) - 1, 15)]
    person = persons[0] if persons else hub2
    org = orgs[0] if orgs else hub1
    rels = [r['relation_type'] for r in o['top_relation_types'] if r['relation_type'] != 'binds'][:2]
    surname = person['name'].split()[-1]
    two_words = ' '.join(person['name'].split()[:2]) if len(person['name'].split()) > 1 else person['name']
    year = '2015'

    cases = [
        # (tool, mode, label, path, params, check)
        ('search', 'exact', f'hub name: {hub1["name"][:30]}', S, {'q': hub1['name'], 'mode': 'exact'}, chk_exact(hub1['name'])),
        ('search', 'exact', 'surname, types=Person', S, {'q': surname, 'mode': 'exact', 'types': 'Person'}, chk_types({'Person'})),
        ('search', 'exact', 'org name, scope=entities', S, {'q': org['name'], 'mode': 'exact', 'scope': 'entities'}, chk_scope('entities')),
        ('search', 'exact', 'year in fact text, scope=facts', S, {'q': year, 'mode': 'exact', 'scope': 'facts'}, chk_scope('facts')),
        ('search', 'keywords', f'any: {two_words[:30]}', S, {'q': two_words, 'mode': 'keywords'}, chk_keyword_order),
        ('search', 'keywords', 'all: name words', S, {'q': two_words, 'mode': 'keywords', 'match': 'all'}, chk_match_all),
        ('search', 'keywords', 'common word, limit=3', S, {'q': 'agreement payment', 'mode': 'keywords', 'limit': 3}, chk_limit(3)),
        ('search', 'keywords', 'common word, limit=50 (max)', S, {'q': 'the agreement', 'mode': 'keywords', 'limit': 50}, chk_limit(50)),
        ('search', 'keywords', 'types=Organization,Person', S,
         {'q': surname, 'mode': 'keywords', 'types': 'Organization,Person'}, chk_types({'Organization', 'Person'})),
        ('search', 'fuzzy', f'typo: {typo(person["name"])[:30]}', S, {'q': typo(person['name']), 'mode': 'fuzzy'},
         lambda d: (lambda a, b: (a[0] and b[0], a[1] + '; ' + b[1]))(chk_scores_desc(0.3)(d), chk_found(surname)(d))),
        ('search', 'fuzzy', f'typo: {typo(org["name"])[:30]}', S, {'q': typo(org['name']), 'mode': 'fuzzy'}, chk_scores_desc(0.3)),
    ]
    for q in GENERIC_SEMANTIC:
        cases.append(('search', 'semantic', q, S, {'q': q, 'mode': 'semantic'}, chk_semantic))
    cases.append(('search', 'semantic', 'types=Person', S, {'q': 'lawyer', 'mode': 'semantic', 'types': 'Person'}, chk_types({'Person'})))

    for label, e in (('biggest hub', hub1), ('2nd hub', hub2), ('mid entity', mid), ('person', person)):
        cases.append(('entity', '-', f'{label}: {e["name"][:30]} (deg {e["degree"]})', f'/api/tools/entity/{e["id"]}', None, chk_entity_basic))
    if rels:
        cases.append(('entity', '-', f'hub, relation_type={",".join(rels)}', f'/api/tools/entity/{hub1["id"]}',
                      {'relation_type': ','.join(rels)}, chk_relations_only(rels)))
    cases.append(('entity', '-', 'hub, limit_per_type=2, max_groups=5', f'/api/tools/entity/{hub1["id"]}',
                  {'limit_per_type': 2, 'max_groups': 5}, chk_per_type(2, 5)))

    cases += [
        ('connections', '-', 'hub1 <-> hub2', '/api/tools/connections', {'from': hub1['id'], 'to': hub2['id']}, chk_connections(3)),
        ('connections', '-', 'hub1 <-> mid', '/api/tools/connections', {'from': hub1['id'], 'to': mid['id']}, chk_connections(3)),
        ('connections', '-', 'person <-> org, max_hops=2', '/api/tools/connections',
         {'from': person['id'], 'to': org['id'], 'max_hops': 2}, chk_connections(2)),
        ('connections', '-', 'mid <-> person, max_hops=4', '/api/tools/connections',
         {'from': mid['id'], 'to': person['id'], 'max_hops': 4}, chk_connections(4)),
        ('timeline', '-', 'global (limit 40)', '/api/tools/timeline', None, chk_timeline()),
        ('timeline', '-', 'global limit=200', '/api/tools/timeline', {'limit': 200}, chk_timeline()),
        ('timeline', '-', 'range 2015-01-01..2016-12-31', '/api/tools/timeline', {'from': '2015-01-01', 'to': '2016-12-31'},
         chk_timeline('2015-01-01', '2016-12-31')),
        ('timeline', '-', f'entity: {hub1["name"][:30]}', '/api/tools/timeline', {'entity_id': hub1['id']}, chk_timeline()),
        ('timeline', '-', f'entity: {person["name"][:30]}', '/api/tools/timeline', {'entity_id': person['id']}, chk_timeline()),
    ]
    return o, ov, cases


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument('--matter', default='59f4b758-33c0-45ed-8438-c724cd5e01da')  # Hahn v. WF
    ap.add_argument('--env', default='production')
    ap.add_argument('--repeat', type=int, default=2)
    ap.add_argument('--out', default=str(Path(__file__).parent / 'results'))
    args = ap.parse_args()

    from src.api import tools as tools_mod
    tools_mod.VECTOR_LOAD_TIMEOUT_MS = 15 * 60 * 1000  # WAN transfer from a laptop; DB exec is measured separately

    rtt = measure_rtt(args.env)
    srv, base = start_server()
    c = Client(base, args.matter, args.env)
    o, ov, cases = build_cases(c)
    print(f"matter: {o['counts']} ; {len(cases)} cases", flush=True)

    def in_region(r):
        app = max(r['server_ms'] - r['explain_wall_ms'] - r['db_ms'], 0)
        return app + r['exec_ms'] + r['queries'] * IN_REGION_RTT_MS, app

    ov_ir, _ = in_region(ov)
    cold = c.get(S, {'q': 'warm up', 'mode': 'semantic', 'limit': 1})
    cold_ir, _ = in_region(cold)

    rows = []
    for tool, mode, label, path, params, check in cases:
        runs = [c.get(path, params) for _ in range(args.repeat)]
        last = runs[-1]
        try:
            passed, detail = check(last['data']) if last['status'] == 200 else (False, f"HTTP {last['status']}: {json.dumps(last['data'])[:200]}")
        except Exception as e:
            passed, detail = False, f'check error: {e!r}'
        irs = [in_region(r) for r in runs]
        rows.append({'tool': tool, 'mode': mode, 'label': label, 'passed': bool(passed), 'detail': detail,
                     'status': last['status'], 'queries': last['queries'], 'kb': last['bytes'] / 1024,
                     'in_region_ms': statistics.median(x[0] for x in irs), 'app_ms': statistics.median(x[1] for x in irs),
                     'db_exec_ms': statistics.median(r['exec_ms'] for r in runs),
                     'wall_ms': statistics.median(r['wall_ms'] for r in runs)})
        print(f"{'PASS' if passed else 'FAIL'} {tool:11} {mode:9} {label[:48]:48} {rows[-1]['in_region_ms']:7.0f} ms  {detail[:120]}", flush=True)
    srv.shutdown()

    n_pass = sum(r['passed'] for r in rows)
    lines = [f'# KG tools scale test ({datetime.now():%Y-%m-%d %H:%M})', '',
             f"- Matter `{args.matter}` ({args.env}): {o['counts']['entities']:,} entities, {o['counts']['relations']:,} relations, "
             f"{o['counts']['documents']:,} documents.",
             f'- {len(rows)} cases x {args.repeat} runs, workload derived from the matter overview. In-region = python + DB execution '
             f'(EXPLAIN ANALYZE) + {IN_REGION_RTT_MS:.0f} ms per round trip; semantic includes the live Gemini query embedding.',
             f'- **Filter / correctness checks: {n_pass}/{len(rows)} passed.** Non-200 responses: {sum(r["status"] != 200 for r in rows)}.',
             f'- overview (uncached): **{ov_ir:.0f} ms** in-region ({ov["exec_ms"]:.0f} ms DB exec).',
             f"- Semantic cold start (load the matter's vectors): {cold_ir:.0f} ms in-region by DB exec + python "
             f"(EXPLAIN does not count transfer); {cold['wall_ms'] / 1000:.0f} s from this machine. Status {cold['status']}.", '',
             '| Tool | Mode | Cases | In-region p50 / max (ms) | DB exec p50 / max (ms) | Python p50 (ms) | Payload p50 / max (KB) |',
             '|---|---|---|---|---|---|---|']
    groups = {}
    for r in rows:
        groups.setdefault((r['tool'], r['mode']), []).append(r)
    for (tool, mode), rs in groups.items():
        med = lambda k: statistics.median(x[k] for x in rs)  # noqa: E731
        mx = lambda k: max(x[k] for x in rs)  # noqa: E731
        lines.append(f"| {tool} | {mode} | {len(rs)} | {med('in_region_ms'):.0f} / {mx('in_region_ms'):.0f} | "
                     f"{med('db_exec_ms'):.0f} / {mx('db_exec_ms'):.0f} | {med('app_ms'):.0f} | {med('kb'):.1f} / {mx('kb'):.1f} |")
    lines += ['', '## Cases', '', '| Result | Tool | Mode | Case | In-region (ms) | DB exec (ms) | Detail |', '|---|---|---|---|---|---|---|']
    for r in rows:
        lines.append(f"| {'PASS' if r['passed'] else '**FAIL**'} | {r['tool']} | {r['mode']} | {r['label']} | "
                     f"{r['in_region_ms']:.0f} | {r['db_exec_ms']:.0f} | {r['detail'].replace('|', '/')[:240]} |")
    report = '\n'.join(lines)
    print('\n' + report)
    out = Path(args.out)
    out.mkdir(parents=True, exist_ok=True)
    stamp = datetime.now().strftime('%Y%m%d-%H%M%S')
    (out / f'scale-{stamp}.md').write_text(report + '\n')
    (out / f'scale-{stamp}.json').write_text(json.dumps({'rtt_ms': rtt, 'overview': o, 'rows': rows}, indent=1))
    print(f'\nwrote {out}/scale-{stamp}.md')


if __name__ == '__main__':
    main()
