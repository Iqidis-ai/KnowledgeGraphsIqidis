#!/usr/bin/env python3
"""
Graph-only agent eval: can a Claude session answer the bench questions using
nothing but the KG tools, and how fast / how many calls does it take?

For each question:
  1. ANSWER  - a fresh `claude -p` session (Sonnet) with:
       --tools ""            no built-in tools (no Read/Bash/Grep/Web)
       --strict-mcp-config   only the kg MCP server (kg_mcp_server.py)
       cwd = empty scratch dir, so no repo files / CLAUDE.md / memory
     The prompt contains ONLY the question text. Expected answers never
     reach this session, and it is told not to look for them.
  2. JUDGE   - a separate session (no tools) compares the answer with the
     expected answer / must_include / must_not_include and returns JSON.

Outputs results/agent-<stamp>/{<qid>.json, report.md, summary.json}.

Usage (repo root):
    .venv/bin/python evals/kg_tool_bench/agent/run_agent_eval.py \
        --queries evals/kg_tool_bench/whitfield.queries.json --parallel 4
    # subset: --ids WF-01,WF-03
"""
import argparse
import json
import os
import re
import shutil
import statistics
import subprocess
import tempfile
import time
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime
from pathlib import Path

HERE = Path(__file__).resolve().parent
ROOT = HERE.parents[2]
PYTHON = str(ROOT / '.venv' / 'bin' / 'python')
CLAUDE = shutil.which('claude', path=os.path.expanduser('~/.local/bin')) or 'claude'

AGENT_SYSTEM = """You are a legal research assistant answering one question about a single legal matter.

You can ONLY use the knowledge-graph tools (kg_overview, kg_search, kg_get_entity, kg_get_document, kg_find_connections, kg_timeline). You have no document reader, no file system, no shell and no web access. Do not try to find evaluation files, expected answers or anything outside these tools; answer purely from what the graph tools return.

About the graph: it was extracted automatically from the matter's documents. Expect gaps, duplicate entities for the same person or organization, mistyped entities, stale facts and some wrong relations. Cross-check important claims across more than one relation or fact when you can, and prefer facts whose text states the point directly.

How to work:
- Resolve names to entity ids with kg_search, then use kg_get_entity, kg_find_connections or kg_timeline.
- kg_search requires a mode: pick the one that fits what you are looking for (exact, keywords, fuzzy or semantic) and switch modes if a search comes back empty or weak.
- Be efficient: stop calling tools once you can answer, or once further calls are unlikely to help.
- If the graph does not contain what the question needs, say so plainly and say what is missing. Do not guess or fill gaps from general knowledge.

Final answer format:
1. The answer (concise, direct).
2. "Sources:" the document filenames the supporting relations/facts came from.
3. "Confidence:" high / medium / low, with one line on why.
4. "Not found in graph:" anything the question asked that the graph could not support (or "nothing")."""

JUDGE_PROMPT = """You are grading an AI assistant's answer to a question about a legal matter. The assistant could only query an automatically extracted knowledge graph (no documents), so missing information is expected; what matters is whether what it says is correct and whether it honestly flags what it could not find.

QUESTION:
{question}

EXPECTED ANSWER (ground truth from the documents):
{expected}

MUST INCLUDE (each is one key point):
{must_include}

MUST NOT INCLUDE (claims that would be wrong):
{must_not_include}

GRADER NOTES (how the graph covers this): coverage={coverage}. {notes}

ASSISTANT ANSWER:
<<<
{answer}
>>>

Grade strictly. Return ONLY a JSON object, no prose, with this shape:
{{
  "must_include": [{{"point": "<point>", "verdict": "hit" | "partial" | "miss"}}],
  "violations": ["<each must_not_include item the answer asserts, or other clearly false claim>"],
  "correctness": <0.0-1.0, share of the expected answer correctly given>,
  "hallucination": <true if the answer asserts something false as fact, false otherwise>,
  "flagged_gaps": <true if the answer explicitly says what it could not find / is unsure about, where relevant>,
  "verdict": "correct" | "partially_correct" | "honest_abstention" | "incorrect",
  "comment": "<one sentence>"
}}
Use "honest_abstention" when the answer mostly says the graph lacks the information and makes no false claims."""


def run_claude(args_list, prompt, cwd, env, timeout):
    t = time.time()
    p = subprocess.run([CLAUDE, '-p', prompt, *args_list], cwd=cwd, env=env, capture_output=True,
                       text=True, timeout=timeout)
    return p, time.time() - t


def answer_question(q, opts, outdir):
    work = Path(tempfile.mkdtemp(prefix=f'kg-agent-{q["id"]}-', dir=opts.scratch))
    log = work / 'tool_calls.jsonl'
    mcp_cfg = work / 'mcp.json'
    mcp_cfg.write_text(json.dumps({'mcpServers': {'kg': {
        'command': PYTHON, 'args': [str(HERE / 'kg_mcp_server.py')],
        'env': {'KG_EVAL_MATTER': opts.matter, 'KG_EVAL_ENV': opts.env, 'KG_EVAL_LOG': str(log),
                'KG_EVAL_VECTOR_CACHE': opts.vector_cache}}}}))
    env = {k: v for k, v in os.environ.items() if not k.startswith('KG_')}
    cli = ['--model', opts.model, '--tools', '', '--strict-mcp-config', '--mcp-config', str(mcp_cfg),
           '--allowedTools', 'mcp__kg', '--append-system-prompt', AGENT_SYSTEM,
           '--output-format', 'stream-json', '--verbose', '--no-session-persistence',
           '--max-turns', str(opts.max_turns)]
    try:
        p, wall = run_claude(cli, q['question'], work, env, opts.timeout)
        stdout, stderr = p.stdout, p.stderr
    except subprocess.TimeoutExpired as e:
        stdout, stderr, wall = (e.stdout or b'').decode() if isinstance(e.stdout, bytes) else (e.stdout or ''), 'timeout', opts.timeout

    events = []
    for line in stdout.splitlines():
        try:
            events.append(json.loads(line))
        except ValueError:
            pass
    result = next((e for e in reversed(events) if e.get('type') == 'result'), {})
    init = next((e for e in events if e.get('type') == 'system' and e.get('subtype') == 'init'), {})
    tool_uses = [c for e in events if e.get('type') == 'assistant'
                 for c in (e.get('message', {}).get('content') or []) if c.get('type') == 'tool_use']
    calls = [json.loads(l) for l in log.read_text().splitlines()] if log.exists() else []

    rec = {
        'id': q['id'], 'category': q.get('category'), 'graph_coverage': q.get('graph_coverage'),
        'difficulty': q.get('difficulty'), 'question': q['question'],
        'answer': result.get('result') or '',
        'is_error': result.get('is_error', True) or not result,
        'error': None if result else (stderr or '')[-2000:],
        'wall_s': round(wall, 1),
        'duration_ms': result.get('duration_ms'), 'duration_api_ms': result.get('duration_api_ms'),
        'num_turns': result.get('num_turns'), 'cost_usd': result.get('total_cost_usd'),
        'usage': result.get('usage'), 'model': (init.get('model') or opts.model),
        'available_tools': init.get('tools'),
        'tool_uses': [{'name': t['name'], 'input': t.get('input')} for t in tool_uses],
        'tool_calls': calls,
    }
    shutil.rmtree(work, ignore_errors=True)
    return rec


def judge(q, rec, opts):
    prompt = JUDGE_PROMPT.format(
        question=q['question'], expected=q['expected_answer'],
        must_include='\n'.join(f'- {x}' for x in q.get('must_include', [])) or '- (none)',
        must_not_include='\n'.join(f'- {x}' for x in q.get('must_not_include', [])) or '- (none)',
        coverage=q.get('graph_coverage'), notes=q.get('notes', ''), answer=rec['answer'] or '(no answer)')
    work = tempfile.mkdtemp(prefix=f'kg-judge-{q["id"]}-', dir=opts.scratch)
    try:
        p, _ = run_claude(['--model', opts.judge_model, '--tools', '', '--strict-mcp-config',
                           '--output-format', 'json', '--no-session-persistence'], prompt, work,
                          dict(os.environ), opts.timeout)
        text = json.loads(p.stdout).get('result', '')
        m = re.search(r'\{.*\}', text, re.S)
        return json.loads(m.group(0)) if m else {'verdict': 'judge_error', 'comment': text[:300]}
    except Exception as e:
        return {'verdict': 'judge_error', 'comment': str(e)[:300]}
    finally:
        shutil.rmtree(work, ignore_errors=True)


def summarize(recs, opts, rtt_ms):
    ok = [r for r in recs if not r['is_error']]
    calls = [c for r in recs for c in r['tool_calls']]
    def med(xs):
        xs = [x for x in xs if x is not None]
        return statistics.median(xs) if xs else 0
    verdicts = {}
    for r in recs:
        v = (r.get('grade') or {}).get('verdict', 'n/a')
        verdicts[v] = verdicts.get(v, 0) + 1
    hits = [h for r in recs for h in (r.get('grade') or {}).get('must_include', [])]
    hit_rate = sum(1 if h.get('verdict') == 'hit' else 0.5 if h.get('verdict') == 'partial' else 0 for h in hits) / max(len(hits), 1)

    lines = [f'# Graph-only agent eval: {datetime.now():%Y-%m-%d %H:%M}', '',
             f'- Matter `{opts.matter}` ({opts.env}); queries `{Path(opts.queries).name}`; {len(recs)} questions.',
             f'- Answering model: `{opts.model}` (tools: kg_* only). Judge: `{opts.judge_model}`.',
             f'- Sessions that errored: {len(recs) - len(ok)}.', '',
             '## Accuracy', '',
             '| Verdict | Count |', '|---|---|'] + [f'| {k} | {v} |' for k, v in sorted(verdicts.items())] + [
             '',
             f'- must_include points hit: **{hit_rate:.0%}** (partial = 0.5) over {len(hits)} points.',
             f'- Answers with a hallucination: **{sum(1 for r in recs if (r.get("grade") or {}).get("hallucination"))}** / {len(recs)}.',
             f'- Mean correctness: **{statistics.mean([(r.get("grade") or {}).get("correctness") or 0 for r in recs]):.2f}**.', '',
             '## Efficiency and speed', '',
             f'- Tool calls per question: median **{med([len(r["tool_calls"]) for r in recs])}**, '
             f'max {max([len(r["tool_calls"]) for r in recs] or [0])}; total {len(calls)}.',
             f'- Turns per question: median {med([r["num_turns"] for r in recs])}.',
             f'- Session wall time: median **{med([r["duration_ms"] for r in recs]) / 1000:.0f}s**, '
             f'p90 {sorted([r["duration_ms"] or 0 for r in recs])[int(0.9 * (len(recs) - 1))] / 1000:.0f}s.',
             f'- Tool latency as measured here (DB round trip {rtt_ms:.0f} ms): median {med([c["wall_ms"] for c in calls]):.0f} ms; '
             f'in-region estimate median {med([max(c["wall_ms"] - c["queries"] * (rtt_ms - 1), 1) for c in calls]):.0f} ms.',
             f'- Time in tools vs total: {sum(c["wall_ms"] for c in calls) / 1000:.0f}s of '
             f'{sum((r["duration_ms"] or 0) for r in recs) / 1000:.0f}s session time.',
             f'- Cost: total ${sum(r["cost_usd"] or 0 for r in recs):.2f}, median ${med([r["cost_usd"] for r in recs]):.3f}/question.',
             f'- Tool errors returned to the model: {sum(1 for c in calls if c["status"] >= 400)} '
             f'({", ".join(sorted({str(c["status"]) for c in calls if c["status"] >= 400})) or "none"}).',
             f'- Tool result size: median {med([c["bytes"] for c in calls]) / 1024:.1f} KB, max {max([c["bytes"] for c in calls] or [0]) / 1024:.1f} KB.',
             '', '| Tool | Calls |', '|---|---|']
    counts = {}
    for c in calls:
        counts[c['tool']] = counts.get(c['tool'], 0) + 1
    lines += [f'| {k} | {v} |' for k, v in sorted(counts.items(), key=lambda kv: -kv[1])]
    lines += ['', '## Per question', '',
              '| ID | Coverage | Verdict | Correct. | Halluc. | Tool calls | Time (s) | Cost | Comment |',
              '|---|---|---|---|---|---|---|---|---|']
    for r in recs:
        g = r.get('grade') or {}
        lines.append(f"| {r['id']} | {r['graph_coverage']} | {g.get('verdict')} | {g.get('correctness')} | "
                     f"{'yes' if g.get('hallucination') else 'no'} | {len(r['tool_calls'])} | "
                     f"{(r['duration_ms'] or 0) / 1000:.0f} | ${r['cost_usd'] or 0:.3f} | "
                     f"{(g.get('comment') or r.get('error') or '').replace('|', '/')[:160]} |")
    return '\n'.join(lines)


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument('--queries', default=str(HERE.parent / 'whitfield.queries.json'))
    ap.add_argument('--matter', default=None, help='defaults to matter_id in the queries file')
    ap.add_argument('--env', default='production')
    ap.add_argument('--model', default='sonnet')
    ap.add_argument('--judge-model', default='opus')
    ap.add_argument('--ids', default='')
    ap.add_argument('--parallel', type=int, default=4)
    ap.add_argument('--max-turns', type=int, default=30)
    ap.add_argument('--timeout', type=int, default=900)
    ap.add_argument('--rtt-ms', type=float, default=280.0, help='DB round trip from this machine (tool_latency.py prints it)')
    ap.add_argument('--scratch', default=os.environ.get('TMPDIR', '/tmp'))
    opts = ap.parse_args()

    data = json.loads(Path(opts.queries).read_text())
    opts.matter = opts.matter or data['matter_id']
    qs = data['queries']
    if opts.ids:
        wanted = set(opts.ids.split(','))
        qs = [q for q in qs if q['id'] in wanted]

    # Semantic search needs the matter's embeddings; fetch them once for all
    # sessions instead of once per session over the WAN.
    opts.vector_cache = str(Path(opts.scratch) / f'vectors-{opts.env}-{opts.matter}.npz')
    if not Path(opts.vector_cache).exists():
        subprocess.run([PYTHON, str(HERE / 'kg_mcp_server.py'), '--build-vector-cache', opts.vector_cache],
                       env={**os.environ, 'KG_EVAL_MATTER': opts.matter, 'KG_EVAL_ENV': opts.env,
                            'KG_TOOLS_TOKEN': 'cache-build'}, check=True)

    stamp = datetime.now().strftime('%Y%m%d-%H%M%S')
    outdir = HERE.parent / 'results' / f'agent-{stamp}'
    outdir.mkdir(parents=True, exist_ok=True)
    print(f'{len(qs)} questions -> {outdir}')

    def one(q):
        rec = answer_question(q, opts, outdir)
        rec['grade'] = judge(q, rec, opts)
        (outdir / f"{q['id']}.json").write_text(json.dumps(rec, indent=1, default=str))
        g = rec['grade']
        print(f"{q['id']}: {g.get('verdict')} corr={g.get('correctness')} calls={len(rec['tool_calls'])} "
              f"t={(rec['duration_ms'] or 0) / 1000:.0f}s ${rec['cost_usd'] or 0:.3f}", flush=True)
        return rec

    with ThreadPoolExecutor(opts.parallel) as ex:
        recs = list(ex.map(one, qs))
    recs.sort(key=lambda r: r['id'])
    report = summarize(recs, opts, opts.rtt_ms)
    (outdir / 'report.md').write_text(report + '\n')
    (outdir / 'summary.json').write_text(json.dumps(recs, indent=1, default=str))
    print('\n' + report)


if __name__ == '__main__':
    main()
