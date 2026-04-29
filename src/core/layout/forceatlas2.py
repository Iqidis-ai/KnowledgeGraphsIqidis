"""Pure ForceAtlas2 layout compute. No DB, no I/O.

Produces (x, y, importance) per node. Importance = degree / max_degree,
normalized to [0, 1]; isolated nodes get importance 0.

Falls back to networkx.spring_layout if fa2_modified is not installed.
"""
from typing import Callable, Dict, Iterable, List, Optional, Tuple

import networkx as nx

ProgressCallback = Optional[Callable[[float], None]]


def compute_layout(
    nodes: Iterable[Dict],
    edges: Iterable[Tuple[str, str]],
    iterations: int = 500,
    progress_cb: ProgressCallback = None,
) -> List[Dict]:
    """Compute (x, y, importance) for each node.

    Args:
        nodes: iterable of dicts with at least `entity_id`.
        edges: iterable of (src_id, tgt_id) tuples.
        iterations: FA2 iterations (default 500).
        progress_cb: optional callback(fraction) reported when compute completes.

    Returns:
        list of {"entity_id", "x", "y", "importance"} dicts, one per input node.
    """
    g = nx.Graph()
    for n in nodes:
        g.add_node(n["entity_id"])
    for src, tgt in edges:
        if g.has_node(src) and g.has_node(tgt) and src != tgt:
            g.add_edge(src, tgt)

    if g.number_of_nodes() == 0:
        return []

    positions = _run_fa2(g, iterations, progress_cb)

    # Importance = normalized degree
    degrees = dict(g.degree())
    max_deg = max(degrees.values()) if degrees else 0
    importance = (
        {nid: deg / max_deg for nid, deg in degrees.items()}
        if max_deg > 0
        else {nid: 0.0 for nid in degrees}
    )

    out = []
    for nid in g.nodes():
        x, y = positions.get(nid, (0.0, 0.0))
        out.append({
            "entity_id": nid,
            "x": float(x),
            "y": float(y),
            "importance": float(importance.get(nid, 0.0)),
        })
    return out


def _run_fa2(g: nx.Graph, iterations: int, progress_cb: ProgressCallback) -> Dict[str, Tuple[float, float]]:
    """Try fa2_modified; fall back to spring_layout."""
    try:
        from fa2_modified import ForceAtlas2  # type: ignore
    except ImportError:
        return nx.spring_layout(g, iterations=min(iterations, 200), seed=42)

    fa2 = ForceAtlas2(
        outboundAttractionDistribution=False,
        edgeWeightInfluence=1.0,
        jitterTolerance=1.0,
        barnesHutOptimize=True,
        barnesHutTheta=1.2,
        scalingRatio=2.0,
        strongGravityMode=False,
        gravity=1.0,
        verbose=False,
    )
    positions = fa2.forceatlas2_networkx_layout(g, pos=None, iterations=iterations)

    if progress_cb is not None:
        progress_cb(1.0)
    return positions
