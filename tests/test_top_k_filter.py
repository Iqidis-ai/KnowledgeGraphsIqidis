"""Unit tests for the top-K filter helper."""
from src.api.server import _apply_top_k, DEFAULT_TYPE_QUOTAS


def _nodes():
    return [
        {"entity_id": "p1", "type": "Person", "importance": 0.9},
        {"entity_id": "p2", "type": "Person", "importance": 0.8},
        {"entity_id": "o1", "type": "Organization", "importance": 0.95},
        {"entity_id": "d1", "type": "Document", "importance": 0.4},
        {"entity_id": "d2", "type": "Document", "importance": 0.3},
        {"entity_id": "d3", "type": "Document", "importance": 0.2},
        {"entity_id": "x1", "type": "Unknown", "importance": 0.1},
    ]


def test_apply_top_k_none_returns_unchanged():
    nodes = _nodes()
    result = _apply_top_k(nodes, "none", top_k=2, per_type={})
    assert len(result) == len(nodes)


def test_apply_top_k_degree_trims_to_top_k():
    nodes = _nodes()
    result = _apply_top_k(nodes, "degree", top_k=3, per_type={})
    assert len(result) == 3
    importances = [n["importance"] for n in result]
    assert importances == sorted(importances, reverse=True)
    assert importances[0] == 0.95


def test_apply_top_k_degree_no_limit_returns_all():
    nodes = _nodes()
    result = _apply_top_k(nodes, "degree", top_k=None, per_type={})
    assert len(result) == len(nodes)


def test_apply_top_k_type_quota_caps_per_type():
    nodes = _nodes()
    # Cap Document at 2; keep others uncapped via large defaults
    per_type = {"Person": 10, "Organization": 10, "Document": 2, "Unknown": 10}
    result = _apply_top_k(nodes, "type_quota", top_k=None, per_type=per_type)
    docs = [n for n in result if n["type"] == "Document"]
    assert len(docs) == 2
    # Highest-importance Documents survived
    assert {d["entity_id"] for d in docs} == {"d1", "d2"}
    # Other types fully preserved
    assert sum(1 for n in result if n["type"] == "Person") == 2


def test_apply_top_k_type_quota_respects_overall_cap():
    nodes = _nodes()
    per_type = {t: 100 for t in DEFAULT_TYPE_QUOTAS}
    result = _apply_top_k(nodes, "type_quota", top_k=3, per_type=per_type)
    assert len(result) == 3
    # Top 3 across all types by importance
    assert [n["entity_id"] for n in result] == ["o1", "p1", "p2"]


def test_apply_top_k_type_quota_unknown_type_falls_back_to_top_k():
    nodes = _nodes()
    # 'Unknown' isn't in per_type; cap=top_k=1 → its single node survives
    per_type = {"Person": 10, "Organization": 10, "Document": 10}
    result = _apply_top_k(nodes, "type_quota", top_k=1, per_type=per_type)
    # top_k=1 trims overall result to highest-importance single node
    assert len(result) == 1
    assert result[0]["entity_id"] == "o1"
