"""Tests for the pure FA2 compute function (no DB)."""
from src.core.layout.forceatlas2 import compute_layout


def test_compute_layout_returns_one_row_per_node():
    nodes = [{"entity_id": "a"}, {"entity_id": "b"}, {"entity_id": "c"}]
    edges = [("a", "b"), ("b", "c")]
    result = compute_layout(nodes, edges, iterations=50)
    assert len(result) == 3
    ids = {r["entity_id"] for r in result}
    assert ids == {"a", "b", "c"}


def test_compute_layout_assigns_finite_coordinates():
    nodes = [{"entity_id": str(i)} for i in range(20)]
    edges = [(str(i), str(i + 1)) for i in range(19)]
    result = compute_layout(nodes, edges, iterations=100)
    for row in result:
        assert isinstance(row["x"], float)
        assert isinstance(row["y"], float)
        assert -1e9 < row["x"] < 1e9
        assert -1e9 < row["y"] < 1e9


def test_importance_is_normalized_zero_to_one():
    nodes = [{"entity_id": str(i)} for i in range(5)]
    # node 0 is a hub
    edges = [("0", str(i)) for i in range(1, 5)]
    result = compute_layout(nodes, edges, iterations=50)
    importances = [r["importance"] for r in result]
    assert max(importances) == 1.0
    assert min(importances) >= 0.0
    hub = next(r for r in result if r["entity_id"] == "0")
    assert hub["importance"] == 1.0


def test_isolated_nodes_get_importance_zero():
    nodes = [{"entity_id": "a"}, {"entity_id": "b"}, {"entity_id": "c"}]
    edges = [("a", "b")]  # c is isolated
    result = compute_layout(nodes, edges, iterations=50)
    isolated = next(r for r in result if r["entity_id"] == "c")
    assert isolated["importance"] == 0.0
