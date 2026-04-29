"""Tests for LayoutRepository CRUD."""
import datetime as dt
from src.core.layout.layout_repository import LayoutRepository


def test_get_meta_returns_none_for_unknown_matter(test_db, test_matter_id):
    repo = LayoutRepository(test_db, test_matter_id)
    assert repo.get_meta() is None


def test_set_status_creates_row(test_db, test_matter_id):
    repo = LayoutRepository(test_db, test_matter_id)
    repo.set_status("computing", progress=0.0)
    meta = repo.get_meta()
    assert meta["status"] == "computing"
    assert meta["progress"] == 0.0


def test_write_positions_round_trip(test_db, test_matter_id):
    repo = LayoutRepository(test_db, test_matter_id)
    rows = [
        {"entity_id": "a", "x": 1.0, "y": 2.0, "importance": 0.5},
        {"entity_id": "b", "x": 3.5, "y": -4.0, "importance": 1.0},
    ]
    repo.write_positions(rows)
    repo.set_status("ready", entity_count=2, computed_at=dt.datetime.utcnow())
    fetched = repo.query_viewport(x_min=-10, x_max=10, y_min=-10, y_max=10, min_importance=0.0, limit=100)
    assert len(fetched) == 2
    by_id = {r["entity_id"]: r for r in fetched}
    assert by_id["a"]["x"] == 1.0 and by_id["a"]["importance"] == 0.5


def test_query_viewport_filters_by_bbox(test_db, test_matter_id):
    repo = LayoutRepository(test_db, test_matter_id)
    repo.write_positions([
        {"entity_id": "in", "x": 0.0, "y": 0.0, "importance": 1.0},
        {"entity_id": "out", "x": 100.0, "y": 100.0, "importance": 1.0},
    ])
    repo.set_status("ready", entity_count=2)
    inside = repo.query_viewport(x_min=-1, x_max=1, y_min=-1, y_max=1, min_importance=0.0, limit=100)
    assert {r["entity_id"] for r in inside} == {"in"}


def test_query_viewport_filters_by_importance(test_db, test_matter_id):
    repo = LayoutRepository(test_db, test_matter_id)
    repo.write_positions([
        {"entity_id": "hub", "x": 0.0, "y": 0.0, "importance": 1.0},
        {"entity_id": "dust", "x": 0.5, "y": 0.5, "importance": 0.05},
    ])
    repo.set_status("ready", entity_count=2)
    only_hubs = repo.query_viewport(x_min=-2, x_max=2, y_min=-2, y_max=2, min_importance=0.5, limit=100)
    assert {r["entity_id"] for r in only_hubs} == {"hub"}


def test_query_viewport_respects_limit(test_db, test_matter_id):
    repo = LayoutRepository(test_db, test_matter_id)
    rows = [{"entity_id": str(i), "x": float(i), "y": 0.0, "importance": float(i) / 10}
            for i in range(10)]
    repo.write_positions(rows)
    repo.set_status("ready", entity_count=10)
    capped = repo.query_viewport(x_min=-1, x_max=20, y_min=-1, y_max=1, min_importance=0.0, limit=3)
    assert len(capped) == 3
    # Highest importance returned first
    assert capped[0]["importance"] >= capped[1]["importance"] >= capped[2]["importance"]
