"""Orchestrates layout compute: reads graph, runs FA2, writes results.

Per-matter locking ensures only one compute runs at a time. Concurrent
callers either trigger compute (first one) or block waiting (subsequent).

Schema notes (verified against live DB 2026-04-29):
  kg_entities: entity PK is 'id'  (plan assumed 'entity_id')
  kg_edges:    edge endpoints are 'source_entity_id' / 'target_entity_id'
               (plan assumed 'source_id' / 'target_id')
"""
import datetime as dt
import logging
import threading
from typing import Dict, Optional

from src.core import get_postgres_url
from src.core.storage.postgres_database import PostgreSQLDatabase
from src.core.layout.forceatlas2 import compute_layout
from src.core.layout.layout_repository import LayoutRepository

log = logging.getLogger(__name__)

# Per-matter locks. Module-level so they survive across requests.
_locks: Dict[str, threading.Lock] = {}
_locks_guard = threading.Lock()


def _lock_for(matter_id: str) -> threading.Lock:
	with _locks_guard:
		if matter_id not in _locks:
			_locks[matter_id] = threading.Lock()
		return _locks[matter_id]


SYNC_THRESHOLD = 5_000  # entities; below this we run synchronously


class LayoutService:
	def __init__(self, matter_id: str, db: Optional[PostgreSQLDatabase] = None):
		self._matter_id = matter_id
		self._db = db or PostgreSQLDatabase(get_postgres_url(), matter_id)
		self._repo = LayoutRepository(self._db, matter_id)

	def status(self) -> Optional[Dict]:
		meta = self._repo.get_meta()
		if meta is None:
			return None
		# Staleness check
		current_count = self._current_entity_count()
		if (
			meta["status"] == "ready"
			and meta["entity_count"]
			and current_count > meta["entity_count"] * 1.10
		):
			self._repo.set_status("stale")
			meta["status"] = "stale"
		return meta

	def trigger_compute(self) -> Dict:
		"""Returns immediately. Compute runs in a background thread unless
		the matter is small enough for the sync fast path."""
		meta = self._repo.get_meta()
		if meta and meta["status"] == "computing":
			return meta

		count = self._current_entity_count()
		if count <= SYNC_THRESHOLD:
			self._compute_blocking()
			return self._repo.get_meta()

		self._repo.set_status("computing", progress=0.0)
		thread = threading.Thread(
			target=self._compute_blocking,
			name=f"layout-{self._matter_id}",
			daemon=True,
		)
		thread.start()
		return self._repo.get_meta()

	def _compute_blocking(self) -> None:
		lock = _lock_for(self._matter_id)
		with lock:
			try:
				self._repo.set_status("computing", progress=0.0)
				nodes, edges = self._read_graph()

				def progress_cb(frac: float):
					self._repo.set_status("computing", progress=frac)

				rows = compute_layout(nodes, edges, iterations=500, progress_cb=progress_cb)
				self._repo.write_positions(rows)
				self._repo.set_status(
					"ready",
					entity_count=len(nodes),
					computed_at=dt.datetime.utcnow(),
					progress=1.0,
				)
			except Exception as exc:  # noqa: BLE001 — caught for status reporting
				log.exception("layout compute failed for matter %s", self._matter_id)
				self._repo.set_status("failed", error=str(exc))

	# ---- helpers ----
	def _current_entity_count(self) -> int:
		cur = self._db._get_cursor()
		# Column is 'id' (PK), not 'entity_id'; filter by matter_id (UUID col).
		# Cast to text so a non-UUID smoke-test string doesn't raise DataError.
		cur.execute(
			"SELECT COUNT(*) FROM kg_entities WHERE matter_id::text = %s",
			(self._matter_id,),
		)
		# RealDictCursor returns the count under a key like 'count'; normalize.
		row = cur.fetchone()
		if row is None:
			return 0
		if isinstance(row, dict):
			return int(next(iter(row.values())))
		return int(row[0])

	def _read_graph(self):
		cur = self._db._get_cursor()
		# 'id' is the entity PK; cast matter_id to text for safety.
		cur.execute(
			"SELECT id FROM kg_entities WHERE matter_id::text = %s",
			(self._matter_id,),
		)
		rows = cur.fetchall()
		nodes = [{"entity_id": (r["id"] if isinstance(r, dict) else r[0])} for r in rows]

		# Edge endpoints are 'source_entity_id' / 'target_entity_id'.
		cur.execute(
			"SELECT source_entity_id, target_entity_id FROM kg_edges WHERE matter_id::text = %s",
			(self._matter_id,),
		)
		rows = cur.fetchall()
		if rows and isinstance(rows[0], dict):
			edges = [(r["source_entity_id"], r["target_entity_id"]) for r in rows]
		else:
			edges = [(r[0], r[1]) for r in rows]
		return nodes, edges
