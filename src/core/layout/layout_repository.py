"""Postgres CRUD for entity_layout and matter_layout_meta."""
import datetime as dt
from typing import Iterable, List, Optional, Dict

from psycopg2.extras import execute_values

from src.core.storage.postgres_database import PostgreSQLDatabase


class LayoutRepository:
    def __init__(self, db: PostgreSQLDatabase, matter_id: str):
        self._db = db
        self._matter_id = matter_id

    def get_meta(self) -> Optional[Dict]:
        cur = self._db._get_cursor()
        cur.execute("""
            SELECT status, computed_at, entity_count, algorithm, progress, error
            FROM matter_layout_meta WHERE matter_id = %s
        """, (self._matter_id,))
        row = cur.fetchone()
        if row is None:
            return None
        # _get_cursor() uses RealDictCursor — rows are already dict-like
        return {
            "status": row["status"],
            "computed_at": row["computed_at"],
            "entity_count": row["entity_count"],
            "algorithm": row["algorithm"],
            "progress": row["progress"],
            "error": row["error"],
        }

    def set_status(
        self,
        status: str,
        progress: Optional[float] = None,
        entity_count: Optional[int] = None,
        computed_at: Optional[dt.datetime] = None,
        error: Optional[str] = None,
    ) -> None:
        cur = self._db._get_cursor()
        cur.execute("""
            INSERT INTO matter_layout_meta
                (matter_id, status, progress, entity_count, computed_at, error, algorithm)
            VALUES (%s, %s, %s, %s, %s, %s, 'forceatlas2')
            ON CONFLICT (matter_id) DO UPDATE SET
                status       = EXCLUDED.status,
                progress     = COALESCE(EXCLUDED.progress, matter_layout_meta.progress),
                entity_count = COALESCE(EXCLUDED.entity_count, matter_layout_meta.entity_count),
                computed_at  = COALESCE(EXCLUDED.computed_at, matter_layout_meta.computed_at),
                error        = EXCLUDED.error
        """, (self._matter_id, status, progress, entity_count, computed_at, error))
        self._db.conn.commit()

    def write_positions(self, rows: Iterable[Dict]) -> None:
        """Bulk-replace positions for this matter."""
        rows = list(rows)
        cur = self._db._get_cursor()
        cur.execute("DELETE FROM entity_layout WHERE matter_id = %s", (self._matter_id,))
        if rows:
            execute_values(cur, """
                INSERT INTO entity_layout (matter_id, entity_id, x, y, importance)
                VALUES %s
            """, [
                (self._matter_id, r["entity_id"], r["x"], r["y"], r["importance"])
                for r in rows
            ])
        self._db.conn.commit()

    def query_viewport(
        self,
        x_min: float,
        x_max: float,
        y_min: float,
        y_max: float,
        min_importance: float,
        limit: int,
    ) -> List[Dict]:
        cur = self._db._get_cursor()
        cur.execute("""
            SELECT entity_id, x, y, importance
            FROM entity_layout
            WHERE matter_id = %s
              AND x BETWEEN %s AND %s
              AND y BETWEEN %s AND %s
              AND importance >= %s
            ORDER BY importance DESC
            LIMIT %s
        """, (self._matter_id, x_min, x_max, y_min, y_max, min_importance, limit))
        return [
            {"entity_id": r["entity_id"], "x": r["x"], "y": r["y"], "importance": r["importance"]}
            for r in cur.fetchall()
        ]
