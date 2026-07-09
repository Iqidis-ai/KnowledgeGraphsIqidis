"""Postgres CRUD for extraction_jobs — backs the async /extract path.

State lives in Postgres rather than per-worker memory so a status poll can be
served by any gunicorn worker, not only the one that started the job.
"""
from typing import Optional, Dict

from psycopg2.extras import Json

from src.core.storage.postgres_database import PostgreSQLDatabase

# Job lifecycle states.
QUEUED = "queued"
RUNNING = "running"
DONE = "done"
FAILED = "failed"


class ExtractionJobRepository:
    def __init__(self, db: PostgreSQLDatabase):
        self._db = db

    def create(self, job_id: str, matter_id: str) -> None:
        cur = self._db._get_cursor()
        cur.execute(
            """
            INSERT INTO extraction_jobs (job_id, matter_id, state)
            VALUES (%s, %s, %s)
            """,
            (job_id, matter_id, QUEUED),
        )
        self._db.conn.commit()

    def set_running(self, job_id: str) -> None:
        self._set_state(job_id, RUNNING)

    def set_done(self, job_id: str, result: Dict) -> None:
        cur = self._db._get_cursor()
        cur.execute(
            """
            UPDATE extraction_jobs
               SET state = %s, result = %s, error = NULL, updated_at = now()
             WHERE job_id = %s
            """,
            (DONE, Json(result), job_id),
        )
        self._db.conn.commit()

    def set_failed(self, job_id: str, error: str) -> None:
        cur = self._db._get_cursor()
        cur.execute(
            """
            UPDATE extraction_jobs
               SET state = %s, error = %s, updated_at = now()
             WHERE job_id = %s
            """,
            (FAILED, error, job_id),
        )
        self._db.conn.commit()

    def get(self, job_id: str) -> Optional[Dict]:
        cur = self._db._get_cursor()
        cur.execute(
            """
            SELECT job_id, matter_id, state, result, error, created_at, updated_at
              FROM extraction_jobs WHERE job_id = %s
            """,
            (job_id,),
        )
        row = cur.fetchone()
        if row is None:
            return None
        # _get_cursor() uses RealDictCursor — rows are already dict-like.
        return {
            "job_id": row["job_id"],
            "matter_id": row["matter_id"],
            "state": row["state"],
            "result": row["result"],
            "error": row["error"],
            "created_at": row["created_at"].isoformat() if row["created_at"] else None,
            "updated_at": row["updated_at"].isoformat() if row["updated_at"] else None,
        }

    def _set_state(self, job_id: str, state: str) -> None:
        cur = self._db._get_cursor()
        cur.execute(
            """
            UPDATE extraction_jobs
               SET state = %s, updated_at = now()
             WHERE job_id = %s
            """,
            (state, job_id),
        )
        self._db.conn.commit()
