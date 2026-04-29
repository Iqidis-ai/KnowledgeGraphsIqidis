"""Shared pytest fixtures."""
import uuid
import pytest
from src.core import get_postgres_url
from src.core.storage.postgres_database import PostgreSQLDatabase


@pytest.fixture
def test_matter_id():
    """Unique matter id per test, isolating DB state."""
    return f"test_{uuid.uuid4().hex[:12]}"


@pytest.fixture
def test_db(test_matter_id):
    """PostgreSQLDatabase instance bound to a unique test matter."""
    db = PostgreSQLDatabase(get_postgres_url(), test_matter_id)
    yield db
    # Cleanup
    cur = db._get_cursor()
    cur.execute("DELETE FROM entity_layout WHERE matter_id = %s", (test_matter_id,))
    cur.execute("DELETE FROM matter_layout_meta WHERE matter_id = %s", (test_matter_id,))
    db.conn.commit()
