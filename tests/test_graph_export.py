"""Unit tests for the whole-graph SQLite export builder."""
import sqlite3

from src.api.graph_export import build_export

M = '11111111-1111-1111-1111-111111111111'
DOC = 'dddddddd-0000-0000-0000-000000000001'
IQ_DOC = 'eeeeeeee-0000-0000-0000-000000000001'
ALICE, ACME, FACT, GONE = (f'aaaaaaaa-0000-0000-0000-00000000000{i}' for i in range(1, 5))


class FakeCursor:
    """Returns canned rows for each export query, keyed by its FROM table."""

    ROWS = {
        'kg_documents': [(DOC, IQ_DOC, 'contract.pdf')],
        'kg_entities': [
            (ALICE, 'Person', 'Alice Smith', {'role': 'CEO', 'chunk_start': 0}, 'extracted'),
            (ACME, 'Organization', 'Acme Corp', {}, 'extracted'),
            (FACT, 'Fact', 'Alice guarantees the loan',
             {'full_text': 'Alice personally guarantees the Acme loan.', 'fact_type': 'obligation'}, 'extracted'),
        ],
        'kg_aliases': [(ALICE, 'A. Smith')],
        'edges': [(ALICE, 'employed_by', ACME, DOC, '{"context": "CEO of"}'),
                  (FACT, 'about', ALICE, DOC, '{}'),
                  (GONE, 'related_to', ACME, DOC, None)],  # endpoint not active: dropped
        'binds': [('Alice must repay', DOC, [ALICE], [ACME, GONE])],
        'kg_mentions': [(ALICE, DOC, 'Alice', 'signed by Alice Smith, CEO of Acme')],
    }

    def execute(self, sql, params=None):
        table = sql.split('FROM')[1].split()[0]
        if table == 'kg_edges':
            table = 'binds' if "relation_type = 'binds'" in sql else 'edges'
        self._rows = self.ROWS[table]

    def fetchall(self):
        return self._rows


def _db(tmp_path):
    data, counts = build_export(FakeCursor(), M)
    path = tmp_path / 'graph.sqlite'
    path.write_bytes(data)
    return sqlite3.connect(path), counts


def test_counts_and_dropped_edges(tmp_path):
    db, counts = _db(tmp_path)
    assert counts['entities'] == 3
    assert counts['relations'] == 2
    assert counts['obligations'] == 1
    assert counts['obligation_parties'] == 2
    assert db.execute('PRAGMA integrity_check').fetchone()[0] == 'ok'


def test_documents_use_iqidis_id_and_props_are_cleaned(tmp_path):
    db, _ = _db(tmp_path)
    assert db.execute('SELECT document_id, filename FROM documents').fetchall() == [(IQ_DOC, 'contract.pdf')]
    props, = db.execute("SELECT props FROM entities WHERE name = 'Alice Smith'").fetchone()
    assert props == '{"role": "CEO"}'
    text, props = db.execute("SELECT text, props FROM entities WHERE type = 'Fact'").fetchone()
    assert text == 'Alice personally guarantees the Acme loan.'
    assert 'full_text' not in props


def test_relations_view_and_search(tmp_path):
    db, _ = _db(tmp_path)
    assert db.execute("SELECT src_name, rel, dst_name, filename FROM relations WHERE rel = 'employed_by'").fetchall() == [
        ('Alice Smith', 'employed_by', 'Acme Corp', 'contract.pdf')]
    assert db.execute("SELECT props FROM edges WHERE rel = 'about'").fetchone()[0] is None
    kinds = {k for k, in db.execute("SELECT kind FROM search WHERE search MATCH 'alice OR smith OR repay'")}
    assert kinds == {'name', 'alias', 'fact', 'mention', 'obligation'}
    readme = db.execute("SELECT value FROM meta WHERE key = 'readme'").fetchone()[0]
    assert 'obligation_parties' in readme
