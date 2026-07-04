"""One-time backfill + cleanup for Document entities.

Fixes three data problems in already-extracted matters (new extractions are
handled at write time by the pipeline):

  1. MISSING UPLOADS — uploads whose document type the structural detector
     didn't recognize (receipts, invoices, …) got no Document entity at all,
     so the matter's own files never appeared in the sidebar. Creates a
     structural Document entity (+ a mention for provenance) named after the
     filename for every processed source file that lacks one.

  2. MISCLASSIFIED DOCUMENTS — LLM-extracted "Document" entities that are
     really identifiers, measurements or payment rails ("411 sft", "IMPS",
     "ICIC0000002") are reclassified to Reference.

  3. NOISE — entities whose names are extraction junk (bare reference
     numbers, boilerplate like "system generated document") are tombstoned.
     Structural (uploaded-file) entities are exempt.

Usage:
    python backfill_document_entities.py --matter-id <uuid> [--dry-run]
    python backfill_document_entities.py --all-matters [--dry-run]

Uses the APP_ENV-selected Postgres URL from .env (same as the API server).
"""
import argparse
import json
import os
import sys
import uuid
from datetime import datetime

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

import psycopg2
import psycopg2.extras

from src.core.config import get_postgres_url
from src.core.extraction.extraction_pipeline import (
    _is_noise_entity_name,
    _reclassify_implausible_document,
)


def backfill_matter(cur, matter_id: str, dry_run: bool) -> dict:
    stats = {"created": 0, "reclassified": 0, "tombstoned": 0}

    # ── 1. Create missing structural Document entities for uploads ──
    cur.execute(
        "SELECT id, filename FROM kg_documents WHERE matter_id = %s AND processed_at IS NOT NULL",
        (matter_id,),
    )
    docs = cur.fetchall()
    cur.execute(
        "SELECT lower(canonical_name) AS name FROM kg_entities "
        "WHERE matter_id = %s AND type = 'Document' AND status = 'active'",
        (matter_id,),
    )
    existing_names = {r["name"] for r in cur.fetchall()}

    for doc in docs:
        stem = os.path.splitext(os.path.basename(doc["filename"] or ""))[0].strip()
        doc_name = stem or f"Doc_{str(doc['id'])[:8]}"
        legacy_name = f"Doc_{str(doc['id'])[:8]}"
        if doc_name.lower() in existing_names or legacy_name.lower() in existing_names:
            continue
        stats["created"] += 1
        print(f"  + create Document entity: {doc_name!r}")
        if dry_run:
            continue
        entity_id = str(uuid.uuid4())
        now = datetime.now()
        cur.execute(
            """
            INSERT INTO kg_entities (id, matter_id, type, canonical_name, properties,
                                     confidence, status, created_at, updated_at)
            VALUES (%s, %s, 'Document', %s, %s, 'confirmed', 'active', %s, %s)
            """,
            (entity_id, matter_id, doc_name,
             json.dumps({"document_type": None, "source": "structural",
                         "backfilled": True}),
             now, now),
        )
        cur.execute(
            """
            INSERT INTO kg_mentions (id, entity_id, doc_id, span_start,
                                     span_end, surface_text, context_snippet)
            VALUES (%s, %s, %s, 0, 0, %s, %s)
            """,
            (str(uuid.uuid4()), entity_id, doc["id"],
             doc["filename"] or doc_name, "source file (backfilled)"),
        )
        existing_names.add(doc_name.lower())

    # ── 2. Reclassify implausible Document entities → Reference ──
    cur.execute(
        "SELECT id, canonical_name, properties FROM kg_entities "
        "WHERE matter_id = %s AND type = 'Document' AND status = 'active'",
        (matter_id,),
    )
    for row in cur.fetchall():
        props = row["properties"] if isinstance(row["properties"], dict) else {}
        if props.get("source") == "structural":
            continue
        demoted = _reclassify_implausible_document(row["canonical_name"])
        if demoted:
            stats["reclassified"] += 1
            print(f"  ~ Document → {demoted}: {row['canonical_name']!r}")
            if not dry_run:
                cur.execute(
                    "UPDATE kg_entities SET type = %s, updated_at = %s WHERE id = %s",
                    (demoted, datetime.now(), row["id"]),
                )

    # ── 3. Tombstone noise entities (non-structural only) ──
    # Facts are excluded: their "obligation:/key_term:" prefixes match the
    # display-side noise patterns, but they carry real extracted content and
    # are already hidden from the canvas by default — destroying them in the
    # store is out of proportion for a cleanup pass.
    cur.execute(
        "SELECT id, canonical_name, type, properties FROM kg_entities "
        "WHERE matter_id = %s AND status = 'active' AND type != 'Fact'",
        (matter_id,),
    )
    for row in cur.fetchall():
        props = row["properties"] if isinstance(row["properties"], dict) else {}
        if props.get("source") == "structural":
            continue
        if _is_noise_entity_name(row["canonical_name"], row["type"]):
            stats["tombstoned"] += 1
            print(f"  - tombstone {row['type']}: {row['canonical_name']!r}")
            if not dry_run:
                cur.execute(
                    "UPDATE kg_entities SET status = 'tombstone', updated_at = %s WHERE id = %s",
                    (datetime.now(), row["id"]),
                )

    return stats


def main():
    ap = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    group = ap.add_mutually_exclusive_group(required=True)
    group.add_argument("--matter-id", help="Backfill a single matter")
    group.add_argument("--all-matters", action="store_true",
                       help="Backfill every matter that has KG data")
    ap.add_argument("--dry-run", action="store_true",
                    help="Print what would change without writing")
    args = ap.parse_args()

    conn = psycopg2.connect(get_postgres_url())
    cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)

    if args.all_matters:
        cur.execute("SELECT DISTINCT matter_id FROM kg_entities")
        matter_ids = [r["matter_id"] for r in cur.fetchall()]
    else:
        matter_ids = [args.matter_id]

    totals = {"created": 0, "reclassified": 0, "tombstoned": 0}
    for mid in matter_ids:
        print(f"\nMatter {mid}:")
        stats = backfill_matter(cur, mid, args.dry_run)
        for k in totals:
            totals[k] += stats[k]
        if not args.dry_run:
            conn.commit()

    mode = "DRY RUN — nothing written" if args.dry_run else "committed"
    print(f"\nDone ({mode}): {totals['created']} entities created, "
          f"{totals['reclassified']} reclassified, {totals['tombstoned']} tombstoned "
          f"across {len(matter_ids)} matter(s).")
    conn.close()


if __name__ == "__main__":
    main()
