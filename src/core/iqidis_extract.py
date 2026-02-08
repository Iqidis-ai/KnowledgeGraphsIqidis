"""
Extract Knowledge Graph from Iqidis matter documents.

Flow:
1. Fetch documents from Iqidis PostgreSQL (matter_documents -> document -> artifact)
2. Download content from S3 (parallel)
3. Parse and extract entities/relations (parallel)
4. Store Knowledge Graph in PostgreSQL (kg_entities, kg_edges, kg_mentions, etc.)
   - All data stored in PostgreSQL with matter_id isolation
"""
from typing import Dict, Any, List, Tuple, Optional
from concurrent.futures import ThreadPoolExecutor, as_completed

from .config import MAX_DOCUMENT_SIZE_BYTES
from .iqidis_data import get_matter_documents, get_matter_by_id
from .s3_fetcher import download_from_s3
from .parsing.document_parser import DocumentParser, ParsedDocument
from .knowledge_graph import KnowledgeGraph


def _download_and_parse_document(
    doc_info: Dict[str, Any],
    parser: DocumentParser,
    verbose: bool
) -> Tuple[Optional[ParsedDocument], Optional[str]]:
    """
    Download and parse a single document.
    Returns (parsed_document, error_message).
    """
    storage_key = doc_info.get("storage_key")
    original_name = doc_info.get("original_name") or "document"
    mime = doc_info.get("mime") or "application/pdf"
    size_byte = doc_info.get("size_byte")

    if verbose:
        print(f"[{original_name}] Downloading from S3...", flush=True)

    if not storage_key:
        return None, f"No storage_key for {original_name}"

    if size_byte is not None and size_byte > MAX_DOCUMENT_SIZE_BYTES:
        return None, f"Skipped {original_name}: exceeds 50MB limit ({size_byte / (1024*1024):.1f}MB)"

    data = download_from_s3(storage_key)
    if not data:
        return None, f"Failed to download {original_name}"

    if len(data) > MAX_DOCUMENT_SIZE_BYTES:
        return None, f"Skipped {original_name}: downloaded size exceeds 50MB limit"

    if verbose:
        print(f"[{original_name}] Parsing...", flush=True)

    parsed = parser.parse_from_bytes(data, original_name, mime)
    if not parsed or not parsed.text.strip():
        return None, f"Could not extract text from {original_name}"

    return parsed, None


def extract_from_iqidis_matter(
    matter_id: str,
    api_key: str,
    verbose: bool = True,
    max_workers: int = 3,  # Parallel downloads and parsing
) -> Dict[str, Any]:
    """
    Extract entities and relationships from Iqidis matter documents.
    Stores the Knowledge Graph in PostgreSQL (kg_entities, kg_edges, etc.).
    
    Args:
        matter_id: The Iqidis matter ID (UUID)
        api_key: Gemini API key
        verbose: Print progress messages
        max_workers: Number of parallel workers for downloading and parsing
    """
    def _log(msg: str):
        if verbose:
            print(msg, flush=True)

    result = {
        "success": False,
        "matter_id": matter_id,
        "matter_name": "",
        "documents_found": 0,
        "documents_processed": 0,
        "documents_failed": 0,
        "errors": [],
        "stats": {},
    }

    matter = get_matter_by_id(matter_id)
    if not matter:
        result["errors"].append(f"Matter {matter_id} not found in Iqidis database")
        return result

    result["matter_name"] = matter.get("matter_name", matter_id)

    docs = get_matter_documents(matter_id)
    result["documents_found"] = len(docs)

    if not docs:
        result["success"] = True
        result["errors"].append("No documents found for this matter")
        return result

    _log(f"Found {len(docs)} documents. Starting parallel download and parsing with {max_workers} workers...")

    # KnowledgeGraph uses PostgreSQL (kg_entities, kg_edges, etc.)
    kg = KnowledgeGraph(matter_id, api_key=api_key)
    parser = DocumentParser()

    # Phase 1: Download and parse documents in parallel
    parsed_documents: List[Tuple[ParsedDocument, str]] = []
    
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        future_to_doc = {
            executor.submit(_download_and_parse_document, doc_info, parser, verbose): doc_info
            for doc_info in docs
        }
        
        for future in as_completed(future_to_doc):
            doc_info = future_to_doc[future]
            original_name = doc_info.get("original_name") or "document"
            
            try:
                parsed, error = future.result()
                if error:
                    result["documents_failed"] += 1
                    result["errors"].append(error)
                    _log(f"  ✗ {original_name}: {error}")
                elif parsed:
                    parsed_documents.append((parsed, original_name))
                    _log(f"  ✓ {original_name}: Downloaded and parsed")
            except Exception as e:
                result["documents_failed"] += 1
                result["errors"].append(f"{original_name}: {str(e)}")
                _log(f"  ✗ {original_name}: {e}")

    # Phase 2: Extract entities and relations sequentially
    # (Sequential because of concurrent write considerations)
    _log(f"\nExtracting Knowledge Graph from {len(parsed_documents)} documents...")
    
    for parsed, original_name in parsed_documents:
        try:
            # Stores to PostgreSQL (kg_entities, kg_edges, kg_mentions, etc.)
            kg.extraction_pipeline.process_parsed_document(parsed, skip_if_exists=True)
            result["documents_processed"] += 1
            _log(f"  ✓ Extracted from {original_name}")
        except Exception as e:
            result["documents_failed"] += 1
            result["errors"].append(f"{original_name}: {str(e)}")
            _log(f"  ✗ Failed extraction for {original_name}: {e}")

    result["stats"] = kg.get_stats()
    kg.close()
    result["success"] = result["documents_processed"] > 0
    return result
