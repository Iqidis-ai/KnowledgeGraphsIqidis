# Legal Knowledge Graph System

A Python system for extracting, querying, and visualizing legal knowledge graphs from matter documents. It combines structural extraction, Gemini-assisted semantic extraction, SQLite storage, FAISS-backed vector search, CLI tools, and a Flask/D3 visualization interface.

## Features

- Parse PDF, DOCX, DOC, and TXT matter documents
- Extract typed entities, aliases, relationships, mentions, documents, events, clauses, money, dates, locations, references, and facts
- Store each matter under `matters/<matter_name>/` with a SQLite graph database and vector index files
- Query the graph in natural language with schema-aware fallbacks
- Export JSON, D3, CSV, GraphML, and archive formats
- Explore and edit the graph through the local visualization server

## Setup

```bash
python -m venv venv
venv\Scripts\activate
pip install -r requirements.txt
copy .env.example .env
```

Edit `.env` and set:

```bash
GEMINI_API_KEY=your-api-key-here
GEMINI_MODEL=gemini-2.5-flash-lite
```

## CLI Usage

Extract a matter:

```bash
python -m src.cli.extract --matter my_case
python -m src.cli.extract --matter my_case --dir .\documents
python -m src.cli.extract --matter my_case --file .\document.pdf
```

Query a matter:

```bash
python -m src.cli.query --matter my_case "Who are the main parties?"
python -m src.cli.query --matter my_case --interactive
python -m src.cli.query --matter my_case --list-entities Person
python -m src.cli.query --matter my_case --stats
```

Export a graph:

```bash
python -m src.cli.export --matter my_case --output graph.json
python -m src.cli.export --matter my_case --format d3 --output viz.json
python -m src.cli.export --matter my_case --format csv-entities --output entities.csv
python -m src.cli.export --matter my_case --format csv-edges --output edges.csv
```

## Visualization

```bash
python visualization_server.py --matter my_case --port 5000
```

Open `http://localhost:5000`.

The visualization server provides graph loading, entity search, natural-language queries, entity CRUD, edge CRUD, entity merge, natural-language graph editing, stats, export endpoints, and static D3 visualization assets.

## Python Usage

```python
from src.core import KnowledgeGraph

kg = KnowledgeGraph("my_case")
kg.add_document("path/to/document.pdf")

result = kg.query("Who are the main parties in this case?")
print(result.answer)

summary = kg.get_entity_summary("ACME Corporation")
print(summary)
```

## Project Structure

```text
KnowledgeGraphsIqidis/
  src/
    core/
      config.py
      knowledge_graph.py
      storage/
        database.py
        models.py
      extraction/
        extraction_pipeline.py
        semantic_extractor.py
        structural_extractor.py
      parsing/
        chunker.py
        document_parser.py
      query/
        nl_query.py
      embeddings/
        vector_store.py
      inference/
        graph_inference.py
    api/
      server.py
    cli/
      extract.py
      query.py
      export.py
    visualization/
      graph_exporter.py
  visualization/
    index.html
    graph_data.json
  matters/
    <matter_name>/
      documents/
      graph.db
      embeddings.faiss
      embeddings.pkl
  visualization_server.py
  requirements.txt
```

## Configuration

| Variable | Default | Description |
| --- | --- | --- |
| `GEMINI_API_KEY` | required | Gemini API key for semantic extraction and NL query/edit flows |
| `GEMINI_MODEL` | `gemini-2.5-flash-lite` | Gemini model used by extraction and query components |

Core settings live in `src/core/config.py`, including chunk size, overlap, entity types, relation types, and confidence labels.

## Data Hygiene

Matter data is intentionally ignored by git. Keep source documents, generated SQLite databases, FAISS indexes, pickles, logs, and `.env` files out of commits. The root-level `citiom_v_gulfstream` file is an ad hoc SQLite database and is ignored.

## Verification

```bash
python -m compileall -q src visualization_server.py generate_visualization.py
python -c "import flask, flask_cors, google.generativeai, fitz, docx, json_repair, numpy; print('imports ok')"
```

Full extraction/query checks require a populated `matters/` directory and `GEMINI_API_KEY`.
