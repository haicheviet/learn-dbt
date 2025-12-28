# Production-Grade dbt + AI Data Pipeline

This project is a learning playground for building structured, production-ready data pipelines. It combines the transformation power of **dbt** (running on **DuckDB**) with a **Python AI Service** for feature engineering and data validation.

## 🏗 Architecture

The pipeline follows the **Medallion Architecture** with an added AI enrichment layer:

1.  **Bronze (Raw)**: Raw data ingestion from CSV seeds (simulating S3/APIs).
2.  **Silver (Staging)**: 
    - Deduplication using `row_number()`.
    - Data cleaning and schema standardization.
    - **Quality Gates**: Filtering out invalid records (e.g., missing prices) to prevent downstream failures.
3.  **Gold (Mart)**: Business-ready dimensions and tables.
4.  **AI Engine (Python)**:
    - **Pydantic Firewall**: Strict data contract validation.
    - **Content-based Hashing**: Generates stable `doc_id` (SHA-256) based on sensitive fields to solve "Ghost Results" in Vector DBs.
    - **Weighted Embedding Prep**: Prepares text for Vector Search with field importance (e.g., Title x3).

## 🚀 Getting Started

### Prerequisites
- Python 3.9+
- dbt-duckdb

### Setup
```bash
# Create and activate virtual environment
python3 -m venv .venv
source .venv/bin/activate

# Install dependencies
pip install -r requirements.txt
```

### Running the Pipeline
The project includes a custom `orchestrator.py` that handles the dbt lifecycle and Python processing:

```bash
python3 orchestrator.py
```

This will:
1. Initialize the database and run dbt migrations/seeds.
2. Build and Test the Silver layer.
3. Build the Gold layer.
4. Process data via the AI Engine and output `ready_to_index.json`.

## 🧪 Testing

We use `pytest` for unit testing the AI logic and dbt tests for data quality.

```bash
# Run all Python tests
python3 -m pytest tests/

# Run dbt tests only
dbt test
```

## 🛠 Features
- **Idempotency**: Rerunning the pipeline results in the same `doc_id` for identical content.
- **Halt-on-Fail**: dbt tests act as a "circuit breaker" for the Python service.
- **CI/CD**: GitHub Actions workflow included in `.github/workflows/ci.yml`.

---
*Created for learning dbt and production data engineering best practices.*
