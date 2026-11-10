# Production-Ready ETL Data Pipeline

A hardened, observable, and modular Python ETL pipeline that extracts raw CSV datasets, validates schema contracts, cleans and transforms data, generates grouped summary aggregations, and writes output atomically.

Built for enterprise reliability with pandas Copy-on-Write support, structured JSON logging, atomic file replacement, comprehensive pytest suite, and non-root multi-stage Docker packaging.

---

## Key Features & Production Capabilities

- **Resilient Extract** — Validates source existence, size, and CSV headers with safe date coercion.
- **Contract & Schema Validation** — Enforces required columns and schema invariants early before processing.
- **Modern Transform** — Uses non-mutating vectorized operations compliant with pandas 2.x/3.x Copy-on-Write (no deprecated `inplace` mutations).
- **Atomic Load** — Writes to unique temporary files and uses atomic filesystem replacement (`os.replace`) to eliminate partial corrupted files on process aborts or disk full events.
- **Enterprise Observability** — Native structured JSON logging (`--json-logs`) with unique run UUIDs, ISO-8601 timestamps, log level controls (`--log-level`), and execution metrics.
- **Standard Process Exit Codes** — Explicit exit codes (0 = Success, 1 = Fatal, 2 = Extract error, 3 = Validation error, 4 = Load error) for workflow orchestrators (Airflow, Dagster, Argo, Prefect, Kubernetes).
- **Hardened Security** — Runs as unprivileged `appuser` (UID 10001) in multi-stage Docker build, includes `.dockerignore` and `.gitignore`.
- **Extensive Test Suite** — Comprehensive pytest test suite covering edge cases, corrupt inputs, schema errors, atomic writing, and CLI exit codes.

---

## Quick Start

```bash
# 1. Clone
git clone https://github.com/Academypeep/data-pipeline.git
cd data-pipeline

# 2. Install dependencies
pip install -r requirements.txt

# 3. Generate sample data
python generate_sample_data.py

# 4. Run the pipeline
python pipeline.py --input data/raw_sales.csv --output data/report.csv

# 5. Run with structured JSON logging for cloud log aggregators
python pipeline.py --input data/raw_sales.csv --output data/report.csv --json-logs --log-level INFO
```

### Exit Codes

| Code | Meaning |
| :--- | :--- |
| `0` | Success |
| `1` | Unexpected fatal runtime error |
| `2` | Extraction error (file missing, empty, or unreadable) |
| `3` | Schema validation error (missing required columns) |
| `4` | Load error (unwritable path, permission failure) |

---

## Docker Deployment

Build and run in a secure, non-root multi-stage container:

```bash
# Build production image
docker build -t etl-pipeline:latest .

# Run container (mounting local data directory)
docker run --rm -v $(pwd)/data:/app/data etl-pipeline:latest \
  --input data/raw_sales.csv --output data/report.csv --json-logs
```

---

## Running Tests

Execute the comprehensive test suite:

```bash
pytest test_pipeline.py -v
```

---

## Project Structure

```
data-pipeline/
├── pipeline.py              # Production ETL pipeline (extract / validate / transform / analyse / load)
├── generate_sample_data.py  # Realistic sales CSV generator
├── test_pipeline.py         # Comprehensive unit & integration tests
├── requirements.txt         # Pinned production dependencies
├── Dockerfile               # Multi-stage, non-root hardened Dockerfile
├── .gitignore               # Git hygiene
├── .dockerignore             # Docker build context hygiene
└── README.md
```

---

## Tech Stack

Python 3.12 · pandas 2/3 · NumPy · pytest · Docker
