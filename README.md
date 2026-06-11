# ETL Data Pipeline

A modular Python ETL pipeline that extracts raw CSV data, cleans and validates it, generates a grouped summary report, and writes the output — all in a single command.

Built to demonstrate practical data engineering skills: pandas-based transformation, integrity tracking, logging, unit testing, and Docker deployment.

---

## Features

- **Extract** — loads any CSV with automatic date parsing
- **Transform** — deduplication, missing-value imputation, column normalisation, derived date fields
- **Analyse** — grouped summary with sum/mean/count aggregations
- **Load** — writes clean output CSV with row-level logging
- **Integrity tracking** — reports % of records retained through the pipeline
- **Unit tested** — pytest suite covering all pipeline stages
- **Dockerised** — runs in an isolated container with one command

---

## Quick start

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
```

**Sample output:**
```
2024-01-15 09:12:03 [INFO] Extracting data from data/raw_sales.csv
2024-01-15 09:12:03 [INFO]   Loaded 500 rows, 6 columns
2024-01-15 09:12:03 [INFO] Transforming data...
2024-01-15 09:12:03 [INFO]   Filled 24 missing values in 'revenue' with median 12543.50
2024-01-15 09:12:03 [INFO]   Removed 12 duplicate/empty rows — data integrity: 97.6%
2024-01-15 09:12:03 [INFO] Generating summary report...
2024-01-15 09:12:03 [INFO]   Summary report: 40 rows
2024-01-15 09:12:03 [INFO] Report written to data/report.csv  (40 rows)
2024-01-15 09:12:03 [INFO] Pipeline complete in 0.31s
```

---

## Docker

```bash
# Build
docker build -t etl-pipeline .

# Run (mounts local data/ folder)
docker run --rm -v $(pwd)/data:/app/data etl-pipeline \
  python pipeline.py --input data/raw_sales.csv --output data/report.csv
```

---

## Running tests

```bash
pytest test_pipeline.py -v
```

---

## Project structure

```
data-pipeline/
├── pipeline.py              # Main ETL script (extract / transform / analyse / load)
├── generate_sample_data.py  # Generates a realistic 500-row test CSV
├── test_pipeline.py         # pytest unit tests
├── requirements.txt
├── Dockerfile
└── README.md
```

---

## Tech stack

Python · pandas · NumPy · pytest · Docker

---

## Author

Peter Griffin — [linkedin.com/in/peter-kamau-ba35b0175](https://linkedin.com/in/peter-kamau-ba35b0175)
