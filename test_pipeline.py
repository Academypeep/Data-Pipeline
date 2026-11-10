"""Production unit and integration tests for the ETL data pipeline."""
from __future__ import annotations

import json
import os
from pathlib import Path

import pandas as pd
import pytest

from pipeline import (
    AtomicLoader,
    AtomicWriteError,
    ConfigurationError,
    DataPipelineError,
    ExtractionError,
    Extractor,
    FileCorruptedError,
    PathSecurityError,
    PipelineConfig,
    PipelineError,
    SchemaValidator,
    SchemaViolationError,
    StorageIOError,
    Transformer,
    ValidationError,
    analyse,
    extract,
    load,
    main,
    run_pipeline,
    sanitize_and_validate_path,
    transform,
    validate_schema,
)


@pytest.fixture
def sample_csv(tmp_path: Path) -> str:
    path = tmp_path / "sales_sample.csv"
    path.write_text(
        "date,region,category,revenue,units\n"
        "01/01/2024,Nairobi,Hardware,10000,5\n"
        "02/01/2024,Mombasa,Software,,3\n"
        "02/01/2024,Mombasa,Software,,3\n"   # duplicate row
        "03/01/2024,Kisumu,Training,8000,2\n"
        ",,,,\n"                              # empty row
    )
    return str(path)


# ── EXTRACT & SECURITY TESTS ──────────────────────────────────────────────────

def test_extract_loads_correct_row_count(sample_csv: str) -> None:
    df = extract(sample_csv)
    # 5 rows total loaded from CSV (including empty and duplicate)
    assert len(df) == 5
    assert "date" in df.columns


def test_extract_missing_file_raises_extraction_error(tmp_path: Path) -> None:
    missing_file = str(tmp_path / "does_not_exist.csv")
    with pytest.raises(ExtractionError, match="Input file not found"):
        extract(missing_file)


def test_extract_empty_file_raises_extraction_error(tmp_path: Path) -> None:
    empty_file = tmp_path / "empty.csv"
    empty_file.write_text("")
    with pytest.raises(FileCorruptedError, match="Input file is empty"):
        extract(str(empty_file))


def test_extract_invalid_csv_raises_extraction_error(tmp_path: Path) -> None:
    # A directory path passed as a file
    with pytest.raises(ExtractionError):
        extract(str(tmp_path))


def test_path_sanitization_null_byte() -> None:
    with pytest.raises(PathSecurityError, match="null byte"):
        sanitize_and_validate_path("some_file\0.csv")


def test_path_sanitization_directory_traversal(tmp_path: Path) -> None:
    base = tmp_path / "sandbox"
    base.mkdir()
    outside_file = tmp_path / "secret.csv"
    outside_file.write_text("secret\n123\n")

    with pytest.raises(PathSecurityError, match="Path traversal detected"):
        sanitize_and_validate_path(str(outside_file), allowed_base_dir=str(base))


def test_extract_file_size_limit_exceeded(tmp_path: Path) -> None:
    big_file = tmp_path / "big.csv"
    big_file.write_text("a" * 1000)
    with pytest.raises(ExtractionError, match="exceeds maximum allowed size"):
        Extractor.extract_dataframe(str(big_file), max_size_bytes=500)


def test_extractor_chunks(sample_csv: str) -> None:
    chunks = list(Extractor.extract_chunks(sample_csv, chunk_size=2))
    assert len(chunks) == 3
    total_rows = sum(len(c) for c in chunks)
    assert total_rows == 5


# ── VALIDATE SCHEMA TESTS ────────────────────────────────────────────────────

def test_validate_schema_passes_when_all_columns_present() -> None:
    df = pd.DataFrame({
        "Date": ["2024-01-01"],
        "Region": ["Nairobi"],
        "Category": ["Hardware"],
        "Revenue": [1000.0]
    })
    validate_schema(df, ("date", "region", "category", "revenue"))


def test_validate_schema_fails_when_missing_column() -> None:
    df = pd.DataFrame({
        "Date": ["2024-01-01"],
        "Region": ["Nairobi"]
    })
    with pytest.raises(SchemaViolationError, match="Missing required schema columns"):
        validate_schema(df, ("date", "region", "category", "revenue"))


def test_validate_schema_invalid_dataframe() -> None:
    with pytest.raises(ValidationError, match="Expected a valid pandas DataFrame"):
        SchemaValidator.validate(None, ("col1",))  # type: ignore


# ── TRANSFORM TESTS ──────────────────────────────────────────────────────────

def test_transform_removes_duplicates_and_empty_rows(sample_csv: str) -> None:
    df = extract(sample_csv)
    clean = transform(df)
    # Original 5 rows: 1 empty row dropped, 1 duplicate row dropped -> 3 rows remain
    assert len(clean) == 3


def test_transform_fills_missing_numeric_with_median(sample_csv: str) -> None:
    df = extract(sample_csv)
    clean = transform(df)
    assert clean["revenue"].isna().sum() == 0
    # Expected median between 10000 and 8000 is 9000.0
    assert 9000.0 in clean["revenue"].values


def test_transform_all_nan_numeric_column() -> None:
    df = pd.DataFrame({
        "date": ["2024-01-01", "2024-01-02"],
        "region": ["Nairobi", "Mombasa"],
        "revenue": [None, None]
    })
    clean = transform(df)
    assert clean["revenue"].isna().sum() == 0
    assert (clean["revenue"] == 0.0).all()


def test_transform_adds_derived_calendar_columns(sample_csv: str) -> None:
    df = extract(sample_csv)
    clean = transform(df)
    assert "year" in clean.columns
    assert "month" in clean.columns
    assert "quarter" in clean.columns
    assert set(clean["year"].unique()) == {2024}


def test_transform_strips_whitespace_from_strings() -> None:
    df = pd.DataFrame({
        "date": [" 2024-01-01 "],
        "region": ["  Nairobi  "],
        "revenue": [100.0]
    })
    clean = transform(df)
    assert clean["region"].iloc[0] == "Nairobi"


# ── ANALYSE TESTS ────────────────────────────────────────────────────────────

def test_analyse_aggregates_metrics(sample_csv: str) -> None:
    df = extract(sample_csv)
    clean = transform(df)
    summary = analyse(clean)
    assert not summary.empty
    assert "revenue_sum" in summary.columns
    assert "revenue_mean" in summary.columns
    assert "revenue_count" in summary.columns


def test_analyse_fallback_on_missing_dimensions() -> None:
    df = pd.DataFrame({"value": [10, 20, 30]})
    summary = analyse(df, group_by_candidates=("non_existent_dim",))
    assert not summary.empty


# ── LOAD (ATOMIC WRITER) TESTS ───────────────────────────────────────────────

def test_load_writes_file_atomically(sample_csv: str, tmp_path: Path) -> None:
    df = extract(sample_csv)
    clean = transform(df)
    out_file = tmp_path / "nested" / "reports" / "summary.csv"
    load(clean, str(out_file))

    assert out_file.exists()
    # Ensure temporary file is gone
    tmp_files = list(out_file.parent.glob(".*.tmp"))
    assert len(tmp_files) == 0

    reloaded = pd.read_csv(out_file)
    assert len(reloaded) == len(clean)


def test_load_fails_on_unwritable_directory() -> None:
    df = pd.DataFrame({"col": [1, 2, 3]})
    with pytest.raises(StorageIOError):
        load(df, "/dev/null/forbidden/out.csv")


# ── PIPELINE INTEGRATION TESTS ───────────────────────────────────────────────

def test_run_pipeline_success(sample_csv: str, tmp_path: Path) -> None:
    out = str(tmp_path / "output_report.csv")
    metrics = run_pipeline(input_path=sample_csv, output_path=out)

    assert metrics["status"] == "SUCCESS"
    assert metrics["rows_in"] == 5
    assert metrics["rows_out"] == 3
    assert metrics["integrity_pct"] == 60.0
    assert "extract" in metrics["stages"]
    assert "transform" in metrics["stages"]
    assert os.path.exists(out)


def test_run_pipeline_schema_failure(tmp_path: Path) -> None:
    bad_csv = tmp_path / "bad.csv"
    bad_csv.write_text("wrong_col,other_col\n1,2\n")
    out = str(tmp_path / "out.csv")

    with pytest.raises(DataPipelineError, match="Missing required schema columns"):
        run_pipeline(input_path=str(bad_csv), output_path=out)


def test_main_cli_exit_codes(sample_csv: str, tmp_path: Path) -> None:
    out = str(tmp_path / "cli_out.csv")
    exit_code = main(["--input", sample_csv, "--output", out, "--log-level", "DEBUG"])
    assert exit_code == 0
    assert os.path.exists(out)


def test_main_cli_missing_file_exit_code(tmp_path: Path) -> None:
    out = str(tmp_path / "cli_out.csv")
    exit_code = main(["--input", "non_existent.csv", "--output", out])
    assert exit_code == 2


def test_main_cli_validation_failure_exit_code(tmp_path: Path) -> None:
    bad_csv = tmp_path / "invalid_schema.csv"
    bad_csv.write_text("only_one_col\nval\n")
    out = str(tmp_path / "cli_out.csv")
    exit_code = main(["--input", str(bad_csv), "--output", out])
    assert exit_code == 3


def test_main_cli_config_error_exit_code() -> None:
    exit_code = main(["--input", " ", "--output", "out.csv"])
    assert exit_code == 5


def test_main_cli_json_logging(sample_csv: str, tmp_path: Path, capsys: pytest.CaptureFixture) -> None:
    out = str(tmp_path / "json_log_out.csv")
    exit_code = main(["--input", sample_csv, "--output", out, "--json-logs"])
    assert exit_code == 0
    captured = capsys.readouterr()
    found_json = False
    for line in captured.out.strip().split("\n"):
        if line.strip():
            try:
                parsed = json.loads(line)
                if "run_id" in parsed and "level" in parsed:
                    found_json = True
                    break
            except json.JSONDecodeError:
                pass
    assert found_json, "Expected structured JSON logs in stdout"
