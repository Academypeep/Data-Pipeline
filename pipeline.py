"""
ETL Data Pipeline (Production-Ready)
------------------------------------
Extracts data from a CSV source, transforms, validates, and cleans it,
and loads an aggregated summary report atomically to an output CSV.

Usage:
    python pipeline.py --input data/raw_sales.csv --output data/report.csv
    python pipeline.py --input data/raw_sales.csv --output data/report.csv --json-logs --log-level DEBUG
"""

from __future__ import annotations

import argparse
import json
import logging
import os
import sys
import tempfile
import uuid
from dataclasses import asdict, dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional, Sequence

import numpy as np
import pandas as pd


# ── CONFIGURATION & MODELS ───────────────────────────────────────────────────

@dataclass(frozen=True)
class PipelineConfig:
    """Immutable runtime configuration for the ETL pipeline."""
    input_path: str
    output_path: str
    date_column: Optional[str] = "date"
    group_by_columns: tuple[str, ...] = ("region", "category", "year", "quarter")
    required_columns: tuple[str, ...] = ("date", "region", "category", "revenue")
    day_first: bool = True
    fail_on_empty: bool = True
    chunk_size: Optional[int] = None


@dataclass(frozen=True)
class PipelineMetrics:
    """Execution statistics and integrity metrics."""
    run_id: str
    start_time: str
    end_time: str
    rows_in: int
    rows_out: int
    integrity_pct: float
    elapsed_seconds: float
    status: str
    error_message: Optional[str] = None

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


# ── OBSERVABILITY / LOGGING ───────────────────────────────────────────────────

class StructuredJsonFormatter(logging.Formatter):
    """Formats log records as single-line JSON objects for log aggregators."""

    def __init__(self, run_id: str):
        super().__init__()
        self.run_id = run_id

    def format(self, record: logging.LogRecord) -> str:
        log_entry: dict[str, Any] = {
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "level": record.levelname,
            "message": record.getMessage(),
            "logger": record.name,
            "run_id": self.run_id,
            "file": record.filename,
            "line": record.lineno,
        }
        if record.exc_info:
            log_entry["exception"] = self.formatException(record.exc_info)
        return json.dumps(log_entry)


def setup_logger(
    name: str = "etl_pipeline",
    level: str = "INFO",
    json_format: bool = False,
    run_id: Optional[str] = None
) -> logging.Logger:
    """Configures and returns a production logger."""
    logger = logging.getLogger(name)
    logger.handlers.clear()
    logger.setLevel(getattr(logging, level.upper(), logging.INFO))

    handler = logging.StreamHandler(sys.stdout)
    if json_format:
        handler.setFormatter(StructuredJsonFormatter(run_id=run_id or "default"))
    else:
        handler.setFormatter(
            logging.Formatter(
                fmt="%(asctime)s [%(levelname)s] [%(name)s] %(message)s",
                datefmt="%Y-%m-%d %H:%M:%S"
            )
        )
    logger.addHandler(handler)
    logger.propagate = False
    return logger


# Default logger for module-level usage
log = setup_logger()


# ── CUSTOM EXCEPTIONS ─────────────────────────────────────────────────────────

class PipelineError(Exception):
    """Base exception for all pipeline failures."""
    pass


class ExtractionError(PipelineError):
    """Raised when data extraction fails."""
    pass


class ValidationError(PipelineError):
    """Raised when input schema or data validation fails."""
    pass


class LoadError(PipelineError):
    """Raised when atomic write or load fails."""
    pass


# ── EXTRACT ──────────────────────────────────────────────────────────────────

def extract(
    filepath: str,
    date_column: Optional[str] = "date",
    day_first: bool = True
) -> pd.DataFrame:
    """
    Safely load raw CSV data into a pandas DataFrame.
    Validates file existence, non-empty content, and parses date columns safely.
    """
    path = Path(filepath)
    if not path.exists():
        raise ExtractionError(f"Input file not found: {filepath}")
    if not path.is_file():
        raise ExtractionError(f"Input path is not a valid file: {filepath}")
    if path.stat().st_size == 0:
        raise ExtractionError(f"Input file is empty: {filepath}")

    log.info(f"Extracting data from {filepath}")
    try:
        # First read raw CSV to check header columns without failing on missing parse_dates
        df = pd.read_csv(filepath)
    except Exception as exc:
        raise ExtractionError(f"Failed to parse CSV file {filepath}: {exc}") from exc

    if df.empty:
        raise ExtractionError(f"Input CSV contains zero data rows: {filepath}")

    # If date_column is requested and present, parse it safely
    if date_column:
        matched_cols = [c for c in df.columns if c.strip().lower() == date_column.lower()]
        if matched_cols:
            col_name = matched_cols[0]
            try:
                df[col_name] = pd.to_datetime(df[col_name], dayfirst=day_first, errors="coerce")
            except Exception as exc:
                log.warning(f"Could not parse dates in column '{col_name}': {exc}")

    log.info(f"  Loaded {len(df):,} rows, {len(df.columns)} columns")
    return df


# ── VALIDATE ─────────────────────────────────────────────────────────────────

def validate_schema(df: pd.DataFrame, required_columns: Sequence[str]) -> None:
    """Ensure all required columns are present in the DataFrame."""
    normalized_cols = {c.strip().lower().replace(" ", "_") for c in df.columns}
    missing = [c for c in required_columns if c.lower() not in normalized_cols]
    if missing:
        raise ValidationError(
            f"Missing required schema columns: {', '.join(missing)}. "
            f"Present columns: {', '.join(sorted(normalized_cols))}"
        )


# ── TRANSFORM ────────────────────────────────────────────────────────────────

def transform(df: pd.DataFrame, date_column: str = "date") -> pd.DataFrame:
    """
    Clean, validate, and enrich the raw data using safe non-inplace operations.
    Fully compatible with modern pandas 2.x and 3.x Copy-on-Write semantics.
    """
    log.info("Transforming data...")
    original_count = len(df)

    # 1. Normalize column names (strip whitespace, lowercase, replace spaces with underscores)
    cleaned_df = df.copy()
    cleaned_df.columns = [c.strip().lower().replace(" ", "_") for c in cleaned_df.columns]

    # 2. Drop fully empty rows
    cleaned_df = cleaned_df.dropna(how="all")

    # 3. Remove duplicates
    cleaned_df = cleaned_df.drop_duplicates()

    # 4. Fill missing numeric values with column median safely (no chained assignment)
    numeric_cols = cleaned_df.select_dtypes(include=[np.number]).columns
    for col in numeric_cols:
        missing = int(cleaned_df[col].isna().sum())
        if missing > 0:
            median_val = cleaned_df[col].median()
            if pd.isna(median_val):
                median_val = 0.0
            cleaned_df[col] = cleaned_df[col].fillna(median_val)
            log.info(f"  Filled {missing} missing values in '{col}' with median {median_val:.2f}")

    # Also handle numeric columns that may have been loaded as object (e.g. all empty)
    for col in cleaned_df.columns:
        if col not in numeric_cols and col not in ("date", "region", "category", "year", "month", "quarter"):
            numeric_attempt = pd.to_numeric(cleaned_df[col], errors="coerce")
            # If at least some values or column name suggests numeric (e.g. revenue, cost, units)
            if col in ("revenue", "cost", "units", "price", "amount") or not numeric_attempt.isna().all():
                missing = int(numeric_attempt.isna().sum())
                med = numeric_attempt.median()
                if pd.isna(med):
                    med = 0.0
                cleaned_df[col] = numeric_attempt.fillna(med)

    # 5. Strip whitespace from string / object / string-like columns safely
    str_cols = cleaned_df.select_dtypes(include=["object", "string"]).columns
    for col in str_cols:
        cleaned_df[col] = cleaned_df[col].astype(str).str.strip()

    # 6. Add derived calendar columns if date column exists
    date_col_normalized = date_column.strip().lower().replace(" ", "_")
    if date_col_normalized in cleaned_df.columns:
        # Ensure it's a datetime series
        if not pd.api.types.is_datetime64_any_dtype(cleaned_df[date_col_normalized]):
            cleaned_df[date_col_normalized] = pd.to_datetime(
                cleaned_df[date_col_normalized],
                errors="coerce"
            )
        dt_series = cleaned_df[date_col_normalized].dt
        cleaned_df["year"] = dt_series.year
        cleaned_df["month"] = dt_series.month
        cleaned_df["quarter"] = dt_series.quarter

    removed = original_count - len(cleaned_df)
    integrity_pct = round((len(cleaned_df) / original_count) * 100, 1) if original_count else 0.0
    log.info(f"  Removed {removed} duplicate/empty rows — data integrity: {integrity_pct}%")
    return cleaned_df


# ── ANALYSE ──────────────────────────────────────────────────────────────────

def analyse(
    df: pd.DataFrame,
    group_by_candidates: Sequence[str] = ("region", "category", "year", "quarter")
) -> pd.DataFrame:
    """Produce an aggregated summary report grouped by available dimension columns."""
    log.info("Generating summary report...")

    group_cols = [c for c in group_by_candidates if c in df.columns]
    numeric_cols = df.select_dtypes(include=[np.number]).columns.tolist()

    # Exclude calendar grouping dimensions from numeric aggregation targets
    metric_cols = [c for c in numeric_cols if c not in group_cols]

    if not group_cols or not metric_cols:
        log.warning("Insufficient columns for grouping — returning descriptive stats only.")
        return df.describe().reset_index()

    agg_funcs = {col: ["sum", "mean", "count"] for col in metric_cols}
    summary = df.groupby(group_cols, as_index=False).agg(agg_funcs)

    # Flatten multi-index column names cleanly
    flattened_cols: list[str] = []
    for col in summary.columns.values:
        if isinstance(col, tuple):
            name = "_".join(str(part) for part in col if part).strip("_")
        else:
            name = str(col)
        flattened_cols.append(name)
    summary.columns = flattened_cols

    log.info(f"  Summary report: {len(summary):,} rows")
    return summary


# ── LOAD (ATOMIC WRITER) ─────────────────────────────────────────────────────

def load(df: pd.DataFrame, filepath: str) -> None:
    """
    Atomically writes the DataFrame to CSV to prevent corrupt partial writes
    in case of unexpected crashes, disk failures, or process termination.
    """
    dest_path = Path(filepath).resolve()
    try:
        dest_path.parent.mkdir(parents=True, exist_ok=True)
    except Exception as exc:
        raise LoadError(f"Cannot create destination directory {dest_path.parent}: {exc}") from exc

    # Write to a temporary file in the same target filesystem directory
    try:
        tmp_file = tempfile.NamedTemporaryFile(
            mode="w",
            delete=False,
            dir=dest_path.parent,
            prefix=f".{dest_path.name}.",
            suffix=".tmp",
            newline=""
        )
        tmp_path = Path(tmp_file.name)
        tmp_file.close()
    except Exception as exc:
        raise LoadError(f"Failed to create temporary file in {dest_path.parent}: {exc}") from exc

    try:
        df.to_csv(tmp_path, index=False)
        # Atomic rename guarantees file integrity
        os.replace(tmp_path, dest_path)
    except Exception as exc:
        if tmp_path.exists():
            try:
                tmp_path.unlink()
            except OSError:
                pass
        raise LoadError(f"Failed to write output to {filepath}: {exc}") from exc

    log.info(f"Report written atomically to {dest_path} ({len(df):,} rows)")


# ── PIPELINE ORCHESTRATION ───────────────────────────────────────────────────

def run_pipeline(
    input_path: str,
    output_path: str,
    config: Optional[PipelineConfig] = None
) -> dict[str, Any]:
    """Executes the full ETL cycle and returns execution metrics."""
    run_id = str(uuid.uuid4())
    start_time = datetime.now(timezone.utc)
    cfg = config or PipelineConfig(input_path=input_path, output_path=output_path)

    log.info(f"Starting ETL Pipeline run {run_id}")
    try:
        raw = extract(cfg.input_path, date_column=cfg.date_column, day_first=cfg.day_first)
        if cfg.required_columns:
            validate_schema(raw, cfg.required_columns)

        clean = transform(raw, date_column=cfg.date_column or "date")
        report = analyse(clean, group_by_candidates=cfg.group_by_columns)
        load(report, cfg.output_path)

        end_time = datetime.now(timezone.utc)
        elapsed = (end_time - start_time).total_seconds()
        integrity = round((len(clean) / len(raw) * 100), 1) if len(raw) else 0.0

        metrics = PipelineMetrics(
            run_id=run_id,
            start_time=start_time.isoformat(),
            end_time=end_time.isoformat(),
            rows_in=len(raw),
            rows_out=len(clean),
            integrity_pct=integrity,
            elapsed_seconds=round(elapsed, 3),
            status="SUCCESS"
        )
        log.info(f"Pipeline complete in {elapsed:.2f}s — {metrics.to_dict()}")
        return metrics.to_dict()

    except Exception as exc:
        end_time = datetime.now(timezone.utc)
        elapsed = (end_time - start_time).total_seconds()
        metrics = PipelineMetrics(
            run_id=run_id,
            start_time=start_time.isoformat(),
            end_time=end_time.isoformat(),
            rows_in=0,
            rows_out=0,
            integrity_pct=0.0,
            elapsed_seconds=round(elapsed, 3),
            status="FAILED",
            error_message=str(exc)
        )
        log.error(f"Pipeline failed: {exc}", exc_info=True)
        # Re-raise specific error or wrap
        if isinstance(exc, PipelineError):
            raise
        raise PipelineError(f"Pipeline run {run_id} failed: {exc}") from exc


# ── CLI ENTRYPOINT ───────────────────────────────────────────────────────────

def parse_args(args: Optional[Sequence[str]] = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Production-Ready ETL Data Pipeline",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter
    )
    parser.add_argument("--input", required=True, help="Path to raw input CSV file")
    parser.add_argument("--output", required=True, help="Path to destination summary CSV file")
    parser.add_argument("--log-level", default="INFO", choices=["DEBUG", "INFO", "WARNING", "ERROR"],
                        help="Logging verbosity level")
    parser.add_argument("--json-logs", action="store_true", help="Emit structured JSON logs")
    return parser.parse_args(args)


def main(cli_args: Optional[Sequence[str]] = None) -> int:
    args = parse_args(cli_args)
    global log
    run_id = str(uuid.uuid4())
    log = setup_logger(
        level=args.log_level,
        json_format=args.json_logs,
        run_id=run_id
    )

    try:
        run_pipeline(args.input, args.output)
        return 0
    except ExtractionError as exc:
        log.error(f"Extraction error: {exc}")
        return 2
    except ValidationError as exc:
        log.error(f"Validation error: {exc}")
        return 3
    except LoadError as exc:
        log.error(f"Load error: {exc}")
        return 4
    except Exception as exc:
        log.error(f"Unexpected fatal error: {exc}")
        return 1


if __name__ == "__main__":
    sys.exit(main())
