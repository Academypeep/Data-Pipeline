"""
ETL Data Pipeline
-----------------
Extracts data from a CSV source, transforms and cleans it,
and loads a summary report to an output CSV.

Usage:
    python pipeline.py --input data/raw_sales.csv --output data/report.csv
"""

import argparse
import logging
import os
import sys
from datetime import datetime

import pandas as pd
import numpy as np

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)]
)
log = logging.getLogger(__name__)


# ── EXTRACT ──────────────────────────────────────────────────────────────────

def extract(filepath: str) -> pd.DataFrame:
    """Load raw CSV data into a DataFrame."""
    if not os.path.exists(filepath):
        raise FileNotFoundError(f"Input file not found: {filepath}")
    log.info(f"Extracting data from {filepath}")
    df = pd.read_csv(filepath, parse_dates=["date"], dayfirst=True)
    log.info(f"  Loaded {len(df):,} rows, {len(df.columns)} columns")
    return df


# ── TRANSFORM ────────────────────────────────────────────────────────────────

def transform(df: pd.DataFrame) -> pd.DataFrame:
    """Clean, validate, and enrich the raw data."""
    log.info("Transforming data...")
    original_count = len(df)

    # 1. Drop fully empty rows
    df.dropna(how="all", inplace=True)

    # 2. Normalise column names
    df.columns = [c.strip().lower().replace(" ", "_") for c in df.columns]

    # 3. Remove duplicates
    df.drop_duplicates(inplace=True)

    # 4. Fill missing numeric values with column median
    numeric_cols = df.select_dtypes(include=[np.number]).columns
    for col in numeric_cols:
        median_val = df[col].median()
        missing = df[col].isna().sum()
        if missing:
            df[col].fillna(median_val, inplace=True)
            log.info(f"  Filled {missing} missing values in '{col}' with median {median_val:.2f}")

    # 5. Strip whitespace from string columns
    str_cols = df.select_dtypes(include=["object"]).columns
    for col in str_cols:
        df[col] = df[col].str.strip()

    # 6. Add derived columns
    if "date" in df.columns:
        df["year"]  = df["date"].dt.year
        df["month"] = df["date"].dt.month
        df["quarter"] = df["date"].dt.quarter

    removed = original_count - len(df)
    integrity_pct = round((len(df) / original_count) * 100, 1) if original_count else 0
    log.info(f"  Removed {removed} duplicate/empty rows — data integrity: {integrity_pct}%")
    return df


# ── ANALYSE ──────────────────────────────────────────────────────────────────

def analyse(df: pd.DataFrame) -> pd.DataFrame:
    """Produce a summary report grouped by relevant dimensions."""
    log.info("Generating summary report...")

    group_cols = [c for c in ["region", "category", "year", "quarter"] if c in df.columns]
    numeric_cols = df.select_dtypes(include=[np.number]).columns.tolist()

    if not group_cols or not numeric_cols:
        log.warning("Insufficient columns for grouping — returning descriptive stats only.")
        return df.describe().reset_index()

    agg_funcs = {col: ["sum", "mean", "count"] for col in numeric_cols}
    summary = df.groupby(group_cols).agg(agg_funcs).reset_index()
    summary.columns = ["_".join(c).strip("_") for c in summary.columns]

    log.info(f"  Summary report: {len(summary):,} rows")
    return summary


# ── LOAD ─────────────────────────────────────────────────────────────────────

def load(df: pd.DataFrame, filepath: str) -> None:
    """Write the processed DataFrame to CSV."""
    os.makedirs(os.path.dirname(filepath) or ".", exist_ok=True)
    df.to_csv(filepath, index=False)
    log.info(f"Report written to {filepath}  ({len(df):,} rows)")


# ── MAIN ─────────────────────────────────────────────────────────────────────

def run_pipeline(input_path: str, output_path: str) -> dict:
    start = datetime.now()
    raw      = extract(input_path)
    clean    = transform(raw)
    report   = analyse(clean)
    load(report, output_path)
    elapsed  = (datetime.now() - start).total_seconds()
    stats = {
        "rows_in":  len(raw),
        "rows_out": len(clean),
        "integrity_pct": round(len(clean) / len(raw) * 100, 1) if len(raw) else 0,
        "elapsed_s": round(elapsed, 2),
    }
    log.info(f"Pipeline complete in {elapsed:.2f}s — {stats}")
    return stats


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="ETL Data Pipeline")
    parser.add_argument("--input",  required=True, help="Path to raw input CSV")
    parser.add_argument("--output", required=True, help="Path to write output CSV")
    args = parser.parse_args()
    run_pipeline(args.input, args.output)
