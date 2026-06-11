"""Unit tests for the ETL pipeline."""
import os, tempfile, pytest
import pandas as pd
from pipeline import extract, transform, analyse, load


@pytest.fixture
def sample_csv(tmp_path):
    path = tmp_path / "test.csv"
    path.write_text(
        "date,region,category,revenue,units\n"
        "01/01/2024,Nairobi,Hardware,10000,5\n"
        "02/01/2024,Mombasa,Software,,3\n"
        "02/01/2024,Mombasa,Software,,3\n"   # duplicate
        "03/01/2024,Kisumu,Training,8000,2\n"
    )
    return str(path)


def test_extract_loads_correct_row_count(sample_csv):
    df = extract(sample_csv)
    assert len(df) == 4


def test_transform_removes_duplicates(sample_csv):
    df = extract(sample_csv)
    clean = transform(df)
    assert len(clean) == 3


def test_transform_fills_missing_numeric(sample_csv):
    df = extract(sample_csv)
    clean = transform(df)
    assert clean["revenue"].isna().sum() == 0


def test_transform_adds_date_columns(sample_csv):
    df = extract(sample_csv)
    clean = transform(df)
    assert "year" in clean.columns
    assert "quarter" in clean.columns


def test_load_writes_file(sample_csv, tmp_path):
    df = extract(sample_csv)
    clean = transform(df)
    out = str(tmp_path / "out.csv")
    load(clean, out)
    assert os.path.exists(out)
    result = pd.read_csv(out)
    assert len(result) == len(clean)
