import pytest

from src.ingestion import data_generator
from src.ingestion.source_reader import SourceError, read_csv
from src.validation.schema_validator import SchemaError


def test_generator_is_deterministic(tmp_path):
    a, b = tmp_path / "a", tmp_path / "b"
    data_generator.generate(300, a, seed=42)
    data_generator.generate(300, b, seed=42)
    for rel in ("baseline/orders.csv", "incoming/order_events.csv", "baseline/customers.csv"):
        assert (a / rel).read_bytes() == (b / rel).read_bytes()


def test_generator_sizes(tmp_path):
    meta = data_generator.generate(100, tmp_path)
    assert meta["size"] == 100 and meta["events"] >= 10


def test_missing_file(tmp_path):
    with pytest.raises(SourceError):
        read_csv(tmp_path / "nope.csv", ["a"])


def test_zero_byte_file(tmp_path):
    p = tmp_path / "e.csv"; p.write_text("")
    with pytest.raises(SourceError):
        read_csv(p, ["a"])


def test_malformed_csv(tmp_path):
    p = tmp_path / "bad.csv"; p.write_text("a,b\n1,2\n3,4,5,6\n")
    with pytest.raises(SourceError):
        read_csv(p, ["a", "b"])


def test_missing_required_column_fails_batch(tmp_path):
    p = tmp_path / "x.csv"; p.write_text("a,b\n1,2\n")
    with pytest.raises(SchemaError):
        read_csv(p, ["a", "b", "c"])


def test_header_only_file_is_empty_batch(tmp_path):
    p = tmp_path / "x.csv"; p.write_text("a,b\n")
    assert len(read_csv(p, ["a", "b"])) == 0
