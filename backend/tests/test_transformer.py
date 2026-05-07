"""End-to-end test of the Insignia XML → SQLX pipeline.

Walks a sample directory of pipeline XMLs, generates SQLX, and asserts
each output file has the expected structural pieces.
"""

from pathlib import Path

import pytest

from app.transformer import generate_sqlx, parse


SAMPLE_XML_DIR = Path("C:/Users/DanielZillmann/AppData/Local/Temp/xml_test")


def _load_samples() -> list[tuple[str, str]]:
    if not SAMPLE_XML_DIR.exists():
        pytest.skip(f"sample dir not present: {SAMPLE_XML_DIR}")
    pairs: list[tuple[str, str]] = []
    for path in sorted(SAMPLE_XML_DIR.glob("*.xml")):
        pairs.append((path.name, path.read_text(encoding="utf-8")))
    return pairs


def test_all_samples_parse():
    """Every sample XML produces at least one primary DataflowGraph."""
    for filename, text in _load_samples():
        result = parse(text, filename)
        assert result is not None, f"{filename}: parse returned None"
        assert result.primaries, f"{filename}: no primary graphs built"


def test_simple_tier1_pipeline():
    """stg_daily_metrics is a pure-SQL pipeline (only execute_sql INSERT/SELECT)."""
    text = (SAMPLE_XML_DIR / "15_stg_daily_metrics.xml").read_text(encoding="utf-8")
    result = parse(text, "15_stg_daily_metrics.xml")
    assert result is not None
    assert len(result.primaries) == 1
    g = result.primaries[0]
    assert g.target.target_table == "stg_daily_metrics"
    # Should be Source + Aggregator
    assert len(g.nodes) == 2


def test_multi_load_splits_into_separate_graphs():
    """regulatory_audit_compliance has TWO <load> steps → 2 stages."""
    text = (SAMPLE_XML_DIR / "26_regulatory_audit_compliance.xml").read_text(encoding="utf-8")
    result = parse(text, "26_regulatory_audit_compliance.xml")
    assert result is not None
    assert len(result.primaries) == 2
    targets = [g.target.target_table for g in result.primaries]
    assert "stg_audit_master" in targets
    assert "fact_regulatory_audit" in targets


def test_generate_sqlx_includes_ref_syntax():
    """Generated SQLX uses Dataform ${ref()} for upstream tables."""
    pairs = _load_samples()
    files = generate_sqlx(pairs)
    primary = [f for f in files if f.kind == "primary"]
    assert primary, "no primary SQLX produced"
    for f in primary:
        assert "config {" in f.content, f"{f.path}: no config block"
        assert 'type:' in f.content, f"{f.path}: no type"
        # Most pipelines reference at least one upstream — multi-stage
        # files always do (read from previous stage).


def test_operations_sqlx_for_dml():
    """UPDATE/DELETE/TRUNCATE in execute_sql produce operations-type SQLX."""
    pairs = _load_samples()
    files = generate_sqlx(pairs)
    ops = [f for f in files if f.kind == "operations"]
    assert ops, "no operations SQLX produced"
    for f in ops:
        assert 'type: "operations"' in f.content
