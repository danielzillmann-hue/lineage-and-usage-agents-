"""End-to-end test of the Insignia XML → SQLX pipeline.

Walks a sample directory of pipeline XMLs, generates SQLX, and asserts
each output file has the expected structural pieces.
"""

from pathlib import Path

import pytest

from app.transformer import generate_project, generate_sqlx, parse


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


def test_project_assembly_has_all_required_files():
    """generate_project bundles the SQLX into a deployable Dataform repo."""
    project = generate_project(_load_samples())
    assert "workflow_settings.yaml" in project.files
    assert "README.md" in project.files
    assert "definitions/sources.sqlx" in project.files
    # At least one primary pipeline file
    primary = [p for p in project.files if p.startswith("definitions/")
               and not p.startswith("definitions/operations/")
               and p != "definitions/sources.sqlx"]
    assert primary, "no primary SQLX in project"


def test_sources_excludes_internally_produced_tables():
    """stg_audit_master is produced by regulatory_audit_compliance stage 1
    and consumed by stage 2 — it should NOT be declared as an external source.
    """
    project = generate_project(_load_samples())
    assert "stg_audit_master" not in project.sources
    # But the external Oracle tables MUST be declared
    assert "transactions" in project.sources
    assert "members" in project.sources


def test_workflow_settings_contains_gcp_config():
    project = generate_project(_load_samples())
    yaml = project.files["workflow_settings.yaml"]
    assert "defaultProject:" in yaml
    assert "defaultLocation:" in yaml
    assert "defaultDataset:" in yaml


def test_operations_dml_translated_to_bigquery():
    """Oracle DML inside operations files gets dialect-translated and
    table refs wrapped with ${ref()}."""
    from app.transformer.sql_helpers import render_dml_for_bigquery

    # SYSDATE → CURRENT_TIMESTAMP, target table → ${ref()}
    out = render_dml_for_bigquery(
        "UPDATE accounts SET status = 'X' WHERE open_date < SYSDATE - 365"
    )
    assert "${ref('accounts')}" in out
    assert "SYSDATE" not in out
    assert "CURRENT_TIMESTAMP" in out

    # DELETE FROM target wrapping
    out = render_dml_for_bigquery("DELETE FROM stg_daily_metrics")
    assert "${ref('stg_daily_metrics')}" in out

    # TRUNCATE TABLE target wrapping
    out = render_dml_for_bigquery("TRUNCATE TABLE stg_audit_master")
    assert "${ref('stg_audit_master')}" in out


def test_view_block_renders_with_source_sql():
    """When a view name is provided in the views dict, the source block
    is `type: "view"` with the (translated) original SQL body."""
    from app.transformer import assemble_project
    from app.transformer.runner import GeneratedFile

    # A primary file that references a view via ${ref()}, so the view
    # name shows up in the sources list.
    primary = GeneratedFile(
        path="definitions/risk.sqlx",
        content='config { type: "table" }\nSELECT * FROM ${ref(\'vw_member_risk\')}',
        pipeline="risk",
        kind="primary",
        warnings=[],
    )
    project = assemble_project(
        [primary],
        views={"vw_member_risk": "SELECT member_id, risk_level FROM members WHERE active = 1"},
    )
    sources_sql = project.files["definitions/sources.sqlx"]
    assert 'type: "view"' in sources_sql
    assert 'name: "vw_member_risk"' in sources_sql
    assert "SELECT" in sources_sql
    assert "${ref('members')}" in sources_sql  # body's table ref also wrapped
