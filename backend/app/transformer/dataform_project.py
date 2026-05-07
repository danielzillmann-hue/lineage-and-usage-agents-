"""Assemble generated SQLX files into a complete Dataform project.

A Dataform repo needs more than just `definitions/*.sqlx` to compile and
deploy — it needs:

- `workflow_settings.yaml` with project / location / dataset
- `definitions/sources.sqlx` declaring every external table the pipelines
  read from (otherwise `${ref('X')}` won't resolve)
- A README explaining the repo layout

This module takes the per-pipeline SQLX from `runner.generate_sqlx()`
plus a target-environment config and returns a `dict[path, content]`
ready to upload to GCS or zip.
"""

from __future__ import annotations

import re
from dataclasses import dataclass, field
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from app.transformer.runner import GeneratedFile


@dataclass
class DataformProjectConfig:
    """Target environment for the assembled Dataform repo."""
    gcp_project: str = "dan-sandpit"
    location: str = "australia-southeast1"
    default_dataset: str = "insignia_demo"
    assertion_dataset: str = "insignia_demo_assertions"
    source_dataset: str = "insignia_raw"
    dataform_core_version: str = "3.0.0"


@dataclass
class AssembledProject:
    """Complete Dataform project ready to deploy or download."""
    files: dict[str, str] = field(default_factory=dict)  # relative path → content
    sources: list[str] = field(default_factory=list)     # source table names declared
    pipelines: list[str] = field(default_factory=list)   # primary table names produced
    operations: list[str] = field(default_factory=list)  # post-load ops emitted
    warnings: list[str] = field(default_factory=list)
    validation: dict | None = None                       # optional validation summary


# Match `${ref('table_name')}` and `${ref("table_name")}` calls.
_REF_PATTERN = re.compile(r"""\$\{ref\(\s*['"]([A-Za-z_][\w]*)['"]\s*\)\}""")
# Match `name: "TableName"` inside a config block (Dataform identifier).
_NAME_PATTERN = re.compile(r"""^\s*name:\s*['"]([^'"]+)['"]""", re.MULTILINE)


def assemble_project(
    generated: list[GeneratedFile],
    config: DataformProjectConfig | None = None,
    views: dict[str, str] | None = None,
) -> AssembledProject:
    """Bundle generated SQLX into a deployable Dataform project.

    `views` is an optional dict of `{lowercase_view_name: oracle_view_sql}`.
    When provided, source declarations for matching tables are upgraded
    from `type: "declaration"` to `type: "view"` with the original SQL
    body (translated to BigQuery dialect).
    """
    config = config or DataformProjectConfig()
    views = views or {}
    out = AssembledProject()

    # 1. Pass-through every generated SQLX into the project tree.
    for gf in generated:
        out.files[gf.path] = gf.content
        if gf.kind == "primary":
            # File stem == the actual produced table name. Multi-stage
            # pipelines (regulatory_audit_compliance) have multiple stems
            # (stg_audit_master, fact_regulatory_audit) — list each one.
            stem = gf.path.split("/")[-1].removesuffix(".sqlx")
            out.pipelines.append(stem)
        elif gf.kind == "operations":
            out.operations.append(gf.path.split("/")[-1].removesuffix(".sqlx"))

    # 2. Compute the set of source tables — every `${ref('X')}` minus every
    # primary table the project itself produces.
    refs = _collect_refs(generated)
    produced = _collect_produced_tables(generated)
    sources = sorted(refs - produced)

    # 3. Generate sources.sqlx — declarations for raw tables, view bodies
    # for known views.
    if sources:
        out.files["definitions/sources.sqlx"] = _build_sources_sqlx(sources, config, views)
        out.sources = sources

    # 4. Project-level workflow_settings.yaml.
    out.files["workflow_settings.yaml"] = _build_workflow_settings(config)

    # 5. Validation pass — runs on every assembled project. The full
    # generated file list (including the just-built sources.sqlx) flows
    # through so cross-file checks like ref resolution see everything.
    from app.transformer.runner import GeneratedFile  # local: avoid cycle
    from app.transformer.validation import validate_project
    full_files = list(generated)
    if "definitions/sources.sqlx" in out.files:
        full_files.append(GeneratedFile(
            path="definitions/sources.sqlx",
            content=out.files["definitions/sources.sqlx"],
            pipeline="(sources)",
            kind="sources",
            warnings=[],
        ))
    validation = validate_project(full_files)
    out.validation = {
        **validation.summary(),
        "errors": [_issue_dict(i) for i in validation.errors],
        "warnings": [_issue_dict(i) for i in validation.warnings],
    }

    # 6. Top-level README — written last so it can include validation info.
    out.files["README.md"] = _build_readme(out, config)

    return out


def _issue_dict(issue) -> dict:
    return {
        "severity": issue.severity,
        "code": issue.code,
        "message": issue.message,
        "file": issue.file_path,
        "detail": issue.detail or "",
    }


# ─── Helpers ─────────────────────────────────────────────────────────────


def _collect_refs(generated: list[GeneratedFile]) -> set[str]:
    """All table names referenced via `${ref('X')}` across all generated files."""
    refs: set[str] = set()
    for gf in generated:
        for m in _REF_PATTERN.finditer(gf.content):
            refs.add(m.group(1))
    return refs


def _collect_produced_tables(generated: list[GeneratedFile]) -> set[str]:
    """All table names produced as primary outputs.

    Includes the file stem (e.g. `stg_audit_master.sqlx` -> `stg_audit_master`),
    the Dataform `name:` field, and the XML pipeline name. ${ref()} calls
    elsewhere in the project may use any of these forms.
    """
    produced: set[str] = set()
    for gf in generated:
        if gf.kind != "primary":
            continue
        # File stem == graph.mapping_name == produced table name.
        stem = gf.path.split("/")[-1].removesuffix(".sqlx")
        produced.add(stem)
        # The Dataform `name:` field (often PascalCase form of the same).
        m = _NAME_PATTERN.search(gf.content)
        if m:
            produced.add(m.group(1))
        # The XML pipeline name (used in single-stage cases).
        produced.add(gf.pipeline)
    return produced


def _build_sources_sqlx(
    sources: list[str],
    config: DataformProjectConfig,
    views: dict[str, str],
) -> str:
    """Sources file. Each external name renders as either:
    - `type: "view"` with the original (translated) SQL body, when the
      name matches a view from the inventory.
    - `type: "declaration"` otherwise — pure pointer to the BQ table
      replicated from Oracle.
    """
    blocks: list[str] = []
    for s in sources:
        view_sql = views.get(s.lower())
        if view_sql:
            blocks.append(_view_block(s, view_sql))
        else:
            blocks.append(_declaration_block(s, config))
    return (
        "-- Sources — declarations for raw replicated tables, plus full\n"
        "-- view definitions for any Oracle views the pipelines read from.\n"
        "-- Declarations are pointers (no compile-time SQL); views are\n"
        "-- materialised by Dataform from the original SQL body.\n\n"
        + "\n\n".join(blocks)
        + "\n"
    )


def _declaration_block(name: str, config: DataformProjectConfig) -> str:
    return f"""config {{
  type: "declaration",
  database: "{config.gcp_project}",
  schema: "{config.source_dataset}",
  name: "{name}",
  description: "Source table replicated from the Oracle warehouse.",
}}"""


def _view_block(name: str, oracle_sql: str) -> str:
    """Render a Dataform `type: "view"` block from the original Oracle
    CREATE VIEW SELECT body. Translates dialect to BigQuery and rewrites
    bare table refs to ${ref()} so the view's deps are tracked.
    """
    # Local import to avoid a circular dependency.
    from app.transformer.sql_helpers import render_dml_for_bigquery
    body = render_dml_for_bigquery(oracle_sql).rstrip(";")
    return (
        f"config {{\n"
        f'  type: "view",\n'
        f'  name: "{name}",\n'
        f'  description: "Oracle view ported to BigQuery — translated from CREATE VIEW source.",\n'
        f"}}\n\n"
        f"{body}"
    )


def _build_workflow_settings(config: DataformProjectConfig) -> str:
    return (
        f"# Dataform project settings - generated by the Insignia migration agents.\n"
        f"defaultProject: {config.gcp_project}\n"
        f"defaultLocation: {config.location}\n"
        f"defaultDataset: {config.default_dataset}\n"
        f"defaultAssertionDataset: {config.assertion_dataset}\n"
        f"dataformCoreVersion: {config.dataform_core_version}\n"
    )


def _build_readme(project: AssembledProject, config: DataformProjectConfig) -> str:
    pipeline_lines = "\n".join(f"- `definitions/{p}.sqlx`" for p in sorted(project.pipelines))
    source_lines = "\n".join(f"- `{s}`" for s in project.sources) or "_(none — all references resolve internally)_"
    op_lines = "\n".join(f"- `definitions/operations/{o}.sqlx`" for o in sorted(project.operations)) or "_(none)_"

    # Validation summary block — shown if validation ran.
    validation_section = ""
    if project.validation:
        v = project.validation
        if v.get("ok"):
            validation_section = (
                f"\n## Validation\n\n"
                f"All {v['files_total']} files passed structural validation "
                f"(refs resolve, SQL parses, no cycles).\n"
            )
        else:
            err_lines = "\n".join(
                f"- **{e['code']}** in `{e['file']}`: {e['message']}"
                + (f"\n  - {e['detail']}" if e.get("detail") else "")
                for e in v.get("errors", [])[:20]
            )
            warn_lines = "\n".join(
                f"- **{w['code']}** in `{w['file']}`: {w['message']}"
                for w in v.get("warnings", [])[:20]
            )
            validation_section = (
                f"\n## Validation\n\n"
                f"{v['files_failing']} of {v['files_total']} files failed "
                f"structural checks ({v['errors']} errors, {v['warnings']} warnings).\n"
                + (f"\n### Errors\n\n{err_lines}\n" if err_lines else "")
                + (f"\n### Warnings\n\n{warn_lines}\n" if warn_lines else "")
            )

    return (
        f"# Insignia migration — generated Dataform project\n\n"
        f"This repo was generated automatically from Oracle pipeline XMLs by the\n"
        f"Insignia migration agents. It is a 1:1 translation of the legacy ETL into\n"
        f"BigQuery + Dataform.\n\n"
        f"## Target environment\n\n"
        f"- **GCP project**: `{config.gcp_project}`\n"
        f"- **Location**: `{config.location}`\n"
        f"- **Default dataset**: `{config.default_dataset}`\n"
        f"- **Source dataset** (Oracle replication target): `{config.source_dataset}`\n\n"
        f"## Layout\n\n"
        f"- `workflow_settings.yaml` — project-level config\n"
        f"- `definitions/sources.sqlx` — `type: \"declaration\"` blocks for every\n"
        f"  external table the pipelines read from\n"
        f"- `definitions/<pipeline>.sqlx` — one per materialised table\n"
        f"- `definitions/operations/<op>.sqlx` — post-load DML statements\n"
        f"  (UPDATE/DELETE/MERGE preserved from the original pipelines)\n\n"
        f"## Pipelines produced ({len(project.pipelines)})\n\n"
        f"{pipeline_lines or '_(none)_'}\n\n"
        f"## External sources required ({len(project.sources)})\n\n"
        f"These tables must already exist in `{config.source_dataset}` before the\n"
        f"pipelines run. They're declared (not built) by Dataform.\n\n"
        f"{source_lines}\n\n"
        f"## Post-load operations ({len(project.operations)})\n\n"
        f"{op_lines}\n"
        f"{validation_section}\n"
        f"## Running\n\n"
        f"```bash\n"
        f"dataform compile\n"
        f"dataform run\n"
        f"```\n"
    )
