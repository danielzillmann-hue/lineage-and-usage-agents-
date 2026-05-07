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
    file_meta: dict[str, dict] = field(default_factory=dict)  # path → {confidence, original_path?, kind}
    originals: dict[str, str] = field(default_factory=dict)   # original_path → content


# Match `${ref('table_name')}` and `${ref("table_name")}` calls.
_REF_PATTERN = re.compile(r"""\$\{ref\(\s*['"]([A-Za-z_][\w]*)['"]\s*\)\}""")
# Match `name: "TableName"` inside a config block (Dataform identifier).
_NAME_PATTERN = re.compile(r"""^\s*name:\s*['"]([^'"]+)['"]""", re.MULTILINE)


def assemble_project(
    generated: list[GeneratedFile],
    config: DataformProjectConfig | None = None,
    views: dict[str, str] | None = None,
    table_metadata: dict[str, dict] | None = None,
) -> AssembledProject:
    """Bundle generated SQLX into a deployable Dataform project.

    `views` is an optional dict of `{lowercase_view_name: oracle_view_sql}`.
    When provided, source declarations for matching tables are upgraded
    from `type: "declaration"` to `type: "view"` with the original SQL
    body (translated to BigQuery dialect).

    `table_metadata` is an optional dict of
    `{lowercase_table_name: {"primary_keys": [...], "non_null": [...]}}`.
    When provided, assertions blocks are emitted on source declarations
    and primary SQLX files so Dataform validates uniqueness + nonNull
    automatically.
    """
    config = config or DataformProjectConfig()
    views = views or {}
    table_metadata = table_metadata or {}
    out = AssembledProject()

    # 1. Pass-through every generated SQLX into the project tree, plus
    # the matching original-source files so the UI can render side-by-side.
    # Primary SQLX files get an `assertions` config block injected when
    # we have inventory metadata for the target table, plus layer-based
    # `tags` so users can run `dataform run --tags=core` to materialise a
    # single layer.
    for gf in generated:
        if gf.kind == "primary":
            target = gf.path.split("/")[-1].removesuffix(".sqlx").lower()
            tags = _tags_for_target(target)
            if tags:
                gf.content = _inject_tags_into_sqlx(gf.content, tags)
            if table_metadata:
                asserts = _assertions_for(target, table_metadata)
                if asserts:
                    gf.content = _inject_assertions_into_sqlx(gf.content, asserts)
        out.files[gf.path] = gf.content
        if gf.kind == "primary":
            # File stem == the actual produced table name. Multi-stage
            # pipelines (regulatory_audit_compliance) have multiple stems
            # (stg_audit_master, fact_regulatory_audit) — list each one.
            stem = gf.path.split("/")[-1].removesuffix(".sqlx")
            out.pipelines.append(stem)
        elif gf.kind == "operations":
            out.operations.append(gf.path.split("/")[-1].removesuffix(".sqlx"))

        # Capture the original source for later display. We dedupe by
        # original_filename so multi-stage primaries pointing at the
        # same XML don't store it multiple times.
        original_path = ""
        if gf.original_filename and gf.original_content:
            original_path = f"_originals/{gf.original_filename}"
            out.originals[original_path] = gf.original_content
        out.file_meta[gf.path] = {
            "kind": gf.kind,
            "pipeline": gf.pipeline,
            "confidence": gf.confidence,
            "original_path": original_path,
        }

    # 2. Compute the set of source tables — every `${ref('X')}` minus every
    # primary table the project itself produces.
    refs = _collect_refs(generated)
    produced = _collect_produced_tables(generated)
    sources = sorted(refs - produced)

    # 3. Generate sources.sqlx — declarations for raw tables, view bodies
    # for known views. Declarations get assertions when we have inventory
    # metadata for the named source.
    if sources:
        out.files["definitions/sources.sqlx"] = _build_sources_sqlx(
            sources, config, views, table_metadata
        )
        out.sources = sources

        # Raw-layer bootstrap: BQ DDL + replication README. Closes the
        # loop so users see how to populate the raw dataset our pipelines
        # read from. Only emitted when at least one source has inventory
        # column-level schema info.
        from app.transformer.raw_bootstrap import (
            generate_raw_schema_sql,
            generate_replication_readme,
        )
        any_with_schema = any(
            (table_metadata.get(s.lower(), {}).get("schema") or [])
            for s in sources
        )
        if any_with_schema:
            out.files["bootstrap/raw_schema.sql"] = generate_raw_schema_sql(
                sources, table_metadata,
                project=config.gcp_project, dataset=config.source_dataset,
            )
            out.files["bootstrap/replication_setup.md"] = generate_replication_readme(
                sources,
                project=config.gcp_project, dataset=config.source_dataset,
                region=config.location,
            )

    # 4. Project-level workflow_settings.yaml.
    out.files["workflow_settings.yaml"] = _build_workflow_settings(config)

    # 5. Validation pass — runs on every assembled project. The full
    # generated file list (including the just-built sources.sqlx) flows
    # through so cross-file checks like ref resolution see everything.
    # Validation also mutates each GeneratedFile.confidence in-place.
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
    # Re-capture confidence per file now that validation has scored them.
    for gf in generated:
        if gf.path in out.file_meta:
            out.file_meta[gf.path]["confidence"] = gf.confidence

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
    table_metadata: dict[str, dict],
) -> str:
    """Sources file. Each external name renders as either:
    - `type: "view"` with the original (translated) SQL body, when the
      name matches a view from the inventory.
    - `type: "declaration"` otherwise — pure pointer to the BQ table
      replicated from Oracle, with `assertions` injected for known PKs
      and NOT NULL columns.
    """
    blocks: list[str] = []
    for s in sources:
        view_sql = views.get(s.lower())
        if view_sql:
            blocks.append(_view_block(s, view_sql))
        else:
            blocks.append(_declaration_block(s, config, table_metadata.get(s.lower())))
    return (
        "-- Sources — declarations for raw replicated tables, plus full\n"
        "-- view definitions for any Oracle views the pipelines read from.\n"
        "-- Declarations are pointers (no compile-time SQL); views are\n"
        "-- materialised by Dataform from the original SQL body. Each\n"
        "-- includes an `assertions` block for known PKs / NOT NULL\n"
        "-- columns inferred from the Oracle inventory.\n\n"
        + "\n\n".join(blocks)
        + "\n"
    )


def _declaration_block(
    name: str,
    config: DataformProjectConfig,
    metadata: dict | None = None,
) -> str:
    asserts_block = ""
    if metadata:
        body = _assertions_body(metadata)
        if body:
            asserts_block = f"\n  assertions: {{\n{body}\n  }},"
    return f"""config {{
  type: "declaration",
  database: "{config.gcp_project}",
  schema: "{config.source_dataset}",
  name: "{name}",
  description: "Source table replicated from the Oracle warehouse.",{asserts_block}
}}"""


# ─── Assertion helpers ──────────────────────────────────────────────────


def _assertions_for(table_name: str, table_metadata: dict[str, dict]) -> dict | None:
    """Look up assertion fields for a generated table by name (case-insensitive)."""
    return table_metadata.get(table_name.lower())


def _assertions_body(metadata: dict) -> str:
    """Build the JS-object body of an `assertions: { ... }` block."""
    pieces: list[str] = []
    pks = metadata.get("primary_keys") or []
    if pks:
        pks_str = ", ".join(f'"{c}"' for c in pks)
        pieces.append(f"    uniqueKey: [{pks_str}],")
    non_null = metadata.get("non_null") or []
    if non_null:
        nn_str = ", ".join(f'"{c}"' for c in non_null)
        pieces.append(f"    nonNull: [{nn_str}],")
    return "\n".join(pieces)


def _tags_for_target(table_name: str) -> list[str]:
    """Layer-based tags inferred from the target table name. Matches both
    prefix and suffix patterns so naming like `accounts_summary` and
    `tax_reporting` get tagged correctly.

    Tags align with Dataform's --tags filter so users can materialise a
    single layer (`dataform run --tags=core`).
    """
    n = table_name.lower()

    # Prefix-based — strongest signal when present.
    if n.startswith(("stg_", "stage_", "raw_", "src_")):
        return ["staging"]
    if n.startswith(("core_", "int_", "intermediate_")):
        return ["core"]
    if n.startswith(("fact_", "dim_", "mart_", "agg_")):
        return ["reporting"]
    if n.startswith(("final_", "dlv_", "delivery_")):
        return ["delivery"]

    # Suffix-based — common naming for downstream layers.
    if n.endswith(("_summary", "_report", "_reporting", "_dashboard", "_kpi")):
        return ["reporting"]
    if n.endswith(("_extract", "_export", "_feed")):
        return ["delivery"]
    if n.endswith(("_stg", "_staging")):
        return ["staging"]

    # Default — every primary file gets at least one tag so users can
    # filter "everything generated by the agents" with --tags=transformation.
    return ["transformation"]


def _inject_tags_into_sqlx(sqlx: str, tags: list[str]) -> str:
    """Insert a `tags: [...]` field into the config block. Idempotent."""
    if not tags or "tags:" in sqlx:
        return sqlx
    cfg_start = sqlx.find("config {")
    if cfg_start < 0:
        return sqlx
    close_idx = sqlx.find("\n}", cfg_start)
    if close_idx < 0:
        return sqlx
    tags_str = ", ".join(f'"{t}"' for t in tags)
    insertion = f"  tags: [{tags_str}],\n"
    return sqlx[:close_idx + 1] + insertion + sqlx[close_idx + 1:]


def _inject_assertions_into_sqlx(sqlx: str, metadata: dict) -> str:
    """Insert an `assertions: { ... }` field into a wrap_sqlx-generated
    config block. Idempotent — if assertions already exist, leaves the
    file alone."""
    body = _assertions_body(metadata)
    if not body or "assertions:" in sqlx:
        return sqlx
    # Find the closing `}` of the first config block. wrap_sqlx outputs
    # the block as `config {\n  ...\n}` starting at column 0.
    cfg_start = sqlx.find("config {")
    if cfg_start < 0:
        return sqlx
    # Find the matching close brace at column 0 (config blocks have no
    # nested `}` at column 0 in our wrapper output).
    close_idx = sqlx.find("\n}", cfg_start)
    if close_idx < 0:
        return sqlx
    insertion = f"  assertions: {{\n{body}\n  }},\n"
    return sqlx[:close_idx + 1] + insertion + sqlx[close_idx + 1:]


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
