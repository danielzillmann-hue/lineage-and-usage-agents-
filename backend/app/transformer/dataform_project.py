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


# Match `${ref('table_name')}` and `${ref("table_name")}` calls.
_REF_PATTERN = re.compile(r"""\$\{ref\(\s*['"]([A-Za-z_][\w]*)['"]\s*\)\}""")
# Match `name: "TableName"` inside a config block (Dataform identifier).
_NAME_PATTERN = re.compile(r"""^\s*name:\s*['"]([^'"]+)['"]""", re.MULTILINE)


def assemble_project(
    generated: list[GeneratedFile],
    config: DataformProjectConfig | None = None,
) -> AssembledProject:
    """Bundle generated SQLX into a deployable Dataform project."""
    config = config or DataformProjectConfig()
    out = AssembledProject()

    # 1. Pass-through every generated SQLX into the project tree.
    for gf in generated:
        out.files[gf.path] = gf.content
        if gf.kind == "primary":
            out.pipelines.append(gf.pipeline)
        elif gf.kind == "operations":
            out.operations.append(gf.path.split("/")[-1].removesuffix(".sqlx"))

    # 2. Compute the set of source tables — every `${ref('X')}` minus every
    # primary table the project itself produces.
    refs = _collect_refs(generated)
    produced = _collect_produced_tables(generated)
    sources = sorted(refs - produced)

    # 3. Generate a single sources.sqlx with one declaration per external table.
    if sources:
        out.files["definitions/sources.sqlx"] = _build_sources_sqlx(sources, config)
        out.sources = sources

    # 4. Project-level workflow_settings.yaml.
    out.files["workflow_settings.yaml"] = _build_workflow_settings(config)

    # 5. Top-level README.
    out.files["README.md"] = _build_readme(out, config)

    return out


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


def _build_sources_sqlx(sources: list[str], config: DataformProjectConfig) -> str:
    """One file with N type:'declaration' blocks — one per external table."""
    blocks: list[str] = []
    for s in sources:
        blocks.append(f"""config {{
  type: "declaration",
  database: "{config.gcp_project}",
  schema: "{config.source_dataset}",
  name: "{s}",
  description: "Source table replicated from the Oracle warehouse.",
}}""")
    return (
        "-- Source declarations — these tables are produced upstream by the\n"
        "-- Oracle → BigQuery replication, not by Dataform itself. Declaring\n"
        "-- them here lets ${ref('table')} calls resolve to the correct BQ\n"
        "-- location.\n\n"
        + "\n\n".join(blocks)
        + "\n"
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
        f"{op_lines}\n\n"
        f"## Running\n\n"
        f"```bash\n"
        f"dataform compile\n"
        f"dataform run\n"
        f"```\n"
    )
