"""Orchestrates pipeline-XML → SQLX file generation.

`generate_sqlx(xml_files)` — given a list of (filename, xml_text), returns
a dict {output_path: sqlx_text} suitable for writing to a Dataform repo.
"""

from __future__ import annotations

from dataclasses import dataclass

from transformation_core import SQLGenerator, wrap_sqlx

from app.transformer.dataform_project import (
    AssembledProject,
    DataformProjectConfig,
    assemble_project,
)
from app.transformer.insignia_to_ir import OperationsScript, TransformResult, parse
from app.transformer.sql_helpers import render_dml_for_bigquery


@dataclass
class GeneratedFile:
    """One output SQLX file."""
    path: str          # e.g. "definitions/stg_daily_metrics.sqlx"
    content: str
    pipeline: str
    kind: str          # "primary" | "operations"
    warnings: list[str]


def generate_sqlx(xml_files: list[tuple[str, str]]) -> list[GeneratedFile]:
    """Transform a list of (filename, xml_text) into Dataform SQLX files.

    Returns a list of GeneratedFile records. Each pipeline produces one
    primary SQLX (`type: "table"`) plus zero or more operations SQLX
    (`type: "operations"`) for post-load DML.
    """
    out: list[GeneratedFile] = []
    gen = SQLGenerator()

    for filename, text in xml_files:
        result = parse(text, filename)
        if result is None:
            continue
        pipeline = result.pipeline_name

        # Primary stages — one SQLX per <load> target table. Single-stage
        # pipelines emit one file named after the pipeline; multi-stage
        # ones emit one file per target table (e.g. stg_audit_master.sqlx
        # + fact_regulatory_audit.sqlx for the regulatory pipeline).
        for graph in result.primaries:
            if not graph.nodes:
                continue
            sql = gen.generate(graph)
            sqlx = wrap_sqlx(graph, sql)
            out.append(GeneratedFile(
                path=f"definitions/{graph.mapping_name}.sqlx",
                content=sqlx,
                pipeline=pipeline,
                kind="primary",
                warnings=list(result.warnings),
            ))

        # Operations (UPDATE/DELETE/MERGE post-load)
        for op in result.operations:
            out.append(GeneratedFile(
                path=f"definitions/operations/{op.name}.sqlx",
                content=_wrap_operations(op),
                pipeline=pipeline,
                kind="operations",
                warnings=[],
            ))

    return out


def generate_project(
    xml_files: list[tuple[str, str]],
    config: DataformProjectConfig | None = None,
    views: dict[str, str] | None = None,
) -> AssembledProject:
    """End-to-end: pipeline XMLs → complete Dataform project.

    Convenience wrapper over `generate_sqlx` + `assemble_project`. The
    returned `AssembledProject.files` is a flat dict[path -> content]
    ready to write to disk, upload to GCS, or zip.

    `views` is an optional dict of `{lowercase_view_name: oracle_view_sql}`
    plumbed through to assemble_project so source declarations for known
    views are upgraded to `type: "view"` with the original SQL body.
    """
    files = generate_sqlx(xml_files)
    return assemble_project(files, config, views=views)


def _wrap_operations(op: OperationsScript) -> str:
    """Wrap a DML operation in a Dataform `type: "operations"` SQLX shell.

    The original Oracle SQL is dialect-translated to BigQuery (SYSDATE →
    CURRENT_DATETIME, etc.) and bare table references are rewritten to
    `${ref('table')}` so Dataform resolves them to the project's declared
    sources.
    """
    depends = (
        f'  dependencies: ["{op.depends_on}"],\n'
        if op.depends_on else ""
    )
    bq_sql = render_dml_for_bigquery(op.sql)
    return (
        f"-- Operation: {op.name}\n"
        f"-- {op.sql_kind.upper()} on {op.target_table}, generated from execute_sql step.\n\n"
        f"config {{\n"
        f'  type: "operations",\n'
        f"{depends}"
        f"  hasOutput: false,\n"
        f"  description: \"{op.sql_kind.lower()} statement against {op.target_table}\",\n"
        f"}}\n\n"
        f"{bq_sql};\n"
    )
