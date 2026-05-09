"""
SQLX Wrapper — Wraps a SQL string with a Dataform SQLX config block.

This is the final stage: takes clean SQL from sql_generator and adds
the Dataform-specific config block, header comments, and SQLX formatting.
"""

from __future__ import annotations

import re
from transformation_core.ir import DataflowGraph
from transformation_core.naming import to_pascal_name


def wrap_sqlx(graph: DataflowGraph, sql: str) -> str:
    """Wrap generated SQL with SQLX config and header.

    Args:
        graph: The DataflowGraph containing metadata.
        sql: The generated SQL from sql_generator.

    Returns:
        Complete SQLX file content.
    """
    # Cache-warming mappings: produce a comment-only file
    if graph.table_type == "skip":
        return (
            f"-- Skipped: {graph.mapping_name}\n"
            f"--\n"
            f"{graph.description}\n"
        )

    parts = []

    # Header comment — use only the first line/sentence to avoid
    # multi-line descriptions leaking bare text before config block.
    # Full description is already in the config block.
    parts.append(f"-- Dataform transformation: {graph.mapping_name}")
    if graph.description:
        # Take first paragraph (up to first blank line or \n\n)
        first_para = re.split(r'\r?\n\s*\r?\n', graph.description)[0]
        # Take first sentence if still long
        first_para = first_para.replace('\r', '').replace('\n', ' ').strip()
        if len(first_para) > 200:
            cut = first_para.rfind('.', 0, 200)
            if cut > 50:
                first_para = first_para[:cut + 1]
            else:
                first_para = first_para[:200]
        parts.append(f"-- Description: {first_para}")
    parts.append("")

    # Config block
    config = _build_config(graph)
    parts.append(config)
    parts.append("")

    # SQL body
    parts.append(sql)

    return "\n".join(parts)


def _build_config(graph: DataflowGraph) -> str:
    """Build the SQLX config block."""
    lines = ["config {"]

    # Type
    table_type = graph.table_type
    lines.append(f'  type: "{table_type}",')

    # Schema
    if graph.schema:
        lines.append(f'  schema: "{graph.schema}",')

    # Name — use mapping_name (== the SQLX file stem) rather than the
    # target table name. Two XML pipelines can write to the same target
    # table (e.g. accounts_summary.xml and final_accounts_extract.xml
    # both producing `accounts_summary`); using target_table makes both
    # files emit `name: "accounts_summary"`, which Dataform rejects as a
    # duplicate canonical target. mapping_name is unique per emitted file.
    raw_name = graph.mapping_name or graph.target.target_table
    if raw_name:
        name = _sanitize_sqlx_name(raw_name)
        lines.append(f'  name: "{name}",')

    # Tags
    if graph.tags:
        tag_str = ", ".join(f'"{t}"' for t in graph.tags)
        lines.append(f"  tags: [{tag_str}],")

    # Incremental config
    if table_type == "incremental" and graph.target.unique_key:
        key_str = ", ".join(f'"{k}"' for k in graph.target.unique_key)
        lines.append(f"  uniqueKey: [{key_str}],")

    # Description
    desc = graph.description or f"Transformation from {graph.mapping_name}"
    desc = desc.replace('"', '\\"')
    lines.append(f'  description: "{desc}",')

    lines.append("}")
    return "\n".join(lines)


def _sanitize_sqlx_name(name: str) -> str:
    """Strip non-identifier characters and lowercase. The result must match
    the file stem and any `${ref('…')}` calls so Dataform resolves
    dependencies — those use snake_case, so target names do too.
    """
    return re.sub(r'[^a-zA-Z0-9_]', '_', name).strip('_').lower()
