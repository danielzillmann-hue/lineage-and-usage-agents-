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

from app.config import get_settings

if TYPE_CHECKING:
    from app.transformer.runner import GeneratedFile


@dataclass
class DataformProjectConfig:
    """Target environment for the assembled Dataform repo.

    Defaults pull from app settings so this stays in sync with the
    deployment environment instead of needing edits in two places.
    """
    gcp_project: str = field(default_factory=lambda: get_settings().gcp_project)
    location: str = field(default_factory=lambda: get_settings().gcp_region)
    default_dataset: str = "migration_demo"
    assertion_dataset: str = "migration_demo_assertions"
    source_dataset: str = "migration_raw"
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

    # 3. Generate one source-declaration file per source.
    # Dataform parses ONE config block per .sqlx — multiple declaration
    # blocks crammed into a single sources.sqlx silently drop all but the
    # first. Splitting into definitions/sources/<name>.sqlx is the
    # canonical pattern.
    #
    # Sources we can't trace to Oracle inventory and aren't ported views
    # (e.g. CSV-sourced tables, or tables referenced by ETL but never
    # actually created upstream) get rendered as empty stub tables instead
    # of declarations — without that, Dataform's run fails with "Table not
    # found" and the demo can't go end-to-end. The stub schema is
    # inferred from how downstream pipelines reference the source.
    source_columns = _collect_source_columns(generated, sources)
    if sources:
        for path, content in _build_source_files(
            sources, config, views, table_metadata, source_columns
        ).items():
            out.files[path] = content
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

    # 4. Project-level config: workflow_settings.yaml + package.json.
    # Dataform's compile resolves workflow_settings.yaml against the
    # @dataform/core npm dependency declared in package.json — without
    # the latter, GCP Dataform fails with "Failed to resolve
    # workflow_settings.yaml".
    out.files["workflow_settings.yaml"] = _build_workflow_settings(config)
    out.files["package.json"] = _build_package_json(config)

    # 5. Validation pass — runs on every assembled project. The full
    # generated file list (including the just-built sources/* files)
    # flows through so cross-file checks like ref resolution see
    # everything. Validation also mutates each GeneratedFile.confidence
    # in-place.
    from app.transformer.runner import GeneratedFile  # local: avoid cycle
    from app.transformer.validation import validate_project
    full_files = list(generated)
    for path, content in out.files.items():
        if path.startswith("definitions/sources/") and path.endswith(".sqlx"):
            full_files.append(GeneratedFile(
                path=path, content=content,
                pipeline="(sources)", kind="sources", warnings=[],
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


def _build_source_files(
    sources: list[str],
    config: DataformProjectConfig,
    views: dict[str, str],
    table_metadata: dict[str, dict],
    source_columns: dict[str, list[str]] | None = None,
) -> dict[str, str]:
    """One Dataform-compatible .sqlx per source. Each contains a single
    config block, which is what Dataform expects.

    For each source we pick the strongest available representation:
      1. If we have ported the original Oracle view → emit `type: "view"`
         with the translated CREATE-VIEW body.
      2. Else if Oracle inventory metadata lists this source → emit a
         `type: "declaration"` and let the replicator script populate it.
      3. Else (no Oracle origin known — typical for CSV inputs and
         ETL-only staging tables) → emit a `type: "table"` stub with an
         empty body but the column shape inferred from how downstream
         pipelines reference it. Project stays self-contained; user
         replaces stubs with real loads when source data lands.

    Returns: dict[relative_path -> file_content]
    """
    source_columns = source_columns or {}
    out: dict[str, str] = {}
    for s in sources:
        view_sql = views.get(s.lower())
        meta = table_metadata.get(s.lower()) or {}
        has_inventory = bool(meta.get("schema")) or bool(meta.get("primary_keys"))
        if view_sql:
            body = _view_block(s, view_sql, config)
            header = (
                "-- Source view ported from Oracle to BigQuery by intelia\n"
                "-- Lineage & Usage Agents.\n\n"
            )
        elif has_inventory:
            body = _declaration_block(s, config, meta)
            header = (
                "-- Source declaration generated by intelia Lineage & Usage\n"
                "-- Agents from the Oracle inventory. The actual table is\n"
                "-- populated upstream by replication (Datastream / bq load /\n"
                "-- BQ federation — see bootstrap/replication_setup.md).\n\n"
            )
        else:
            body = _stub_table_block(s, config, source_columns.get(s, []))
            header = (
                f"-- Source stub generated by intelia Lineage & Usage Agents.\n"
                f"-- The original `{s}` was not present in the Oracle\n"
                f"-- inventory (typical for CSV inputs or never-created\n"
                f"-- tables). The schema below was inferred from how this\n"
                f"-- source is referenced downstream. Replace the empty\n"
                f"-- SELECT with a real load (CSV / BQ external / replication)\n"
                f"-- when source data is available.\n\n"
            )
        out[f"definitions/sources/{s}.sqlx"] = header + body + "\n"
    return out


# ─── Stub-table support ─────────────────────────────────────────────────


_REF_SELECT_BLOCK_RE = re.compile(
    r"SELECT\s+(?P<cols>.+?)\s+FROM\s+\$\{ref\(\s*['\"](?P<src>[A-Za-z_][\w]*)['\"]\s*\)\}",
    re.IGNORECASE | re.DOTALL,
)
_QUALIFIED_COL_RE = re.compile(r"\b([A-Za-z_][\w]*)\.([A-Za-z_][\w]*)\b")


def _collect_source_columns(generated, sources: list[str]) -> dict[str, list[str]]:
    """For each source name, derive the column list referenced across
    every signal we have:

    1. Generated SQLX `SELECT a, b FROM ${ref('source')}` blocks — works
       when the IR emitted explicit columns from an `<extract>SELECT
       cols FROM source</extract>` step.
    2. Original-XML attributes for sources that came in via `<extract_csv>`
       (where the IR has no SELECT to crib columns from). The XML's
       `<join on=...>`, `<math col1=... col2=...>`, `<calculate_category
       column=...>`, `<sum column=...>` all imply columns on the source
       — we union them.

    Without (2), CSV-sourced stubs end up with an empty schema and downstream
    pipelines that reference TAX_RATE / MARKET_BENCHMARK_RETURN error out.
    """
    if not sources:
        return {}
    wanted = {s for s in sources}
    cols_per: dict[str, list[str]] = {s: [] for s in sources}
    seen_per: dict[str, set[str]] = {s: set() for s in sources}

    def _add(src: str, col: str) -> None:
        col_clean = col.strip().rstrip(",").strip()
        if not col_clean or col_clean == "*":
            return
        # Strip alias ("col AS x") and table-prefix
        # `proj.sql()` may emit "src.COL AS COL" — keep COL.
        token = col_clean.split()[0]
        if "." in token:
            token = token.split(".")[-1]
        token = token.strip("`\"'")
        if not token or not token.replace("_", "").isalnum():
            return
        upper = token.upper()
        if upper in seen_per[src]:
            return
        seen_per[src].add(upper)
        cols_per[src].append(token)

    # Pass 1 — explicit SELECT lists in generated SQLX.
    for gf in generated:
        content = getattr(gf, "content", "")
        if not content:
            continue
        for m in _REF_SELECT_BLOCK_RE.finditer(content):
            src_name = m.group("src")
            if src_name not in wanted:
                continue
            cols_blob = m.group("cols")
            # Naive split — works for the IR's emitted SELECTs which list
            # one column per line. Brackets/parens in expressions are rare
            # in these wrapper SELECTs.
            for piece in cols_blob.split(","):
                _add(src_name, piece)

    # Pass 2 — XML attribute mining for CSV-sourced (or otherwise SELECT-*)
    # sources whose columns the IR couldn't capture.
    for gf in generated:
        xml_text = getattr(gf, "original_content", "") or ""
        if "<pipeline" not in xml_text:
            continue
        _mine_xml_for_source_columns(xml_text, wanted, _add)

    return {s: cols_per[s] for s in sources if cols_per[s]}


# Match `<extract_csv id="X" path=".../<csv_name>.csv"/>`. The id is the
# step id used by JOIN's left/right; the csv_name (basename minus extension)
# is what the IR uses as the source's table_ref.
_EXTRACT_CSV_RE = re.compile(
    r'<extract_csv\s+[^>]*?id\s*=\s*"(?P<id>[^"]+)"\s+[^>]*?path\s*=\s*"(?P<path>[^"]+\.csv)"',
    re.IGNORECASE,
)
# Same but with attribute order reversed.
_EXTRACT_CSV_RE_ALT = re.compile(
    r'<extract_csv\s+[^>]*?path\s*=\s*"(?P<path>[^"]+\.csv)"\s+[^>]*?id\s*=\s*"(?P<id>[^"]+)"',
    re.IGNORECASE,
)


def _mine_xml_for_source_columns(xml: str, wanted: set[str], add) -> None:
    """Pull column attributes out of XML for any `<extract_csv>` whose
    derived source name is in `wanted`.
    """
    # Map step-id → source-name for every CSV input.
    step_to_source: dict[str, str] = {}
    for pat in (_EXTRACT_CSV_RE, _EXTRACT_CSV_RE_ALT):
        for m in pat.finditer(xml):
            csv_name = m.group("path").rsplit("/", 1)[-1].rsplit(".", 1)[0].lower()
            if csv_name in wanted:
                step_to_source[m.group("id")] = csv_name

    if not step_to_source:
        return

    # Helper: collect step ids that are downstream of any CSV step. The
    # IR's <math>/<transform>/<aggregate> use named inputs, which we
    # follow transitively so column hints propagate (e.g. join_tax →
    # transform calc_tax → math col1=TAX_RATE).
    downstream: dict[str, set[str]] = {sid: {sid} for sid in step_to_source}

    # First pass: <join> registers an output id whose columns include
    # both sides' columns. <on=COL> is a join key on both sides.
    for m in re.finditer(
        r'<join\s+[^>]*?id\s*=\s*"(?P<id>[^"]+)"[^>]*?>',
        xml,
        re.IGNORECASE,
    ):
        join_attrs = m.group(0)
        join_id = m.group("id")
        left_m = re.search(r'left\s*=\s*"([^"]+)"', join_attrs, re.IGNORECASE)
        right_m = re.search(r'right\s*=\s*"([^"]+)"', join_attrs, re.IGNORECASE)
        on_m = re.search(r'on\s*=\s*"([^"]+)"', join_attrs, re.IGNORECASE)
        left_id = left_m.group(1) if left_m else ""
        right_id = right_m.group(1) if right_m else ""
        # The join's output is downstream of both inputs.
        for src_step, downs in downstream.items():
            if left_id in downs or right_id in downs:
                downs.add(join_id)
        # `on=` column belongs to whichever input is a CSV source.
        if on_m:
            on_col = on_m.group(1).strip()
            for csv_step, src_name in step_to_source.items():
                if csv_step in (left_id, right_id):
                    add(src_name, on_col)

    # Second pass: any <transform input="X">/<aggregate input="X"> with
    # X downstream of a CSV step — its inner <math>/<sum>/<calculate_category>
    # column references are candidate columns of the CSV.
    transform_re = re.compile(
        r'<(?:transform|aggregate)\s+[^>]*?input\s*=\s*"([^"]+)"[^>]*?>(.+?)</(?:transform|aggregate)>',
        re.IGNORECASE | re.DOTALL,
    )
    for m in transform_re.finditer(xml):
        input_id = m.group(1)
        body = m.group(2)
        affected = [
            src_name
            for csv_step, src_name in step_to_source.items()
            if input_id in downstream.get(csv_step, {input_id})
        ]
        if not affected:
            continue
        # math col1/col2 (skipping val1/val2 — those are literals).
        for mm in re.finditer(
            r'<math\s+[^>]*?col1\s*=\s*"([^"]+)"',
            body,
            re.IGNORECASE,
        ):
            for src_name in affected:
                add(src_name, mm.group(1))
        for mm in re.finditer(
            r'<math\s+[^>]*?col2\s*=\s*"([^"]+)"',
            body,
            re.IGNORECASE,
        ):
            for src_name in affected:
                add(src_name, mm.group(1))
        for mm in re.finditer(
            r'<(?:sum|min|max|avg|count|calculate_category)\s+[^>]*?(?:column|col)\s*=\s*"([^"]+)"',
            body,
            re.IGNORECASE,
        ):
            for src_name in affected:
                add(src_name, mm.group(1))


# Heuristic name → BigQuery type mapping for stub schemas. Used when the
# agent has no inventory data for a source and has to make up a schema.
_TYPE_HINTS = (
    (("_date", "_dt"), "DATE"),
    (("_at", "_time", "timestamp"), "DATETIME"),
    (
        ("_pct", "_percent", "_rate", "_ratio", "_amount", "_return",
         "_value", "_score", "_income", "_balance", "_tax", "_fee",
         "_total", "_sum", "_avg"),
        "FLOAT64",
    ),
    (("_count", "_qty", "_quantity", "_num"), "INT64"),
    (("_id",), "INT64"),
    (("is_", "has_", "_flag"), "BOOL"),
)


def _infer_bq_type(col: str) -> str:
    n = col.lower()
    for tokens, bq in _TYPE_HINTS:
        for tok in tokens:
            if tok.startswith("_") and n.endswith(tok):
                return bq
            if not tok.startswith("_") and (n.startswith(tok) or tok in n):
                return bq
    return "STRING"


def _stub_table_block(name: str, config: DataformProjectConfig, columns: list[str]) -> str:
    """Render a `type: "table"` block that materialises an empty BQ table
    with the inferred column shape. Dependents compile and run; users
    swap the body for a real load when source data lands.

    `SELECT … FROM UNNEST(ARRAY<INT64>[])` is the standard BigQuery idiom
    for "table with this schema and zero rows" — `WHERE FALSE` alone
    fails BQ's "Query without FROM clause cannot have a WHERE clause"
    check.
    """
    if not columns:
        # Nothing to infer — emit a single STRING column so the table
        # can at least be queried without errors. User will replace.
        columns = ["placeholder"]
    casts = ",\n  ".join(
        f"CAST(NULL AS {_infer_bq_type(c)}) AS {c}" for c in columns
    )
    return (
        f"config {{\n"
        f'  type: "table",\n'
        f'  database: "{config.gcp_project}",\n'
        f'  schema: "{config.source_dataset}",\n'
        f'  name: "{name}",\n'
        f'  description: "Stub source — schema inferred from downstream usage; populate with real data when available.",\n'
        f"}}\n\n"
        f"SELECT\n  {casts}\nFROM UNNEST(ARRAY<INT64>[])"
    )


def _declaration_block(
    name: str,
    config: DataformProjectConfig,
    metadata: dict | None = None,
) -> str:
    # Note: Dataform's declaration config does not accept `assertions:` —
    # only `columns/database/description/name/schema/type`. Assertions on
    # source tables would need to be emitted as separate assertion actions.
    return f"""config {{
  type: "declaration",
  database: "{config.gcp_project}",
  schema: "{config.source_dataset}",
  name: "{name}",
  description: "Source table replicated from the Oracle warehouse.",
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


def _view_block(name: str, oracle_sql: str, config: DataformProjectConfig) -> str:
    """Render a Dataform `type: "view"` block from the original Oracle
    CREATE VIEW SELECT body. Translates dialect to BigQuery and rewrites
    bare table refs to ${ref()} so the view's deps are tracked.

    The view is created in the source dataset alongside the declaration
    blocks. Without an explicit `schema:`, Dataform defaults to
    `defaultDataset` and the view collides with any downstream primary
    that produces a same-named table.
    """
    # Local import to avoid a circular dependency.
    from app.transformer.sql_helpers import render_dml_for_bigquery
    body = render_dml_for_bigquery(oracle_sql).rstrip(";")
    return (
        f"config {{\n"
        f'  type: "view",\n'
        f'  database: "{config.gcp_project}",\n'
        f'  schema: "{config.source_dataset}",\n'
        f'  name: "{name}",\n'
        f'  description: "Oracle view ported to BigQuery — translated from CREATE VIEW source.",\n'
        f"}}\n\n"
        f"{body}"
    )


def _build_workflow_settings(config: DataformProjectConfig) -> str:
    # Note: dataformCoreVersion is intentionally omitted — Dataform 3.x
    # rejects it in workflow_settings.yaml when a package.json is present
    # (the version is read from package.json's @dataform/core dependency).
    return (
        f"# Dataform project settings - generated by intelia Lineage & Usage Agents.\n"
        f"defaultProject: {config.gcp_project}\n"
        f"defaultLocation: {config.location}\n"
        f"defaultDataset: {config.default_dataset}\n"
        f"defaultAssertionDataset: {config.assertion_dataset}\n"
    )


def _build_package_json(config: DataformProjectConfig) -> str:
    """Minimal package.json so Dataform's compile can resolve
    @dataform/core. Without this, GCP Dataform fails with
    "Failed to resolve workflow_settings.yaml".
    """
    return (
        "{\n"
        '  "name": "dataform-project",\n'
        f'  "version": "1.0.0",\n'
        '  "dependencies": {\n'
        f'    "@dataform/core": "{config.dataform_core_version}"\n'
        '  }\n'
        "}\n"
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
        f"# Generated Dataform project\n\n"
        f"This repo was generated automatically from Oracle pipeline XMLs by\n"
        f"intelia's Lineage & Usage Agents. It is a 1:1 translation of the\n"
        f"legacy ETL into BigQuery + Dataform.\n\n"
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
