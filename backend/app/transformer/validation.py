"""Python-side validation pass for generated Dataform projects.

Runs deterministic structural checks that catch most issues `dataform
compile` would catch — without requiring Node + the Dataform CLI:

- **Unresolved refs**: every `${ref('X')}` points at a known target.
- **SQL syntax**: each SQL body parses cleanly with sqlglot in BigQuery
  dialect.
- **Config block sanity**: each primary file declares `type:` and either
  `name:` or has a usable file stem.
- **Cycle detection**: no circular `${ref()}` dependencies between
  primary files.

Returns a `ValidationResult` per file plus a summary, ready to attach
to the project manifest and surface in the UI.
"""

from __future__ import annotations

import logging
import re
from collections import defaultdict
from dataclasses import dataclass, field
from typing import TYPE_CHECKING

import sqlglot

if TYPE_CHECKING:
    from app.transformer.runner import GeneratedFile

log = logging.getLogger(__name__)

_REF_PATTERN = re.compile(r"""\$\{ref\(\s*['"]([A-Za-z_][\w]*)['"]\s*\)\}""")
_NAME_PATTERN = re.compile(r"""^\s*name:\s*['"]([^'"]+)['"]""", re.MULTILINE)
_TYPE_PATTERN = re.compile(r"""^\s*type:\s*['"]([^'"]+)['"]""", re.MULTILINE)
_CONFIG_BLOCK = re.compile(r"config\s*\{(.*?)\}", re.DOTALL)


@dataclass
class ValidationIssue:
    """One problem found in one file."""
    severity: str   # "error" | "warning"
    code: str       # short stable identifier, e.g. "unresolved_ref"
    message: str
    file_path: str
    detail: str = ""


@dataclass
class FileValidation:
    """Per-file roll-up."""
    path: str
    refs: list[str] = field(default_factory=list)
    issues: list[ValidationIssue] = field(default_factory=list)
    confidence: int = 100  # 0-100, lower = more manual review needed

    @property
    def ok(self) -> bool:
        return not any(i.severity == "error" for i in self.issues)

    @property
    def confidence_bucket(self) -> str:
        if self.confidence >= 90: return "high"
        if self.confidence >= 70: return "medium"
        return "low"


@dataclass
class ProjectValidation:
    """Whole-project roll-up — what the manifest serialises."""
    files: list[FileValidation] = field(default_factory=list)
    issues: list[ValidationIssue] = field(default_factory=list)

    @property
    def errors(self) -> list[ValidationIssue]:
        return [i for i in self._all_issues() if i.severity == "error"]

    @property
    def warnings(self) -> list[ValidationIssue]:
        return [i for i in self._all_issues() if i.severity == "warning"]

    @property
    def ok(self) -> bool:
        return not self.errors

    def _all_issues(self) -> list[ValidationIssue]:
        return [i for f in self.files for i in f.issues] + list(self.issues)

    def summary(self) -> dict:
        return {
            "ok": self.ok,
            "files_total": len(self.files),
            "files_failing": sum(1 for f in self.files if not f.ok),
            "errors": len(self.errors),
            "warnings": len(self.warnings),
        }


# ─── Public entry point ──────────────────────────────────────────────────


def validate_project(generated: list["GeneratedFile"]) -> ProjectValidation:
    """Run all checks across the project."""
    out = ProjectValidation()

    # First pass: per-file extraction (refs, declared names) so cross-file
    # checks have the data they need.
    declared = _collect_declared_names(generated)
    file_refs: dict[str, list[str]] = {}

    for gf in generated:
        fv = FileValidation(path=gf.path, refs=_extract_refs(gf.content))
        file_refs[gf.path] = fv.refs

        if gf.kind in ("primary", "operations") and gf.path != "definitions/sources.sqlx":
            _check_config_block(gf, fv)
            _check_sql_syntax(gf, fv)

        _check_ref_resolution(gf, fv, declared)

        fv.confidence = _confidence_for(gf, fv)
        # Mirror onto the GeneratedFile so the runner / manifest can
        # surface it without holding the validation result separately.
        try:
            gf.confidence = fv.confidence
        except Exception:
            pass

        out.files.append(fv)

    # Cross-file: cycle detection on primary files only.
    cycles = _detect_cycles(generated, file_refs)
    if cycles:
        for cycle in cycles:
            out.issues.append(ValidationIssue(
                severity="error",
                code="cycle",
                message=f"Circular ref dependency: {' -> '.join(cycle)} -> {cycle[0]}",
                file_path=cycle[0],
                detail="Dataform compile would reject this — break the loop.",
            ))

    return out


# ─── Per-file checks ─────────────────────────────────────────────────────


def _check_config_block(gf: "GeneratedFile", fv: FileValidation) -> None:
    config_match = _CONFIG_BLOCK.search(gf.content)
    if not config_match:
        fv.issues.append(ValidationIssue(
            severity="error",
            code="missing_config",
            message="No config { ... } block found",
            file_path=gf.path,
        ))
        return
    config_text = config_match.group(0)
    if not _TYPE_PATTERN.search(config_text):
        fv.issues.append(ValidationIssue(
            severity="error",
            code="missing_type",
            message="Config block missing required `type:` field",
            file_path=gf.path,
        ))
    # `name:` is best-practice but Dataform falls back to the file stem,
    # so absence is a warning rather than an error.
    if gf.kind == "primary" and not _NAME_PATTERN.search(config_text):
        fv.issues.append(ValidationIssue(
            severity="warning",
            code="missing_name",
            message="Config block has no explicit `name:` — Dataform will use the file stem",
            file_path=gf.path,
        ))


def _check_sql_syntax(gf: "GeneratedFile", fv: FileValidation) -> None:
    """Strip the config block + comments, then sqlglot-parse the SQL body."""
    body = _strip_config(gf.content)
    body = _strip_template_refs(body)  # ${ref('x')} → x for parsing
    body = body.strip()
    if not body:
        return
    try:
        sqlglot.parse_one(body, dialect="bigquery")
    except Exception as e:  # noqa: BLE001
        fv.issues.append(ValidationIssue(
            severity="error",
            code="sql_parse_error",
            message="SQL body fails to parse as BigQuery",
            file_path=gf.path,
            detail=str(e)[:300],
        ))


def _check_ref_resolution(
    gf: "GeneratedFile",
    fv: FileValidation,
    declared: set[str],
) -> None:
    for ref in fv.refs:
        if ref not in declared:
            fv.issues.append(ValidationIssue(
                severity="error",
                code="unresolved_ref",
                message=f"${{ref('{ref}')}} points at no declared table",
                file_path=gf.path,
                detail="Add to sources.sqlx or check the upstream pipeline name.",
            ))


# ─── Cross-file: cycle detection ────────────────────────────────────────


def _detect_cycles(
    generated: list["GeneratedFile"],
    file_refs: dict[str, list[str]],
) -> list[list[str]]:
    """Find any cycles in the primary-file dependency graph."""
    # Map declared name → file path for primary files only.
    name_to_path: dict[str, str] = {}
    for gf in generated:
        if gf.kind != "primary":
            continue
        m = _NAME_PATTERN.search(gf.content)
        stem = gf.path.split("/")[-1].removesuffix(".sqlx")
        if m:
            name_to_path[m.group(1)] = gf.path
        name_to_path[stem] = gf.path

    # Build adjacency: primary file path → primary files it depends on.
    adj: dict[str, set[str]] = defaultdict(set)
    for gf in generated:
        if gf.kind != "primary":
            continue
        for ref in file_refs.get(gf.path, []):
            target = name_to_path.get(ref)
            if target and target != gf.path:
                adj[gf.path].add(target)

    cycles: list[list[str]] = []
    visiting: dict[str, int] = {}  # 0=unvisited, 1=on-stack, 2=done

    def dfs(node: str, stack: list[str]) -> None:
        visiting[node] = 1
        stack.append(node)
        for n in adj.get(node, ()):
            if visiting.get(n, 0) == 1:
                # Found a cycle — slice from where we re-entered
                idx = stack.index(n)
                cycles.append(stack[idx:])
            elif visiting.get(n, 0) == 0:
                dfs(n, stack)
        stack.pop()
        visiting[node] = 2

    for n in adj:
        if visiting.get(n, 0) == 0:
            dfs(n, [])
    return cycles


# ─── Helpers ────────────────────────────────────────────────────────────


def _collect_declared_names(generated: list["GeneratedFile"]) -> set[str]:
    """Every name a `${ref()}` is allowed to resolve to."""
    out: set[str] = set()
    for gf in generated:
        # File stems are valid refs.
        out.add(gf.path.split("/")[-1].removesuffix(".sqlx"))
        # `name:` field if present.
        for m in _NAME_PATTERN.finditer(gf.content):
            out.add(m.group(1))
    return out


def _extract_refs(content: str) -> list[str]:
    seen: set[str] = set()
    out: list[str] = []
    for m in _REF_PATTERN.finditer(content):
        n = m.group(1)
        if n not in seen:
            seen.add(n)
            out.append(n)
    return out


def _strip_config(content: str) -> str:
    return _CONFIG_BLOCK.sub("", content, count=1)


def _strip_template_refs(content: str) -> str:
    """Replace ${ref('x')} with the bare name `x` so sqlglot can parse."""
    return _REF_PATTERN.sub(lambda m: m.group(1), content)


# ─── Confidence scoring ──────────────────────────────────────────────────


def _confidence_for(gf, fv: FileValidation) -> int:
    """Deduct from 100 based on signals that the translation may need
    manual review. Tuned for transparency over precision — every deduction
    has a clear reason a user can map back to a fix."""
    score = 100

    # Validation signals — strongest indicator something will fail.
    for issue in fv.issues:
        score -= 25 if issue.severity == "error" else 10

    # Parser warnings the runner attached to this file (typically pipeline-
    # wide notices about steps the parser couldn't fully decompose).
    parser_warnings = getattr(gf, "warnings", None) or []
    score -= 5 * len(parser_warnings)

    # Sources file always trusts its declarations — no SQL body to check.
    if gf.path.endswith("/sources.sqlx"):
        return max(0, min(100, score))

    # `custom_sql` fallback: when the parser couldn't fully decompose a
    # SELECT (e.g. multi-table joins), we punted by passing the original
    # SQL through verbatim. Still valid output, just less structurally
    # decomposed — flag for review.
    if "SELECT * FROM" in gf.content and gf.kind == "primary":
        # Heuristic: a real Source CTE doing SELECT * FROM ${ref('x')} is
        # only generated by the custom_sql path. (Native SourceNodes emit
        # explicit columns, not *.)
        if "SELECT *\n  FROM" not in gf.content:  # ignore extract-csv source CTEs
            score -= 10

    return max(0, min(100, score))
