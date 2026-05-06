"""Handover document generators — Markdown + print-friendly HTML.

The output is the artefact a consultant would circulate after a workshop:
exec summary, findings, decommission verdicts, sequencing waves, multi-writer
classifications, business rules, scope counts. Both formats are designed to
be human-readable; the HTML version has print-friendly CSS so opening it
and using "Save as PDF" in any browser produces a polished deliverable.
"""

from __future__ import annotations

from datetime import datetime
from html import escape

from app.models.run import Run
from app.models.schema import Inventory, RunResults


def render_markdown(run: Run, results: RunResults) -> str:
    inv = results.inventory
    summary = results.summary
    lines: list[str] = []

    title = "Lineage & Usage Agents — Handover"
    lines.append(f"# {title}")
    lines.append("")
    lines.append(f"**Run**: `{run.id}`")
    lines.append(f"**Generated**: {datetime.utcnow().isoformat()}Z")
    if run.oracle_dsn:
        lines.append(f"**Database**: `{run.oracle_dsn}`")
    if run.bucket:
        path = f"{run.bucket}{('/' + run.prefix) if run.prefix else ''}"
        lines.append(f"**Bucket**: `{path}`")
    lines.append("")

    if summary:
        lines.append("## Executive Summary")
        lines.append("")
        if summary.headline:
            lines.append(f"> {summary.headline}")
            lines.append("")
        for b in summary.bullets:
            lines.append(f"- {b}")
        lines.append("")

        if summary.metrics:
            lines.append("### Key metrics")
            lines.append("")
            lines.append("| Metric | Value |")
            lines.append("|---|---|")
            for k, v in summary.metrics.items():
                lines.append(f"| {k.replace('_', ' ')} | {v} |")
            lines.append("")

        if summary.findings:
            lines.append("## Findings")
            lines.append("")
            for f in summary.findings:
                lines.append(f"### `{f.severity.upper()}` — {f.title}")
                lines.append("")
                lines.append(f.detail)
                if f.object_fqns:
                    lines.append("")
                    lines.append("**Objects**: " + ", ".join(f"`{o}`" for o in f.object_fqns[:10]))
                if f.recommendation:
                    lines.append("")
                    lines.append(f"**Recommendation**: {f.recommendation}")
                lines.append("")

    if inv:
        lines.append("## Inventory")
        lines.append("")
        layer_counts: dict[str, int] = {}
        for t in inv.tables:
            layer_counts[t.layer.value] = layer_counts.get(t.layer.value, 0) + 1
        kind_counts: dict[str, int] = {}
        for t in inv.tables:
            kind_counts[t.kind] = kind_counts.get(t.kind, 0) + 1
        lines.append(f"- **{len(inv.tables)} objects** by layer: " +
                     ", ".join(f"{n} {l}" for l, n in sorted(layer_counts.items())))
        lines.append(f"- **{kind_counts.get('TABLE', 0)} tables**, "
                     f"{kind_counts.get('VIEW', 0)} views, "
                     f"{kind_counts.get('CSV', 0)} CSV outputs")
        lines.append(f"- **{len(inv.pipelines)} ETL pipelines** defined")
        if inv.orphan_runs:
            lines.append(f"- **{len(inv.orphan_runs)} pipelines run without an XML definition** (governance gap)")
        if inv.rules:
            lines.append(f"- **{len(inv.rules)} embedded business rules** extracted")
        lines.append("")

        # Decommission readiness
        if inv.decommission:
            safe = [a for a in inv.decommission if a.verdict == "safe"]
            review = [a for a in inv.decommission if a.verdict == "review"]
            blocked = [a for a in inv.decommission if a.verdict == "blocked"]
            lines.append("## Decommission Readiness")
            lines.append("")
            lines.append(f"- **{len(safe)} safe** to retire (no active dependencies)")
            lines.append(f"- **{len(review)} need review** (some dependencies — investigate)")
            lines.append(f"- **{len(blocked)} blocked** (active in pipelines / views)")
            lines.append("")
            if safe:
                lines.append("### Top decommission candidates")
                lines.append("")
                lines.append("| Object | Score | Drivers |")
                lines.append("|---|---|---|")
                for a in safe[:15]:
                    drivers = "; ".join(a.drivers[:2])
                    lines.append(f"| `{a.object_fqn}` | {a.score} | {drivers} |")
                lines.append("")

        # Sequencing
        if inv.sequencing:
            lines.append("## Migration Sequencing")
            lines.append("")
            for w in inv.sequencing:
                lines.append(f"### Wave {w.wave} — {w.description}")
                lines.append("")
                if w.table_fqns:
                    lines.append("**Tables**: " + ", ".join(f"`{t}`" for t in w.table_fqns[:20]))
                    if len(w.table_fqns) > 20:
                        lines.append(f"  *(+{len(w.table_fqns) - 20} more)*")
                if w.pipeline_names:
                    lines.append("**Pipelines**: " + ", ".join(f"`{p}`" for p in w.pipeline_names[:20]))
                lines.append("")

        # Multi-writer
        if inv.multi_writers:
            lines.append("## Multi-Writer Targets")
            lines.append("")
            lines.append("| Target | Pattern | Writers |")
            lines.append("|---|---|---|")
            for m in inv.multi_writers:
                lines.append(f"| `{m.target_fqn}` | {m.pattern} | {', '.join(m.writer_pipelines)} |")
            lines.append("")

        # PII reach
        pii_inherited = [
            (t.fqn, c.name, c.inherited_sensitivity_from)
            for t in inv.tables
            for c in t.columns
            if c.inherited_sensitivity_from
        ]
        if pii_inherited:
            lines.append("## PII / Sensitive Data Reach")
            lines.append("")
            lines.append(f"{len(pii_inherited)} downstream columns inherit PII or sensitive classifications via lineage.")
            lines.append("")
            lines.append("| Target column | Inherited from |")
            lines.append("|---|---|")
            for fqn, col, sources in pii_inherited[:30]:
                sources_text = ", ".join(f"`{s}`" for s in sources[:3])
                if len(sources) > 3:
                    sources_text += f" *(+{len(sources) - 3})*"
                lines.append(f"| `{fqn}.{col}` | {sources_text} |")
            if len(pii_inherited) > 30:
                lines.append(f"\n*(+{len(pii_inherited) - 30} more rows in scope.json)*")
            lines.append("")

        # Pipeline failure summary
        failing = [p for p in inv.pipelines if p.runs and p.runs.runs_failed > 0]
        if failing:
            lines.append("## Pipeline Failure Hotspots")
            lines.append("")
            lines.append("| Pipeline | Total runs | Failed | Last run |")
            lines.append("|---|---|---|---|")
            for p in sorted(failing, key=lambda x: -(x.runs.runs_failed if x.runs else 0))[:10]:
                r = p.runs
                lines.append(f"| `{p.name}` | {r.runs_total} | {r.runs_failed} | {r.last_run or '—'} |")
            lines.append("")

        # Business rules
        if inv.rules:
            lines.append("## Embedded Business Rules")
            lines.append("")
            lines.append("Rules buried in PL/SQL views and ETL transforms. **These must be preserved on Bluedoor.**")
            lines.append("")
            for r in inv.rules[:25]:
                lines.append(f"- **`{r.rule_type}`** in `{r.source_object}`"
                             f"{(' — `' + r.column + '`') if r.column else ''}")
                lines.append(f"  - {r.natural_language}")
                lines.append(f"  - `{r.expression}`")
            if len(inv.rules) > 25:
                lines.append(f"\n*(+{len(inv.rules) - 25} more in scope.json)*")
            lines.append("")

    lines.append("---")
    lines.append("")
    lines.append("*Generated by intelia · Lineage & Usage Agents.*")
    return "\n".join(lines)


def render_html(run: Run, results: RunResults) -> str:
    """Print-friendly HTML — open in browser, then File > Save as PDF."""
    md = render_markdown(run, results)
    body = _markdown_to_html(md)
    title = "Lineage & Usage Agents — Handover"
    return f"""<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="utf-8">
<title>{escape(title)} · {escape(run.id[:8])}</title>
<style>
  @page {{ size: A4; margin: 18mm 16mm; }}
  :root {{
    --ink: #0F1F2C; --ink-2: #3B4A57; --ink-3: #6B7884;
    --bg: #FFFFFF; --bg-sunk: #F4F3EE; --line: #E7E5DE;
    --emerald: #0FB37A; --emerald-700: #0A8A5C;
    --warn: #C77B0A; --crit: #C0362C;
    --serif: "Source Serif 4", Georgia, "Times New Roman", serif;
    --sans: "Inter Tight", -apple-system, "Segoe UI", Helvetica, Arial, sans-serif;
    --mono: "JetBrains Mono", "SF Mono", Menlo, Consolas, monospace;
  }}
  * {{ box-sizing: border-box; }}
  body {{
    font-family: var(--sans); color: var(--ink); background: var(--bg);
    max-width: 880px; margin: 0 auto; padding: 32px 32px 64px;
    line-height: 1.5; font-size: 13.5px;
  }}
  h1 {{ font-size: 28px; font-weight: 500; letter-spacing: -0.02em; margin: 0 0 12px; }}
  h2 {{ font-size: 18px; font-weight: 500; letter-spacing: -0.01em;
        margin: 32px 0 8px; padding-bottom: 6px; border-bottom: 1px solid var(--line); }}
  h3 {{ font-size: 14.5px; font-weight: 500; margin: 20px 0 6px; }}
  p, li {{ font-size: 13.5px; }}
  blockquote {{
    margin: 12px 0; padding: 10px 14px;
    background: var(--bg-sunk); border-left: 3px solid var(--emerald);
    font-style: normal; color: var(--ink-2);
  }}
  code {{
    font-family: var(--mono); font-size: 12px;
    padding: 1px 4px; background: var(--bg-sunk);
    border: 1px solid var(--line); border-radius: 3px;
  }}
  pre {{
    font-family: var(--mono); font-size: 11.5px;
    background: var(--bg-sunk); border: 1px solid var(--line);
    border-radius: 4px; padding: 10px 12px; overflow-x: auto;
  }}
  table {{
    width: 100%; border-collapse: collapse; margin: 10px 0 16px;
    font-size: 12px;
  }}
  th, td {{
    text-align: left; padding: 6px 8px; vertical-align: top;
    border-bottom: 1px solid var(--line);
  }}
  th {{
    font-family: var(--mono); font-size: 10.5px; font-weight: 500;
    text-transform: uppercase; letter-spacing: 0.08em;
    color: var(--ink-3); background: var(--bg-sunk);
  }}
  hr {{ border: 0; border-top: 1px solid var(--line); margin: 32px 0 12px; }}
  .footer {{ font-size: 11px; color: var(--ink-3); margin-top: 16px; }}
  @media print {{
    body {{ padding: 0; }}
    h2 {{ page-break-after: avoid; }}
    table {{ page-break-inside: avoid; }}
    .no-print {{ display: none; }}
  }}
</style>
</head>
<body>
<div class="no-print" style="
  position: sticky; top: 0; padding: 8px 12px;
  background: var(--bg-sunk); border-bottom: 1px solid var(--line);
  font-family: var(--mono); font-size: 11px; color: var(--ink-3);
  margin: -32px -32px 24px;
">
  Use File → Print → Save as PDF for a print-quality copy.
  &nbsp;·&nbsp;
  <a href="?format=md" style="color: var(--emerald-700);">View as Markdown</a>
</div>
{body}
<div class="footer">Generated by intelia · Lineage &amp; Usage Agents · run {escape(run.id)}</div>
</body>
</html>"""


def _markdown_to_html(md: str) -> str:
    """Lightweight Markdown → HTML for our own subset.

    Supports: # h1, ## h2, ### h3, **bold**, `code`, > blockquote, - list, |table|.
    """
    out: list[str] = []
    lines = md.split("\n")
    i = 0
    in_list = False
    in_table = False

    def close_blocks():
        nonlocal in_list, in_table
        if in_list:
            out.append("</ul>")
            in_list = False
        if in_table:
            out.append("</tbody></table>")
            in_table = False

    while i < len(lines):
        line = lines[i]
        stripped = line.rstrip()

        # Tables
        if "|" in stripped and stripped.startswith("|"):
            cells = [c.strip() for c in stripped.strip("|").split("|")]
            if not in_table:
                close_blocks()
                # Look ahead — header separator?
                if i + 1 < len(lines) and "---" in lines[i + 1]:
                    out.append('<table><thead><tr>' + "".join(f"<th>{_inline(c)}</th>" for c in cells) + "</tr></thead><tbody>")
                    in_table = True
                    i += 2
                    continue
            else:
                out.append("<tr>" + "".join(f"<td>{_inline(c)}</td>" for c in cells) + "</tr>")
                i += 1
                continue
        elif in_table:
            close_blocks()

        if stripped.startswith("# "):
            close_blocks(); out.append(f"<h1>{_inline(stripped[2:])}</h1>")
        elif stripped.startswith("## "):
            close_blocks(); out.append(f"<h2>{_inline(stripped[3:])}</h2>")
        elif stripped.startswith("### "):
            close_blocks(); out.append(f"<h3>{_inline(stripped[4:])}</h3>")
        elif stripped.startswith("> "):
            close_blocks(); out.append(f"<blockquote>{_inline(stripped[2:])}</blockquote>")
        elif stripped.startswith("- "):
            if not in_list:
                close_blocks()
                out.append("<ul>")
                in_list = True
            out.append(f"<li>{_inline(stripped[2:])}</li>")
        elif stripped.startswith("---"):
            close_blocks(); out.append("<hr>")
        elif stripped == "":
            close_blocks()
        else:
            close_blocks()
            out.append(f"<p>{_inline(stripped)}</p>")
        i += 1
    close_blocks()
    return "\n".join(out)


def _inline(text: str) -> str:
    """Apply inline markdown — bold, code — to a snippet of HTML-safe text."""
    text = escape(text)
    # Code spans first
    out: list[str] = []
    rest = text
    while True:
        a = rest.find("`")
        if a < 0:
            out.append(rest)
            break
        b = rest.find("`", a + 1)
        if b < 0:
            out.append(rest)
            break
        out.append(rest[:a])
        out.append(f"<code>{rest[a+1:b]}</code>")
        rest = rest[b + 1:]
    text = "".join(out)
    # Bold
    parts = text.split("**")
    if len(parts) > 1:
        text = ""
        for idx, seg in enumerate(parts):
            text += (f"<strong>{seg}</strong>" if idx % 2 == 1 else seg)
    return text
