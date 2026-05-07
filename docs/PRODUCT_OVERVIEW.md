# Lineage & Usage Agents — Product Overview

_For the Insignia Financial sales meeting, Tuesday._

## One-liner

A multi-agent application that analyses an Oracle data warehouse end-to-end and
produces a deployable Dataform project for BigQuery — with lineage, usage,
ROI estimates, and post-migration validation built in.

## The problem we're solving

Migration from a legacy Oracle warehouse to a cloud platform like BigQuery is
typically:

- **Slow** — 6 to 18 months for a mid-sized estate, mostly spent on manual
  pipeline rewriting, lineage tracing, and decommission analysis.
- **Risky** — pipelines run undocumented, tables are written nightly but
  nobody reads them, dependencies aren't actually known, business logic
  hides in stored procedures.
- **Expensive** — senior data engineers at AUD 1,500–2,000/day, plus the
  business cost of running both warehouses in parallel during the migration.

Existing tools are either deep but narrow (Datafold validates parity but
doesn't translate; Manta does lineage but no migration; AWS SCT translates
schema but not pipelines) or all-in-one consultancy services that scale
linearly with engineer-hours.

## The solution: six agents, one workflow

The application orchestrates six specialist AI agents that share context.
They run in sequence; each later agent reads the earlier ones' output:

| # | Agent | What it does |
|---|---|---|
| 1 | **Inventory** | Live Oracle introspection — every table, view, column, FK, stored procedure, audit-log run history, and ETL pipeline definition (parsed from XML). |
| 2 | **Lineage** | Column-level lineage graph — sources → ETL steps → outputs. Draws an interactive chart with focus filtering and pipeline-internals expansion. |
| 3 | **Usage** | Pipeline run history, success rates, hot-table detection, dead-object detection, undocumented-execution flagging. |
| 4 | **Summary** | Gemini 2.5 Pro synthesises the headline, bullet findings, and recommendations across the three above. |
| 5 | **Transformation** | Translates every Oracle pipeline + procedure into a deployable Dataform SQLX project — proper `${ref()}` syntax, source declarations, post-load operations, assertions, layer tags, confidence scoring per file. |
| 6 | **Orchestration** | Emits a `.github/workflows/dataform.yaml` so the project compiles on every push and runs on a daily schedule once committed to GitHub. |

Each agent runs as a separate stage with streaming progress shown to the
user. Toggle any of them on or off in the picker on the New Analysis page.

## What you get out

For a fully-run analysis, the user ends up with:

1. **Inventory tab** — searchable grid of every database object with PII
   classification, layer (raw / staging / core / reporting), and column-level
   annotations.
2. **Lineage tab** — interactive column-level lineage chart. Click any
   pipeline to focus the chain; toggle "Pipeline internals" to see the full
   step DAG inside that pipeline. Export as CSV or PNG.
3. **Usage tab** — pipeline run-stats, hot tables, dead objects,
   undocumented executions. Filter chips for each category.
4. **Migration tab** — decommission readiness per object, sequencing waves,
   multi-writer pattern detection, data parity preview, scope export.
5. **Transform tab** — file tree of the generated Dataform project, side-by-side
   view of original Oracle source ↔ generated SQLX, confidence pill per file,
   review queue filter, "Push to GitHub" button (PR mode + force-push mode).
6. **Ask tab** — conversational interface backed by Gemini 2.5 Pro grounded in
   the run's results. Example: "Which pipelines write to FACT_REGULATORY_AUDIT?"
   gets a structured, sourced answer.
7. **Findings tab** — every critical/warn finding from the summary agent.
8. **Overview tab** — exec-friendly metrics: tables, edges, reporting reach,
   findings count, Migration ROI panel (manual vs accelerated weeks/cost),
   BigQuery cost projection.

The generated Dataform project is downloadable as a zip or pushable to a
GitHub repo (with the orchestration workflow YAML included).

## End-to-end user flow

1. **New Analysis page** — the user enters the Oracle connection details and
   the GCS bucket containing the pipeline XMLs. Picks which agents to run.
   Hits Run.
2. **Live run page** — streaming progress per agent (inventory → lineage →
   usage → summary → transformation → orchestration). Each agent emits
   "thinking" events the user can read in real time.
3. **Results page** — eight tabs land populated with the agent outputs.
   Most demo time is spent on Lineage, Transform, and Ask.
4. **Push to GitHub** — opens a PR with the executive summary as the
   description; CI compiles immediately via the orchestration workflow.

Total wall-clock for a typical Insignia-sized run: 3–5 minutes.

## Why this is different

| Tool | What they do | What we do that they don't |
|---|---|---|
| **Datafold** | Post-migration data-parity validation (their moat). Best-in-class data diff. | We translate the SQL itself. Datafold validates after you've hand-ported. We hand you the port. (Parity preview tab is on roadmap.) |
| **Manta / IBM** | Deep cross-system lineage. | We act on the lineage — generate code, score risk, drive ROI estimates. |
| **AWS SCT / Google BQ Migration Service** | Schema-level translation, sometimes SQL. | They don't parse ETL pipelines or run the agents in concert. No lineage view, no usage, no chat, no Dataform output. |
| **Dataform orchestrator** (the GCP service) | Compiles, schedules, and runs an existing SQLX project. | We _produce_ the SQLX project that Dataform then orchestrates. Dataform itself assumes the project already exists. |
| **Consultancy** | Manual port, project-managed. | 6× faster for the translation-heavy phase, with the migration playbook (sequencing waves, decommission scoring) computed automatically. |

The combination of multi-agent analysis _plus_ deterministic SQLX generation
_plus_ the ability to push a PR from the same UI is the differentiator. No
other tool does all three.

## Recommended demo script (≈18 min)

1. **Open New Analysis page** (1 min) — point out the six agents in the right
   panel, mention each is independent, hit Run with all six on.
2. **Live run** (2 min) — let the user watch agents stream. Skip ahead if
   needed by opening a pre-baked completed run.
3. **Overview tab** (2 min) — start with the ROI panel ("manual: 24 weeks,
   accelerated: 6 weeks, AUD 540k saved") and the BigQuery cost projection.
   This frames the rest of the demo for execs.
4. **Lineage tab** (3 min) — focus on one complex pipeline
   (`regulatory_audit_compliance`), toggle Pipeline internals to expand the
   step DAG. Mention column-level provenance.
5. **Migration tab** (2 min) — decommission readiness table + sequencing
   waves. Point at the parity preview card.
6. **Transform tab** (4 min) — open `core_account_summary.sqlx` in split
   view to show original Oracle ↔ generated SQLX side-by-side. Note the
   confidence pill, layer tag, assertions. Click "Push to GitHub" → "Open as
   pull request" → show the resulting PR in GitHub.
7. **Ask tab** (3 min) — ask three questions live: "Which pipelines write to
   FACT_REGULATORY_AUDIT?", "What's the most complex pipeline?", "Summarise
   the critical findings in 50 words." The audience can suggest one too.
8. **Wrap** (1 min) — recap the four-quadrant ROI: time saved, cost saved,
   risk reduced, quality improved.

## Tech stack

- **Backend** — FastAPI (Python 3.12) on Cloud Run, australia-southeast1
- **Agents** — Vertex AI: Gemini 2.5 Flash for inventory annotations and
  rule extraction; Gemini 2.5 Pro for summary, chat, and PL/SQL conversion
- **Lineage / SQL parsing** — sqlglot, deterministic IR
- **SQLX generation** — `transformation-core` library lifted from the
  Crown Transformation Agent (8K LOC, source-system agnostic)
- **Frontend** — Next.js 16 + Tailwind 4, hosted on Cloud Run
- **Storage** — GCS for results, Firestore for run state
- **Source systems supported** — Oracle (live introspection); ETL XMLs in
  the format Direnc's demo uses; future: Informatica via the existing TA
  parser
- **Target stack** — BigQuery + Dataform (could extend to Snowflake +
  dbt with adapter changes)

## What's not built (post-Tuesday roadmap)

- **Real data parity validation** — the Datafold-style row-count + value-level
  diff between Oracle source and BQ target. Today we show the preview UI; the
  diff itself runs once BQ has data.
- **DDL bridge** — generated SQLX has column names but not column types.
  Wiring Oracle types into the IR's `bq_data_type` field is straightforward
  but wasn't a Tuesday-blocker.
- **More source-system parsers** — Informatica is supported via the
  Transformation Agent's existing parser; adding SSIS, DataStage, Talend
  would expand the addressable estate.
- **Dataform compile validation** — we run Python-side validation today
  (refs resolve, SQL parses, no cycles, structural rules). Layering the
  actual `dataform compile` CLI in Docker would catch the rest.

---

## Key claims you can put on slides

- **6 specialist agents** running in concert, sharing context.
- **6× faster** than a manual migration for the translation-heavy phase
  (industry rule-of-thumb: 7 days/pipeline manual vs ~0.5 days/pipeline
  reviewed).
- **Confidence-scored output** — every generated file gets a score so
  reviewers know where to focus.
- **Deployable, not advisory** — the output is a complete Dataform project
  that compiles and runs in BigQuery on day one.
- **Datafold validates after you migrate. We migrate, then validate.**

## Target audience for Tuesday's pitch

- **Project Indigo** — Insignia's MasterKey / P&I / Plum migration to
  SS&C Bluedoor.
- **Engineering managers** care about: confidence scores, validation,
  CI/CD integration, reproducibility.
- **Migration architects** care about: sequencing waves, decommission
  readiness, multi-writer detection.
- **Execs** care about: ROI panel, cost projection, time-to-cutover.

---

_Generated by intelia — questions to Daniel Zillmann._
