# Tuesday Talking Cards — Insignia Demo

One card per stage. Each card has: **the line**, **what to show**, **if asked**.
Optimised to scan from a second monitor while you present.

---

## Card 1 · Opening (60–90 seconds)

**The line:**
"Migrating an Oracle warehouse to BigQuery normally takes 6–18 months — most of it spent on manual pipeline rewriting, lineage tracing, and figuring out which jobs are still in use. We built a multi-agent system that analyses the source end-to-end, generates a deployable Dataform project, and verifies the data matches. Today I'll run it live against a synthetic super-fund warehouse."

**Beats:**
1. Problem: slow, risky, expensive
2. Existing tools either translate OR validate OR draw lineage — never all three
3. Our angle: six agents that share context, one workflow

**Don't say:** "100% automated" — say "automated translation with confidence scoring."

---

## Card 2 · Setup screen (30 seconds)

**The line:**
"Two inputs: an Oracle connection and a GCS bucket with the ETL XML pipelines, DDL, and audit log extracts. I've prefilled the demo defaults — host, credentials, bucket prefix."

**What to show:**
- The "New analysis" page
- Point to the agent toggles on the right — six agents, all on by default
- Click **Start**

**If asked "how secure is the credentials handling":**
- Stored in memory only for the duration of the run, not persisted to Firestore
- Verification re-prompts for credentials if Cloud Run restarted (we saw it happen during the build itself)

---

## Card 3 · Live run / streaming events

**The line:**
"Each agent emits status events to the UI as it works. The orchestrator runs them in sequence: inventory first because the others depend on it, then lineage and usage in parallel, then summary."

**What to show:**
- The streaming log on the live-run screen
- Watch for: "Live introspection: schema X · N tables · M pipelines in audit log"
- The progress rail at the top

**If asked "what model":**
- Inventory + lineage + usage: deterministic Python (sqlglot, etree). No LLM.
- Summary: Gemini 2.5 Pro (us-central1)
- Procedure fallback: Gemini 2.5 Pro for PL/SQL we can't decompose

---

## Card 4 · Inventory tab

**The line:**
"Live introspection. Every table, view, column, foreign key, and stored procedure from Oracle, plus every ETL pipeline parsed from the XML files in the bucket. The agent also reads the audit log to tell us which pipelines actually run and how often."

**What to show:**
- Switch to the **Inventory** tab
- Scroll the table list (~30 objects classified by layer: raw/staging/integration/reporting)
- Open one row, show columns + PII annotations + comments

**Key callout — pipelines with no XML:**
- "Notice these — `regulatory_audit_compliance`, `stg_accounts`, `core_account_summary`. They ran in production but have no XML documentation. The agent surfaces them so you don't get blindsided mid-cutover. In a real Insignia warehouse, this is where decommission candidates and undocumented compliance jobs would show up."

**If asked "how do you classify the layer":**
- Heuristic on naming conventions + write-only-vs-read patterns from usage stats

---

## Card 5 · Lineage tab

**The line:**
"Column-level lineage. Every column you see traces back through the pipeline steps to the original Oracle source. Drawn deterministically from the parsed XML — no LLM."

**What to show:**
- Switch to **Lineage** tab
- Cytoscape graph with nodes by layer (colour-coded)
- Click a node — show focus mode (only paths through that node)
- Toggle "expand pipeline internals" to show the step-by-step DAG

**If asked "what about lineage for PL/SQL procedures":**
- Two paths: (1) sqlglot parses the procedure body directly when it's deterministic SQL, (2) Gemini falls back for PL/SQL with complex control flow. Procedure-derived edges are tagged with a lower confidence.

---

## Card 6 · Usage tab

**The line:**
"Pipeline run history joined onto the lineage. Hot tables, dead objects, write-only orphans, undocumented executions, success rates. Pulled from `etl_execution_logs` plus AWR if you have it."

**What to show:**
- Switch to **Usage** tab
- Top reads chart (hot tables)
- Failing-pipeline pill if any
- Dead objects count

**Demo line:**
"This tells you what to actually migrate first and what to decommission. You wouldn't want to spend three weeks porting a pipeline nobody's read since 2024."

---

## Card 7 · Executive Summary

**The line:**
"Gemini synthesises the findings across all three agents into a headline, bullet findings, and recommendations. This is what an executive sees."

**What to show:**
- The Summary tab
- The headline + 3-5 findings
- The recommendations list (decommission candidates, migration phases)

**If asked "is this hallucinated":**
- The findings reference object names and counts pulled from the deterministic inventory/lineage/usage — it's synthesis, not invention. Numbers are auditable.

---

## Card 8 · Transformation — THE BIG MOMENT (3–4 minutes)

This is the deepest stage. Spend the time.

**The line:**
"Now the agent takes every Oracle pipeline definition and translates it to a deployable Dataform project — proper `${ref()}` syntax, source declarations, post-load operations, layer tags, confidence scoring per file."

**What to show — top-to-bottom flow:**

1. **The generated project tree** — `definitions/sources.sqlx`, `definitions/<pipeline>.sqlx`, `workflow_settings.yaml`
2. **Click a primary SQLX file** — show the CTE structure
3. **Confidence badges** — "the agent flags files <70 as review queue"
4. **Click an original XML** in split view — "this is what we started from"
5. **Push to GitHub** — pre-pushed earlier, but show the button

**If they want the technical depth — open the source:**
- `backend/app/parsers/etl_xml.py` → "this is the parser, 575 lines, deterministic, no LLM"
- `transformation-core/transformation_core/ir.py` → "this is the IR — 11 node types, immutable, source-agnostic"
- `backend/app/transformer/insignia_to_ir.py` → "the bridge: parsed pipeline → IR nodes"
- `backend/app/transformer/sql_helpers.py` → "Oracle→BigQuery dialect patches — SYSDATE, TRUNC, date arithmetic"

**Key soundbite:**
"The IR is the bit that makes this multi-source. The parser is per-source; the IR is universal; the emitter is BigQuery-specific. We pay the cost once."

---

## Card 9 · Multi-source breadth (only if asked)

**The line:**
"This isn't a one-trick parser. We have parsers for Insignia ETL XML, SQL Server SSIS (`.dtsx`), Teradata BTEQ scripts, and Snowflake Scripting + Tasks. All four emit the same IR."

**What to show — open in IDE side by side:**
- `backend/app/parsers/etl_xml.py` — the reference implementation (Insignia)
- `backend/app/parsers/ssis_xml.py` — DTSX, namespace-aware, `componentClassID` dispatch
- `backend/app/parsers/teradata_bteq.py` — dual-grammar lexer (dot-commands + SQL)
- `backend/app/parsers/snowflake_pipeline.py` — STREAMs + TASKs DAG + Procedures

**Each parser has a top-of-file section: "Differences vs Insignia ETL XML parser."** That's the soundbite — scroll up to it.

**Sample inputs** in `demo-data/multi-source/{ssis,teradata,snowflake}/` if they want to see actual source files.

**If asked "have you run these in production":**
- "These are demo-grade — full coverage of the most common constructs in each system. The same architecture (parser → IR → emitter) scales to production; we'd harden each parser as the engagement scope demands."

---

## Card 10 · Deploy + Verify (the proof)

**The line:**
"The generated Dataform project compiles cleanly and runs in BigQuery. Then the verification agent compares every migrated table side-by-side against the original Oracle source."

**What to show:**
- The Deploy stage checklist (5 manual steps — GitHub, workspace pull, compile, replicate raw, execute)
- All five ticked
- Switch to **Verify** tab
- Summary cards: 6 Match · 0 Drift · 10 BQ output · 0 Missing · 0 Skipped · 0 Error
- Expand `migration_raw.transactions` — 63,544 rows on both sides, per-column sum/min/max aligned

**The killer line:**
"Six source tables: every row, every sum, every distinct count matches Oracle exactly. Ten output tables: produced by the migrated pipelines as BigQuery outputs — in the Oracle world these were CSV files going to downstream systems, in the BigQuery world they're proper tables."

**If asked "why are 10 missing in Oracle":**
- Original Insignia ETL ended with `<load type="csv" path="...">` writing flat files to downstream consumers (Services Australia, ATO, etc.). Dataform writes tables, not files. Same data, different artifact.

**If asked "does verification take long":**
- 60-90 seconds for this dataset. Scales with row count; uses exact `COUNT(DISTINCT)` to avoid HLL drift.

---

## Card 11 · Closing (60 seconds)

**The line:**
"What we just did in five minutes — analyse, plan, generate, deploy, verify — is what a team of two senior data engineers typically does in two to four weeks for a single warehouse this size. We're not replacing the engineers; we're giving them a confidence-scored starting point and validation framework so they can focus on the 5% that needs human judgement."

**Three takeaways:**
1. **Deterministic where it matters** — parser, IR, emitter, verification: all reproducible
2. **LLM where it adds value** — executive synthesis and PL/SQL fallback
3. **Source-agnostic IR** — Oracle today, SSIS / Teradata / Snowflake the same architecture

**Call to action:** "If you want us to run this against a slice of your real warehouse this week, I can have it set up by Wednesday."

---

## Q&A Cheat Sheet

| Question | Crisp answer |
|---|---|
| "How accurate is the translation?" | Per-file confidence scoring (0–100). ≥90 = ship; 70–89 = review; <70 = manual. Validation pass checks every `${ref()}` resolves and SQL parses. |
| "What if the pipeline format is something you haven't seen?" | New source = ~300 lines of parser + ~200 lines of IR bridge. We did SSIS/Teradata/Snowflake in a day. The IR doesn't change. |
| "Do you handle stored procedures?" | Yes — `procedure_converter.py`. PL/SQL → BigQuery procedural SQL via Gemini. Confidence-scored lower because of LLM variance; flagged for review. |
| "How do you know nothing got lost?" | Verification agent. Per-column null counts, distinct counts (exact, not HLL), sum/min/max for numerics. Row-level if needed. |
| "What about data types?" | sqlglot handles the common Oracle→BigQuery mappings. Edge cases (Oracle TIMESTAMP precision, BINARY_DOUBLE) are flagged in validation. |
| "Can we edit the output?" | Yes. It's a normal Dataform project — clone, edit, PR. We don't lock you into anything. |
| "What about Informatica?" | Same architecture, new parser. We picked Insignia's XML format for the demo because it's representative — Informatica adds a richer step model but the IR absorbs that. |
| "What's the LLM bill?" | Gemini 2.5 Pro for summary (~5 calls per run) + Gemini 2.5 Pro for procedures we can't decompose deterministically (variable). For this demo: under $0.50. |
| "How does this run in production?" | Cloud Run for the API + frontend, Firestore for run state, GCS for artifacts. Customer brings their own GCP project. |
| "What if our warehouse is 10,000 tables?" | Inventory + lineage are O(N) and we've designed for it. Verification scales with table size; for huge tables we cap aggregates. Transformation is per-pipeline so parallel. |
| "Why six agents not one big LLM?" | Determinism + auditability + cost. The hard parts (parsing, IR, SQL emission) don't need LLM reasoning. Where we use Gemini, the reasoning is auditable against the deterministic inputs. |
| "What happens if the source changes mid-migration?" | Re-run inventory. Lineage and transformation regenerate. Verification re-runs to catch drift. This is designed to be run repeatedly. |

---

## Things to keep in mind on the day

- **Don't promise data parity** — say "verification flags drift" not "no drift."
- **Don't say "100% automated"** — say "automated translation, confidence-scored, review queue for the hard cases."
- **If something breaks** — point at the verification tab. "This is exactly why we have it. Production migrations will hit cases we haven't tested. The validation framework catches them before they ship."
- **If they push on a feature we don't have** — "Not in the demo, but the architecture supports it — would take [N days/weeks]." Don't fake.
- **If asked "is this open source"** — depends on what was decided. Default answer: "It's a working prototype. The architecture is what matters more than the code right now."

---

## Backup if the live demo breaks

If Cloud Run is sluggish or anything 5xxs:
1. Open the GitHub repo — show the generated SQLX files directly
2. Open `docs/TUESDAY_TECHNICAL_GUIDE.md` — walk the architecture diagram
3. Open `backend/app/parsers/etl_xml.py` and the IR — show the code that runs
4. Bring up the verification report JSON from the last successful run in GCS

Recovery line: *"Live demos are honest about what's real — this is built on Cloud Run and depends on the network, same as any production system. Let me show you the artifacts instead."*
