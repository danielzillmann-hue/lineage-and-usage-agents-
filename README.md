# Lineage and Usage Agents

Multi-agent analyzer for Oracle data warehouses. Reads DDL, data-dictionary exports, and AWR/V$SQL extracts from a GCS bucket and produces:

- **Inventory** — structured schema model with layer (raw / staging / integration / reporting) and domain classification
- **Lineage** — table- and column-level DAG, deterministic from views/CTAS via sqlglot, LLM-augmented for PL/SQL procedures
- **Usage** — query-frequency analysis joined onto the lineage graph: hot tables, write-only orphans, reporting-layer reachability

Built for the Insignia Financial sales pitch (2026-05-12). FastAPI + Vertex Gemini on Cloud Run, Next.js frontend.

## Layout

```
backend/    FastAPI service, three agents, GCS reader, sqlglot lineage core
frontend/   Next.js 15 + Tailwind + shadcn/ui
infra/      Cloud Build + Cloud Run deployment
demo-data/  Synthetic Oracle wealth-management warehouse for the demo
docs/       Architecture notes, agent prompts, schema design
```

## GCP

- Project: `dan-sandpit`
- Account: `daniel.zillmann@intelia.com.au`
- Region: `australia-southeast1`

Use inline flags — do not change the global gcloud account.
