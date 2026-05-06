# Demo data — Insignia synthetic warehouse

Pretends to be a wealth-management warehouse stitched together from MLC, IOOF, ANZ Wealth and Pendal acquisitions. Includes deliberate legacy sprawl so the agents have something interesting to find.

## Layers (Oracle schemas)

| Schema | Layer | Purpose |
|---|---|---|
| `WH_RAW` | raw | Verbatim source extracts from each acquired platform |
| `WH_STG` | staging | Cleansed, conformed, deduped |
| `WH_DW`  | integration | Dimensional model — DIM_*, FACT_* |
| `WH_RPT` | reporting | Aggregates feeding Power BI / Cognos |
| `WH_LEGACY` | unknown | Orphans we deliberately included for the demo to discover |

## Files

- `ddl/01_schemas.sql` — schema/user creates
- `ddl/02_raw_tables.sql` — RAW layer
- `ddl/03_staging_tables.sql` — STG layer
- `ddl/04_integration_tables.sql` — DIM/FACT layer
- `ddl/05_reporting_tables.sql` — RPT layer
- `ddl/06_views.sql` — view + MVIEW source SQL (lineage agent reads this)
- `ddl/07_legacy_orphans.sql` — orphan tables left over from acquisitions
- `dictionary/all_tab_columns.csv` — data-dictionary export
- `dictionary/all_dependencies.csv` — explicit dependency graph (Oracle's view of it)
- `dictionary/dba_segments.csv` — row counts and bytes
- `awr/dba_hist_sqlstat.csv` — query history
- `procs/pkg_*.pls` — PL/SQL packages with the ETL logic

## Story the agents should produce

1. **247 objects across 4 layers** (give-or-take depending on final count) — ~25 raw, 35 staging, 40 integration, 20 reporting, 18 legacy/orphan.
2. **Lineage**: ~600 column-level edges. Most raw → staging → integration → reporting paths trace cleanly.
3. **Usage**:
   - Hot tables: `RPT_MEMBER_DASHBOARD`, `RPT_ADVISER_BOOK_OF_BUSINESS`, `FACT_HOLDING_DAILY`
   - Write-only orphans: `STG_MEMBER_MLC_ORPHAN`, `STG_PORTFOLIO_REBALANCE_TEMP`, `RAW_TAX_FILING_TEMP`
   - Dead objects: `WH_LEGACY.*`, `STG_CASE_NOTES_LEGACY`
   - Reporting unreachable: `RAW_CASE_NOTES_LEGACY`, `RAW_PENDAL_AUDIT_FEED`
4. **Findings** the summary agent should produce:
   - **Critical** — Three MLC-era staging tables write nightly but nothing reads them (15GB of wasted storage)
   - **Critical** — `WH_LEGACY.PKG_PORTFOLIO_REBALANCE_OLD` package compiled but no executions in 180 days
   - **Warn** — 7 raw tables don't reach the reporting layer (orphan ingestion paths)
   - **Warn** — `FACT_FEE` is in integration layer but only used by 1 reporting view (review necessity)
   - **Info** — Adviser dimension has duplicate logic in staging and integration (consolidation opportunity)
