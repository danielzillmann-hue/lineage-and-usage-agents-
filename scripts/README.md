# Demo support scripts

## `load_raw_layer.py`

One-shot replicator from the demo Oracle box → BigQuery raw layer. Run
once before the Tuesday end-to-end demo so the generated Dataform
pipelines have source data to read.

### Setup

```bash
# 1. Install deps into a venv (or your backend venv)
python -m venv .venv-scripts
source .venv-scripts/bin/activate     # on Windows: .venv-scripts\Scripts\activate
pip install -r scripts/requirements.txt

# 2. Auth to GCP (BigQuery uses Application Default Credentials)
gcloud auth application-default login
```

### Run

The defaults match the demo — Direnc's Oracle host (`35.201.6.195`),
`transformation-agent-demo.migration_raw` target, every table the demo pipelines read
from. So you can usually just:

```bash
python scripts/load_raw_layer.py
```

Common options:

```bash
# Cap rows per table (faster sanity run)
python scripts/load_raw_layer.py --limit 5000

# Replicate a specific subset
python scripts/load_raw_layer.py --tables accounts,members,transactions

# Append instead of replacing
python scripts/load_raw_layer.py --if-exists append

# Override Oracle / BQ targets
python scripts/load_raw_layer.py \
  --oracle-host 35.201.6.195 \
  --bq-project transformation-agent-demo --bq-dataset migration_raw
```

### What it does

1. Connects to Oracle and to BigQuery (ADC).
2. Creates the BQ dataset if it doesn't exist.
3. For each table:
   - `SELECT * FROM <table>` (with optional `FETCH FIRST N ROWS ONLY`).
   - Reads cursor metadata, derives a BQ schema (Oracle types → BQ types
     using the same rules as the transformer's raw-bootstrap module).
   - Loads rows via `google.cloud.bigquery.load_table_from_dataframe`.
4. Prints a summary table with per-table row counts + status.

Failures don't abort the batch — each table reports its own status, and
the script exits non-zero if any failed.

### Default table list

Aligned to what the agents' generated Dataform project references via
`${ref()}`:

- `accounts`, `account_types`, `account_investments`
- `members`, `member_addresses`
- `transactions`
- `investment_options`
- `vw_member_risk_profile` (view; replicated as a snapshot table)

`tax_brackets` and `market_benchmarks` are CSV-only inputs (no Oracle
counterpart) — the Dataform project emits stub source declarations for
them, so `${ref()}` resolves without an Oracle copy.

Override with `--tables` if the demo bucket adds or renames anything.
