"""Generate the synthetic data-dictionary and AWR CSVs for the demo.

Run from repo root:
    python demo-data/generate_demo_data.py

Produces:
    demo-data/dictionary/dba_segments.csv         row counts + bytes per object
    demo-data/dictionary/all_dependencies.csv     Oracle's view of dependencies
    demo-data/awr/dba_hist_sqlstat.csv            ~300 representative queries with execution counts
"""

from __future__ import annotations

import csv
import random
from datetime import datetime, timedelta
from pathlib import Path

random.seed(42)

ROOT = Path(__file__).parent
(ROOT / "dictionary").mkdir(exist_ok=True)
(ROOT / "awr").mkdir(exist_ok=True)


# Tables with intended sizes — proportional to layer.
TABLES: list[tuple[str, int, int]] = [
    # (FQN, row_count, MB)
    # ─── RAW ───
    ("WH_RAW.RAW_MEMBER_MLC",          1_850_000,  340),
    ("WH_RAW.RAW_MEMBER_IOOF",         1_120_000,  220),
    ("WH_RAW.RAW_MEMBER_ANZW",           890_000,  170),
    ("WH_RAW.RAW_MEMBER_PNDL",           420_000,   85),
    ("WH_RAW.RAW_ACCOUNT_MLC",         2_400_000,  390),
    ("WH_RAW.RAW_ACCOUNT_IOOF",        1_320_000,  210),
    ("WH_RAW.RAW_ACCOUNT_ANZW",        1_100_000,  180),
    ("WH_RAW.RAW_ACCOUNT_PNDL",          560_000,   95),
    ("WH_RAW.RAW_TRANSACTION_FEED",   42_300_000, 6_800),
    ("WH_RAW.RAW_HOLDING_FEED",       18_900_000, 2_100),
    ("WH_RAW.RAW_FEE_FEED",            6_400_000,   820),
    ("WH_RAW.RAW_ADVISER_MLC",            12_400,    4),
    ("WH_RAW.RAW_ADVISER_IOOF",            8_900,    3),
    ("WH_RAW.RAW_PRODUCT_CATALOG",         2_240,    1),
    ("WH_RAW.RAW_FUND_PRICES",         1_280_000,  140),
    ("WH_RAW.RAW_MEMBER_ADVISER_LINK",   980_000,   88),
    ("WH_RAW.RAW_PENDAL_AUDIT_FEED",     345_000, 1_230),
    ("WH_RAW.RAW_TAX_FILING_TEMP",        78_000,  340),
    ("WH_RAW.RAW_CASE_NOTES_LEGACY",   2_140_000, 5_400),
    # ─── STG ───
    ("WH_STG.STG_MEMBER",              4_280_000,  610),
    ("WH_STG.STG_ACCOUNT",             5_380_000,  720),
    ("WH_STG.STG_TRANSACTION",        42_300_000, 5_900),
    ("WH_STG.STG_HOLDING",            18_900_000, 1_900),
    ("WH_STG.STG_FEE",                 6_400_000,   780),
    ("WH_STG.STG_ADVISER",                21_300,    8),
    ("WH_STG.STG_PRODUCT",                 2_240,    1),
    ("WH_STG.STG_MEMBER_ADVISER_LINK",   980_000,   84),
    ("WH_STG.STG_FUND_PRICE",          1_280_000,  130),
    ("WH_STG.STG_MEMBER_MLC_ORPHAN",   1_850_000, 4_800),
    ("WH_STG.STG_PORTFOLIO_REBALANCE_TEMP", 5_380_000, 9_200),
    ("WH_STG.STG_CASE_NOTES_LEGACY",   2_140_000, 5_300),
    ("WH_STG.STG_ADVISER_DUP_LEGACY",     12_400,    9),
    # ─── DW ───
    ("WH_DW.DIM_DATE",                    18_300,   2),
    ("WH_DW.DIM_MEMBER",               5_120_000, 980),
    ("WH_DW.DIM_ACCOUNT",              5_380_000, 870),
    ("WH_DW.DIM_PRODUCT",                  2_240,   1),
    ("WH_DW.DIM_ADVISER",                 21_300,   8),
    ("WH_DW.DIM_FUND",                       720,   1),
    ("WH_DW.FACT_TRANSACTION",        42_300_000, 7_200),
    ("WH_DW.FACT_HOLDING_DAILY",   1_245_000_000, 89_000),
    ("WH_DW.FACT_FEE",                 6_400_000,   910),
    ("WH_DW.FACT_ACCOUNT_DAILY_BALANCE", 980_000_000, 56_000),
    ("WH_DW.BRIDGE_MEMBER_ADVISER",      980_000,   72),
    ("WH_DW.INT_MEMBER_360",           4_280_000,  610),
    # ─── RPT ───
    ("WH_RPT.RPT_MEMBER_DASHBOARD",    4_280_000,  580),
    ("WH_RPT.RPT_ADVISER_BOOK_OF_BUSINESS", 21_300, 12),
    ("WH_RPT.RPT_FEE_LEAKAGE",         1_840_000,  220),
    ("WH_RPT.RPT_PORTFOLIO_PERFORMANCE_DAILY", 1_780_000_000, 92_000),
    ("WH_RPT.RPT_FUND_FLOWS_WEEKLY",      37_400,   18),
    ("WH_RPT.RPT_REGULATORY_SUPER_RETURN", 2_880,    1),
    ("WH_RPT.RPT_CHURN_ANALYSIS",      4_280_000,  340),
    ("WH_RPT.RPT_PRODUCT_HOLDINGS_SUMMARY", 720_000, 78),
    # ─── LEGACY ───
    ("WH_LEGACY.LEG_MLC_MEMBER_ARCHIVE", 3_400_000, 720),
    ("WH_LEGACY.LEG_IOOF_FUND_ARCHIVE",      9_400,   3),
    ("WH_LEGACY.LEG_PORTFOLIO_REBALANCE_QUEUE", 12_800_000, 18_000),
    ("WH_LEGACY.LEG_ANZ_CLIENT_OVERRIDES", 245_000,  88),
]

VIEWS = [
    "WH_DW.V_MEMBER_PRIMARY",
    "WH_DW.V_ACCOUNT_CURRENT",
    "WH_DW.V_HOLDING_LATEST",
    "WH_RPT.V_MEMBER_DASHBOARD",
    "WH_RPT.V_ADVISER_BOOK_OF_BUSINESS",
    "WH_RPT.V_PRODUCT_HOLDINGS_SUMMARY",
    "WH_RPT.V_FUND_FLOWS_WEEKLY",
    "WH_STG.V_MEMBER_CONFORMED",
    "WH_LEGACY.V_MLC_MEMBER_PHONE_AUDIT",
]


def write_segments() -> None:
    with (ROOT / "dictionary" / "dba_segments.csv").open("w", newline="") as f:
        w = csv.writer(f)
        w.writerow(["OWNER", "SEGMENT_NAME", "SEGMENT_TYPE", "BYTES", "NUM_ROWS", "LAST_ANALYZED"])
        for fqn, rows, mb in TABLES:
            owner, name = fqn.split(".")
            last = (datetime(2026, 5, 6) - timedelta(days=random.randint(0, 6))).isoformat()
            w.writerow([owner, name, "TABLE", mb * 1_048_576, rows, last])


def write_dependencies() -> None:
    """Hand-curated dependency edges that match the views and likely PL/SQL paths."""
    edges: list[tuple[str, str, str, str]] = [
        # owner, name, referenced_owner, referenced_name
        ("WH_DW", "V_MEMBER_PRIMARY", "WH_DW", "DIM_MEMBER"),
        ("WH_DW", "V_ACCOUNT_CURRENT", "WH_DW", "DIM_ACCOUNT"),
        ("WH_DW", "V_HOLDING_LATEST", "WH_DW", "FACT_HOLDING_DAILY"),
        ("WH_DW", "V_HOLDING_LATEST", "WH_DW", "DIM_DATE"),
        ("WH_RPT", "V_MEMBER_DASHBOARD", "WH_DW", "V_MEMBER_PRIMARY"),
        ("WH_RPT", "V_MEMBER_DASHBOARD", "WH_DW", "INT_MEMBER_360"),
        ("WH_RPT", "V_MEMBER_DASHBOARD", "WH_DW", "DIM_ADVISER"),
        ("WH_RPT", "V_ADVISER_BOOK_OF_BUSINESS", "WH_DW", "DIM_ADVISER"),
        ("WH_RPT", "V_ADVISER_BOOK_OF_BUSINESS", "WH_DW", "BRIDGE_MEMBER_ADVISER"),
        ("WH_RPT", "V_ADVISER_BOOK_OF_BUSINESS", "WH_DW", "INT_MEMBER_360"),
        ("WH_RPT", "V_ADVISER_BOOK_OF_BUSINESS", "WH_DW", "V_ACCOUNT_CURRENT"),
        ("WH_RPT", "V_PRODUCT_HOLDINGS_SUMMARY", "WH_DW", "FACT_HOLDING_DAILY"),
        ("WH_RPT", "V_PRODUCT_HOLDINGS_SUMMARY", "WH_DW", "DIM_ACCOUNT"),
        ("WH_RPT", "V_PRODUCT_HOLDINGS_SUMMARY", "WH_DW", "DIM_PRODUCT"),
        ("WH_RPT", "V_FUND_FLOWS_WEEKLY", "WH_DW", "FACT_TRANSACTION"),
        ("WH_RPT", "V_FUND_FLOWS_WEEKLY", "WH_DW", "DIM_DATE"),
        ("WH_STG", "V_MEMBER_CONFORMED", "WH_RAW", "RAW_MEMBER_MLC"),
        ("WH_STG", "V_MEMBER_CONFORMED", "WH_RAW", "RAW_MEMBER_IOOF"),
        ("WH_STG", "V_MEMBER_CONFORMED", "WH_RAW", "RAW_MEMBER_ANZW"),
        ("WH_STG", "V_MEMBER_CONFORMED", "WH_RAW", "RAW_MEMBER_PNDL"),
        ("WH_LEGACY", "V_MLC_MEMBER_PHONE_AUDIT", "WH_LEGACY", "LEG_MLC_MEMBER_ARCHIVE"),
        ("WH_LEGACY", "V_MLC_MEMBER_PHONE_AUDIT", "WH_RAW", "RAW_MEMBER_MLC"),
        # PL/SQL load packages (PROCEDURE-level deps)
        ("WH_DW", "PKG_MEMBER_LOAD", "WH_STG", "STG_MEMBER"),
        ("WH_DW", "PKG_MEMBER_LOAD", "WH_DW", "DIM_MEMBER"),
        ("WH_DW", "PKG_MEMBER_LOAD", "WH_DW", "INT_MEMBER_360"),
        ("WH_DW", "PKG_MEMBER_LOAD", "WH_DW", "DIM_ACCOUNT"),
        ("WH_DW", "PKG_MEMBER_LOAD", "WH_DW", "FACT_HOLDING_DAILY"),
        ("WH_DW", "PKG_MEMBER_LOAD", "WH_DW", "FACT_TRANSACTION"),
        ("WH_DW", "PKG_MEMBER_LOAD", "WH_DW", "FACT_FEE"),
        ("WH_DW", "PKG_MEMBER_LOAD", "WH_DW", "BRIDGE_MEMBER_ADVISER"),
        ("WH_DW", "PKG_MEMBER_LOAD", "WH_DW", "DIM_ADVISER"),
        ("WH_DW", "PKG_MEMBER_LOAD", "WH_RPT", "RPT_MEMBER_DASHBOARD"),
        ("WH_DW", "PKG_HOLDINGS_DAILY", "WH_STG", "STG_HOLDING"),
        ("WH_DW", "PKG_HOLDINGS_DAILY", "WH_DW", "DIM_ACCOUNT"),
        ("WH_DW", "PKG_HOLDINGS_DAILY", "WH_DW", "DIM_FUND"),
        ("WH_DW", "PKG_HOLDINGS_DAILY", "WH_DW", "FACT_HOLDING_DAILY"),
        ("WH_DW", "PKG_HOLDINGS_DAILY", "WH_DW", "FACT_ACCOUNT_DAILY_BALANCE"),
        ("WH_DW", "PKG_HOLDINGS_DAILY", "WH_RPT", "RPT_PORTFOLIO_PERFORMANCE_DAILY"),
        ("WH_LEGACY", "PKG_PORTFOLIO_REBALANCE_OLD", "WH_STG", "STG_ACCOUNT"),
        ("WH_LEGACY", "PKG_PORTFOLIO_REBALANCE_OLD", "WH_LEGACY", "LEG_PORTFOLIO_REBALANCE_QUEUE"),
        ("WH_LEGACY", "PKG_PORTFOLIO_REBALANCE_OLD", "WH_STG", "STG_PORTFOLIO_REBALANCE_TEMP"),
    ]
    with (ROOT / "dictionary" / "all_dependencies.csv").open("w", newline="") as f:
        w = csv.writer(f)
        w.writerow(["OWNER", "NAME", "TYPE", "REFERENCED_OWNER", "REFERENCED_NAME", "REFERENCED_TYPE"])
        for o, n, ro, rn in edges:
            obj_type = "PACKAGE" if n.startswith("PKG_") else "VIEW"
            ref_type = "TABLE" if not rn.startswith(("V_", "PKG_")) else "VIEW"
            w.writerow([o, n, obj_type, ro, rn, ref_type])


def query_for(target: str) -> str:
    schema, name = target.split(".")
    if name.startswith("RPT_") or name.startswith("V_"):
        return f"SELECT * FROM {schema}.{name} WHERE refresh_dt >= :p"
    if name.startswith("FACT_"):
        return f"SELECT date_key, COUNT(*) FROM {schema}.{name} WHERE date_key BETWEEN :s AND :e GROUP BY date_key"
    if name.startswith("DIM_"):
        return f"SELECT * FROM {schema}.{name} WHERE current_flg = 'Y'"
    if name.startswith("STG_"):
        return f"INSERT INTO {schema}.{name} SELECT * FROM ..."
    if name.startswith("RAW_"):
        return f"INSERT INTO {schema}.{name} SELECT * FROM external_feed"
    return f"SELECT * FROM {schema}.{name}"


# Hot, cold and dead patterns — execution counts per object family.
EXEC_COUNTS = {
    # Reporting layer = hot
    "WH_RPT.RPT_MEMBER_DASHBOARD":             182_440,
    "WH_RPT.RPT_ADVISER_BOOK_OF_BUSINESS":     124_780,
    "WH_RPT.RPT_PORTFOLIO_PERFORMANCE_DAILY":   88_120,
    "WH_RPT.RPT_FUND_FLOWS_WEEKLY":             46_240,
    "WH_RPT.RPT_FEE_LEAKAGE":                   31_900,
    "WH_RPT.RPT_CHURN_ANALYSIS":                28_140,
    "WH_RPT.RPT_PRODUCT_HOLDINGS_SUMMARY":      18_770,
    "WH_RPT.RPT_REGULATORY_SUPER_RETURN":          840,  # quarterly
    # Views (also reads downstream)
    "WH_RPT.V_MEMBER_DASHBOARD":               58_310,
    "WH_RPT.V_ADVISER_BOOK_OF_BUSINESS":       42_140,
    "WH_RPT.V_PRODUCT_HOLDINGS_SUMMARY":       19_560,
    "WH_RPT.V_FUND_FLOWS_WEEKLY":               7_240,
    "WH_DW.V_MEMBER_PRIMARY":                  74_680,
    "WH_DW.V_ACCOUNT_CURRENT":                 49_220,
    "WH_DW.V_HOLDING_LATEST":                  21_400,
    # Integration layer = warm reads
    "WH_DW.INT_MEMBER_360":                    32_180,
    "WH_DW.FACT_HOLDING_DAILY":                64_330,
    "WH_DW.FACT_TRANSACTION":                  41_220,
    "WH_DW.FACT_ACCOUNT_DAILY_BALANCE":        38_900,
    "WH_DW.FACT_FEE":                           4_120,
    "WH_DW.DIM_MEMBER":                        19_440,
    "WH_DW.DIM_ACCOUNT":                       17_320,
    "WH_DW.DIM_PRODUCT":                       12_840,
    "WH_DW.DIM_ADVISER":                       11_660,
    "WH_DW.DIM_DATE":                          18_900,
    "WH_DW.DIM_FUND":                           6_280,
    "WH_DW.BRIDGE_MEMBER_ADVISER":              4_400,
    # Staging = mostly writes from ETL, low reads
    "WH_STG.STG_MEMBER":                          184,
    "WH_STG.STG_ACCOUNT":                         142,
    "WH_STG.STG_TRANSACTION":                      96,
    "WH_STG.STG_HOLDING":                         128,
    "WH_STG.STG_FEE":                              74,
    "WH_STG.STG_ADVISER":                          18,
    "WH_STG.STG_PRODUCT":                          14,
    "WH_STG.STG_MEMBER_ADVISER_LINK":              22,
    "WH_STG.STG_FUND_PRICE":                      210,
    "WH_STG.V_MEMBER_CONFORMED":                  340,
    # Raw = pure ingest writes
    "WH_RAW.RAW_MEMBER_MLC":                       28,
    "WH_RAW.RAW_MEMBER_IOOF":                      28,
    "WH_RAW.RAW_MEMBER_ANZW":                      28,
    "WH_RAW.RAW_MEMBER_PNDL":                      28,
    "WH_RAW.RAW_ACCOUNT_MLC":                      24,
    "WH_RAW.RAW_ACCOUNT_IOOF":                     24,
    "WH_RAW.RAW_ACCOUNT_ANZW":                     24,
    "WH_RAW.RAW_ACCOUNT_PNDL":                     24,
    "WH_RAW.RAW_TRANSACTION_FEED":                 30,
    "WH_RAW.RAW_HOLDING_FEED":                     30,
    "WH_RAW.RAW_FEE_FEED":                         30,
    "WH_RAW.RAW_ADVISER_MLC":                       4,
    "WH_RAW.RAW_ADVISER_IOOF":                      4,
    "WH_RAW.RAW_PRODUCT_CATALOG":                   4,
    "WH_RAW.RAW_FUND_PRICES":                      30,
    "WH_RAW.RAW_MEMBER_ADVISER_LINK":               4,
    # Orphans — write-only, never read
    "WH_RAW.RAW_PENDAL_AUDIT_FEED":                30,   # writes only, no reads
    "WH_RAW.RAW_TAX_FILING_TEMP":                   2,   # rarely written, never read
    "WH_RAW.RAW_CASE_NOTES_LEGACY":                 0,   # dead
    "WH_STG.STG_MEMBER_MLC_ORPHAN":                28,   # writes only
    "WH_STG.STG_PORTFOLIO_REBALANCE_TEMP":         28,   # writes only
    "WH_STG.STG_CASE_NOTES_LEGACY":                 0,   # dead
    "WH_STG.STG_ADVISER_DUP_LEGACY":                0,   # dead
    "WH_LEGACY.LEG_MLC_MEMBER_ARCHIVE":             0,
    "WH_LEGACY.LEG_IOOF_FUND_ARCHIVE":              0,
    "WH_LEGACY.LEG_PORTFOLIO_REBALANCE_QUEUE":      0,   # PKG hasn't run in 180 days
    "WH_LEGACY.LEG_ANZ_CLIENT_OVERRIDES":           0,
    "WH_LEGACY.V_MLC_MEMBER_PHONE_AUDIT":           0,
}


USERS = ["BI_PROD", "BI_DEV", "ETL_LOADER", "ANALYST_TEAM", "REPORTING_API", "ADHOC_USER", "FINANCE_TEAM", "RISK_TEAM"]


def is_write(name: str) -> bool:
    n = name.split(".")[-1]
    return n.startswith(("STG_", "RAW_")) or n.startswith("LEG_")


def write_awr() -> None:
    rows: list[dict[str, str | int]] = []
    end = datetime(2026, 5, 6, 23, 0, 0)
    for fqn, total in EXEC_COUNTS.items():
        if total == 0:
            continue
        # Spread executions across a few SQL_IDs so it looks like real traffic
        n_sql = max(1, min(8, int(total ** 0.35)))
        remaining = total
        for i in range(n_sql):
            share = remaining if i == n_sql - 1 else int(total / n_sql + random.randint(-int(total / (n_sql * 4)), int(total / (n_sql * 4))))
            share = max(1, share)
            remaining -= share
            sql_text = ("MERGE " if is_write(fqn) and i == 0 else "INSERT " if is_write(fqn) else "SELECT ") + " ... " + fqn + " ..."
            sql_text = query_for(fqn) if not is_write(fqn) else sql_text
            last_active = end - timedelta(minutes=random.randint(0, 60 * 24 * 6))  # within last 6 days
            user = random.choice(USERS) if not is_write(fqn) else "ETL_LOADER"
            rows.append({
                "SQL_ID": f"{abs(hash(fqn + str(i))) % (16**13):013x}",
                "PARSING_SCHEMA_NAME": user,
                "EXECUTIONS_TOTAL": share,
                "ELAPSED_TIME_TOTAL": share * random.randint(1200, 8400),
                "BUFFER_GETS_TOTAL": share * random.randint(80, 4000),
                "DISK_READS_TOTAL": share * random.randint(0, 240),
                "ROWS_PROCESSED_TOTAL": share * random.randint(1, 1500),
                "LAST_ACTIVE_TIME": last_active.isoformat(),
                "SQL_TEXT": sql_text,
            })
    # Add some 100% useless queries against legacy that NEVER ran (zero counts pad context)
    # Sort by exec count desc for nicer presentation
    rows.sort(key=lambda r: -int(r["EXECUTIONS_TOTAL"]))
    with (ROOT / "awr" / "dba_hist_sqlstat.csv").open("w", newline="") as f:
        w = csv.DictWriter(f, fieldnames=list(rows[0].keys()))
        w.writeheader()
        for r in rows:
            w.writerow(r)


if __name__ == "__main__":
    write_segments()
    write_dependencies()
    write_awr()
    print(f"Wrote {len(TABLES)} segments, {sum(1 for _ in (ROOT/'dictionary'/'all_dependencies.csv').open()) - 1} dep edges, AWR rows.")
