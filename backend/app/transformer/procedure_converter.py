"""Oracle PL/SQL procedure → BigQuery procedural SQL via Gemini.

Procedures are too varied for deterministic translation (cursors, loops,
exception blocks, dynamic SQL). We hand the source to Gemini 2.5 Pro
with a focused prompt, capture the output, and wrap it in a Dataform
`type: "operations"` SQLX shell.

Each procedure becomes one file at `definitions/procedures/<name>.sqlx`.
"""

from __future__ import annotations

import asyncio
import logging
from dataclasses import dataclass

from google.genai import types

from app.agents.base import gemini

log = logging.getLogger(__name__)


@dataclass
class ConvertedProcedure:
    name: str
    schema: str
    sqlx: str
    warnings: list[str]


_PROMPT = """\
Convert the following Oracle PL/SQL procedure to BigQuery procedural SQL.

Output rules:
1. Emit ONLY the BigQuery SQL — no markdown fences, no commentary.
2. Use BigQuery procedural syntax: BEGIN ... END;, DECLARE, SET, IF/ELSE,
   WHILE LOOP, FOR row IN (SELECT ...) DO ... END FOR, RETURN, etc.
3. Replace SYSDATE → CURRENT_DATETIME(), NVL → IFNULL, ROWNUM → row_number,
   DUAL → (SELECT 1), and other Oracle-specific functions with their BQ
   equivalents.
4. Reference tables as fully qualified `project.dataset.table` placeholders
   like `${{project}}.${{dataset}}.TABLE_NAME` so Dataform's compile fills
   them in. (Use uppercase table names exactly as in the source.)
5. CURSOR-based loops → use `FOR row IN (SELECT ...) DO ... END FOR;`.
6. Exception blocks → `BEGIN ... EXCEPTION WHEN ERROR THEN ... END;`.
7. If a construct has no clean BigQuery equivalent (e.g. SAVEPOINT,
   AUTONOMOUS_TRANSACTION, REF CURSOR), leave a `-- TODO:` comment
   describing the gap and emit best-effort code.
8. Wrap the entire procedure in `CREATE OR REPLACE PROCEDURE
   <name>() BEGIN ... END;` syntax. Keep parameter names + types when
   present; map Oracle types to BQ (NUMBER → NUMERIC, VARCHAR2 → STRING,
   DATE → DATE or DATETIME, etc.).
"""


async def convert_procedure(
    name: str,
    schema: str,
    oracle_sql: str,
    timeout_seconds: int = 60,
) -> ConvertedProcedure:
    """Translate one procedure. Returns the wrapped SQLX text."""
    client = gemini()
    cfg = types.GenerateContentConfig(
        system_instruction=_PROMPT,
        max_output_tokens=8192,
        temperature=0.1,
    )
    user = (
        f"Procedure name: {schema}.{name}\n\n"
        f"Oracle PL/SQL source:\n```sql\n{oracle_sql.strip()}\n```\n"
    )
    try:
        resp = await asyncio.wait_for(
            client.aio.models.generate_content(
                model="gemini-2.5-pro",
                contents=user,
                config=cfg,
            ),
            timeout=timeout_seconds,
        )
        bq_sql = (getattr(resp, "text", None) or "").strip()
    except asyncio.TimeoutError:
        bq_sql = ""
        warning = f"timed out after {timeout_seconds}s"
    except Exception as e:  # noqa: BLE001
        bq_sql = ""
        warning = f"Gemini call failed: {e}"
    else:
        warning = ""

    # Strip any accidental markdown code fences.
    if bq_sql.startswith("```"):
        bq_sql = "\n".join(bq_sql.split("\n")[1:])
        if bq_sql.endswith("```"):
            bq_sql = bq_sql[:-3].rstrip()

    if not bq_sql:
        # Stub: keep the original as a TODO so the Dataform compile fails
        # loudly and the user knows to revisit.
        bq_sql = (
            f"-- TODO: PL/SQL conversion failed for {schema}.{name}.\n"
            f"-- Original Oracle source preserved below for manual port.\n"
            f"/*\n{oracle_sql}\n*/"
        )

    sqlx = (
        f"-- Procedure: {schema}.{name}\n"
        f"-- Translated from Oracle PL/SQL by Gemini 2.5 Pro.\n\n"
        f"config {{\n"
        f'  type: "operations",\n'
        f"  hasOutput: false,\n"
        f'  description: "Ported from Oracle procedure {schema}.{name}",\n'
        f"}}\n\n"
        f"{bq_sql.rstrip(';')};\n"
    )
    return ConvertedProcedure(
        name=name,
        schema=schema,
        sqlx=sqlx,
        warnings=[warning] if warning else [],
    )


async def convert_all(
    procedures: list[tuple[str, str, str]],
    concurrency: int = 4,
) -> list[ConvertedProcedure]:
    """Translate a batch in parallel.

    `procedures` is a list of (name, schema, oracle_sql) tuples.
    """
    if not procedures:
        return []
    sem = asyncio.Semaphore(concurrency)

    async def _one(item: tuple[str, str, str]) -> ConvertedProcedure:
        name, schema, src = item
        async with sem:
            return await convert_procedure(name, schema, src)

    return await asyncio.gather(*(_one(p) for p in procedures))
