"""Live Oracle introspection — schema, FKs, row counts, and the ETL audit log.

Connection details come from the first <connection> element in any ETL XML
file found in the source bucket — keeps the user from having to enter creds.
For real Insignia work this would route through Secret Manager; for the demo
the creds are committed in the XMLs.
"""

from __future__ import annotations

import logging
from contextlib import contextmanager
from dataclasses import dataclass
from datetime import datetime
from typing import Any

import oracledb

log = logging.getLogger(__name__)


@dataclass
class OracleConn:
    host: str
    port: int
    service: str
    user: str
    password: str

    @property
    def dsn(self) -> str:
        return f"{self.host}:{self.port}/{self.service}"


@contextmanager
def connect(c: OracleConn):
    conn = oracledb.connect(user=c.user, password=c.password, dsn=c.dsn)
    try:
        yield conn
    finally:
        conn.close()


@dataclass
class OracleColumn:
    name: str
    data_type: str
    nullable: bool
    is_pk: bool
    is_fk: bool
    fk_target: str | None  # "TABLE.COLUMN"


@dataclass
class OracleTable:
    schema: str
    name: str
    columns: list[OracleColumn]
    row_count: int | None
    last_analyzed: str | None
    bytes: int | None

    @property
    def fqn(self) -> str:
        return f"{self.schema}.{self.name}"


@dataclass
class PipelineRunStat:
    pipeline_name: str
    csv_generated: str | None
    runs_total: int
    runs_success: int
    runs_failed: int
    first_run: str | None
    last_run: str | None


@dataclass
class OracleSnapshot:
    schema: str
    tables: list[OracleTable]
    pipeline_runs: list[PipelineRunStat]
    fetched_at: str


def snapshot(conn: OracleConn) -> OracleSnapshot:
    """One-shot DB introspection."""
    with connect(conn) as c:
        cur = c.cursor()
        cur.execute("SELECT user FROM dual")
        schema = cur.fetchone()[0]

        # Tables and basic stats
        cur.execute("""
            SELECT t.table_name, NVL(s.bytes, 0), t.last_analyzed, t.num_rows
            FROM   user_tables t
            LEFT   JOIN user_segments s ON s.segment_name = t.table_name AND s.segment_type = 'TABLE'
            ORDER  BY t.table_name
        """)
        table_meta = cur.fetchall()

        # Columns
        cur.execute("""
            SELECT table_name, column_name, data_type, data_length, nullable
            FROM   user_tab_columns
            ORDER  BY table_name, column_id
        """)
        col_rows = cur.fetchall()
        cols_by_table: dict[str, list[tuple]] = {}
        for tn, cn, dt, dl, nl in col_rows:
            cols_by_table.setdefault(tn, []).append((cn, dt, dl, nl))

        # PKs
        cur.execute("""
            SELECT cc.table_name, cc.column_name
            FROM   user_constraints c
            JOIN   user_cons_columns cc ON cc.constraint_name = c.constraint_name
            WHERE  c.constraint_type = 'P'
        """)
        pk_set = {(t, c) for t, c in cur.fetchall()}

        # FKs
        cur.execute("""
            SELECT a.table_name, a.column_name, b.table_name, b.column_name
            FROM   user_cons_columns a
            JOIN   user_constraints c ON a.constraint_name = c.constraint_name AND a.table_name = c.table_name
            JOIN   user_cons_columns b ON c.r_constraint_name = b.constraint_name
            WHERE  c.constraint_type = 'R'
        """)
        fk_map: dict[tuple[str, str], str] = {}
        for tab, col, ref_tab, ref_col in cur.fetchall():
            fk_map[(tab, col)] = f"{ref_tab}.{ref_col}"

        # Live row counts (small DB — exact counts are cheap and trustworthy)
        row_counts: dict[str, int] = {}
        for tn, *_ in table_meta:
            try:
                cur.execute(f"SELECT COUNT(*) FROM {tn}")
                row_counts[tn] = int(cur.fetchone()[0])
            except Exception as e:  # noqa: BLE001
                log.warning("count failed on %s: %s", tn, e)
                row_counts[tn] = -1

        tables: list[OracleTable] = []
        for tn, byts, last, num_rows in table_meta:
            cols: list[OracleColumn] = []
            for cn, dt, dl, nl in cols_by_table.get(tn, []):
                fk = fk_map.get((tn, cn))
                cols.append(OracleColumn(
                    name=cn,
                    data_type=f"{dt}({dl})" if dl and dt in ("VARCHAR2", "CHAR", "NVARCHAR2") else dt,
                    nullable=(nl == "Y"),
                    is_pk=(tn, cn) in pk_set,
                    is_fk=fk is not None,
                    fk_target=fk,
                ))
            tables.append(OracleTable(
                schema=schema, name=tn, columns=cols,
                row_count=row_counts.get(tn, num_rows),
                last_analyzed=str(last) if last else None,
                bytes=int(byts or 0) or None,
            ))

        # ETL audit log — only if the table exists
        pipeline_runs: list[PipelineRunStat] = []
        try:
            cur.execute("""
                SELECT pipeline_name,
                       MAX(csv_generated),
                       COUNT(*),
                       SUM(CASE WHEN status='SUCCESS' THEN 1 ELSE 0 END),
                       SUM(CASE WHEN status<>'SUCCESS' THEN 1 ELSE 0 END),
                       MIN(start_time),
                       MAX(start_time)
                FROM   etl_execution_logs
                GROUP  BY pipeline_name
            """)
            for pn, csv, total, ok, fail, first, last in cur.fetchall():
                pipeline_runs.append(PipelineRunStat(
                    pipeline_name=pn,
                    csv_generated=csv,
                    runs_total=int(total),
                    runs_success=int(ok or 0),
                    runs_failed=int(fail or 0),
                    first_run=str(first) if first else None,
                    last_run=str(last) if last else None,
                ))
        except oracledb.DatabaseError as e:
            log.info("etl_execution_logs unavailable: %s", e)

        return OracleSnapshot(
            schema=schema,
            tables=tables,
            pipeline_runs=pipeline_runs,
            fetched_at=datetime.utcnow().isoformat(),
        )


def find_connection_in_xml(xml_text: str) -> OracleConn | None:
    """Pull connection metadata out of an ETL XML <connection> element."""
    from xml.etree import ElementTree as ET

    try:
        root = ET.fromstring(xml_text)
    except ET.ParseError:
        return None
    el = root.find(".//connection[@type='oracle']")
    if el is None:
        return None
    try:
        return OracleConn(
            host=el.attrib["host"],
            port=int(el.attrib.get("port", "1521")),
            service=el.attrib["service"],
            user=el.attrib["user"],
            password=el.attrib["password"],
        )
    except (KeyError, ValueError):
        return None
