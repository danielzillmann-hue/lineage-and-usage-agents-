"""Smoke test: build a tiny IR by hand, generate SQL + SQLX, assert structure."""

from transformation_core import (
    ColumnDef,
    DataflowGraph,
    ExpressionNode,
    SourceNode,
    SQLGenerator,
    TargetMapping,
    wrap_sqlx,
)


def _trivial_graph() -> DataflowGraph:
    """One source → one expression → target. Mirrors a stg_daily_metrics-style INSERT."""
    src = SourceNode(
        cte_name="cte_transactions",
        table_ref="transactions",
        columns=[
            ColumnDef(name="account_id", expression="account_id", is_passthrough=True),
            ColumnDef(name="amount", expression="amount", is_passthrough=True),
            ColumnDef(name="transaction_date", expression="transaction_date", is_passthrough=True),
        ],
        where="amount > 0",
    )
    expr = ExpressionNode(
        cte_name="cte_aggregated",
        upstream="cte_transactions",
        columns=[
            ColumnDef(name="account_id", expression="account_id", source_node="cte_transactions",
                      source_column="account_id", is_passthrough=True),
            ColumnDef(name="metric_date", expression="DATE(transaction_date)",
                      source_node="cte_transactions", source_column="transaction_date"),
            ColumnDef(name="total_in", expression="SUM(amount)",
                      source_node="cte_transactions", source_column="amount"),
        ],
        pass_upstream=False,
    )
    target = TargetMapping(
        target_table="stg_daily_metrics",
        target_schema="staging",
        columns=[
            ColumnDef(name="account_id", expression="account_id"),
            ColumnDef(name="metric_date", expression="metric_date"),
            ColumnDef(name="total_in", expression="total_in"),
        ],
    )
    return DataflowGraph(
        mapping_name="stg_daily_metrics",
        description="Daily per-account contribution totals",
        nodes=[src, expr],
        target=target,
        table_type="table",
        schema="staging",
    )


def test_sql_generator_emits_ctes():
    g = _trivial_graph()
    sql = SQLGenerator().generate(g)
    assert "cte_transactions" in sql
    assert "cte_aggregated" in sql
    assert "transactions" in sql
    assert "amount > 0" in sql


def test_sqlx_wrapper_produces_config_block():
    g = _trivial_graph()
    sql = SQLGenerator().generate(g)
    sqlx = wrap_sqlx(g, sql)
    assert "config {" in sqlx
    assert 'type: "table"' in sqlx
    assert 'schema: "staging"' in sqlx
    assert "cte_transactions" in sqlx
    assert "cte_aggregated" in sqlx


if __name__ == "__main__":
    test_sql_generator_emits_ctes()
    test_sqlx_wrapper_produces_config_block()
    print("smoke OK")
    g = _trivial_graph()
    sql = SQLGenerator().generate(g)
    print("\n--- generated SQL ---")
    print(sql)
    print("\n--- generated SQLX ---")
    print(wrap_sqlx(g, sql))
