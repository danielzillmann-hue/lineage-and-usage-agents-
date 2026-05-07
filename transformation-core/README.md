# transformation-core

Source-agnostic core lifted from the Transformation Agent. Provides:

- `ir` — Intermediate Representation (NodeType, ColumnDef, Node, DataflowGraph)
- `sql_generator` — IR → BigQuery-compatible SQL (CTE chains)
- `sqlx_wrapper` — wraps generated SQL in a Dataform `.sqlx` shell (config block, refs, dependencies, assertions)
- `sqlx_post_processor` — final cleanup pass on SQLX output
- `sqlx_validator` — validates generated SQLX against a set of structural rules
- `naming` — naming conventions (PascalCase, snake_case, etc.)
- `default_value_registry` — default-value lookups for typed columns
- `sqlx_template` — thin SQLX template builder (source-agnostic replacement for the legacy `sqlx_template_generator`)

## What this package does NOT include

- **No source-system parsers.** Informatica XML, Sybase DDL, Oracle DDL, Insignia XML — all live in the consumer.
- **No IR builder.** Each source system writes its own `xxx_to_ir.py` builder that constructs `DataflowGraph` instances and hands them to `sql_generator`.

## Usage shape

```python
from transformation_core.ir import DataflowGraph, Node, NodeType, ColumnDef
from transformation_core.sql_generator import generate_sql
from transformation_core.sqlx_wrapper import wrap_as_sqlx

# 1. Build IR (consumer responsibility)
graph = my_source_to_ir(pipeline_xml)

# 2. Generate SQL
sql = generate_sql(graph)

# 3. Wrap as Dataform SQLX
sqlx = wrap_as_sqlx(sql, schema="my_schema", target_table="my_table", ...)
```

## Lift provenance

Lifted on 2026-05-07 from `agents/transformation_agent/src/`. Imports rewritten from `src.X` → `transformation_core.X`. The Informatica-coupled `ir_builder.py` and `sqlx_template_generator.py` were intentionally NOT lifted — see consumer code for replacements.
