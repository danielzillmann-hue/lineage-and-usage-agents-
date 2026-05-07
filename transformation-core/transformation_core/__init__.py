"""transformation-core — source-agnostic IR + SQL/SQLX generation.

Public API. Consumers (Informatica adapter, Insignia XML adapter, etc.) build
DataflowGraph instances and pass them to generate_sql / wrap_sqlx.
"""

from transformation_core.ir import (
    AggregatorNode,
    ColumnDef,
    DataflowGraph,
    ExpressionNode,
    FilterNode,
    JoinerNode,
    JoinType,
    LookupNode,
    NodeType,
    NormalizerNode,
    RouterGroup,
    RouterNode,
    SourceNode,
    TargetMapping,
    UnionNode,
)
from transformation_core.naming import to_pascal_name
from transformation_core.sql_generator import SQLGenerator
from transformation_core.sqlx_wrapper import wrap_sqlx

__all__ = [
    # IR types
    "AggregatorNode",
    "ColumnDef",
    "DataflowGraph",
    "ExpressionNode",
    "FilterNode",
    "JoinerNode",
    "JoinType",
    "LookupNode",
    "NodeType",
    "NormalizerNode",
    "RouterGroup",
    "RouterNode",
    "SourceNode",
    "TargetMapping",
    "UnionNode",
    # Generators
    "SQLGenerator",
    "wrap_sqlx",
    # Naming
    "to_pascal_name",
]
