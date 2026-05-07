"""
Intermediate Representation (IR) for Informatica-to-SQLX conversion.

The IR normalizes an Informatica mapping's visual dataflow graph into a
linear sequence of SQL-ready nodes.  Each node maps to one CTE in the
final output.  Column lineage is explicit — every column knows where it
came from and what expression produces it.

Design principles:
  - Immutable after construction (built by ir_builder, consumed by sql_generator)
  - No Informatica-specific terminology leaks past the builder
  - Every column carries its full lineage — no fuzzy matching needed downstream
  - Deterministic: same input always produces the same IR
"""

from __future__ import annotations

from dataclasses import dataclass, field
from enum import Enum, auto
from typing import Optional


# ── Node types ───────────────────────────────────────────────────────────

class NodeType(Enum):
    """What SQL construct a node emits."""
    SOURCE = auto()       # FROM table (possibly with custom SQL)
    EXPRESSION = auto()   # SELECT computed_cols FROM upstream
    FILTER = auto()       # WHERE condition
    LOOKUP = auto()       # LEFT JOIN on condition
    JOINER = auto()       # JOIN between two streams
    AGGREGATOR = auto()   # GROUP BY + aggregate functions
    UNION = auto()        # UNION ALL of N streams
    NORMALIZER = auto()   # CROSS JOIN UNNEST (row explosion)
    ROUTER = auto()       # WHERE-based stream split
    SEQUENCE = auto()     # ROW_NUMBER() surrogate key
    TARGET = auto()       # Final SELECT (column projection)


class JoinType(Enum):
    LEFT = "LEFT JOIN"
    INNER = "INNER JOIN"
    FULL = "FULL OUTER JOIN"
    RIGHT = "RIGHT JOIN"
    CROSS = "CROSS JOIN"


# ── Column lineage ──────────────────────────────────────────────────────

@dataclass(frozen=True)
class ColumnDef:
    """A single column in a node's output schema.

    Attributes:
        name:        Output column name (what downstream nodes see).
        expression:  SQL expression that produces this column.
                     For pass-throughs this is just ``{upstream_alias}.{source_column}``.
                     For computed columns it's the full BigQuery SQL expression.
        source_node: Which upstream node this column originates from (None for literals).
        source_column: The column name in the upstream node (None for computed).
        is_passthrough: True if this column is just forwarded unchanged.
        bq_data_type: BigQuery data type (e.g. 'STRING', 'INT64', 'DATETIME').
                      Empty string when BQ schema is unavailable.
    """
    name: str
    expression: str
    source_node: Optional[str] = None
    source_column: Optional[str] = None
    is_passthrough: bool = False
    bq_data_type: str = ""


# ── Nodes ───────────────────────────────────────────────────────────────

@dataclass
class SourceNode:
    """Reads from a base table, optionally with custom SQL."""
    cte_name: str                          # e.g. "cte_dacom_locations"
    node_type: NodeType = NodeType.SOURCE
    table_ref: str = ""                    # e.g. "s_dacom_location" for ${ref()}
    custom_sql: str = ""                   # raw SQ SQL override
    columns: list[ColumnDef] = field(default_factory=list)
    where: str = ""                        # source filter
    is_distinct: bool = False
    # For multi-source SQs with user-defined joins
    joined_tables: list[str] = field(default_factory=list)  # additional ${ref()} targets
    join_condition: str = ""               # user-defined join ON clause (monolithic fallback)
    # Per-table join conditions: joined_table_ref -> "pred1 AND pred2"
    # When populated, _emit_source() uses per-JOIN ON clauses instead of monolithic
    join_conditions: dict[str, str] = field(default_factory=dict)
    # Per-table join types: joined_table_ref -> "INNER JOIN" | "LEFT OUTER JOIN" etc.
    join_types: dict[str, str] = field(default_factory=dict)


@dataclass
class ExpressionNode:
    """Computes new columns from upstream."""
    cte_name: str
    node_type: NodeType = NodeType.EXPRESSION
    upstream: str = ""                     # CTE name to read from
    columns: list[ColumnDef] = field(default_factory=list)
    pass_upstream: bool = True             # include upstream.* in addition to new cols


@dataclass
class FilterNode:
    """Applies a WHERE predicate."""
    cte_name: str
    node_type: NodeType = NodeType.FILTER
    upstream: str = ""
    condition: str = ""
    columns: list[ColumnDef] = field(default_factory=list)  # pass-through


@dataclass
class LookupNode:
    """LEFT JOIN to a dimension/reference table."""
    cte_name: str
    node_type: NodeType = NodeType.LOOKUP
    upstream: str = ""                     # main stream CTE
    lookup_table: str = ""                 # ${ref()} target
    lookup_alias: str = ""                 # SQL alias (e.g. "lkp_site")
    join_type: JoinType = JoinType.LEFT
    join_condition: str = ""               # ON clause
    return_columns: list[ColumnDef] = field(default_factory=list)
    sql_override: str = ""                 # lookup SQL override (subquery)
    # Whether to merge into the previous lookup's CTE or create a new one
    merge_with_previous: bool = True
    # Return column names that shadow upstream columns — used by sql_generator
    # to emit SELECT src.* EXCEPT(...) instead of src.* to avoid duplicate cols
    shadowed_columns: set[str] = field(default_factory=set)


@dataclass
class JoinerNode:
    """JOIN between two streams (Informatica Joiner transformation)."""
    cte_name: str
    node_type: NodeType = NodeType.JOINER
    master_upstream: str = ""              # master/detail terminology from Informatica
    detail_upstream: str = ""
    join_type: JoinType = JoinType.LEFT
    join_condition: str = ""
    columns: list[ColumnDef] = field(default_factory=list)
    # Unique aliases for this joiner (avoids duplicate aliases across sequential joiners)
    detail_alias: str = "detail"
    master_alias: str = "master"


@dataclass
class AggregatorNode:
    """GROUP BY with aggregate functions."""
    cte_name: str
    node_type: NodeType = NodeType.AGGREGATOR
    upstream: str = ""
    group_by: list[str] = field(default_factory=list)
    columns: list[ColumnDef] = field(default_factory=list)


@dataclass
class UnionNode:
    """UNION ALL of multiple streams."""
    cte_name: str
    node_type: NodeType = NodeType.UNION
    upstreams: list[str] = field(default_factory=list)
    columns: list[ColumnDef] = field(default_factory=list)
    # Per-upstream column mappings: upstream_name -> {output_col: input_col}
    column_mappings: dict[str, dict[str, str]] = field(default_factory=dict)


@dataclass
class NormalizerNode:
    """Row explosion via CROSS JOIN UNNEST."""
    cte_name: str
    node_type: NodeType = NodeType.NORMALIZER
    upstream: str = ""
    occurs: int = 1
    flat_columns: list[str] = field(default_factory=list)   # pass-through
    group_columns: list[str] = field(default_factory=list)  # repeated
    columns: list[ColumnDef] = field(default_factory=list)


@dataclass
class RouterGroup:
    """One output group from a Router transformation."""
    name: str             # e.g. "NEW", "UPDATE", "DEFAULT1"
    condition: str        # WHERE predicate
    is_default: bool = False


@dataclass
class RouterNode:
    """Splits a stream based on conditions (becomes WHERE in SQL)."""
    cte_name: str  # not really used as a CTE — controls final SELECT WHERE
    node_type: NodeType = NodeType.ROUTER
    upstream: str = ""
    groups: list[RouterGroup] = field(default_factory=list)
    columns: list[ColumnDef] = field(default_factory=list)


@dataclass
class TargetMapping:
    """Maps final CTE columns to target table columns."""
    target_table: str = ""
    target_schema: str = ""
    router_group: str = ""          # which router group feeds this target (empty = all)
    columns: list[ColumnDef] = field(default_factory=list)
    is_incremental: bool = False    # DD_UPDATE -> MERGE
    unique_key: list[str] = field(default_factory=list)


# ── The complete IR ─────────────────────────────────────────────────────

# Union type for all node kinds
Node = (SourceNode | ExpressionNode | FilterNode | LookupNode |
        JoinerNode | AggregatorNode | UnionNode | NormalizerNode | RouterNode)


@dataclass
class DataflowGraph:
    """Complete IR for one Informatica mapping.

    ``nodes`` is in topological order — each node only references
    upstream nodes that appear earlier in the list.

    The sql_generator walks this list front-to-back, emitting one CTE
    per node.
    """
    mapping_name: str
    description: str = ""
    nodes: list[Node] = field(default_factory=list)
    target: TargetMapping = field(default_factory=TargetMapping)
    all_targets: list[TargetMapping] = field(default_factory=list)
    variables: dict[str, str] = field(default_factory=dict)

    # Metadata for the SQLX config block
    table_type: str = "table"         # "table", "incremental", "view", "operations"
    schema: str = "CDWH_Store"
    tags: list[str] = field(default_factory=list)

    # Reference name map for ${ref()} — lowercase key -> display name
    ref_name_map: dict[str, str] = field(default_factory=dict)

    def get_node(self, cte_name: str) -> Node | None:
        """Find a node by its CTE name."""
        for node in self.nodes:
            if node.cte_name == cte_name:
                return node
        return None

    def get_output_columns(self, cte_name: str) -> list[ColumnDef]:
        """Get the output column schema of a node."""
        node = self.get_node(cte_name)
        if node is None:
            return []
        return node.columns

    def get_final_cte(self) -> str:
        """Name of the last CTE (what the final SELECT reads from)."""
        if self.nodes:
            return self.nodes[-1].cte_name
        return ""
