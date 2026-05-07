"""Insignia pipeline XML → Dataform SQLX transformer.

Public API:
    parse(xml_text, filename) -> TransformResult
    generate_sqlx(result) -> dict[filename, sqlx_text]
"""

from app.transformer.insignia_to_ir import (
    OperationsScript,
    TransformResult,
    parse,
)
from app.transformer.runner import generate_sqlx

__all__ = ["TransformResult", "OperationsScript", "parse", "generate_sqlx"]
