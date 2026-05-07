"""Insignia pipeline XML → Dataform SQLX transformer.

Public API:
    parse(xml_text, filename)     -> TransformResult         # one pipeline → IR
    generate_sqlx(xml_files)      -> list[GeneratedFile]     # XML → SQLX records
    generate_project(xml_files)   -> AssembledProject        # SQLX → full repo
"""

from app.transformer.dataform_project import (
    AssembledProject,
    DataformProjectConfig,
    assemble_project,
)
from app.transformer.insignia_to_ir import (
    OperationsScript,
    TransformResult,
    parse,
)
from app.transformer.runner import GeneratedFile, generate_project, generate_sqlx

__all__ = [
    "AssembledProject",
    "DataformProjectConfig",
    "GeneratedFile",
    "OperationsScript",
    "TransformResult",
    "assemble_project",
    "generate_project",
    "generate_sqlx",
    "parse",
]
