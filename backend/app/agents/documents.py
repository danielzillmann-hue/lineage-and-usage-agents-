"""Documentation cross-check pass.

Reads delivery-specification PDFs/Markdown files from a GCS prefix; sends each
to Gemini to extract per-CSV delivery metadata; cross-references against the
pipeline outputs we already discovered. Output:

  - inv.deliveries: list[DeliverySpec]    one per CSV named in the docs
  - inv.undocumented_outputs: list[str]   CSVs that exist but lack a spec

The undocumented set is a strong governance signal — these CSVs are produced
and shipped somewhere, but no formal contract exists for who consumes them.
"""

from __future__ import annotations

import json
import logging
from typing import Any

from google.genai import types

from app.agents.annotations import _parse_array_tolerant
from app.agents.base import EmitFn, gemini, log_event
from app.config import get_settings
from app.models.run import AgentName
from app.models.schema import DeliverySpec, Inventory
from app.services import gcs

log = logging.getLogger(__name__)


_PROMPT = """\
You are extracting per-CSV delivery specifications from internal documentation.

Each document defines where one or more CSV files are delivered (destination,
protocol, endpoint, authentication, frequency, details). Output a JSON array
of delivery specs — ONE entry per CSV the document covers. Be precise and
quote endpoints / hosts verbatim where present.

Each entry shape:
  {
    "csv_name":      "members_extract.csv",
    "kind":          "internal" | "external",
    "destination":   "Services Australia (Medicare / Centrelink)",
    "protocol":      "SFTP" | "REST API" | "SMTP" | "...",
    "endpoint":      full URL / sftp host:port/path / email address,
    "auth":          "RSA 4096-bit SSH Key" | "Bearer Token" | "MTLS" | ...,
    "frequency":     "Daily" | "Weekly (Mondays)" | null,
    "details":       short note (under 30 words)
  }

Detect kind from the document title or context (e.g. "External Partner
Delivery", "Internal Downstream Delivery"). If ambiguous, leave as "unknown".

Output ONLY the JSON array. No prose.
"""


async def cross_check_documents(req, inv: Inventory, emit: EmitFn) -> None:
    """Read every PDF/MD in documents_prefix; extract specs; cross-check."""
    if not req.bucket or req.documents_prefix is None:
        return

    doc_files: list[tuple[str, bytes, str]] = []
    for f in gcs.iter_classified(req.bucket, req.documents_prefix):
        name = f.name.lower()
        if not (name.endswith(".pdf") or name.endswith(".md") or name.endswith(".txt")):
            continue
        try:
            content = gcs.read_bytes(req.bucket, f.name)
            mime = "application/pdf" if name.endswith(".pdf") else "text/plain"
            doc_files.append((f.name, content, mime))
        except Exception as e:  # noqa: BLE001
            log.warning("doc fetch failed for %s: %s", f.name, e)

    if not doc_files:
        return

    await log_event(emit, AgentName.INVENTORY, f"Documentation pass: parsing {len(doc_files)} delivery spec file(s)")

    s = get_settings()
    client = gemini()
    all_specs: list[DeliverySpec] = []

    for filename, content, mime in doc_files:
        try:
            specs = await _extract_from_doc(client, s.inventory_model, filename, content, mime)
            all_specs.extend(specs)
            await log_event(emit, AgentName.INVENTORY, f"  · {filename.split('/')[-1]} → {len(specs)} delivery spec(s)")
        except Exception as e:  # noqa: BLE001
            log.warning("doc parse failed for %s: %s", filename, e)
            await log_event(emit, AgentName.INVENTORY, f"  · {filename.split('/')[-1]} skipped: {e}")

    inv.deliveries = all_specs

    # Cross-check: for each output CSV we know about (from pipelines or
    # CSV-kind tables), see if a spec covers it.
    documented = {d.csv_name.lower() for d in all_specs}
    produced: set[str] = set()
    for p in inv.pipelines:
        if p.output_csv:
            produced.add(p.output_csv.lower())
    for t in inv.tables:
        if t.kind == "CSV":
            produced.add(f"{t.name.lower()}.csv")
    undocumented = sorted(produced - documented)
    inv.undocumented_outputs = undocumented

    if undocumented:
        from app.models.schema import InventoryFlag
        inv.flags.append(InventoryFlag(
            severity="critical",
            title=f"{len(undocumented)} CSV output(s) lack a delivery specification",
            detail=(
                f"These CSVs are produced by ETL pipelines (and present in the bucket) "
                f"but have no entry in the documented Internal/External delivery specs: "
                f"{', '.join(undocumented)}. The data ships somewhere — the contract "
                f"is missing. Migration to Bluedoor must surface and ratify the "
                f"consumer / protocol / SLA before cutover."
            ),
        ))

    await log_event(
        emit, AgentName.INVENTORY,
        f"Delivery cross-check: {len(documented)} CSVs documented, "
        f"{len(undocumented)} produced without a spec.",
    )


async def _extract_from_doc(client, model: str, filename: str, content: bytes, mime: str) -> list[DeliverySpec]:
    """Send a single doc to Gemini, parse the returned JSON array."""
    parts: list[Any] = []
    parts.append(types.Part.from_bytes(data=content, mime_type=mime))
    parts.append(types.Part.from_text(text=f"\nFile: {filename.split('/')[-1]}"))

    cfg = types.GenerateContentConfig(
        system_instruction=_PROMPT,
        max_output_tokens=16384,
        temperature=0.2,
        response_mime_type="application/json",
    )
    resp = await client.aio.models.generate_content(model=model, contents=parts, config=cfg)
    text = resp.text or ""
    rows = _parse_array_tolerant(text, log_label="documents")
    out: list[DeliverySpec] = []
    for r in rows:
        try:
            r["source_doc"] = filename.split("/")[-1]
            out.append(DeliverySpec.model_validate(r))
        except Exception as e:  # noqa: BLE001
            log.debug("delivery spec row dropped: %s — %s", r, e)
    return out


__all__ = ["cross_check_documents"]
