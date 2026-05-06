"use client";

import { useMemo, useState } from "react";
import { Search } from "lucide-react";
import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/Card";
import { Input } from "@/components/ui/Input";
import { Badge } from "@/components/ui/Badge";
import type { Inventory, Layer } from "@/lib/types";
import { formatNumber } from "@/lib/utils";

const LAYER_TINT: Record<Layer, string> = {
  raw:         "border-[rgba(40,82,148,0.6)] bg-[rgba(40,82,148,0.15)] text-[#a9bad4]",
  staging:     "border-[rgba(126,188,249,0.4)] bg-[rgba(126,188,249,0.12)] text-[#7ebcf9]",
  integration: "border-[rgba(0,180,240,0.4)] bg-[rgba(0,180,240,0.10)] text-[#00b4f0]",
  reporting:   "border-[rgba(255,107,71,0.4)] bg-[rgba(255,107,71,0.10)] text-[#ff6b47]",
  unknown:     "border-[var(--color-border)] bg-[var(--color-bg-elev-2)] text-[var(--color-fg-subtle)]",
};

export function InventoryView({ inventory }: { inventory: Inventory | undefined }) {
  const [q, setQ] = useState("");
  const [layerFilter, setLayerFilter] = useState<Layer | "all">("all");

  const tables = inventory?.tables ?? [];
  const filtered = useMemo(() => {
    const t = q.trim().toLowerCase();
    return tables.filter((row) => {
      if (layerFilter !== "all" && row.layer !== layerFilter) return false;
      if (!t) return true;
      return row.name.toLowerCase().includes(t) || row.schema_name.toLowerCase().includes(t);
    });
  }, [q, layerFilter, tables]);

  const byLayer = useMemo(() => {
    const counts: Record<string, number> = {};
    for (const t of tables) counts[t.layer] = (counts[t.layer] ?? 0) + 1;
    return counts;
  }, [tables]);

  if (!inventory) return <EmptyState message="No inventory data — Inventory agent did not run." />;

  return (
    <Card>
      <CardHeader>
        <div className="flex flex-col md:flex-row md:items-center md:justify-between gap-3">
          <div>
            <CardTitle>Schema inventory</CardTitle>
            <div className="mt-1 text-[12px] text-[var(--color-fg-muted)]">
              {formatNumber(tables.length)} tables &amp; views · {formatNumber(inventory.procedures.length)} procedures · {formatNumber(inventory.flags.length)} flags
            </div>
          </div>
          <div className="flex flex-wrap items-center gap-2">
            <FilterPill label="all" active={layerFilter === "all"} onClick={() => setLayerFilter("all")} count={tables.length} />
            {(["raw", "staging", "integration", "reporting", "unknown"] as Layer[]).map((l) => (
              <FilterPill key={l} label={l} active={layerFilter === l} onClick={() => setLayerFilter(l)} count={byLayer[l] ?? 0} tint={LAYER_TINT[l]} />
            ))}
          </div>
        </div>
        <div className="mt-3 relative">
          <Search className="h-4 w-4 absolute left-3 top-1/2 -translate-y-1/2 text-[var(--color-fg-subtle)]" />
          <Input value={q} onChange={(e) => setQ(e.target.value)} placeholder="Search by schema or table name…" className="pl-9" />
        </div>
      </CardHeader>
      <CardContent className="p-0">
        <div className="overflow-x-auto">
          <table className="w-full text-[12.5px]">
            <thead>
              <tr className="border-y border-[var(--color-border-soft)] text-[10.5px] uppercase tracking-wider text-[var(--color-fg-subtle)]">
                <th className="text-left px-5 py-2 font-medium">Object</th>
                <th className="text-left px-3 py-2 font-medium">Layer</th>
                <th className="text-left px-3 py-2 font-medium">Domain</th>
                <th className="text-left px-3 py-2 font-medium">Kind</th>
                <th className="text-right px-3 py-2 font-medium">Columns</th>
                <th className="text-right px-5 py-2 font-medium">Rows</th>
              </tr>
            </thead>
            <tbody>
              {filtered.map((t) => (
                <tr key={`${t.schema_name}.${t.name}`} className="border-b border-[var(--color-border-soft)] hover:bg-white/[0.02] transition">
                  <td className="px-5 py-2.5 font-mono">
                    <span className="text-[var(--color-fg-subtle)]">{t.schema_name}.</span>
                    <span className="text-white">{t.name}</span>
                  </td>
                  <td className="px-3 py-2.5"><span className={`inline-flex items-center rounded px-2 py-0.5 text-[10.5px] font-medium border ${LAYER_TINT[t.layer]}`}>{t.layer}</span></td>
                  <td className="px-3 py-2.5"><Badge variant="neutral">{t.domain}</Badge></td>
                  <td className="px-3 py-2.5 text-[var(--color-fg-muted)]">{t.kind}</td>
                  <td className="px-3 py-2.5 text-right tabular-nums text-[var(--color-fg-muted)]">{t.columns.length}</td>
                  <td className="px-5 py-2.5 text-right tabular-nums text-[var(--color-fg-muted)]">{formatNumber(t.row_count)}</td>
                </tr>
              ))}
              {filtered.length === 0 && (
                <tr><td colSpan={6} className="px-5 py-12 text-center text-[var(--color-fg-muted)]">No objects match.</td></tr>
              )}
            </tbody>
          </table>
        </div>
      </CardContent>
    </Card>
  );
}

function FilterPill({ label, count, active, onClick, tint }: { label: string; count: number; active: boolean; onClick: () => void; tint?: string }) {
  return (
    <button
      onClick={onClick}
      className={`inline-flex items-center gap-1.5 rounded-full px-2.5 py-0.5 text-[11px] border transition ${
        active ? (tint ?? "border-[var(--color-cyan-accent)] bg-[rgba(0,180,240,0.10)] text-[var(--color-cyan-soft)]")
        : "border-[var(--color-border)] text-[var(--color-fg-muted)] hover:text-white"
      }`}
    >
      {label}
      <span className="font-mono text-[10px] opacity-70">{count}</span>
    </button>
  );
}

function EmptyState({ message }: { message: string }) {
  return (
    <Card><CardContent className="py-16 text-center text-[var(--color-fg-muted)]">{message}</CardContent></Card>
  );
}
