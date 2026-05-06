"use client";

import { useEffect, useState } from "react";
import { X, Key, Link2, FileText, Database, Activity, GitBranch, Pencil, Shield } from "lucide-react";
import { Badge } from "@/components/ui/Badge";
import { api } from "@/lib/api";
import type { Column, ColumnNature, ETLPipeline, Sensitivity, Table } from "@/lib/types";
import { formatBytes, formatNumber } from "@/lib/utils";

interface Props {
  table: Table | null;
  onClose: () => void;
  pipelines?: ETLPipeline[];
  runId?: string;
  onColumnEdit?: (tableFqn: string, column: Column) => void;
}

const SENSITIVITY_OPTIONS: Sensitivity[] = ["pii", "tax", "financial", "internal", "public"];
const NATURE_OPTIONS: ColumnNature[] = ["key", "audit", "calculated", "reference", "data"];

export function TableDetailDrawer({ table, onClose, pipelines = [], runId, onColumnEdit }: Props) {
  const [editingCol, setEditingCol] = useState<string | null>(null);
  const [savingCol, setSavingCol] = useState<string | null>(null);
  // Local override of column values for optimistic UI
  const [localOverrides, setLocalOverrides] = useState<Record<string, Partial<Column>>>({});

  // Close on ESC
  useEffect(() => {
    if (!table) return;
    const onKey = (e: KeyboardEvent) => {
      if (e.key === "Escape") {
        if (editingCol) setEditingCol(null);
        else onClose();
      }
    };
    window.addEventListener("keydown", onKey);
    return () => window.removeEventListener("keydown", onKey);
  }, [table, onClose, editingCol]);

  if (!table) return null;

  const applyEdit = async (col: Column, patch: { sensitivity?: Sensitivity; nature?: ColumnNature }) => {
    if (!runId) return;
    const key = col.name;
    setSavingCol(key);
    setLocalOverrides((prev) => ({ ...prev, [key]: { ...prev[key], ...patch, user_overridden: true } }));
    try {
      const updated = await api.patchColumn(runId, {
        table_fqn: `${table.schema_name}.${table.name}`,
        column_name: col.name,
        sensitivity: patch.sensitivity,
        nature: patch.nature,
      });
      setLocalOverrides((prev) => ({ ...prev, [key]: updated }));
      onColumnEdit?.(`${table.schema_name}.${table.name}`, updated);
    } catch (e) {
      // revert on error
      setLocalOverrides((prev) => {
        const { [key]: _drop, ...rest } = prev;
        return rest;
      });
    } finally {
      setSavingCol(null);
      setEditingCol(null);
    }
  };

  const colWithOverride = (c: Column): Column => ({ ...c, ...(localOverrides[c.name] ?? {}) });

  // Pipelines that READ this table (its name appears in source_tables)
  const reading = pipelines.filter((p) =>
    p.source_tables.some((s) => s.toUpperCase() === table.name.toUpperCase()),
  );
  // Pipelines that PRODUCE this table (CSV outputs match)
  const producing = pipelines.filter((p) => {
    if (table.kind !== "CSV") return false;
    const csv = (p.output_csv || "").toLowerCase();
    return csv.includes(table.name.toLowerCase());
  });

  const pkCount = table.columns.filter((c) => c.is_pk).length;
  const fkCount = table.columns.filter((c) => c.is_fk).length;
  const nullableCount = table.columns.filter((c) => c.nullable).length;

  return (
    <>
      {/* Backdrop */}
      <div
        className="fixed inset-0 z-50 bg-[rgba(6,11,26,0.65)] backdrop-blur-sm transition-opacity"
        onClick={onClose}
        aria-hidden="true"
      />

      {/* Drawer */}
      <aside
        className="fixed right-0 top-0 z-50 h-screen w-full max-w-[640px] bg-[var(--color-bg-elev-1)] border-l border-[var(--color-border)] shadow-[0_0_60px_rgba(0,0,0,0.5)] flex flex-col"
        role="dialog"
        aria-modal="true"
      >
        {/* Header */}
        <div className="flex items-start justify-between gap-4 p-5 border-b border-[var(--color-border)] bg-gradient-to-b from-[var(--color-bg-elev-2)] to-transparent">
          <div className="min-w-0">
            <div className="flex items-center gap-2 text-[11px] uppercase tracking-wider text-[var(--color-fg-subtle)] mb-1">
              <Database className="h-3 w-3" />
              {table.kind === "CSV" ? "CSV output" : table.kind}
              <span>·</span>
              <span>{table.layer}</span>
              <span>·</span>
              <span>{table.domain}</span>
            </div>
            <h2 className="text-[22px] font-semibold tracking-tight text-[var(--ink)] font-mono break-all">
              <span className="text-[var(--color-fg-subtle)]">{table.schema_name}.</span>
              {table.name}
            </h2>
            {table.comment && (
              <p className="mt-2 text-[12.5px] text-[var(--color-fg-muted)]">{table.comment}</p>
            )}
          </div>
          <button
            onClick={onClose}
            className="flex-shrink-0 h-8 w-8 inline-flex items-center justify-center rounded-md border border-[var(--color-border)] text-[var(--color-fg-muted)] hover:text-[var(--ink)] hover:border-[var(--color-cyan-accent)] transition"
            aria-label="Close"
          >
            <X className="h-4 w-4" />
          </button>
        </div>

        {/* Stats row */}
        <div className="grid grid-cols-4 gap-3 px-5 py-4 border-b border-[var(--color-border-soft)]">
          <Stat label="Columns" value={String(table.columns.length)} />
          <Stat label="Rows" value={formatNumber(table.row_count)} />
          <Stat label="Size" value={formatBytes(table.bytes)} />
          <Stat label="PK / FK" value={`${pkCount} / ${fkCount}`} tint="text-[var(--color-cyan-soft)]" />
        </div>

        {/* Body — scrollable */}
        <div className="flex-1 overflow-y-auto px-5 py-4 space-y-5">
          {/* Pipelines that read or produce this table */}
          {(reading.length > 0 || producing.length > 0) && (
            <section>
              <SectionTitle icon={GitBranch}>Pipelines</SectionTitle>
              <div className="space-y-2">
                {producing.map((p) => (
                  <PipelineRef key={`p-${p.name}`} pipeline={p} role="produces" />
                ))}
                {reading.map((p) => (
                  <PipelineRef key={`r-${p.name}`} pipeline={p} role="reads" />
                ))}
              </div>
            </section>
          )}

          {/* Columns */}
          <section>
            <SectionTitle icon={FileText}>
              Columns
              <span className="ml-2 text-[11px] font-normal text-[var(--color-fg-subtle)]">
                {nullableCount} nullable · {table.columns.length - nullableCount} not null
              </span>
            </SectionTitle>
            <div className="rounded-lg border border-[var(--color-border-soft)] overflow-hidden">
              <table className="w-full text-[12.5px]">
                <thead>
                  <tr className="bg-[var(--color-bg-elev-2)]/60 text-[10.5px] uppercase tracking-wider text-[var(--color-fg-subtle)]">
                    <th className="text-left px-3 py-2 font-medium">Name</th>
                    <th className="text-left px-3 py-2 font-medium">Type</th>
                    <th className="text-left px-3 py-2 font-medium">Sensitivity</th>
                    <th className="text-left px-3 py-2 font-medium">Nature</th>
                    <th className="text-center px-2 py-2 font-medium">Null</th>
                    <th className="text-left px-3 py-2 font-medium">Key</th>
                  </tr>
                </thead>
                <tbody>
                  {table.columns.map((rawCol) => {
                    const col = colWithOverride(rawCol);
                    const sens = col.sensitivity ?? "internal";
                    const sensVariant =
                      sens === "pii" ? "crit" :
                      sens === "tax" ? "warn" :
                      sens === "financial" ? "warn" :
                      sens === "public" ? "ok" : "neutral";
                    const isInherited = (col.inherited_sensitivity_from?.length ?? 0) > 0;
                    const isEditingThis = editingCol === col.name;
                    return (
                      <tr key={col.name} className="border-t border-[var(--color-border-soft)] hover:bg-white/[0.02]">
                        <td className="px-3 py-2 font-mono text-[var(--ink)]">{col.name}</td>
                        <td className="px-3 py-2 text-[11.5px] font-mono text-[var(--color-fg-muted)]">{col.data_type}</td>
                        <td className="px-3 py-2 relative">
                          <button
                            onClick={() => setEditingCol(isEditingThis ? null : col.name)}
                            disabled={!runId}
                            title={col.annotation_notes ?? "click to edit"}
                            style={{
                              background: "transparent", border: 0, padding: 0,
                              cursor: runId ? "pointer" : "default",
                              display: "inline-flex", alignItems: "center", gap: 4,
                            }}
                          >
                            <Badge variant={sensVariant}>{sens}</Badge>
                            {isInherited && !col.user_overridden && (
                              <span title={`Inherited from: ${col.inherited_sensitivity_from!.join(", ")}`}>
                                <Shield className="h-2.5 w-2.5" strokeWidth={1.5} style={{ color: "var(--ink-3)" }} />
                              </span>
                            )}
                            {col.user_overridden && (
                              <span title="Human-overridden">
                                <Pencil className="h-2.5 w-2.5" strokeWidth={1.5} style={{ color: "var(--brand-emerald)" }} />
                              </span>
                            )}
                          </button>
                          {isEditingThis && runId && (
                            <EditPopover
                              kind="sensitivity"
                              current={sens}
                              options={SENSITIVITY_OPTIONS}
                              saving={savingCol === col.name}
                              onPick={(v) => applyEdit(rawCol, { sensitivity: v as Sensitivity })}
                              onClose={() => setEditingCol(null)}
                            />
                          )}
                        </td>
                        <td className="px-3 py-2 text-[11.5px] text-[var(--color-fg-muted)]">{col.nature ?? "data"}</td>
                        <td className="px-2 py-2 text-center text-[11px]">
                          {col.nullable
                            ? <span className="text-[var(--color-fg-subtle)]">yes</span>
                            : <span className="text-[var(--color-amber)]">no</span>}
                        </td>
                        <td className="px-3 py-2">
                          <div className="flex items-center gap-1.5 flex-wrap">
                            {col.is_pk && <Badge variant="info"><Key className="h-2.5 w-2.5" /> PK</Badge>}
                            {col.is_fk && (
                              <span className="inline-flex items-center gap-1 text-[10.5px] font-mono text-[var(--color-cyan-soft)]">
                                <Link2 className="h-2.5 w-2.5" />
                                {col.fk_target}
                              </span>
                            )}
                          </div>
                        </td>
                      </tr>
                    );
                  })}
                </tbody>
              </table>
            </div>
          </section>

          {/* View / source text if present */}
          {table.source_text && (
            <section>
              <SectionTitle icon={Activity}>Source SQL</SectionTitle>
              <pre className="rounded-md border border-[var(--color-border-soft)] bg-[var(--color-bg-elev-2)]/40 p-3 text-[11.5px] font-mono text-[var(--color-fg-muted)] overflow-x-auto whitespace-pre-wrap">
                {table.source_text}
              </pre>
            </section>
          )}
        </div>
      </aside>
    </>
  );
}

function Stat({ label, value, tint }: { label: string; value: string; tint?: string }) {
  return (
    <div>
      <div className="text-[10.5px] uppercase tracking-wider text-[var(--color-fg-subtle)]">{label}</div>
      <div className={`mt-0.5 text-[18px] font-semibold tabular-nums ${tint ?? "text-[var(--ink)]"}`}>{value}</div>
    </div>
  );
}

function SectionTitle({ icon: Icon, children }: { icon: React.ComponentType<{ className?: string }>; children: React.ReactNode }) {
  return (
    <div className="flex items-center gap-2 mb-2.5 text-[11.5px] uppercase tracking-wider text-[var(--color-fg-muted)] font-medium">
      <Icon className="h-3.5 w-3.5 text-[var(--color-cyan-soft)]" />
      <div>{children}</div>
    </div>
  );
}

function EditPopover<T extends string>({
  kind, current, options, saving, onPick, onClose,
}: {
  kind: string;
  current: T;
  options: T[];
  saving: boolean;
  onPick: (v: T) => void;
  onClose: () => void;
}) {
  return (
    <>
      <div
        className="fixed inset-0 z-[60]"
        onClick={onClose}
        aria-hidden="true"
      />
      <div
        className="absolute z-[70] mt-1 left-0"
        style={{
          background: "var(--bg-elev)",
          border: "1px solid var(--line)",
          borderRadius: 6,
          boxShadow: "0 4px 14px rgba(15,31,44,0.08)",
          padding: 4, minWidth: 140,
        }}
      >
        <div className="px-2 py-1 text-[10.5px]" style={{
          fontFamily: "var(--font-mono)", textTransform: "uppercase",
          letterSpacing: "0.1em", color: "var(--ink-3)",
        }}>
          set {kind}
        </div>
        {options.map((o) => (
          <button
            key={o}
            onClick={(e) => { e.stopPropagation(); onPick(o); }}
            disabled={saving}
            style={{
              width: "100%", textAlign: "left",
              padding: "5px 8px", fontSize: 12.5,
              background: o === current ? "var(--bg-sunk)" : "transparent",
              color: o === current ? "var(--ink)" : "var(--ink-2)",
              border: 0, borderRadius: 4, cursor: saving ? "wait" : "pointer",
              fontFamily: "var(--font-sans)",
            }}
          >
            {o}{o === current ? "  (current)" : ""}
          </button>
        ))}
      </div>
    </>
  );
}

function PipelineRef({ pipeline, role }: { pipeline: ETLPipeline; role: "reads" | "produces" }) {
  const runs = pipeline.runs;
  return (
    <div className="rounded-md border border-[var(--color-border-soft)] bg-[var(--color-bg-elev-1)]/60 px-3 py-2 flex items-center justify-between gap-3">
      <div className="min-w-0">
        <div className="flex items-center gap-2 text-[12.5px]">
          <Badge variant={role === "produces" ? "accent" : "info"}>{role}</Badge>
          <span className="font-mono text-[var(--ink)] truncate">{pipeline.name}</span>
        </div>
        {pipeline.output_csv && (
          <div className="text-[10.5px] font-mono text-[var(--color-fg-subtle)] mt-0.5 truncate">→ {pipeline.output_csv}</div>
        )}
      </div>
      <div className="flex-shrink-0 text-right text-[11px] text-[var(--color-fg-muted)]">
        {runs ? (
          <>
            <div className="tabular-nums">{runs.runs_total} runs</div>
            {runs.runs_failed > 0 && (
              <div className="text-[var(--color-rose)] tabular-nums">{runs.runs_failed} failed</div>
            )}
          </>
        ) : (
          <span className="text-[var(--color-amber)]">never run</span>
        )}
      </div>
    </div>
  );
}
