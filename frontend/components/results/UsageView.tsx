"use client";

import { useMemo, useState } from "react";
import { Bar, BarChart, ResponsiveContainer, XAxis, YAxis, Tooltip, CartesianGrid, Cell } from "recharts";
import { Activity, AlertTriangle, FileText, ArrowRight, AlertCircle, CheckCircle2, PauseCircle } from "lucide-react";
import { Card, CardContent, CardDescription, CardHeader, CardTitle } from "@/components/ui/Card";
import { Badge } from "@/components/ui/Badge";
import type { Inventory, PipelineUsage, UsageReport } from "@/lib/types";
import { formatNumber } from "@/lib/utils";

// ─── Filter pill (toggleable) ────────────────────────────────────────────────

const PILL_TONES = {
  ok:      { fg: "var(--brand-emerald-700)", bg: "var(--brand-emerald-100)", border: "rgba(15,179,122,0.35)" },
  warn:    { fg: "var(--warn)",               bg: "var(--warn-bg)",          border: "rgba(199,123,10,0.35)" },
  crit:    { fg: "var(--crit)",               bg: "var(--crit-bg)",          border: "rgba(192,54,44,0.35)" },
} as const;

function FilterPill({
  label, icon, tone, active, disabled, onClick,
}: {
  label: string;
  icon: React.ReactNode;
  tone: keyof typeof PILL_TONES;
  active: boolean;
  disabled: boolean;
  onClick: () => void;
}) {
  const t = PILL_TONES[tone];
  return (
    <button
      onClick={onClick}
      disabled={disabled}
      type="button"
      style={{
        display: "inline-flex", alignItems: "center", gap: 6,
        fontFamily: "var(--font-mono)", fontSize: 11, fontWeight: 500,
        letterSpacing: "0.02em",
        padding: "4px 10px", borderRadius: 99,
        background: active ? t.fg : t.bg,
        color: active ? "#FFFFFF" : t.fg,
        border: `1px solid ${active ? t.fg : t.border}`,
        cursor: disabled ? "not-allowed" : "pointer",
        opacity: disabled ? 0.4 : 1,
        transition: "background .12s, color .12s, border-color .12s",
      }}
    >
      {icon}
      {label}
    </button>
  );
}

export function UsageView({ usage, inventory }: { usage?: UsageReport; inventory?: Inventory }) {
  const objectsByFqn = useMemo(() => {
    const m = new Map<string, { read: number; write: number }>();
    for (const o of usage?.objects ?? []) m.set(o.fqn, { read: o.read_count, write: o.write_count });
    return m;
  }, [usage]);

  const topReads = useMemo(() => {
    return [...(usage?.objects ?? [])]
      .sort((a, b) => b.read_count - a.read_count)
      .slice(0, 12)
      .map((o) => ({ name: o.fqn.split(".").pop() ?? o.fqn, full: o.fqn, reads: o.read_count, writes: o.write_count }));
  }, [usage]);

  if (!usage) return <Card><CardContent className="py-16 text-center text-[var(--color-fg-muted)]">No usage data.</CardContent></Card>;

  const reachable = usage.reporting_reachable_sources.length;
  const unreachable = usage.reporting_unreachable_sources.length;
  const reachPct = reachable + unreachable === 0 ? 0 : (reachable / (reachable + unreachable)) * 100;
  const hasPipelines = (usage.pipelines?.length ?? 0) > 0;

  return (
    <div className="space-y-6">
      {hasPipelines && <PipelineHealthPanel usage={usage} />}
      <div className="grid grid-cols-1 lg:grid-cols-3 gap-6">
        <Card className="lg:col-span-2">
          <CardHeader>
            <CardTitle>Top reads</CardTitle>
            <CardDescription>Object-level read counts from AWR / V$SQL.</CardDescription>
          </CardHeader>
          <CardContent className="h-[280px]">
            <ResponsiveContainer width="100%" height="100%">
              <BarChart data={topReads} layout="vertical" margin={{ top: 5, right: 16, left: 60, bottom: 5 }}>
                <CartesianGrid stroke="#16204a" horizontal={false} />
                <XAxis type="number" stroke="#6471a0" fontSize={10} tickLine={false} axisLine={false} />
                <YAxis dataKey="name" type="category" stroke="#97a3c2" fontSize={11} tickLine={false} axisLine={false} width={120} />
                <Tooltip
                  cursor={{ fill: "rgba(0,180,240,0.07)" }}
                  contentStyle={{ background: "#0c1530", border: "1px solid #1f2c5a", borderRadius: 8, fontSize: 12 }}
                  labelStyle={{ color: "#fff" }}
                />
                <Bar dataKey="reads" radius={[0, 4, 4, 0]}>
                  {topReads.map((_, i) => (
                    <Cell key={i} fill={`url(#readGrad-${i})`} />
                  ))}
                </Bar>
                <defs>
                  {topReads.map((_, i) => (
                    <linearGradient key={i} id={`readGrad-${i}`} x1="0" y1="0" x2="1" y2="0">
                      <stop offset="0%" stopColor="#285294" />
                      <stop offset="100%" stopColor="#00b4f0" />
                    </linearGradient>
                  ))}
                </defs>
              </BarChart>
            </ResponsiveContainer>
          </CardContent>
        </Card>

        <Card>
          <CardHeader>
            <CardTitle>Reporting reachability</CardTitle>
            <CardDescription>Raw sources that flow to a reporting-layer object.</CardDescription>
          </CardHeader>
          <CardContent>
            <div className="text-[40px] font-semibold tracking-tight text-[var(--ink)] tabular-nums leading-none">
              {reachPct.toFixed(0)}<span className="text-[24px] text-[var(--color-fg-muted)]">%</span>
            </div>
            <div className="mt-2 text-[12.5px] text-[var(--color-fg-muted)]">
              <span className="text-[var(--color-emerald)]">{reachable}</span> reachable ·{" "}
              <span className="text-[var(--color-rose)]">{unreachable}</span> unreachable
            </div>
            <div className="mt-4 h-2 rounded-full bg-[var(--color-bg-elev-2)] overflow-hidden flex">
              <div className="h-full bg-gradient-to-r from-[var(--color-navy-500)] to-[var(--color-emerald)]" style={{ width: `${reachPct}%` }} />
              <div className="h-full bg-[var(--color-rose)]" style={{ width: `${100 - reachPct}%` }} />
            </div>
            {usage.reporting_unreachable_sources.length > 0 && (
              <div className="mt-4 rounded-md border border-[rgba(244,71,107,0.4)] bg-[rgba(244,71,107,0.06)] p-3">
                <div className="flex items-center gap-2 text-[12px] text-[var(--color-rose)] font-medium">
                  <AlertTriangle className="h-3.5 w-3.5" /> Unreachable raw sources
                </div>
                <div className="mt-2 space-y-1 max-h-32 overflow-y-auto font-mono text-[11px] text-[var(--color-fg-muted)]">
                  {usage.reporting_unreachable_sources.slice(0, 12).map((s) => <div key={s}>{s}</div>)}
                </div>
              </div>
            )}
          </CardContent>
        </Card>
      </div>

      <div className="grid grid-cols-1 md:grid-cols-3 gap-6">
        <ListCard
          icon={Activity} tint="text-[var(--color-cyan-soft)]"
          title="Hot tables"
          description="Most-read in window"
          items={usage.hot_tables}
        />
        <ListCard
          icon={FileText} tint="text-[var(--color-amber)]"
          title="Write-only orphans"
          description="ETL writes, nothing reads"
          items={usage.write_only_orphans}
          severityCount={usage.write_only_orphans.length}
        />
        <ListCard
          icon={AlertTriangle} tint="text-[var(--color-rose)]"
          title="Dead objects"
          description="No reads or writes in window"
          items={usage.dead_objects}
          severityCount={usage.dead_objects.length}
        />
      </div>
    </div>
  );
}

type StatusKey = "healthy" | "failing" | "off-grid" | "never-run" | "undocumented";

function statusOf(p: PipelineUsage): StatusKey {
  if (!p.has_definition) return "undocumented";
  if (p.ran_without_logging) return "off-grid";
  if (p.runs_total === 0) return "never-run";
  if (p.runs_failed > 0) return "failing";
  return "healthy";
}

function PipelineHealthPanel({ usage }: { usage: UsageReport }) {
  const [activeFilters, setActiveFilters] = useState<Set<StatusKey>>(new Set());
  const toggleFilter = (k: StatusKey) =>
    setActiveFilters((s) => {
      const next = new Set(s);
      if (next.has(k)) next.delete(k); else next.add(k);
      return next;
    });

  const sorted = useMemo(
    () => [...usage.pipelines].sort((a, b) => b.runs_total - a.runs_total),
    [usage.pipelines],
  );
  const total = sorted.length;
  const neverRun = sorted.filter((p) => statusOf(p) === "never-run").length;
  const undocumented = sorted.filter((p) => statusOf(p) === "undocumented").length;
  const failing = sorted.filter((p) => statusOf(p) === "failing").length;
  const offGrid = sorted.filter((p) => statusOf(p) === "off-grid").length;
  const healthy = sorted.filter((p) => statusOf(p) === "healthy").length;

  const visible = activeFilters.size === 0
    ? sorted
    : sorted.filter((p) => activeFilters.has(statusOf(p)));

  return (
    <Card>
      <CardHeader>
        <div className="flex items-end justify-between flex-wrap gap-3">
          <div>
            <CardTitle>Pipeline health</CardTitle>
            <CardDescription>From <span className="font-mono">ETL_EXECUTION_LOGS</span> on the live database.</CardDescription>
          </div>
          <div className="flex flex-wrap gap-2 items-center">
            <FilterPill
              label={`${healthy} healthy`} icon={<CheckCircle2 className="h-3 w-3" strokeWidth={1.5} />}
              tone="ok" active={activeFilters.has("healthy")} disabled={healthy === 0}
              onClick={() => toggleFilter("healthy")}
            />
            <FilterPill
              label={`${failing} failing`} icon={<AlertTriangle className="h-3 w-3" strokeWidth={1.5} />}
              tone="warn" active={activeFilters.has("failing")} disabled={failing === 0}
              onClick={() => toggleFilter("failing")}
            />
            <FilterPill
              label={`${offGrid} off-grid`} icon={<AlertCircle className="h-3 w-3" strokeWidth={1.5} />}
              tone="crit" active={activeFilters.has("off-grid")} disabled={offGrid === 0}
              onClick={() => toggleFilter("off-grid")}
            />
            <FilterPill
              label={`${neverRun} never run`} icon={<PauseCircle className="h-3 w-3" strokeWidth={1.5} />}
              tone="warn" active={activeFilters.has("never-run")} disabled={neverRun === 0}
              onClick={() => toggleFilter("never-run")}
            />
            <FilterPill
              label={`${undocumented} undocumented`} icon={<AlertCircle className="h-3 w-3" strokeWidth={1.5} />}
              tone="crit" active={activeFilters.has("undocumented")} disabled={undocumented === 0}
              onClick={() => toggleFilter("undocumented")}
            />
            {activeFilters.size > 0 && (
              <button
                onClick={() => setActiveFilters(new Set())}
                style={{
                  fontSize: 11, padding: "3px 8px", marginLeft: 4,
                  background: "transparent", color: "var(--ink-3)",
                  border: "1px solid var(--line)", borderRadius: 99, cursor: "pointer",
                  fontFamily: "var(--font-sans)",
                }}
                title="Clear all filters"
              >
                clear · {visible.length} of {total}
              </button>
            )}
            <span className="ml-auto" style={{ fontSize: 11, color: "var(--ink-4)" }}>
              {activeFilters.size === 0 ? `${total} tracked` : `${visible.length} shown`}
            </span>
          </div>
        </div>
      </CardHeader>
      <CardContent className="p-0">
        <div className="overflow-x-auto">
          <table className="w-full text-[12.5px]">
            <thead>
              <tr className="border-y border-[var(--color-border-soft)] text-[10.5px] uppercase tracking-wider text-[var(--color-fg-subtle)]">
                <th className="text-left px-5 py-2 font-medium">Pipeline</th>
                <th className="text-right px-3 py-2 font-medium">Runs</th>
                <th className="text-right px-3 py-2 font-medium">Success</th>
                <th className="text-right px-3 py-2 font-medium">Fail</th>
                <th className="text-right px-3 py-2 font-medium">Rate</th>
                <th className="text-right px-3 py-2 font-medium">Last run</th>
                <th className="text-left px-5 py-2 font-medium">Status</th>
              </tr>
            </thead>
            <tbody>
              {visible.map((p) => <PipelineRow key={p.pipeline_name + p.has_definition} p={p} />)}
              {visible.length === 0 && (
                <tr>
                  <td colSpan={7} className="px-5 py-12 text-center" style={{ color: "var(--ink-3)" }}>
                    No pipelines match the active filters.
                  </td>
                </tr>
              )}
            </tbody>
          </table>
        </div>
      </CardContent>
    </Card>
  );
}

function PipelineRow({ p }: { p: PipelineUsage }) {
  const failPct = p.runs_total > 0 ? (100 * p.runs_failed) / p.runs_total : 0;
  const status =
    !p.has_definition ? "undocumented" :
    p.ran_without_logging ? "running off-grid" :
    p.runs_total === 0 ? "never run" :
    failPct >= 10 ? "high failure" :
    failPct > 0 ? "occasional failure" : "healthy";
  const statusVariant =
    status === "undocumented" ? "crit" :
    status === "running off-grid" ? "crit" :
    status === "high failure" ? "crit" :
    status === "never run" ? "warn" :
    status === "occasional failure" ? "warn" : "ok";
  const rowTint = !p.has_definition || p.ran_without_logging ? "bg-[rgba(244,71,107,0.04)]" : "";
  return (
    <tr className={`border-b border-[var(--color-border-soft)] hover:bg-white/[0.02] ${rowTint}`}>
      <td className="px-5 py-2.5">
        <div className={`font-mono ${p.has_definition ? "text-[var(--ink)]" : "text-[var(--color-rose)]"}`}>{p.pipeline_name}</div>
        {p.output_csv && <div className="text-[10.5px] text-[var(--color-fg-subtle)] font-mono">→ {p.output_csv}</div>}
      </td>
      <td className="px-3 py-2.5 text-right tabular-nums text-[var(--ink)]">{p.runs_total}</td>
      <td className="px-3 py-2.5 text-right tabular-nums text-[var(--color-emerald)]">{p.runs_success}</td>
      <td className="px-3 py-2.5 text-right tabular-nums">
        {p.runs_failed > 0 ? <span className="text-[var(--color-rose)]">{p.runs_failed}</span> : <span className="text-[var(--color-fg-subtle)]">0</span>}
      </td>
      <td className="px-3 py-2.5 text-right tabular-nums">
        {p.runs_total > 0 ? (
          <span className={failPct >= 10 ? "text-[var(--color-rose)]" : "text-[var(--color-fg-muted)]"}>
            {p.success_rate.toFixed(0)}%
          </span>
        ) : <span className="text-[var(--color-fg-subtle)]">—</span>}
      </td>
      <td className="px-3 py-2.5 text-right text-[11px] text-[var(--color-fg-muted)] tabular-nums">
        {p.last_run ? p.last_run.replace("T", " ").slice(0, 16) : "—"}
      </td>
      <td className="px-5 py-2.5"><Badge variant={statusVariant}>{status}</Badge></td>
    </tr>
  );
}

function ListCard({
  icon: Icon, tint, title, description, items, severityCount,
}: {
  icon: React.ComponentType<{ className?: string }>; tint: string;
  title: string; description: string; items: string[]; severityCount?: number;
}) {
  return (
    <Card>
      <CardHeader>
        <div className="flex items-center justify-between">
          <CardTitle className="flex items-center gap-2"><Icon className={`h-4 w-4 ${tint}`} /> {title}</CardTitle>
          {severityCount !== undefined && severityCount > 0 && <Badge variant={severityCount > 0 ? "warn" : "neutral"}>{severityCount}</Badge>}
        </div>
        <CardDescription>{description}</CardDescription>
      </CardHeader>
      <CardContent className="p-0">
        <div className="max-h-72 overflow-y-auto divide-y divide-[var(--color-border-soft)]">
          {items.length === 0 && <div className="px-5 py-6 text-center text-[12.5px] text-[var(--color-fg-muted)]">None.</div>}
          {items.slice(0, 50).map((it) => (
            <div key={it} className="px-5 py-2 flex items-center justify-between text-[12.5px] hover:bg-white/[0.02]">
              <span className="font-mono text-[var(--ink)] truncate">{it}</span>
              <ArrowRight className="h-3 w-3 text-[var(--color-fg-subtle)] flex-shrink-0" />
            </div>
          ))}
        </div>
      </CardContent>
    </Card>
  );
}
