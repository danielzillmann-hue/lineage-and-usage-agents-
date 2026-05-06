"use client";

import { useMemo, useState } from "react";
import { Download, ShieldAlert, AlertTriangle, CheckCircle2, GitMerge, FileText } from "lucide-react";
import { Card, CardContent, CardDescription, CardHeader, CardTitle } from "@/components/ui/Card";
import { Badge } from "@/components/ui/Badge";
import type { DecommissionAssessment, Inventory, MultiWriterTarget } from "@/lib/types";

const API_BASE = process.env.NEXT_PUBLIC_API_BASE ?? "http://localhost:8080";

export function MigrationView({ inventory, runId }: { inventory?: Inventory; runId: string }) {
  const [verdictFilter, setVerdictFilter] = useState<"all" | "safe" | "review" | "blocked">("all");

  const assessments = inventory?.decommission ?? [];
  const sequencing = inventory?.sequencing ?? [];
  const multi = inventory?.multi_writers ?? [];

  const counts = useMemo(() => ({
    safe: assessments.filter((a) => a.verdict === "safe").length,
    review: assessments.filter((a) => a.verdict === "review").length,
    blocked: assessments.filter((a) => a.verdict === "blocked").length,
  }), [assessments]);

  const filtered = verdictFilter === "all"
    ? assessments
    : assessments.filter((a) => a.verdict === verdictFilter);

  if (!inventory) {
    return <Card><CardContent className="py-16 text-center" style={{ color: "var(--ink-3)" }}>No inventory data.</CardContent></Card>;
  }

  return (
    <div className="space-y-6">
      {/* ─── Top metric strip ───────────────────────────────────── */}
      <div className="grid grid-cols-1 md:grid-cols-4 gap-4">
        <MetricCard
          label="Decommission-safe"
          value={counts.safe}
          sub="No active dependencies"
          icon={CheckCircle2}
          tint="var(--ok)"
        />
        <MetricCard
          label="Needs review"
          value={counts.review}
          sub="Some dependencies — investigate"
          icon={AlertTriangle}
          tint="var(--warn)"
        />
        <MetricCard
          label="Blocked"
          value={counts.blocked}
          sub="Active in pipelines / views"
          icon={ShieldAlert}
          tint="var(--crit)"
        />
        <MetricCard
          label="Multi-writer targets"
          value={multi.length}
          sub="Need pattern classification"
          icon={GitMerge}
          tint="var(--info)"
        />
      </div>

      {/* ─── Scope export header ─────────────────────────────────── */}
      <Card>
        <CardHeader>
          <div className="flex items-end justify-between gap-3 flex-wrap">
            <div>
              <CardTitle>Migration scope</CardTitle>
              <CardDescription>
                Manifest the Transformation Agent ingests. Pipelines and tables flagged decommission-safe are filtered out.
              </CardDescription>
            </div>
            <div className="flex gap-2">
              <a href={`${API_BASE}/api/runs/${runId}/scope.json`} download style={btnSecondary}>
                <Download className="h-3.5 w-3.5" strokeWidth={1.25} /> Export JSON
              </a>
              <a href={`${API_BASE}/api/runs/${runId}/scope.csv`} download style={btnSecondary}>
                <Download className="h-3.5 w-3.5" strokeWidth={1.25} /> Export CSV
              </a>
            </div>
          </div>
        </CardHeader>
      </Card>

      {/* ─── Decommission readiness table ─────────────────────────── */}
      <Card>
        <CardHeader>
          <div className="flex items-center justify-between flex-wrap gap-3">
            <div>
              <CardTitle>Decommission readiness</CardTitle>
              <CardDescription>Per-object verdict with evidence drivers. Higher score = safer to retire.</CardDescription>
            </div>
            <div className="flex gap-1">
              {(["all", "safe", "review", "blocked"] as const).map((v) => (
                <button
                  key={v}
                  onClick={() => setVerdictFilter(v)}
                  style={{
                    fontSize: 12, padding: "5px 10px",
                    background: verdictFilter === v ? "var(--bg-sunk)" : "transparent",
                    border: `1px solid ${verdictFilter === v ? "var(--line-strong)" : "var(--line)"}`,
                    color: verdictFilter === v ? "var(--ink)" : "var(--ink-3)",
                    borderRadius: 4, cursor: "pointer", textTransform: "capitalize",
                  }}
                >
                  {v}
                </button>
              ))}
            </div>
          </div>
        </CardHeader>
        <CardContent className="p-0">
          <div className="overflow-x-auto">
            <table className="w-full text-[12.5px]">
              <thead>
                <tr style={{ borderTop: "1px solid var(--line)", borderBottom: "1px solid var(--line)" }}>
                  <th className="text-left px-5 py-2" style={hdrStyle}>Object</th>
                  <th className="text-left px-3 py-2" style={hdrStyle}>Verdict</th>
                  <th className="text-right px-3 py-2" style={hdrStyle}>Score</th>
                  <th className="text-right px-3 py-2" style={hdrStyle}>Pipelines</th>
                  <th className="text-right px-3 py-2" style={hdrStyle}>Views</th>
                  <th className="text-left px-5 py-2" style={hdrStyle}>Drivers</th>
                </tr>
              </thead>
              <tbody>
                {filtered.map((a) => <DecommRow key={a.object_fqn} a={a} />)}
                {filtered.length === 0 && (
                  <tr><td colSpan={6} className="px-5 py-12 text-center" style={{ color: "var(--ink-3)" }}>No objects match.</td></tr>
                )}
              </tbody>
            </table>
          </div>
        </CardContent>
      </Card>

      {/* ─── Migration sequencing waves ───────────────────────────── */}
      {sequencing.length > 0 && (
        <Card>
          <CardHeader>
            <CardTitle>Migration sequencing</CardTitle>
            <CardDescription>Order of conversion based on lineage dependency depth. Migrate Wave 1 first; each later wave assumes earlier ones are live.</CardDescription>
          </CardHeader>
          <CardContent>
            <div className="grid gap-4" style={{ gridTemplateColumns: `repeat(${Math.min(sequencing.length, 4)}, minmax(0, 1fr))` }}>
              {sequencing.slice(0, 4).map((w) => <WaveCard key={w.wave} wave={w} />)}
            </div>
            {sequencing.length > 4 && (
              <div className="mt-4 text-[12px]" style={{ color: "var(--ink-3)" }}>
                +{sequencing.length - 4} additional waves not shown.
              </div>
            )}
          </CardContent>
        </Card>
      )}

      {/* ─── Multi-writer targets ─────────────────────────────────── */}
      {multi.length > 0 && (
        <Card>
          <CardHeader>
            <CardTitle>Multi-writer targets</CardTitle>
            <CardDescription>
              Targets written by more than one pipeline — classified for the Transformation Agent&apos;s registry (disjoint / lifecycle / update-back).
            </CardDescription>
          </CardHeader>
          <CardContent className="p-0">
            <table className="w-full text-[12.5px]">
              <thead>
                <tr style={{ borderTop: "1px solid var(--line)", borderBottom: "1px solid var(--line)" }}>
                  <th className="text-left px-5 py-2" style={hdrStyle}>Target</th>
                  <th className="text-left px-3 py-2" style={hdrStyle}>Pattern</th>
                  <th className="text-left px-5 py-2" style={hdrStyle}>Writers</th>
                </tr>
              </thead>
              <tbody>
                {multi.map((m) => <MultiRow key={m.target_fqn} m={m} />)}
              </tbody>
            </table>
          </CardContent>
        </Card>
      )}

      {/* ─── Embedded business rules ─────────────────────────────── */}
      {(inventory.rules?.length ?? 0) > 0 && (
        <Card>
          <CardHeader>
            <div className="flex items-center justify-between">
              <CardTitle>Embedded business rules</CardTitle>
              <Badge variant="info"><FileText className="h-3 w-3" strokeWidth={1.25} /> {inventory.rules.length} extracted</Badge>
            </div>
            <CardDescription>
              Rules buried in PL/SQL views and ETL transforms — must be preserved on Bluedoor.
            </CardDescription>
          </CardHeader>
          <CardContent>
            <div className="space-y-2">
              {inventory.rules.slice(0, 30).map((r, i) => <RuleRow key={i} rule={r} />)}
              {inventory.rules.length > 30 && (
                <div className="text-[12px] pt-2" style={{ color: "var(--ink-3)" }}>
                  +{inventory.rules.length - 30} more rules in the scope export.
                </div>
              )}
            </div>
          </CardContent>
        </Card>
      )}
    </div>
  );
}

// ─── Subcomponents ──────────────────────────────────────────────────────────

const hdrStyle: React.CSSProperties = {
  fontFamily: "var(--font-mono)", fontSize: 10.5, fontWeight: 500,
  letterSpacing: "0.12em", textTransform: "uppercase", color: "var(--ink-3)",
};

const btnSecondary: React.CSSProperties = {
  display: "inline-flex", alignItems: "center", gap: 6,
  fontFamily: "var(--font-sans)", fontSize: 13, fontWeight: 500, lineHeight: 1,
  padding: "8px 12px", borderRadius: "var(--r-md)",
  background: "var(--bg-elev)", color: "var(--ink)",
  border: "1px solid var(--line)", cursor: "pointer", textDecoration: "none",
};

function MetricCard({
  label, value, sub, icon: Icon, tint,
}: {
  label: string; value: number; sub: string;
  icon: React.ComponentType<{ className?: string; strokeWidth?: number; color?: string }>;
  tint: string;
}) {
  return (
    <Card>
      <CardContent>
        <div className="flex items-start justify-between mb-2">
          <Icon className="h-4 w-4" strokeWidth={1.25} color={tint} />
        </div>
        <div className="eyebrow">{label}</div>
        <div className="mt-1 tabular-nums" style={{ fontSize: 28, fontWeight: 500, color: "var(--ink)", letterSpacing: "-0.01em" }}>
          {value}
        </div>
        <div style={{ fontSize: 11.5, color: "var(--ink-3)", marginTop: 2 }}>{sub}</div>
      </CardContent>
    </Card>
  );
}

function DecommRow({ a }: { a: DecommissionAssessment }) {
  const variant = a.verdict === "safe" ? "ok" : a.verdict === "review" ? "warn" : "crit";
  return (
    <tr style={{ borderBottom: "1px solid var(--line)" }}>
      <td className="px-5 py-2.5 mono" style={{ color: "var(--ink)" }}>{a.object_fqn}</td>
      <td className="px-3 py-2.5"><Badge variant={variant}>{a.verdict}</Badge></td>
      <td className="px-3 py-2.5 text-right tabular-nums mono" style={{ color: "var(--ink-2)" }}>{a.score}</td>
      <td className="px-3 py-2.5 text-right tabular-nums" style={{ color: a.downstream_pipeline_count > 0 ? "var(--warn)" : "var(--ink-3)" }}>
        {a.downstream_pipeline_count}
      </td>
      <td className="px-3 py-2.5 text-right tabular-nums" style={{ color: a.downstream_view_count > 0 ? "var(--warn)" : "var(--ink-3)" }}>
        {a.downstream_view_count}
      </td>
      <td className="px-5 py-2.5" style={{ color: "var(--ink-3)", fontSize: 11.5 }}>
        {a.drivers.slice(0, 2).map((d, i) => <div key={i}>{d}</div>)}
        {a.drivers.length > 2 && <div style={{ color: "var(--ink-4)" }}>+{a.drivers.length - 2} more</div>}
      </td>
    </tr>
  );
}

function WaveCard({ wave }: { wave: { wave: number; description: string; table_fqns: string[]; pipeline_names: string[] } }) {
  return (
    <div style={{ border: "1px solid var(--line)", borderRadius: 8, background: "var(--bg-elev)", padding: 14 }}>
      <div className="eyebrow" style={{ color: "var(--brand-emerald-700)" }}>
        Wave {wave.wave}
      </div>
      <div style={{ fontSize: 13.5, fontWeight: 500, marginTop: 4, color: "var(--ink)" }}>
        {wave.description}
      </div>
      <div className="mt-2.5 mono" style={{ fontSize: 11, color: "var(--ink-3)" }}>
        {wave.table_fqns.length} tables · {wave.pipeline_names.length} pipelines
      </div>
      <div className="mt-2 max-h-40 overflow-y-auto space-y-0.5">
        {wave.table_fqns.slice(0, 5).map((t) => (
          <div key={t} className="mono" style={{ fontSize: 11, color: "var(--ink-2)" }}>{t}</div>
        ))}
        {wave.pipeline_names.slice(0, 5).map((p) => (
          <div key={p} className="mono" style={{ fontSize: 11, color: "var(--brand-emerald-700)" }}>→ {p}</div>
        ))}
        {(wave.table_fqns.length + wave.pipeline_names.length) > 10 && (
          <div style={{ fontSize: 11, color: "var(--ink-4)" }}>
            +{wave.table_fqns.length + wave.pipeline_names.length - 10} more
          </div>
        )}
      </div>
    </div>
  );
}

function MultiRow({ m }: { m: MultiWriterTarget }) {
  const patternVariant = m.pattern === "disjoint" ? "ok" : m.pattern === "lifecycle" ? "info" : m.pattern === "update_back" ? "warn" : "neutral";
  return (
    <tr style={{ borderBottom: "1px solid var(--line)" }}>
      <td className="px-5 py-2.5 mono" style={{ color: "var(--ink)" }}>{m.target_fqn}</td>
      <td className="px-3 py-2.5"><Badge variant={patternVariant}>{m.pattern}</Badge></td>
      <td className="px-5 py-2.5 mono" style={{ color: "var(--ink-2)", fontSize: 11.5 }}>
        {m.writer_pipelines.join(", ")}
      </td>
    </tr>
  );
}

function RuleRow({ rule }: { rule: { rule_type: string; source_object: string; column?: string | null; expression: string; natural_language: string; confidence: number } }) {
  return (
    <div style={{ borderTop: "1px solid var(--line)", padding: "10px 0" }}>
      <div className="flex items-baseline gap-2 flex-wrap">
        <Badge variant="neutral">{rule.rule_type}</Badge>
        <span className="mono" style={{ fontSize: 11.5, color: "var(--ink-3)" }}>
          {rule.source_object}{rule.column ? ` · ${rule.column}` : ""}
        </span>
      </div>
      <div className="mt-1" style={{ fontSize: 13, color: "var(--ink)" }}>{rule.natural_language}</div>
      <div className="mt-1 mono" style={{ fontSize: 11, color: "var(--ink-3)", whiteSpace: "pre-wrap" }}>
        {rule.expression}
      </div>
    </div>
  );
}
