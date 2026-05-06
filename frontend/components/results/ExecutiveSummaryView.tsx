"use client";

import { Database, GitBranch, Activity, AlertTriangle, CheckCircle2, FileText } from "lucide-react";
import { Card, CardContent, CardDescription, CardHeader, CardTitle } from "@/components/ui/Card";
import { Badge } from "@/components/ui/Badge";
import type { RunResults } from "@/lib/types";
import { formatNumber } from "@/lib/utils";

export function ExecutiveSummaryView({ results }: { results: RunResults }) {
  const inv = results.inventory;
  const lin = results.lineage;
  const usg = results.usage;
  const sum = results.summary;

  const tableCount = inv?.tables.length ?? 0;
  const procCount = inv?.procedures.length ?? 0;
  const edgeCount = lin?.edges.length ?? 0;
  const hotCount = usg?.hot_tables.length ?? 0;
  const orphanCount = usg?.write_only_orphans.length ?? 0;
  const deadCount = usg?.dead_objects.length ?? 0;
  const reachable = usg?.reporting_reachable_sources.length ?? 0;
  const unreachable = usg?.reporting_unreachable_sources.length ?? 0;
  const reachPct = reachable + unreachable === 0 ? 0 : (reachable / (reachable + unreachable)) * 100;

  const critFindings = sum?.findings.filter((f) => f.severity === "critical").length ?? 0;
  const warnFindings = sum?.findings.filter((f) => f.severity === "warn").length ?? 0;

  return (
    <div className="space-y-6">
      <div className="grid grid-cols-2 lg:grid-cols-4 gap-4">
        <MetricCard label="Tables &amp; views" value={formatNumber(tableCount)} sub={`${formatNumber(procCount)} procedures`} icon={Database} tint="from-[#285294] to-[#7ebcf9]" />
        <MetricCard label="Lineage edges" value={formatNumber(edgeCount)} sub="column-level mappings" icon={GitBranch} tint="from-[#7ebcf9] to-[#00b4f0]" />
        <MetricCard label="Reporting-reachable" value={`${reachPct.toFixed(0)}%`} sub={`${reachable} of ${reachable + unreachable} raw sources`} icon={Activity} tint="from-[#00b4f0] to-[#18c29c]" />
        <MetricCard label="Findings" value={formatNumber((sum?.findings.length) ?? 0)} sub={`${critFindings} critical · ${warnFindings} warn`} icon={AlertTriangle} tint="from-[#ff6b47] to-[#f6b400]" />
      </div>

      {sum && (
        <Card>
          <CardHeader>
            <CardTitle>Headline</CardTitle>
            <CardDescription>Synthesized by Gemini 2.5 Pro from the agents&apos; outputs.</CardDescription>
          </CardHeader>
          <CardContent className="space-y-4">
            <p className="text-[15px] leading-relaxed text-[var(--ink)] text-pretty">{sum.headline}</p>
            <ul className="space-y-2">
              {sum.bullets.map((b, i) => (
                <li key={i} className="flex items-start gap-2.5 text-[13.5px] leading-relaxed text-[var(--color-fg)]">
                  <CheckCircle2 className="h-4 w-4 text-[var(--color-cyan-accent)] flex-shrink-0 mt-0.5" />
                  <span>{b}</span>
                </li>
              ))}
            </ul>
          </CardContent>
        </Card>
      )}

      <div className="grid grid-cols-1 md:grid-cols-3 gap-4">
        <DistributionCard title="Hot tables" count={hotCount} description="Most-read objects in the AWR window" icon={Activity} tint="text-[var(--color-cyan-soft)]" />
        <DistributionCard title="Write-only orphans" count={orphanCount} description="ETL targets nobody queries" icon={FileText} tint="text-[var(--color-amber)]" />
        <DistributionCard title="Dead objects" count={deadCount} description="No reads or writes in the window" icon={AlertTriangle} tint="text-[var(--color-rose)]" />
      </div>

      {sum && Object.keys(sum.metrics).length > 0 && (
        <Card>
          <CardHeader><CardTitle>Key metrics</CardTitle></CardHeader>
          <CardContent>
            <div className="grid grid-cols-2 md:grid-cols-4 gap-4">
              {Object.entries(sum.metrics).map(([k, v]) => (
                <div key={k} className="rounded-md border border-[var(--color-border-soft)] bg-[var(--color-bg-elev-1)]/40 p-3">
                  <div className="text-[10.5px] uppercase tracking-wider text-[var(--color-fg-subtle)]">{k.replaceAll("_", " ")}</div>
                  <div className="mt-1 text-[18px] font-semibold text-[var(--ink)] tabular-nums">{String(v)}</div>
                </div>
              ))}
            </div>
          </CardContent>
        </Card>
      )}
    </div>
  );
}

function MetricCard({ label, value, sub, icon: Icon, tint }: {
  label: React.ReactNode; value: string; sub: string; icon: React.ComponentType<{ className?: string }>; tint: string;
}) {
  return (
    <Card>
      <CardContent className="pt-5">
        <div className="flex items-start justify-between mb-3">
          <div className={`h-9 w-9 rounded-md bg-gradient-to-br ${tint} flex items-center justify-center shadow-md`}>
            <Icon className="h-4 w-4 text-[var(--ink)] drop-shadow" />
          </div>
        </div>
        <div className="text-[11.5px] uppercase tracking-wider text-[var(--color-fg-subtle)]">{label}</div>
        <div className="mt-1 text-[28px] font-semibold tracking-tight text-[var(--ink)] tabular-nums">{value}</div>
        <div className="mt-0.5 text-[11.5px] text-[var(--color-fg-muted)]">{sub}</div>
      </CardContent>
    </Card>
  );
}

function DistributionCard({ title, count, description, icon: Icon, tint }: {
  title: string; count: number; description: string; icon: React.ComponentType<{ className?: string }>; tint: string;
}) {
  return (
    <Card>
      <CardContent className="pt-5">
        <div className="flex items-center justify-between">
          <div className="text-[12.5px] font-semibold text-[var(--ink)]">{title}</div>
          <Icon className={`h-4 w-4 ${tint}`} />
        </div>
        <div className="mt-1.5 flex items-baseline gap-2">
          <span className="text-[24px] font-semibold text-[var(--ink)] tabular-nums">{count}</span>
          <Badge variant="neutral">objects</Badge>
        </div>
        <div className="mt-1 text-[11.5px] text-[var(--color-fg-muted)]">{description}</div>
      </CardContent>
    </Card>
  );
}
