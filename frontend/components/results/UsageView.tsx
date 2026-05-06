"use client";

import { useMemo } from "react";
import { Bar, BarChart, ResponsiveContainer, XAxis, YAxis, Tooltip, CartesianGrid, Cell } from "recharts";
import { Activity, AlertTriangle, FileText, ArrowRight } from "lucide-react";
import { Card, CardContent, CardDescription, CardHeader, CardTitle } from "@/components/ui/Card";
import { Badge } from "@/components/ui/Badge";
import type { Inventory, UsageReport } from "@/lib/types";
import { formatNumber } from "@/lib/utils";

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

  return (
    <div className="space-y-6">
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
            <div className="text-[40px] font-semibold tracking-tight text-white tabular-nums leading-none">
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
              <span className="font-mono text-white truncate">{it}</span>
              <ArrowRight className="h-3 w-3 text-[var(--color-fg-subtle)] flex-shrink-0" />
            </div>
          ))}
        </div>
      </CardContent>
    </Card>
  );
}
