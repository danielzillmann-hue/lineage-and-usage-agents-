"use client";

import { useState } from "react";
import { AlertTriangle, AlertCircle, Info, ChevronDown, ChevronRight, Lightbulb } from "lucide-react";
import { Card, CardContent } from "@/components/ui/Card";
import { Badge } from "@/components/ui/Badge";
import type { Finding } from "@/lib/types";

const SEV_META = {
  critical: { Icon: AlertCircle, badge: "crit" as const, tint: "text-[var(--color-rose)]", border: "border-[rgba(244,71,107,0.4)]" },
  warn:     { Icon: AlertTriangle, badge: "warn" as const, tint: "text-[var(--color-amber)]", border: "border-[rgba(246,180,0,0.4)]" },
  info:     { Icon: Info, badge: "info" as const, tint: "text-[var(--color-cyan-soft)]", border: "border-[rgba(0,180,240,0.4)]" },
};

export function FindingsView({ findings }: { findings: Finding[] }) {
  if (findings.length === 0) {
    return (
      <Card>
        <CardContent className="py-16 text-center text-[var(--color-fg-muted)]">
          No findings — clean run.
        </CardContent>
      </Card>
    );
  }
  const sorted = [...findings].sort((a, b) => severityOrder(a.severity) - severityOrder(b.severity));
  return (
    <div className="space-y-3">
      {sorted.map((f, i) => <FindingCard key={i} finding={f} />)}
    </div>
  );
}

function FindingCard({ finding }: { finding: Finding }) {
  const [expanded, setExpanded] = useState(false);
  const sev = SEV_META[finding.severity as keyof typeof SEV_META] ?? SEV_META.info;
  const Icon = sev.Icon;
  return (
    <div className={`rounded-xl border bg-[var(--color-bg-elev-1)]/60 backdrop-blur ${sev.border}`}>
      <button onClick={() => setExpanded((v) => !v)} className="w-full text-left p-4 flex items-start gap-3 hover:bg-white/[0.02] transition rounded-xl">
        <Icon className={`h-5 w-5 ${sev.tint} flex-shrink-0 mt-0.5`} />
        <div className="flex-1 min-w-0">
          <div className="flex items-center gap-2 flex-wrap">
            <Badge variant={sev.badge}>{finding.severity}</Badge>
            <span className="text-[14.5px] font-semibold text-[var(--ink)]">{finding.title}</span>
          </div>
          <p className="mt-1 text-[12.5px] text-[var(--color-fg-muted)] leading-relaxed">{finding.detail}</p>
          {finding.object_fqns.length > 0 && (
            <div className="mt-2 flex flex-wrap gap-1.5">
              {finding.object_fqns.slice(0, 6).map((o) => (
                <span key={o} className="font-mono text-[10.5px] text-[var(--color-cyan-soft)] bg-[rgba(0,180,240,0.08)] border border-[rgba(0,180,240,0.25)] rounded px-1.5 py-0.5">{o}</span>
              ))}
              {finding.object_fqns.length > 6 && (
                <span className="text-[10.5px] text-[var(--color-fg-subtle)]">+{finding.object_fqns.length - 6} more</span>
              )}
            </div>
          )}
        </div>
        {expanded ? <ChevronDown className="h-4 w-4 text-[var(--color-fg-subtle)]" /> : <ChevronRight className="h-4 w-4 text-[var(--color-fg-subtle)]" />}
      </button>
      {expanded && finding.recommendation && (
        <div className="px-4 pb-4 ml-8 pl-3 border-l border-[var(--color-border-soft)]">
          <div className="flex items-start gap-2 text-[12.5px] text-[var(--color-fg)]">
            <Lightbulb className="h-3.5 w-3.5 text-[var(--color-amber)] mt-0.5 flex-shrink-0" />
            <span>{finding.recommendation}</span>
          </div>
        </div>
      )}
    </div>
  );
}

function severityOrder(s: string): number {
  return s === "critical" ? 0 : s === "warn" ? 1 : 2;
}
