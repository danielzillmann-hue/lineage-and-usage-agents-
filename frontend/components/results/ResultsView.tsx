"use client";

import { useEffect, useState } from "react";
import Link from "next/link";
import { Download, Loader2 } from "lucide-react";
import { Tabs, TabsContent, TabsList, TabsTrigger } from "@/components/ui/Tabs";
import { api } from "@/lib/api";
import { relativeTime } from "@/lib/utils";
import type { Run, RunResults } from "@/lib/types";

import { ExecutiveSummaryView } from "./ExecutiveSummaryView";
import { InventoryView } from "./InventoryView";
import { LineageView } from "./LineageView";
import { UsageView } from "./UsageView";
import { FindingsView } from "./FindingsView";
import { MigrationView } from "./MigrationView";

export function ResultsView({ runId }: { runId: string }) {
  const [run, setRun] = useState<Run | null>(null);
  const [results, setResults] = useState<RunResults | null>(null);
  const [tab, setTab] = useState("overview");

  useEffect(() => {
    api.getRun(runId).then(setRun).catch(() => {});
    api.getResults(runId).then(setResults).catch(() => {});
  }, [runId]);

  if (!run || !results) {
    return (
      <div className="px-8 py-16 flex items-center gap-3" style={{ color: "var(--ink-3)" }}>
        <Loader2 className="h-4 w-4 animate-spin" /> Loading results…
      </div>
    );
  }

  const findingCount = results.summary?.findings.length ?? 0;
  const path = run.bucket ? `${run.bucket}${run.prefix ? "/" + run.prefix : ""}` : (run.oracle_dsn ?? "");

  return (
    <div className="flex-1 flex flex-col" style={{ background: "var(--bg)" }}>
      {/* ─── Run header strip ─────────────────────────────── */}
      <div style={{ padding: "28px 32px 0", borderBottom: "1px solid var(--line)", background: "var(--bg-elev)" }}>
        <div style={{ maxWidth: 1400, margin: "0 auto" }}>
          {/* Breadcrumbs */}
          <div
            style={{
              display: "flex", alignItems: "center", gap: 10,
              color: "var(--ink-3)", fontSize: 13, marginBottom: 8,
            }}
          >
            <Link href="/runs" style={{ color: "var(--ink-3)", textDecoration: "none" }}>
              ← Runs
            </Link>
            <span>/</span>
            <span className="mono" style={{ color: "var(--ink-2)" }}>{runId.slice(0, 8)}</span>
          </div>

          {/* Headline + meta + export */}
          <div style={{ display: "flex", justifyContent: "space-between", alignItems: "flex-end", gap: 24 }}>
            <div>
              <h1
                className="text-balance"
                style={{
                  fontFamily: "var(--font-sans)", fontSize: 28, fontWeight: 500,
                  lineHeight: 1.2, letterSpacing: "-0.018em", margin: 0, maxWidth: 900,
                  color: "var(--ink)",
                }}
              >
                {results.summary?.headline ?? "Analysis complete"}
              </h1>
              <div
                style={{
                  display: "flex", alignItems: "center", gap: 14,
                  marginTop: 12, fontSize: 13, color: "var(--ink-3)",
                  flexWrap: "wrap",
                }}
              >
                <span style={{ display: "inline-flex", alignItems: "center", gap: 6, color: run.status === "completed" ? "var(--ok)" : run.status === "failed" ? "var(--crit)" : "var(--warn)" }}>
                  <span className={`dot ${run.status === "completed" ? "ok" : run.status === "failed" ? "crit" : "warn"}`} />
                  {run.status}
                </span>
                <span>·</span>
                <span className="mono">{path}</span>
                <span>·</span>
                <span>{relativeTime(run.created_at)}</span>
              </div>
            </div>
            <a
              href={`${process.env.NEXT_PUBLIC_API_BASE ?? "http://localhost:8080"}/api/runs/${runId}/handover.html`}
              target="_blank"
              rel="noopener noreferrer"
              title="Open handover document — File > Save as PDF for a print copy"
              style={{
                display: "inline-flex", alignItems: "center", gap: 8,
                fontSize: 14, padding: "8px 14px",
                background: "var(--bg-elev)", color: "var(--ink)",
                border: "1px solid var(--line)", borderRadius: "var(--r-md)",
                cursor: "pointer", fontWeight: 500, textDecoration: "none",
              }}
            >
              <Download className="h-3.5 w-3.5" strokeWidth={1.25} /> Export
            </a>
          </div>

          {/* Tabs strip */}
          <Tabs value={tab} onValueChange={setTab}>
            <TabsList className="mt-6 -mb-px">
              <TabsTrigger value="overview">Overview</TabsTrigger>
              <TabsTrigger value="inventory">Inventory</TabsTrigger>
              <TabsTrigger value="lineage">Lineage</TabsTrigger>
              <TabsTrigger value="usage">Usage</TabsTrigger>
              <TabsTrigger value="migration">Migration</TabsTrigger>
              <TabsTrigger value="findings">
                Findings
                {findingCount > 0 && (
                  <span
                    className="mono"
                    style={{
                      marginLeft: 8, fontSize: 11, padding: "2px 7px",
                      background: "var(--crit-bg)", color: "var(--crit)",
                      borderRadius: 99, fontWeight: 500,
                    }}
                  >
                    {findingCount}
                  </span>
                )}
              </TabsTrigger>
            </TabsList>

            {/* ─── Tab bodies (rendered below the strip) ─── */}
            <TabsContent value="overview" className="mt-0">
              <div style={{ maxWidth: 1400, margin: "0 auto", padding: "32px 32px 64px" }}>
                <ExecutiveSummaryView results={results} />
              </div>
            </TabsContent>
            <TabsContent value="inventory" className="mt-0">
              <div style={{ maxWidth: 1400, margin: "0 auto", padding: "32px 32px 64px" }}>
                <InventoryView inventory={results.inventory} runId={runId} />
              </div>
            </TabsContent>
            <TabsContent value="lineage" className="mt-0">
              <div style={{ marginTop: 0 }}>
                <LineageView lineage={results.lineage} inventory={results.inventory} />
              </div>
            </TabsContent>
            <TabsContent value="usage" className="mt-0">
              <div style={{ maxWidth: 1400, margin: "0 auto", padding: "32px 32px 64px" }}>
                <UsageView usage={results.usage} inventory={results.inventory} />
              </div>
            </TabsContent>
            <TabsContent value="migration" className="mt-0">
              <div style={{ maxWidth: 1400, margin: "0 auto", padding: "32px 32px 64px" }}>
                <MigrationView inventory={results.inventory} runId={runId} />
              </div>
            </TabsContent>
            <TabsContent value="findings" className="mt-0">
              <div style={{ maxWidth: 1400, margin: "0 auto", padding: "32px 32px 64px" }}>
                <FindingsView findings={results.summary?.findings ?? []} />
              </div>
            </TabsContent>
          </Tabs>
        </div>
      </div>
    </div>
  );
}
