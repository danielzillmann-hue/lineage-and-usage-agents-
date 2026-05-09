"use client";

import { useEffect, useMemo, useState } from "react";
import Link from "next/link";
import { ArrowRight, Download, Loader2, MessageCircle } from "lucide-react";

import { Tabs, TabsContent, TabsList, TabsTrigger } from "@/components/ui/Tabs";
import { api } from "@/lib/api";
import { relativeTime } from "@/lib/utils";
import type { Run, RunResults } from "@/lib/types";
import type { TransformManifestResponse, VerificationReport } from "@/lib/api";

import { ExecutiveSummaryView } from "./ExecutiveSummaryView";
import { InventoryView } from "./InventoryView";
import { LineageView } from "./LineageView";
import { UsageView } from "./UsageView";
import { FindingsView } from "./FindingsView";
import { MigrationView } from "./MigrationView";
import { TransformView } from "./TransformView";
import { VerificationView } from "./VerificationView";
import { AskView } from "./AskView";
import { JourneyRail, type Stage, type StageInfo, type StageState } from "./JourneyRail";
import { SnapshotView } from "./SnapshotView";
import { DeployView, deployChecklistComplete, deployChecklistProgress } from "./DeployView";

const STAGES: Stage[] = ["snapshot", "discover", "plan", "generate", "deploy", "verify"];

const STAGE_LABELS: Record<Stage, string> = {
  snapshot: "Snapshot",
  discover: "Discover",
  plan: "Plan",
  generate: "Generate",
  deploy: "Deploy",
  verify: "Verify",
};

export function ResultsView({ runId }: { runId: string }) {
  const [run, setRun] = useState<Run | null>(null);
  const [results, setResults] = useState<RunResults | null>(null);
  const [stage, setStage] = useState<Stage>("snapshot");
  const [askOpen, setAskOpen] = useState(false);

  // Manifests fetched lazily; presence drives stage state (Generate / Verify).
  const [transformManifest, setTransformManifest] = useState<TransformManifestResponse | null>(null);
  const [verifyReport, setVerifyReport] = useState<VerificationReport | null>(null);

  // Sub-tab state for stages that have multiple lenses.
  const [discoverTab, setDiscoverTab] = useState("inventory");
  const [planTab, setPlanTab] = useState("migration");

  // Re-poll deploy checklist progress when localStorage changes (cross-tab) or
  // the user toggles a step in the Deploy view (forces a recompute).
  const [deployTick, setDeployTick] = useState(0);
  useEffect(() => {
    const onStorage = () => setDeployTick((n) => n + 1);
    window.addEventListener("storage", onStorage);
    return () => window.removeEventListener("storage", onStorage);
  }, []);

  useEffect(() => {
    api.getRun(runId).then(setRun).catch(() => {});
    api.getResults(runId).then(setResults).catch(() => {});
    api.transformManifest(runId).then(setTransformManifest).catch(() => setTransformManifest(null));
    api.verifyReport(runId).then(setVerifyReport).catch(() => setVerifyReport(null));
  }, [runId]);

  // When stage flips to "deploy", force a re-read of checklist progress on
  // mount (in case the user came back from another browser tab).
  useEffect(() => {
    if (stage === "deploy") setDeployTick((n) => n + 1);
  }, [stage]);

  const stageInfos = useMemo<StageInfo[]>(() => {
    return STAGES.map((key) => buildStageInfo(key, {
      results,
      run,
      transformManifest,
      verifyReport,
      runId,
      deployTick,
    }));
  }, [results, run, transformManifest, verifyReport, runId, deployTick]);

  const next = useMemo(() => firstActionableStage(stageInfos), [stageInfos]);

  if (!run || !results) {
    return (
      <div className="px-8 py-16 flex items-center gap-3" style={{ color: "var(--ink-3)" }}>
        <Loader2 className="h-4 w-4 animate-spin" /> Loading results…
      </div>
    );
  }

  const path = run.bucket
    ? `${run.bucket}${run.prefix ? "/" + run.prefix : ""}`
    : (run.oracle_dsn ?? "");

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
            <span>/</span>
            <span style={{ color: "var(--ink-2)" }}>{STAGE_LABELS[stage]}</span>
          </div>

          {/* Headline + meta + (next-action pill, ask, export) */}
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
                Analysis Results
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
            <div style={{ display: "flex", alignItems: "center", gap: 10 }}>
              {next && (
                <button
                  onClick={() => setStage(next.key)}
                  style={{
                    display: "inline-flex", alignItems: "center", gap: 8,
                    fontSize: 13, padding: "8px 14px",
                    background: "var(--ink)", color: "var(--bg)",
                    border: "1px solid var(--ink)", borderRadius: "var(--r-md)",
                    cursor: "pointer", fontWeight: 500,
                  }}
                  title="Jump to the next stage"
                >
                  Next: {next.action} <ArrowRight className="h-3.5 w-3.5" />
                </button>
              )}
              <button
                onClick={() => setAskOpen((v) => !v)}
                title="Ask the run anything"
                style={{
                  display: "inline-flex", alignItems: "center", gap: 6,
                  fontSize: 14, padding: "8px 12px",
                  background: askOpen ? "var(--bg)" : "var(--bg-elev)",
                  color: "var(--ink)",
                  border: "1px solid var(--line)", borderRadius: "var(--r-md)",
                  cursor: "pointer", fontWeight: 500,
                }}
              >
                <MessageCircle className="h-3.5 w-3.5" strokeWidth={1.25} /> Ask
              </button>
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
          </div>

          {/* Journey rail */}
          <JourneyRail
            stages={stageInfos}
            current={stage}
            onSelect={setStage}
          />
        </div>
      </div>

      {/* ─── Stage content ─────────────────────────────────── */}
      <div style={{ flex: 1, background: "var(--bg)" }}>
        {stage === "snapshot" && (
          <div style={{ maxWidth: 1400, margin: "0 auto", padding: "32px 32px 64px" }}>
            <SnapshotView
              stages={stageInfos}
              results={results}
              onJump={setStage}
              nextStage={next?.key ?? null}
            />
          </div>
        )}

        {stage === "discover" && (
          <Tabs value={discoverTab} onValueChange={setDiscoverTab}>
            <div style={{
              maxWidth: 1400, margin: "0 auto",
              padding: "16px 32px 0",
            }}>
              <TabsList>
                <TabsTrigger value="inventory">Inventory</TabsTrigger>
                <TabsTrigger value="lineage">Lineage</TabsTrigger>
                <TabsTrigger value="usage">Usage</TabsTrigger>
              </TabsList>
            </div>
            <TabsContent value="inventory" className="mt-0">
              <div style={{ maxWidth: 1400, margin: "0 auto", padding: "20px 32px 64px" }}>
                <InventoryView inventory={results.inventory} runId={runId} />
              </div>
            </TabsContent>
            <TabsContent value="lineage" className="mt-0">
              <div style={{ marginTop: 0 }}>
                <LineageView lineage={results.lineage} inventory={results.inventory} runId={runId} />
              </div>
            </TabsContent>
            <TabsContent value="usage" className="mt-0">
              <div style={{ maxWidth: 1400, margin: "0 auto", padding: "20px 32px 64px" }}>
                <UsageView usage={results.usage} inventory={results.inventory} />
              </div>
            </TabsContent>
          </Tabs>
        )}

        {stage === "plan" && (
          <Tabs value={planTab} onValueChange={setPlanTab}>
            <div style={{
              maxWidth: 1400, margin: "0 auto",
              padding: "16px 32px 0",
            }}>
              <TabsList>
                <TabsTrigger value="migration">Migration</TabsTrigger>
                <TabsTrigger value="findings">
                  Findings
                  {(results.summary?.findings.length ?? 0) > 0 && (
                    <span
                      className="mono"
                      style={{
                        marginLeft: 8, fontSize: 11, padding: "2px 7px",
                        background: "var(--crit-bg)", color: "var(--crit)",
                        borderRadius: 99, fontWeight: 500,
                      }}
                    >
                      {results.summary?.findings.length}
                    </span>
                  )}
                </TabsTrigger>
              </TabsList>
            </div>
            <TabsContent value="migration" className="mt-0">
              <div style={{ maxWidth: 1400, margin: "0 auto", padding: "20px 32px 64px" }}>
                <MigrationView inventory={results.inventory} runId={runId} />
              </div>
            </TabsContent>
            <TabsContent value="findings" className="mt-0">
              <div style={{ maxWidth: 1400, margin: "0 auto", padding: "20px 32px 64px" }}>
                <FindingsView findings={results.summary?.findings ?? []} />
              </div>
            </TabsContent>
          </Tabs>
        )}

        {stage === "generate" && (
          <div style={{ marginTop: 0 }}>
            <TransformView runId={runId} />
          </div>
        )}

        {stage === "deploy" && (
          <div style={{ maxWidth: 1100, margin: "0 auto", padding: "32px 32px 64px" }}>
            <DeployView runId={runId} />
          </div>
        )}

        {stage === "verify" && (
          <div style={{ maxWidth: 1400, margin: "0 auto", padding: "32px 32px 64px" }}>
            <VerificationView runId={runId} />
          </div>
        )}
      </div>

      {/* Ask drawer */}
      {askOpen && (
        <div
          style={{
            position: "fixed", top: 0, right: 0, bottom: 0,
            width: 480, maxWidth: "94vw",
            background: "var(--bg)",
            borderLeft: "1px solid var(--line)",
            zIndex: 50,
            display: "flex", flexDirection: "column",
            boxShadow: "-12px 0 24px rgba(0,0,0,0.06)",
          }}
        >
          <div style={{
            padding: "14px 16px",
            borderBottom: "1px solid var(--line)",
            display: "flex", alignItems: "center", justifyContent: "space-between",
          }}>
            <span style={{ fontWeight: 500, fontSize: 14, color: "var(--ink)" }}>
              Ask the run
            </span>
            <button
              onClick={() => setAskOpen(false)}
              style={{ background: "none", border: "none", cursor: "pointer", color: "var(--ink-3)", fontSize: 14 }}
              aria-label="Close"
            >
              ✕
            </button>
          </div>
          <div style={{ flex: 1, overflow: "auto" }}>
            <AskView runId={runId} />
          </div>
        </div>
      )}
    </div>
  );
}


// ─── Stage state computation ──────────────────────────────────────────────


type StageContext = {
  results: RunResults | null;
  run: Run | null;
  transformManifest: TransformManifestResponse | null;
  verifyReport: VerificationReport | null;
  runId: string;
  deployTick: number;
};

function buildStageInfo(key: Stage, ctx: StageContext): StageInfo {
  switch (key) {
    case "snapshot":
      return { key, label: "Snapshot", state: "done", summary: "Run overview" };

    case "discover": {
      const inv = ctx.results?.inventory;
      if (!inv) return { key, label: "Discover", state: "in_progress", summary: "Inventorying Oracle…" };
      const tables = inv.tables.length;
      const procs = inv.procedures.length;
      return {
        key, label: "Discover", state: "done",
        summary: `${tables} tables · ${procs} procedures`,
      };
    }

    case "plan": {
      const sum = ctx.results?.summary;
      if (!sum) {
        return { key, label: "Plan", state: ctx.results?.inventory ? "in_progress" : "not_started",
                 summary: "Building findings…" };
      }
      const crit = sum.findings.filter((f) => f.severity === "critical").length;
      const warn = sum.findings.filter((f) => f.severity === "warn").length;
      return {
        key, label: "Plan", state: "done",
        summary:
          crit + warn === 0
            ? `${sum.findings.length} findings`
            : `${crit} critical · ${warn} warn`,
      };
    }

    case "generate": {
      if (!ctx.transformManifest) {
        return { key, label: "Generate", state: ctx.results?.inventory ? "waiting" : "not_started",
                 summary: ctx.results?.inventory ? "Ready to generate" : "—" };
      }
      const m = ctx.transformManifest;
      return {
        key, label: "Generate", state: "done",
        summary: `${m.pipelines.length} pipelines · ${m.sources.length} sources · ${m.files.length} files`,
      };
    }

    case "deploy": {
      const generated = !!ctx.transformManifest;
      if (!generated) {
        return { key, label: "Deploy", state: "not_started",
                 summary: "Generate first" };
      }
      const progress = deployChecklistProgress(ctx.runId);
      const done = deployChecklistComplete(ctx.runId);
      return {
        key, label: "Deploy",
        state: done ? "done" : (progress.done > 0 ? "in_progress" : "waiting"),
        summary: `${progress.done} / ${progress.total} steps`,
      };
    }

    case "verify": {
      const generated = !!ctx.transformManifest;
      const deployDone = deployChecklistComplete(ctx.runId);
      if (!generated) {
        return { key, label: "Verify", state: "not_started", summary: "Deploy first" };
      }
      if (!ctx.verifyReport) {
        return {
          key, label: "Verify",
          state: deployDone ? "waiting" : "not_started",
          summary: deployDone ? "Ready to verify" : "Finish deploy",
        };
      }
      const s = ctx.verifyReport.summary;
      const drift = s.drifted + s.missing + s.errors;
      return {
        key, label: "Verify",
        state: drift === 0 ? "done" : "blocked",
        summary: drift === 0
          ? `${s.matched} / ${s.total} match`
          : `${drift} need attention`,
      };
    }
  }
}


type NextAction = { key: Stage; action: string };


function firstActionableStage(infos: StageInfo[]): NextAction | null {
  for (const s of infos) {
    if (s.key === "snapshot") continue;
    if (s.state === "waiting") {
      return { key: s.key, action: stageActionLabel(s.key) };
    }
    if (s.state === "blocked") {
      return { key: s.key, action: "Investigate " + s.key };
    }
  }
  // No waiting stages — first not_started becomes the next.
  for (const s of infos) {
    if (s.key === "snapshot") continue;
    if (s.state === "not_started") {
      return { key: s.key, action: stageActionLabel(s.key) };
    }
  }
  return null;
}


function stageActionLabel(s: Stage): string {
  switch (s) {
    case "discover": return "Run analysis";
    case "plan":     return "Review findings";
    case "generate": return "Generate Dataform project";
    case "deploy":   return "Deploy to BigQuery";
    case "verify":   return "Run verification";
    default:         return "Open " + s;
  }
}
