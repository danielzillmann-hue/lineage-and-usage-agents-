"use client";

import { useEffect, useRef, useState } from "react";
import Link from "next/link";
import {
  Database, GitBranch, Activity, Sparkles, CheckCircle2, AlertCircle, Loader2, ArrowRight,
} from "lucide-react";
import { Card, CardContent, CardDescription, CardHeader, CardTitle } from "@/components/ui/Card";
import { Badge } from "@/components/ui/Badge";
import { Button } from "@/components/ui/Button";
import { Progress } from "@/components/ui/Progress";
import { api, streamRun } from "@/lib/api";
import { relativeTime } from "@/lib/utils";
import type { AgentName, AgentRunState, Run, StreamEvent } from "@/lib/types";

const AGENT_META: Record<AgentName, { title: string; icon: React.ComponentType<{ className?: string }>; tint: string }> = {
  inventory: { title: "Inventory",         icon: Database,  tint: "from-[#285294] to-[#7ebcf9]" },
  lineage:   { title: "Lineage",           icon: GitBranch, tint: "from-[#7ebcf9] to-[#00b4f0]" },
  usage:     { title: "Usage",             icon: Activity,  tint: "from-[#00b4f0] to-[#18c29c]" },
  summary:   { title: "Executive summary", icon: Sparkles,  tint: "from-[#ff6b47] to-[#f6b400]" },
};

interface AgentTranscriptEntry {
  agent: AgentName;
  kind: StreamEvent["event"];
  text: string;
  ts: string;
}

export function LiveRun({ runId }: { runId: string }) {
  const [run, setRun] = useState<Run | null>(null);
  const [transcript, setTranscript] = useState<AgentTranscriptEntry[]>([]);
  const [activeAgent, setActiveAgent] = useState<AgentName | null>(null);
  const [results, setResults] = useState<Record<AgentName, Record<string, unknown> | null>>({
    inventory: null, lineage: null, usage: null, summary: null,
  });
  const [done, setDone] = useState(false);
  const transcriptRef = useRef<HTMLDivElement>(null);

  useEffect(() => {
    api.getRun(runId).then(setRun).catch(() => {});
    const close = streamRun(runId, (ev) => {
      if (ev.agent) setActiveAgent(ev.agent);
      if (ev.event === "thinking" || ev.event === "log") {
        setTranscript((prev) => {
          // Coalesce consecutive thinking deltas from same agent for readability
          const last = prev[prev.length - 1];
          if (last && ev.event === "thinking" && last.kind === "thinking" && last.agent === ev.agent) {
            const merged = { ...last, text: last.text + (ev.message ?? "") };
            return [...prev.slice(0, -1), merged];
          }
          return [...prev, { agent: (ev.agent ?? activeAgent ?? "inventory"), kind: ev.event, text: ev.message ?? "", ts: ev.ts }];
        });
      }
      if (ev.event === "result" && ev.agent && ev.data) {
        setResults((prev) => ({ ...prev, [ev.agent!]: ev.data! }));
      }
      if (ev.event === "error") {
        setTranscript((p) => [...p, { agent: ev.agent ?? activeAgent ?? "inventory", kind: "error", text: ev.message ?? "Error", ts: ev.ts }]);
      }
      if (ev.event === "done") {
        setDone(true);
        api.getRun(runId).then(setRun).catch(() => {});
      }
    });
    const poll = setInterval(() => api.getRun(runId).then(setRun).catch(() => {}), 4000);
    return () => { close(); clearInterval(poll); };
  }, [runId, activeAgent]);

  useEffect(() => {
    if (!transcriptRef.current) return;
    transcriptRef.current.scrollTop = transcriptRef.current.scrollHeight;
  }, [transcript]);

  const completedCount = run?.agents.filter((a) => a.status === "completed").length ?? 0;
  const totalCount = run?.agents.length ?? 1;
  const overallPct = (completedCount / totalCount) * 100;

  return (
    <div className="space-y-6">
      <div className="flex flex-col md:flex-row md:items-end md:justify-between gap-4">
        <div>
          <div className="text-[11.5px] uppercase tracking-wider text-[var(--color-fg-subtle)]">Run</div>
          <h1 className="text-[26px] font-semibold tracking-tight text-[var(--ink)] mt-1 font-mono">{runId.slice(0, 8)}</h1>
          {run && (
            <div className="mt-2 flex items-center gap-3 text-[12px] text-[var(--color-fg-muted)]">
              <Badge variant={run.status === "completed" ? "ok" : run.status === "failed" ? "crit" : "info"}>
                {run.status === "running" && <Loader2 className="h-3 w-3 animate-spin" />}
                {run.status}
              </Badge>
              <span className="font-mono">{run.bucket}{run.prefix && `/${run.prefix}`}</span>
              <span>· started {relativeTime(run.created_at)}</span>
            </div>
          )}
        </div>
        {done && (
          <Link href={`/runs/${runId}/results`}>
            <Button size="lg">View results <ArrowRight className="h-4 w-4" /></Button>
          </Link>
        )}
      </div>

      <Card>
        <CardContent className="pt-5">
          <div className="flex items-center justify-between mb-2">
            <div className="text-[12px] text-[var(--color-fg-muted)]">{completedCount} of {totalCount} agents complete</div>
            <div className="text-[12px] font-mono text-[var(--color-cyan-soft)]">{Math.round(overallPct)}%</div>
          </div>
          <Progress value={overallPct} />
        </CardContent>
      </Card>

      <div className="grid grid-cols-1 lg:grid-cols-5 gap-6">
        <div className="lg:col-span-2 space-y-3">
          {run?.agents.map((a) => (
            <AgentCard key={a.name} state={a} result={results[a.name]} active={activeAgent === a.name} />
          ))}
        </div>

        <Card className="lg:col-span-3 flex flex-col min-h-[520px]">
          <CardHeader>
            <CardTitle>Agent transcript</CardTitle>
            <CardDescription>Live reasoning from each agent as it works.</CardDescription>
          </CardHeader>
          <CardContent className="flex-1 flex flex-col p-0">
            <div ref={transcriptRef} className="flex-1 overflow-y-auto px-5 pb-5 space-y-3">
              {transcript.length === 0 && (
                <div className="h-full flex items-center justify-center text-[12.5px] text-[var(--color-fg-subtle)]">
                  Waiting for first agent…
                </div>
              )}
              {transcript.map((t, i) => <TranscriptLine key={i} entry={t} />)}
            </div>
          </CardContent>
        </Card>
      </div>
    </div>
  );
}

function AgentCard({ state, result, active }: { state: AgentRunState; result: Record<string, unknown> | null; active: boolean }) {
  const meta = AGENT_META[state.name];
  const Icon = meta.icon;
  const StatusIcon =
    state.status === "completed" ? CheckCircle2 :
    state.status === "failed" ? AlertCircle :
    state.status === "running" ? Loader2 : null;
  return (
    <div className={`relative rounded-lg border p-4 transition ${
      active ? "border-[var(--color-cyan-accent)] bg-[var(--color-bg-elev-2)]/80 pulse-glow" :
      state.status === "completed" ? "border-[rgba(24,194,156,0.4)] bg-[var(--color-bg-elev-1)]/60" :
      state.status === "failed" ? "border-[rgba(244,71,107,0.5)]" :
      "border-[var(--color-border-soft)] bg-[var(--color-bg-elev-1)]/40"
    }`}>
      <div className="flex items-start gap-3">
        <div className={`flex-shrink-0 h-9 w-9 rounded-md bg-gradient-to-br ${meta.tint} flex items-center justify-center shadow-md`}>
          <Icon className="h-4 w-4 text-[var(--ink)] drop-shadow" />
        </div>
        <div className="flex-1 min-w-0">
          <div className="flex items-center justify-between gap-2">
            <div className="text-[13.5px] font-semibold text-[var(--ink)]">{meta.title}</div>
            {StatusIcon && (
              <StatusIcon className={`h-4 w-4 ${
                state.status === "completed" ? "text-[var(--color-emerald)]" :
                state.status === "failed" ? "text-[var(--color-rose)]" :
                "text-[var(--color-cyan-accent)] animate-spin"
              }`} />
            )}
          </div>
          <div className="mt-0.5 text-[11.5px] text-[var(--color-fg-muted)]">
            {state.status === "running" && "Working…"}
            {state.status === "completed" && state.completed_at && `Completed ${relativeTime(state.completed_at)}`}
            {state.status === "pending" && "Queued"}
            {state.status === "failed" && (state.error ?? "Failed")}
          </div>
          {result && (
            <div className="mt-2 grid grid-cols-2 gap-x-3 gap-y-0.5 text-[11.5px] font-mono">
              {Object.entries(result).slice(0, 6).map(([k, v]) => (
                <div key={k} className="flex items-center justify-between gap-2">
                  <span className="text-[var(--color-fg-subtle)] truncate">{k}</span>
                  <span className="text-[var(--color-cyan-soft)]">{typeof v === "object" ? JSON.stringify(v).slice(0, 18) : String(v)}</span>
                </div>
              ))}
            </div>
          )}
        </div>
      </div>
    </div>
  );
}

function TranscriptLine({ entry }: { entry: AgentTranscriptEntry }) {
  const meta = AGENT_META[entry.agent];
  const Icon = meta.icon;
  return (
    <div className="group flex items-start gap-3 text-[12.5px] leading-relaxed">
      <div className={`flex-shrink-0 mt-0.5 h-5 w-5 rounded bg-gradient-to-br ${meta.tint} flex items-center justify-center`}>
        <Icon className="h-3 w-3 text-[var(--ink)]" />
      </div>
      <div className="flex-1 min-w-0">
        <div className="flex items-baseline gap-2">
          <span className="text-[10.5px] uppercase tracking-wider text-[var(--color-fg-subtle)]">{meta.title}</span>
          {entry.kind === "thinking" && <span className="text-[10px] text-[var(--color-cyan-accent)]">thinking</span>}
          {entry.kind === "error" && <span className="text-[10px] text-[var(--color-rose)]">error</span>}
        </div>
        <div className={`whitespace-pre-wrap break-words ${entry.kind === "thinking" ? "text-[var(--color-fg-muted)] font-mono text-[11.5px]" : "text-[var(--ink)]"}`}>
          {entry.text}
        </div>
      </div>
    </div>
  );
}
