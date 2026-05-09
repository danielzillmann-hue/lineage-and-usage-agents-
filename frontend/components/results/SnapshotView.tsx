"use client";

import { ArrowRight, CheckCircle2, Circle, Loader2, AlertTriangle, Home } from "lucide-react";
import type { RunResults } from "@/lib/types";
import { ExecutiveSummaryView } from "./ExecutiveSummaryView";
import type { Stage, StageInfo, StageState } from "./JourneyRail";

const ICONS = {
  done: CheckCircle2,
  in_progress: Loader2,
  waiting: AlertTriangle,
  blocked: AlertTriangle,
  not_started: Circle,
} as const;

const TONE: Record<StageState, string> = {
  done: "var(--ok)",
  in_progress: "var(--info)",
  waiting: "var(--warn)",
  blocked: "var(--crit)",
  not_started: "var(--ink-3)",
};

const STATE_LABEL: Record<StageState, string> = {
  done: "Complete",
  in_progress: "Running",
  waiting: "Waiting on you",
  blocked: "Blocked",
  not_started: "Not started",
};

export function SnapshotView({
  stages, results, onJump, nextStage,
}: {
  stages: StageInfo[];
  results: RunResults;
  onJump: (s: Stage) => void;
  nextStage: Stage | null;
}) {
  // Drop "snapshot" itself from the journey timeline.
  const timeline = stages.filter((s) => s.key !== "snapshot");

  return (
    <div className="space-y-8">
      {/* Journey timeline cards */}
      <section>
        <div className="flex items-baseline justify-between mb-3">
          <h2 className="text-[15px] font-medium" style={{ color: "var(--ink)" }}>
            Migration journey
          </h2>
          <span className="text-[12px]" style={{ color: "var(--ink-3)" }}>
            Click a stage to open it
          </span>
        </div>
        <div className="grid gap-3" style={{ gridTemplateColumns: "repeat(5, minmax(0, 1fr))" }}>
          {timeline.map((s) => (
            <StageCard
              key={s.key}
              stage={s}
              isNext={nextStage === s.key}
              onClick={() => onJump(s.key)}
            />
          ))}
        </div>
      </section>

      {/* Existing executive summary, kept intact */}
      <section>
        <h2 className="text-[15px] font-medium mb-3" style={{ color: "var(--ink)" }}>
          Snapshot
        </h2>
        <ExecutiveSummaryView results={results} />
      </section>
    </div>
  );
}

function StageCard({
  stage, isNext, onClick,
}: {
  stage: StageInfo;
  isNext: boolean;
  onClick: () => void;
}) {
  const Icon = ICONS[stage.state];
  const color = TONE[stage.state];
  return (
    <button
      onClick={onClick}
      style={{
        textAlign: "left",
        background: "var(--bg-elev)",
        border: `1px solid ${isNext ? "var(--ink-2)" : "var(--line)"}`,
        boxShadow: isNext ? "0 0 0 3px rgba(0,0,0,0.04)" : "none",
        borderRadius: "var(--r-md)",
        padding: "14px 14px 12px",
        cursor: "pointer",
        position: "relative",
      }}
    >
      <div style={{ display: "flex", alignItems: "center", gap: 8 }}>
        <Icon
          className={`h-4 w-4 ${stage.state === "in_progress" ? "animate-spin" : ""}`}
          style={{ color, flexShrink: 0 }}
        />
        <span style={{ fontSize: 13.5, fontWeight: 600, color: "var(--ink)" }}>
          {stage.label}
        </span>
      </div>
      <div
        style={{
          marginTop: 6,
          fontSize: 11.5,
          color,
          textTransform: "uppercase",
          letterSpacing: "0.04em",
          fontWeight: 500,
        }}
      >
        {STATE_LABEL[stage.state]}
      </div>
      {stage.summary && (
        <div
          style={{
            marginTop: 8,
            fontSize: 12,
            color: "var(--ink-2)",
            lineHeight: 1.4,
            minHeight: "2.4em",
          }}
        >
          {stage.summary}
        </div>
      )}
      {isNext && (
        <div
          style={{
            marginTop: 10,
            display: "inline-flex",
            alignItems: "center",
            gap: 4,
            fontSize: 11.5,
            color: "var(--ink)",
            fontWeight: 500,
          }}
        >
          Open <ArrowRight className="h-3 w-3" />
        </div>
      )}
    </button>
  );
}
