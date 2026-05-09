"use client";

import { CheckCircle2, Circle, Loader2, AlertTriangle, Home } from "lucide-react";

export type Stage =
  | "snapshot"
  | "discover"
  | "plan"
  | "generate"
  | "deploy"
  | "verify";

export type StageState =
  | "done"
  | "in_progress"
  | "waiting"
  | "blocked"
  | "not_started";

export type StageInfo = {
  key: Stage;
  label: string;
  state: StageState;
  summary?: string;
};

const ICONS = {
  done: CheckCircle2,
  in_progress: Loader2,
  waiting: AlertTriangle,
  blocked: AlertTriangle,
  not_started: Circle,
} as const;

const TONE: Record<StageState, { color: string; ring: string }> = {
  done:        { color: "var(--ok)",    ring: "rgba(15,179,122,0.25)" },
  in_progress: { color: "var(--info)",  ring: "rgba(46,111,180,0.25)" },
  waiting:     { color: "var(--warn)",  ring: "rgba(199,123,10,0.30)" },
  blocked:     { color: "var(--crit)",  ring: "rgba(192,54,44,0.30)" },
  not_started: { color: "var(--ink-3)", ring: "var(--line)" },
};

export function JourneyRail({
  stages, current, onSelect,
}: {
  stages: StageInfo[];
  current: Stage;
  onSelect: (s: Stage) => void;
}) {
  return (
    <div
      style={{
        display: "grid",
        gridTemplateColumns: `auto repeat(${stages.length - 1}, 1fr)`,
        gap: 8,
        marginTop: 18,
        marginBottom: -1,  // sit flush with content border-top
      }}
    >
      {stages.map((s, i) => (
        <Cell
          key={s.key}
          stage={s}
          isFirst={i === 0}
          active={current === s.key}
          onClick={() => onSelect(s.key)}
        />
      ))}
    </div>
  );
}

function Cell({
  stage, active, onClick, isFirst,
}: {
  stage: StageInfo;
  active: boolean;
  onClick: () => void;
  isFirst: boolean;
}) {
  const tone = TONE[stage.state];
  const Icon = stage.key === "snapshot" ? Home : ICONS[stage.state];
  const animated = stage.state === "in_progress";
  return (
    <button
      onClick={onClick}
      style={{
        textAlign: "left",
        padding: "12px 14px",
        background: active ? "var(--bg)" : "var(--bg-elev)",
        borderTop: `1px solid var(--line)`,
        borderLeft: `1px solid var(--line)`,
        borderRight: `1px solid var(--line)`,
        borderBottom: active ? "1px solid var(--bg)" : "1px solid var(--line)",
        borderTopLeftRadius: "var(--r-md)",
        borderTopRightRadius: "var(--r-md)",
        cursor: "pointer",
        position: "relative",
        outline: "none",
        minWidth: isFirst ? 92 : undefined,
      }}
    >
      <div style={{ display: "flex", alignItems: "center", gap: 8 }}>
        <Icon
          className={`h-3.5 w-3.5 ${animated ? "animate-spin" : ""}`}
          style={{ color: tone.color, flexShrink: 0 }}
        />
        <span
          style={{
            fontSize: 13,
            fontWeight: active ? 600 : 500,
            color: active ? "var(--ink)" : "var(--ink-2)",
          }}
        >
          {stage.label}
        </span>
      </div>
      {stage.summary && (
        <div
          style={{
            marginTop: 4,
            fontSize: 11.5,
            color: active ? "var(--ink-3)" : "var(--ink-3)",
            lineHeight: 1.35,
            whiteSpace: "nowrap",
            overflow: "hidden",
            textOverflow: "ellipsis",
          }}
        >
          {stage.summary}
        </div>
      )}
    </button>
  );
}
