"use client";

import { useEffect, useState } from "react";
import {
  CheckCircle2, Circle, ExternalLink,
  Database, PlayCircle,
  GitPullRequestArrow,
} from "lucide-react";

const STORAGE_KEY = (runId: string) => `deploy-checklist:${runId}`;

type StepKey =
  | "github_pushed"
  | "workspace_pointed"
  | "compile_clean"
  | "raw_loaded"
  | "pipelines_executed";

type Step = {
  key: StepKey;
  label: string;
  detail: string;
  Icon: typeof GitPullRequestArrow;
  externalLabel?: string;
};

const STEPS: Step[] = [
  {
    key: "github_pushed",
    label: "Push the project to GitHub",
    detail: "Use the Generate stage's “Push to GitHub” button. The agent force-pushes the latest output to your branch.",
    Icon: GitPullRequestArrow,
  },
  {
    key: "workspace_pointed",
    label: "Point your Dataform workspace at the latest branch",
    detail: "In the Dataform UI: delete + recreate the workspace against the same branch, or hit “Pull from remote” if the workspace is already there.",
    Icon: ExternalLink,
    externalLabel: "Open Dataform",
  },
  {
    key: "compile_clean",
    label: "Confirm Dataform compile is clean",
    detail: "Open the workspace, run “Compile”. Should pass with zero errors after the latest fixes.",
    Icon: CheckCircle2,
  },
  {
    key: "raw_loaded",
    label: "Replicate Oracle data into migration_raw",
    detail: "Run scripts/load_raw_layer.py once to populate the raw dataset before pipelines can find their sources.",
    Icon: Database,
  },
  {
    key: "pipelines_executed",
    label: "Execute the Dataform pipelines",
    detail: "From the workspace: Start execution → Execute all actions → Include dependencies → Start. Wait for it to finish before running Verify.",
    Icon: PlayCircle,
  },
];

export function DeployView({ runId }: { runId: string }) {
  const [done, setDone] = useState<Record<StepKey, boolean>>({
    github_pushed: false,
    workspace_pointed: false,
    compile_clean: false,
    raw_loaded: false,
    pipelines_executed: false,
  });

  // Load from localStorage so progress survives reloads.
  useEffect(() => {
    if (typeof window === "undefined") return;
    try {
      const raw = window.localStorage.getItem(STORAGE_KEY(runId));
      if (raw) setDone(JSON.parse(raw));
    } catch {
      /* ignore */
    }
  }, [runId]);

  const toggle = (k: StepKey) => {
    setDone((prev) => {
      const next = { ...prev, [k]: !prev[k] };
      try {
        window.localStorage.setItem(STORAGE_KEY(runId), JSON.stringify(next));
      } catch {
        /* ignore */
      }
      return next;
    });
  };

  const completedCount = Object.values(done).filter(Boolean).length;

  return (
    <div className="space-y-6">
      <div>
        <h2 className="text-[15px] font-medium" style={{ color: "var(--ink)" }}>
          Deploy
        </h2>
        <p
          className="mt-1 text-[12.5px]"
          style={{ color: "var(--ink-3)", maxWidth: 720 }}
        >
          Manual handoff steps to materialise the generated Dataform project in
          your BigQuery environment. Once all five are done, run Verify on the
          next stage to compare Oracle ↔ BigQuery.
        </p>
        <div
          className="mt-3 text-[12px]"
          style={{ color: "var(--ink-3)" }}
        >
          {completedCount} / {STEPS.length} complete
        </div>
      </div>

      <ol className="space-y-2">
        {STEPS.map((s, i) => (
          <DeployStep
            key={s.key}
            step={s}
            index={i + 1}
            checked={done[s.key]}
            onToggle={() => toggle(s.key)}
          />
        ))}
      </ol>
    </div>
  );
}

function DeployStep({
  step, index, checked, onToggle,
}: {
  step: Step;
  index: number;
  checked: boolean;
  onToggle: () => void;
}) {
  const Icon = checked ? CheckCircle2 : Circle;
  return (
    <li
      style={{
        background: "var(--bg-elev)",
        border: `1px solid var(--line)`,
        borderRadius: "var(--r-md)",
        padding: "14px 16px",
        opacity: checked ? 0.78 : 1,
      }}
    >
      <div className="flex items-start gap-3">
        <button
          onClick={onToggle}
          style={{ background: "none", border: "none", padding: 0, cursor: "pointer", flexShrink: 0, marginTop: 2 }}
          aria-label={checked ? "Mark incomplete" : "Mark complete"}
        >
          <Icon
            className="h-5 w-5"
            style={{ color: checked ? "var(--ok)" : "var(--ink-3)" }}
          />
        </button>
        <div className="flex-1 min-w-0">
          <div className="flex items-center gap-2">
            <span
              className="mono"
              style={{ fontSize: 11, color: "var(--ink-3)" }}
            >
              Step {index}
            </span>
            <span
              style={{
                fontSize: 13.5,
                fontWeight: 500,
                color: checked ? "var(--ink-3)" : "var(--ink)",
                textDecoration: checked ? "line-through" : "none",
              }}
            >
              {step.label}
            </span>
          </div>
          <p
            className="mt-1 text-[12px]"
            style={{ color: "var(--ink-3)", lineHeight: 1.5 }}
          >
            {step.detail}
          </p>
        </div>
      </div>
    </li>
  );
}

export function deployChecklistComplete(runId: string): boolean {
  if (typeof window === "undefined") return false;
  try {
    const raw = window.localStorage.getItem(STORAGE_KEY(runId));
    if (!raw) return false;
    const parsed = JSON.parse(raw) as Record<StepKey, boolean>;
    return STEPS.every((s) => !!parsed[s.key]);
  } catch {
    return false;
  }
}

export function deployChecklistProgress(runId: string): { done: number; total: number } {
  if (typeof window === "undefined") return { done: 0, total: STEPS.length };
  try {
    const raw = window.localStorage.getItem(STORAGE_KEY(runId));
    if (!raw) return { done: 0, total: STEPS.length };
    const parsed = JSON.parse(raw) as Record<StepKey, boolean>;
    return {
      done: STEPS.filter((s) => !!parsed[s.key]).length,
      total: STEPS.length,
    };
  } catch {
    return { done: 0, total: STEPS.length };
  }
}
