"use client";

import { useEffect, useState } from "react";
import {
  CheckCircle2, AlertTriangle, AlertCircle, Info, Loader2,
  ChevronDown, ChevronRight, Database,
} from "lucide-react";

import { Card, CardContent } from "@/components/ui/Card";
import { Badge } from "@/components/ui/Badge";
import { api } from "@/lib/api";
import type {
  VerificationReport, TableComparison, VerifyStatus, VerifyClassification,
} from "@/lib/api";

const STATUS_META: Record<
  VerifyStatus,
  { label: string; tone: "ok" | "warn" | "crit" | "info" | "neutral"; Icon: typeof CheckCircle2 }
> = {
  match:              { label: "Match",              tone: "ok",      Icon: CheckCircle2 },
  drift:              { label: "Drift",              tone: "warn",    Icon: AlertTriangle },
  missing_in_bq:      { label: "Missing in BQ",      tone: "crit",    Icon: AlertCircle },
  missing_in_oracle:  { label: "Missing in Oracle",  tone: "info",    Icon: Info },
  missing_both:       { label: "Missing both",       tone: "crit",    Icon: AlertCircle },
  skipped:            { label: "Skipped",            tone: "neutral", Icon: Info },
  error:              { label: "Error",              tone: "crit",    Icon: AlertCircle },
};

const CLASSIFICATION_LABEL: Record<VerifyClassification, string> = {
  oracle_origin: "Oracle source",
  view_origin:   "Oracle view",
  csv_stub:      "CSV stub",
  bq_derived:    "BQ derived",
};

const TONE_COLOR: Record<string, string> = {
  ok:      "var(--ok)",
  warn:    "var(--warn)",
  crit:    "var(--crit)",
  info:    "var(--color-cyan-soft)",
  neutral: "var(--ink-3)",
};

export function VerificationView({ runId }: { runId: string }) {
  const [report, setReport] = useState<VerificationReport | null>(null);
  const [error, setError] = useState<string | null>(null);
  const [loading, setLoading] = useState(true);
  const [running, setRunning] = useState(false);
  const [runError, setRunError] = useState<string | null>(null);
  const [filter, setFilter] = useState<"all" | VerifyStatus>("all");

  useEffect(() => {
    setLoading(true);
    api.verifyReport(runId)
      .then((r) => { setReport(r); setError(null); })
      .catch((e) => setError(String(e)))
      .finally(() => setLoading(false));
  }, [runId]);

  const triggerVerify = async () => {
    setRunning(true);
    setRunError(null);
    try {
      await api.verifyTrigger(runId);
      const fresh = await api.verifyReport(runId);
      setReport(fresh);
      setError(null);
    } catch (e) {
      setRunError(String(e));
    } finally {
      setRunning(false);
    }
  };

  if (loading) {
    return (
      <div className="flex items-center gap-3 py-12" style={{ color: "var(--ink-3)" }}>
        <Loader2 className="h-4 w-4 animate-spin" /> Loading verification report…
      </div>
    );
  }

  if (error || !report) {
    return (
      <Card>
        <CardContent className="py-12 text-center" style={{ color: "var(--ink-3)" }}>
          <Info className="h-5 w-5 mx-auto mb-3" style={{ color: "var(--ink-3)" }} />
          <p className="mb-2 text-[14px]" style={{ color: "var(--ink-2)" }}>
            No verification report yet for this run.
          </p>
          <p className="mb-5 text-[12.5px]" style={{ color: "var(--ink-3)", maxWidth: 540, margin: "0 auto" }}>
            Run this <em>after</em> you've pushed the generated Dataform project
            to GitHub, set up the Dataform workspace, and executed the
            pipelines in BigQuery — verification compares Oracle to whatever
            is actually in BQ right now.
          </p>
          <RunButton onClick={triggerVerify} running={running} />
          {runError && (
            <p className="mt-4 text-[12px] mono" style={{ color: "var(--crit)", maxWidth: 600, margin: "12px auto 0" }}>
              {runError}
            </p>
          )}
        </CardContent>
      </Card>
    );
  }

  const summary = report.summary;
  const tables = filter === "all"
    ? report.tables
    : report.tables.filter((t) => t.status === filter);

  return (
    <div className="space-y-6">
      {/* Header summary cards */}
      <div className="grid grid-cols-2 md:grid-cols-5 gap-3">
        <SummaryCard label="Total" value={summary.total} tone="neutral" />
        <SummaryCard label="Match"   value={summary.matched}  tone="ok" />
        <SummaryCard label="Drift"   value={summary.drifted}  tone="warn" />
        <SummaryCard label="Missing" value={summary.missing}  tone="crit" />
        <SummaryCard label="Skipped" value={summary.skipped}  tone="neutral" />
      </div>

      {/* Header copy */}
      <div className="flex items-start justify-between gap-6 flex-wrap">
        <div className="flex-1 min-w-[280px]">
          <div className="flex items-center gap-2 mb-2">
            <Database className="h-4 w-4" style={{ color: "var(--ink-3)" }} />
            <h2 className="text-[16px] font-medium" style={{ color: "var(--ink)" }}>
              Oracle ↔ BigQuery verification
            </h2>
          </div>
          <p className="text-[12.5px]" style={{ color: "var(--ink-3)", maxWidth: 700 }}>
            Row counts and per-column aggregates compared on both sides for every
            migrated table. Tables sourced from CSV or other non-Oracle inputs are
            tagged <em>skipped</em> — comparison is N/A. <span className="mono">{report.bq_project}</span>{" "}
            ·{" "}
            <span className="mono">{report.raw_dataset}</span>{" "}
            +{" "}
            <span className="mono">{report.derived_dataset}</span>
          </p>
        </div>
        <div className="flex items-center gap-3 flex-wrap">
          <FilterPills
            value={filter}
            onChange={setFilter}
            counts={summary.by_status as Record<VerifyStatus, number>}
          />
          <RunButton onClick={triggerVerify} running={running} compact />
        </div>
      </div>
      {runError && (
        <div className="text-[12px] mono" style={{ color: "var(--crit)" }}>
          {runError}
        </div>
      )}

      {/* Table list */}
      {tables.length === 0 ? (
        <Card>
          <CardContent className="py-10 text-center" style={{ color: "var(--ink-3)" }}>
            No tables in this filter.
          </CardContent>
        </Card>
      ) : (
        <div className="space-y-2">
          {tables.map((t) => <TableRow key={`${t.bq_dataset}.${t.name}`} table={t} />)}
        </div>
      )}
    </div>
  );
}

function RunButton({
  onClick, running, compact,
}: { onClick: () => void; running: boolean; compact?: boolean }) {
  return (
    <button
      onClick={onClick}
      disabled={running}
      style={{
        display: "inline-flex",
        alignItems: "center",
        gap: 8,
        padding: compact ? "6px 12px" : "9px 18px",
        fontSize: compact ? 12 : 13,
        fontWeight: 500,
        background: running ? "var(--bg-elev)" : "var(--ink)",
        color: running ? "var(--ink-3)" : "var(--bg)",
        border: "1px solid var(--ink)",
        borderRadius: "var(--r-md)",
        cursor: running ? "default" : "pointer",
        opacity: running ? 0.7 : 1,
      }}
    >
      {running
        ? <Loader2 className="h-3.5 w-3.5 animate-spin" />
        : <CheckCircle2 className="h-3.5 w-3.5" />}
      {running ? "Comparing…" : compact ? "Re-run verification" : "Run verification"}
    </button>
  );
}


function SummaryCard({
  label, value, tone,
}: { label: string; value: number; tone: "ok" | "warn" | "crit" | "info" | "neutral" }) {
  const color = TONE_COLOR[tone];
  return (
    <div
      style={{
        background: "var(--bg-elev)",
        border: "1px solid var(--line)",
        borderRadius: "var(--r-md)",
        padding: "12px 14px",
      }}
    >
      <div className="text-[11px] uppercase tracking-wider" style={{ color: "var(--ink-3)" }}>
        {label}
      </div>
      <div
        className="mt-1 text-[22px] font-semibold mono"
        style={{ color, lineHeight: 1.1 }}
      >
        {value}
      </div>
    </div>
  );
}

function FilterPills({
  value, onChange, counts,
}: {
  value: "all" | VerifyStatus;
  onChange: (v: "all" | VerifyStatus) => void;
  counts: Record<VerifyStatus, number>;
}) {
  const pills: { key: "all" | VerifyStatus; label: string; count?: number }[] = [
    { key: "all", label: "All" },
    { key: "match", label: "Match", count: counts.match },
    { key: "drift", label: "Drift", count: counts.drift },
    { key: "missing_in_bq", label: "Missing in BQ", count: counts.missing_in_bq },
    { key: "missing_in_oracle", label: "Missing in Oracle", count: counts.missing_in_oracle },
    { key: "skipped", label: "Skipped", count: counts.skipped },
    { key: "error", label: "Error", count: counts.error },
  ];
  return (
    <div className="flex items-center gap-1.5 flex-wrap">
      {pills.map(({ key, label, count }) => {
        const active = value === key;
        return (
          <button
            key={key}
            onClick={() => onChange(key)}
            disabled={key !== "all" && (count ?? 0) === 0}
            style={{
              fontSize: 12,
              padding: "5px 10px",
              borderRadius: 99,
              border: `1px solid ${active ? "var(--ink-2)" : "var(--line)"}`,
              background: active ? "var(--bg-elev)" : "transparent",
              color: active ? "var(--ink)" : "var(--ink-3)",
              cursor: (key !== "all" && (count ?? 0) === 0) ? "default" : "pointer",
              opacity: (key !== "all" && (count ?? 0) === 0) ? 0.4 : 1,
              fontWeight: active ? 500 : 400,
            }}
          >
            {label}
            {count !== undefined && (
              <span
                className="ml-1.5 mono"
                style={{ color: active ? "var(--ink-2)" : "var(--ink-3)" }}
              >
                {count}
              </span>
            )}
          </button>
        );
      })}
    </div>
  );
}

function TableRow({ table }: { table: TableComparison }) {
  const [expanded, setExpanded] = useState(false);
  const meta = STATUS_META[table.status];
  const Icon = meta.Icon;
  const color = TONE_COLOR[meta.tone];
  const hasDetails = table.column_diffs.length > 0 || !!table.error;

  return (
    <div
      style={{
        background: "var(--bg-elev)",
        border: "1px solid var(--line)",
        borderRadius: "var(--r-md)",
      }}
    >
      <button
        onClick={() => hasDetails && setExpanded((v) => !v)}
        disabled={!hasDetails}
        className="w-full text-left p-4 flex items-start gap-3"
        style={{
          cursor: hasDetails ? "pointer" : "default",
          background: "transparent",
          border: "none",
          width: "100%",
        }}
      >
        <Icon className="h-4 w-4 flex-shrink-0 mt-0.5" style={{ color }} />
        <div className="flex-1 min-w-0">
          <div className="flex items-center gap-2 flex-wrap">
            <span className="mono text-[13.5px]" style={{ color: "var(--ink)" }}>
              {table.bq_dataset}.{table.name}
            </span>
            <Badge variant={meta.tone}>{meta.label}</Badge>
            <span
              className="text-[11px]"
              style={{
                color: "var(--ink-3)",
                background: "var(--bg)",
                border: "1px solid var(--line)",
                padding: "1.5px 8px",
                borderRadius: 99,
              }}
            >
              {CLASSIFICATION_LABEL[table.classification]}
            </span>
          </div>
          <div className="mt-1.5 text-[12px]" style={{ color: "var(--ink-2)" }}>
            {formatRows(table.oracle_rows)} in Oracle · {formatRows(table.bq_rows)} in BQ
            {table.columns_compared.length > 0 && (
              <span style={{ color: "var(--ink-3)" }}>
                {" "}· {table.columns_compared.length} columns checked
              </span>
            )}
          </div>
          {table.notes && (
            <p className="mt-1 text-[12px]" style={{ color: "var(--ink-3)" }}>
              {table.notes}
            </p>
          )}
        </div>
        {hasDetails && (
          expanded
            ? <ChevronDown className="h-4 w-4 flex-shrink-0" style={{ color: "var(--ink-3)" }} />
            : <ChevronRight className="h-4 w-4 flex-shrink-0" style={{ color: "var(--ink-3)" }} />
        )}
      </button>
      {expanded && hasDetails && (
        <div
          style={{
            borderTop: "1px solid var(--line)",
            padding: "12px 16px 16px 36px",
            background: "var(--bg)",
          }}
        >
          {table.error && (
            <div
              className="mb-3 text-[12px] mono"
              style={{ color: "var(--crit)" }}
            >
              {table.error}
            </div>
          )}
          {table.column_diffs.length > 0 && (
            <ColumnDiffs diffs={table.column_diffs} />
          )}
        </div>
      )}
    </div>
  );
}

function ColumnDiffs({ diffs }: { diffs: TableComparison["column_diffs"] }) {
  return (
    <div style={{ overflowX: "auto" }}>
      <table className="w-full text-[12px]" style={{ borderCollapse: "collapse" }}>
        <thead>
          <tr style={{ color: "var(--ink-3)", textAlign: "left" }}>
            <th style={th()}>Column</th>
            <th style={th()}>Type</th>
            <th style={th()}>Match</th>
            <th style={th()}>Oracle</th>
            <th style={th()}>BigQuery</th>
          </tr>
        </thead>
        <tbody>
          {diffs.map((d) => (
            <tr key={d.column} style={{ borderTop: "1px solid var(--line)" }}>
              <td style={td()}><span className="mono">{d.column}</span></td>
              <td style={td()}><span className="mono" style={{ color: "var(--ink-3)" }}>{d.bq_type}</span></td>
              <td style={td()}>
                {d.match
                  ? <Badge variant="ok">match</Badge>
                  : <Badge variant="warn">diff</Badge>}
              </td>
              <td style={td()}><AggCell agg={d.oracle} /></td>
              <td style={td()}><AggCell agg={d.bq} /></td>
            </tr>
          ))}
        </tbody>
      </table>
    </div>
  );
}

function AggCell({ agg }: { agg: TableComparison["column_diffs"][number]["oracle"] }) {
  const parts: string[] = [];
  if (agg.null_count !== undefined) parts.push(`null=${agg.null_count}`);
  if (agg.distinct_count !== undefined) parts.push(`distinct≈${agg.distinct_count}`);
  if (agg.sum !== undefined && agg.sum !== null) parts.push(`sum=${truncate(agg.sum, 14)}`);
  if (agg.min !== undefined && agg.min !== null) parts.push(`min=${truncate(agg.min, 14)}`);
  if (agg.max !== undefined && agg.max !== null) parts.push(`max=${truncate(agg.max, 14)}`);
  if (parts.length === 0) return <span style={{ color: "var(--ink-3)" }}>—</span>;
  return (
    <span className="mono" style={{ color: "var(--ink-2)", fontSize: 11.5 }}>
      {parts.join("  ")}
    </span>
  );
}

function th(): React.CSSProperties {
  return {
    padding: "8px 10px",
    fontSize: 11,
    fontWeight: 500,
    textTransform: "uppercase",
    letterSpacing: "0.05em",
  };
}

function td(): React.CSSProperties {
  return { padding: "8px 10px", verticalAlign: "top" };
}

function formatRows(n?: number | null): string {
  if (n === null || n === undefined) return "n/a";
  return n.toLocaleString();
}

function truncate(s: string, max: number): string {
  if (!s) return s;
  return s.length > max ? s.slice(0, max - 1) + "…" : s;
}
