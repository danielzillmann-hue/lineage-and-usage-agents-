"use client";

import { useEffect, useMemo, useState } from "react";
import { useRouter } from "next/navigation";
import {
  Database, Folder, ArrowRight, Loader2, CheckCircle2, AlertCircle, Plug, Search,
  GitBranch, Activity, Sparkles, FileCode2, Workflow,
} from "lucide-react";
import { api } from "@/lib/api";
import type { AgentName, OracleConnection, TestConnectionResponse } from "@/lib/types";

type AgentSpec = {
  id: AgentName;
  name: string;
  desc: string;
  icon: React.ComponentType<{ className?: string; strokeWidth?: number }>;
  tint: string; // foreground accent colour
  bg: string;   // tinted background when active
};

const AGENTS: AgentSpec[] = [
  { id: "inventory",     name: "Inventory",     desc: "Live Oracle introspection — tables, views, columns, FKs, audit log",        icon: Database,  tint: "#0288D1", bg: "#E1F5FE" },
  { id: "lineage",       name: "Lineage",       desc: "Column-level lineage from ETL XML pipelines and FK relationships",          icon: GitBranch, tint: "#00838F", bg: "#E0F2F1" },
  { id: "usage",         name: "Usage",         desc: "Pipeline run history, success rates, undocumented executions",              icon: Activity,  tint: "#388E3C", bg: "#E8F5E9" },
  { id: "summary",       name: "Summary",       desc: "Gemini synthesis: headline, findings, recommendations",                     icon: Sparkles,  tint: "#F57C00", bg: "#FFF3E0" },
  { id: "transform",     name: "Transformation", desc: "Generate Dataform SQLX from the Oracle pipelines (BigQuery target)",       icon: FileCode2, tint: "#0FB37A", bg: "#E8F5E9" },
  { id: "orchestration", name: "Orchestration", desc: "Generate a GitHub Actions workflow (compile-on-push + scheduled run)",      icon: Workflow,  tint: "#0A8B5E", bg: "#E0F2F1" },
];

export function SetupForm() {
  const router = useRouter();
  const [conn, setConn] = useState<OracleConnection | null>(null);
  const [bucket, setBucket] = useState("");
  const [prefix, setPrefix] = useState("");
  const [outputsPrefix, setOutputsPrefix] = useState<string | null>(null);
  const [active, setActive] = useState<AgentName[]>(["inventory", "lineage", "usage", "summary", "transform", "orchestration"]);
  const [submitting, setSubmitting] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const [testing, setTesting] = useState(false);
  const [testResult, setTestResult] = useState<TestConnectionResponse | null>(null);

  useEffect(() => {
    api.demoDefaults().then((d) => {
      setConn(d.oracle);
      setBucket(d.bucket);
      setPrefix(d.prefix);
      setOutputsPrefix(d.outputs_prefix ?? "");
    }).catch((e) => setError(String(e)));
  }, []);

  const toggle = (id: AgentName) =>
    setActive((a) => a.includes(id) ? a.filter((x) => x !== id) : [...a, id]);

  const canRun = !!conn && active.length > 0 && !submitting;

  const onTest = async () => {
    if (!conn) return;
    setTesting(true);
    setTestResult(null);
    try {
      setTestResult(await api.testOracle(conn));
    } catch (e) {
      setTestResult({ ok: false, error: String(e) });
    } finally {
      setTesting(false);
    }
  };

  const onRun = async () => {
    if (!canRun || !conn) return;
    setSubmitting(true);
    setError(null);
    try {
      const run = await api.createRun({
        oracle: conn,
        bucket: bucket || undefined,
        prefix,
        outputs_prefix: outputsPrefix ?? undefined,
        agents: active,
      });
      router.push(`/runs/${run.id}`);
    } catch (e) {
      setError(String(e));
      setSubmitting(false);
    }
  };

  if (!conn) {
    return (
      <div className="px-8 py-16 flex items-center gap-3" style={{ color: "var(--ink-3)" }}>
        <Loader2 className="h-4 w-4 animate-spin" /> Loading defaults…
      </div>
    );
  }

  return (
    <div className="mx-auto" style={{ maxWidth: 1180, padding: "56px 32px 80px" }}>
      {/* Eyebrow */}
      <div className="eyebrow flex items-center gap-2">
        <span className="dot ok" /> Multi-agent · Oracle warehouse
      </div>

      {/* Display H1 */}
      <h1
        className="text-balance"
        style={{
          fontSize: 44, lineHeight: 1.08, letterSpacing: "-0.022em",
          fontWeight: 500, margin: "20px 0 16px",
          color: "var(--ink)", maxWidth: 720,
        }}
      >
        Map your warehouse end-to-end.
        <br />
        <span style={{ color: "var(--ink-3)" }}>Find what&apos;s actually used.</span>
      </h1>

      {/* Lede */}
      <p
        className="text-pretty"
        style={{ fontSize: 17, lineHeight: 1.55, color: "var(--ink-2)", maxWidth: 640, margin: 0 }}
      >
        Six agents introspect your live database, map column-level lineage, score usage, generate
        Dataform SQLX, and emit a CI workflow — in minutes, not weeks. Toggle any agent on or off
        in the picker to the right.
      </p>

      <div
        className="grid gap-10"
        style={{ gridTemplateColumns: "1fr 420px", marginTop: 56, alignItems: "start" }}
      >
        {/* ─── LEFT column — form ────────────────────────────────── */}
        <div>
          {/* Section 01 — Connection */}
          <section>
            <div className="eyebrow">01 · Connection</div>
            <h2 style={{ fontSize: 20, fontWeight: 500, margin: "8px 0 4px", letterSpacing: "-0.01em" }}>
              Connect to your Oracle database
            </h2>
            <p style={{ color: "var(--ink-3)", fontSize: 14, margin: "0 0 20px" }}>
              Live introspection — schema, FKs, audit log. No extracts to upload.
            </p>

            <div className="grid grid-cols-1 md:grid-cols-3 gap-3">
              <MonoInputField
                label="HOST"
                icon={<Database className="h-4 w-4" strokeWidth={1.25} />}
                value={conn.host}
                onChange={(v) => setConn({ ...conn, host: v })}
                className="md:col-span-2"
              />
              <MonoInputField
                label="PORT"
                value={String(conn.port)}
                type="number"
                onChange={(v) => setConn({ ...conn, port: Number(v || 1521) })}
              />
            </div>
            <div className="grid grid-cols-1 md:grid-cols-3 gap-3 mt-3">
              <MonoInputField label="SERVICE" value={conn.service} onChange={(v) => setConn({ ...conn, service: v })} />
              <MonoInputField label="USER" value={conn.user} onChange={(v) => setConn({ ...conn, user: v })} />
              <MonoInputField label="PASSWORD" type="password" value={conn.password} onChange={(v) => setConn({ ...conn, password: v })} />
            </div>

            <div className="mt-3 flex items-center justify-between">
              <span className="mono" style={{ fontSize: 11.5, color: "var(--ink-4)" }}>
                {conn.host}:{conn.port}/{conn.service}
              </span>
              <button onClick={onTest} disabled={testing} style={btnSecondary}>
                {testing
                  ? <Loader2 className="h-3.5 w-3.5 animate-spin" />
                  : <Plug className="h-3.5 w-3.5" strokeWidth={1.25} />}
                Test connection
              </button>
            </div>
            {testResult && <ConnTestPanel result={testResult} />}
          </section>

          {/* Section 02 — ETL Source */}
          <section style={{ marginTop: 48 }}>
            <div className="eyebrow">02 · Source</div>
            <h2 style={{ fontSize: 20, fontWeight: 500, margin: "8px 0 4px", letterSpacing: "-0.01em" }}>
              Point at the ETL pipeline definitions
            </h2>
            <p style={{ color: "var(--ink-3)", fontSize: 14, margin: "0 0 20px" }}>
              GCS bucket containing the XML pipelines. Output CSVs picked up automatically.
            </p>

            <MonoInputField
              label="BUCKET"
              icon={<Database className="h-4 w-4" strokeWidth={1.25} />}
              value={bucket}
              onChange={setBucket}
            />
            <div style={{ marginTop: 12 }}>
              <MonoInputField
                label="PREFIX (optional)"
                icon={<Folder className="h-4 w-4" strokeWidth={1.25} />}
                value={prefix}
                onChange={setPrefix}
              />
            </div>
          </section>

        </div>

        {/* ─── RIGHT column — sticky aside ───────────────────────── */}
        <aside style={{ position: "sticky", top: 96, display: "flex", flexDirection: "column", gap: 16 }}>
          {/* Run button — sits above the agents picker so it's the
              clearest call-to-action on the page. */}
          <div>
            <button
              onClick={onRun}
              disabled={!canRun}
              style={{
                ...btnPrimary,
                width: "100%", justifyContent: "center",
                padding: "14px 16px", fontSize: 15, fontWeight: 500,
                opacity: canRun ? 1 : 0.5,
                cursor: canRun ? "pointer" : "not-allowed",
              }}
            >
              {submitting ? <Loader2 className="h-4 w-4 animate-spin" /> : null}
              {submitting
                ? "Starting…"
                : `Run ${active.length} of ${AGENTS.length} agent${active.length === 1 ? "" : "s"}`}
              {!submitting && <ArrowRight className="h-4 w-4" strokeWidth={1.5} />}
            </button>
            <div className="mono" style={{
              fontSize: 11, color: "var(--ink-4)", textAlign: "center",
              marginTop: 8,
            }}>
              ~ 3 min · Vertex AI · australia-southeast1
            </div>
            {error && (
              <div className="px-3 py-2 mt-2 text-xs"
                   style={{ color: "var(--crit)", background: "var(--crit-bg)", borderRadius: 6 }}>
                {error}
              </div>
            )}
          </div>

          {/* Agents picker — always visible while you fill the form */}
          <div style={{ border: "1px solid var(--line)", borderRadius: 8, background: "var(--bg-elev)", overflow: "hidden" }}>
            <div style={{ padding: "16px 18px", borderBottom: "1px solid var(--line)" }}>
              <div className="eyebrow">Agents</div>
              <div style={{ fontSize: 15, fontWeight: 500, marginTop: 6, color: "var(--ink)" }}>
                Pick what runs
              </div>
              <div style={{ fontSize: 12.5, color: "var(--ink-3)", marginTop: 4, lineHeight: 1.5 }}>
                Each agent feeds the next where useful — Lineage uses Inventory, Summary uses
                Inventory + Lineage + Usage, Transformation reads everything. Toggle individually
                or use the shortcuts below.
              </div>
              <div style={{ display: "flex", gap: 6, marginTop: 10 }}>
                <button
                  onClick={() => setActive(AGENTS.map((a) => a.id))}
                  className="mono"
                  style={{
                    fontSize: 11, padding: "3px 9px", background: "transparent",
                    color: "var(--ink-2)", border: "1px solid var(--line)",
                    borderRadius: 99, cursor: "pointer",
                  }}
                >
                  all on
                </button>
                <button
                  onClick={() => setActive([])}
                  className="mono"
                  style={{
                    fontSize: 11, padding: "3px 9px", background: "transparent",
                    color: "var(--ink-2)", border: "1px solid var(--line)",
                    borderRadius: 99, cursor: "pointer",
                  }}
                >
                  all off
                </button>
              </div>
            </div>
            <ul style={{ listStyle: "none", padding: 8, margin: 0 }}>
              {AGENTS.map((a) => {
                const on = active.includes(a.id);
                const Icon = a.icon;
                return (
                  <li key={a.id} style={{ marginBottom: 4 }}>
                    <button
                      onClick={() => toggle(a.id)}
                      style={{
                        display: "flex", gap: 12, alignItems: "flex-start",
                        width: "100%", padding: "10px 12px",
                        textAlign: "left", cursor: "pointer",
                        background: on ? a.bg : "transparent",
                        border: `1px solid ${on ? a.tint : "var(--line)"}`,
                        borderRadius: 6,
                        opacity: on ? 1 : 0.65,
                        transition: "opacity .15s, background .15s, border-color .15s",
                      }}
                    >
                      <div
                        style={{
                          width: 30, height: 30, borderRadius: 6,
                          background: on ? a.tint : "var(--bg)",
                          color: on ? "#fff" : "var(--ink-3)",
                          display: "flex", alignItems: "center", justifyContent: "center",
                          flexShrink: 0,
                        }}
                      >
                        <Icon className="h-4 w-4" strokeWidth={1.5} />
                      </div>
                      <div style={{ flex: 1, minWidth: 0 }}>
                        <div style={{
                          display: "flex", alignItems: "center", justifyContent: "space-between",
                          gap: 8,
                        }}>
                          <span style={{
                            fontSize: 13.5, fontWeight: 500,
                            color: on ? "var(--ink)" : "var(--ink-2)",
                          }}>{a.name}</span>
                          <span
                            className="mono"
                            style={{
                              fontSize: 10, padding: "2px 7px",
                              background: on ? a.tint : "transparent",
                              color: on ? "#fff" : "var(--ink-3)",
                              border: on ? "none" : "1px solid var(--line)",
                              borderRadius: 99, flexShrink: 0,
                            }}
                          >
                            {on ? "ON" : "OFF"}
                          </span>
                        </div>
                        <div style={{
                          fontSize: 11.5, color: "var(--ink-3)",
                          lineHeight: 1.45, marginTop: 3,
                        }}>
                          {a.desc}
                        </div>
                      </div>
                    </button>
                  </li>
                );
              })}
            </ul>
          </div>

        </aside>
      </div>
    </div>
  );
}

// ─── Subcomponents ──────────────────────────────────────────────────────────

const btnPrimary: React.CSSProperties = {
  display: "inline-flex", alignItems: "center", gap: 8,
  fontFamily: "var(--font-sans)", fontSize: 14, fontWeight: 500, lineHeight: 1,
  padding: "10px 14px", borderRadius: "var(--r-md)",
  background: "var(--brand-ink)", color: "#FFFFFF", border: "1px solid var(--brand-ink)",
  cursor: "pointer", transition: "background .15s, border-color .15s",
};

const btnSecondary: React.CSSProperties = {
  display: "inline-flex", alignItems: "center", gap: 8,
  fontFamily: "var(--font-sans)", fontSize: 13, fontWeight: 500, lineHeight: 1,
  padding: "8px 12px", borderRadius: "var(--r-md)",
  background: "var(--bg-elev)", color: "var(--ink)", border: "1px solid var(--line)",
  cursor: "pointer", transition: "background .15s, border-color .15s",
};

function MonoInputField({
  label, icon, value, onChange, type = "text", className,
}: {
  label: string;
  icon?: React.ReactNode;
  value: string;
  onChange: (v: string) => void;
  type?: string;
  className?: string;
}) {
  return (
    <label className={className} style={{ display: "block" }}>
      <div className="mono" style={{ fontSize: 12, color: "var(--ink-3)", marginBottom: 6, letterSpacing: "0.02em" }}>
        {label}
      </div>
      <div style={{
        display: "flex", border: "1px solid var(--line)", borderRadius: 6,
        background: "var(--bg-elev)", overflow: "hidden",
      }}>
        {icon && (
          <div style={{
            padding: "0 12px", display: "flex", alignItems: "center",
            color: "var(--ink-3)", borderRight: "1px solid var(--line)",
            background: "var(--bg-sunk)",
          }}>
            {icon}
          </div>
        )}
        <input
          className="mono"
          type={type}
          value={value}
          onChange={(e) => onChange(e.target.value)}
          style={{
            flex: 1, border: 0, outline: 0,
            padding: "12px 14px", fontSize: 13.5,
            color: "var(--ink)", background: "transparent",
          }}
        />
      </div>
    </label>
  );
}

function ConnTestPanel({ result }: { result: TestConnectionResponse }) {
  if (result.ok) {
    return (
      <div
        className="mt-3 px-3 py-2 flex items-start gap-2 text-sm"
        style={{
          background: "var(--brand-emerald-100)", color: "var(--brand-emerald-700)",
          borderRadius: 6, fontSize: 13,
        }}
      >
        <CheckCircle2 className="h-4 w-4 flex-shrink-0 mt-0.5" strokeWidth={1.5} />
        <div>
          <span style={{ fontWeight: 500 }}>Connected to</span>{" "}
          <span className="mono">{result.schema_name}</span>
          <span style={{ color: "var(--brand-emerald-700)", opacity: 0.85, marginLeft: 8 }}>
            {result.table_count} tables · {result.pipeline_runs ?? 0} pipelines tracked
          </span>
        </div>
      </div>
    );
  }
  return (
    <div
      className="mt-3 px-3 py-2 flex items-start gap-2 text-sm"
      style={{ background: "var(--crit-bg)", color: "var(--crit)", borderRadius: 6, fontSize: 13 }}
    >
      <AlertCircle className="h-4 w-4 flex-shrink-0 mt-0.5" strokeWidth={1.5} />
      <span className="mono" style={{ fontSize: 12 }}>{result.error}</span>
    </div>
  );
}
