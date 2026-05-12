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
  { id: "summary",       name: "Summary and planning", desc: "Gemini synthesis: headline, findings, recommendations",              icon: Sparkles,  tint: "#F57C00", bg: "#FFF3E0" },
  { id: "transform",     name: "Transformation", desc: "Generate Dataform SQLX from the legacy pipelines (strategic target)",      icon: FileCode2, tint: "#0FB37A", bg: "#E8F5E9" },
  { id: "orchestration", name: "Deployment Agent", desc: "Generate a GitHub Actions workflow (compile-on-push + scheduled run)",   icon: Workflow,  tint: "#0A8B5E", bg: "#E0F2F1" },
];

// ─── Source-system catalogue ────────────────────────────────────────────────
//
// Oracle is the only source we currently introspect end-to-end (and the only
// option that drives the demo). The others are visible in the dropdown so the
// solution looks ready for real-world heterogeneity — selecting them swaps
// the connection field labels and shows a "Beta" pill.

type SourceType = "oracle" | "teradata" | "mssql" | "bigquery" | "snowflake" | "sybase";

type SourceField = { key: string; label: string; placeholder?: string };

type SourceSpec = {
  id: SourceType;
  name: string;
  defaultPort: number;
  fields: SourceField[];
  status: "supported" | "beta";
};

const SOURCE_TYPES: SourceSpec[] = [
  { id: "oracle",    name: "Oracle",       defaultPort: 1521, status: "supported",
    fields: [
      { key: "host", label: "HOST" },
      { key: "port", label: "PORT" },
      { key: "service", label: "SERVICE", placeholder: "XEPDB1" },
      { key: "user", label: "USER" },
      { key: "password", label: "PASSWORD" },
    ],
  },
  { id: "teradata",  name: "Teradata",     defaultPort: 1025, status: "beta",
    fields: [
      { key: "host", label: "HOST" },
      { key: "port", label: "PORT" },
      { key: "service", label: "DATABASE" },
      { key: "user", label: "USER" },
      { key: "password", label: "PASSWORD" },
    ],
  },
  { id: "mssql",     name: "MS SQL Server", defaultPort: 1433, status: "beta",
    fields: [
      { key: "host", label: "SERVER" },
      { key: "port", label: "PORT" },
      { key: "service", label: "DATABASE" },
      { key: "user", label: "USER" },
      { key: "password", label: "PASSWORD" },
    ],
  },
  { id: "bigquery",  name: "BigQuery",     defaultPort: 0, status: "beta",
    fields: [
      { key: "host", label: "PROJECT", placeholder: "my-gcp-project" },
      { key: "service", label: "DATASET", placeholder: "raw" },
      { key: "user", label: "SERVICE ACCOUNT", placeholder: "agent@project.iam.gserviceaccount.com" },
    ],
  },
  { id: "snowflake", name: "Snowflake",    defaultPort: 443, status: "beta",
    fields: [
      { key: "host", label: "ACCOUNT", placeholder: "xy12345.ap-southeast-2" },
      { key: "service", label: "WAREHOUSE", placeholder: "COMPUTE_WH" },
      { key: "port", label: "DATABASE" },
      { key: "user", label: "USER" },
      { key: "password", label: "PASSWORD" },
    ],
  },
  { id: "sybase",    name: "Sybase IQ",    defaultPort: 2638, status: "beta",
    fields: [
      { key: "host", label: "HOST" },
      { key: "port", label: "PORT" },
      { key: "service", label: "DATABASE" },
      { key: "user", label: "USER" },
      { key: "password", label: "PASSWORD" },
    ],
  },
];


export function SetupForm() {
  const router = useRouter();
  const [conn, setConn] = useState<OracleConnection | null>(null);
  const [sourceType, setSourceType] = useState<SourceType>("oracle");
  const [bucket, setBucket] = useState("");
  const [prefix, setPrefix] = useState("");
  const [outputsPrefix, setOutputsPrefix] = useState<string | null>(null);
  const [active, setActive] = useState<AgentName[]>(["inventory", "lineage", "usage", "summary", "transform", "orchestration"]);
  const [submitting, setSubmitting] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const [testing, setTesting] = useState(false);
  const [testResult, setTestResult] = useState<TestConnectionResponse | null>(null);

  const sourceSpec = SOURCE_TYPES.find((s) => s.id === sourceType) ?? SOURCE_TYPES[0];
  const isBeta = sourceSpec.status === "beta";

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

  const canRun = !!conn && active.length > 0 && !submitting && !isBeta;

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
        Multiple agents — customisable, extensible — introspect your warehouse (Oracle,
        Teradata, MS SQL, BigQuery, Snowflake, or Sybase), map column-level lineage, score
        usage, generate Dataform SQLX, and emit a CI workflow. Minutes, not weeks. Add or
        remove agents in the picker to the right.
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
              Connect to your warehouse
            </h2>
            <p style={{ color: "var(--ink-3)", fontSize: 14, margin: "0 0 20px" }}>
              Live introspection — schema, FKs, audit log. No extracts to upload.
            </p>

            {/* Source type dropdown */}
            <div style={{ marginBottom: 18 }}>
              <div className="eyebrow" style={{ fontSize: 10.5, marginBottom: 6 }}>Source type</div>
              <div style={{ display: "flex", gap: 8, alignItems: "center" }}>
                <select
                  value={sourceType}
                  onChange={(e) => {
                    const next = e.target.value as SourceType;
                    setSourceType(next);
                    setTestResult(null);
                    // Reset port to the new source's default if user hasn't typed
                    const spec = SOURCE_TYPES.find((s) => s.id === next);
                    if (spec && conn) setConn({ ...conn, port: spec.defaultPort });
                  }}
                  className="mono"
                  style={{
                    flex: 1, height: 38, padding: "0 12px",
                    fontSize: 13, color: "var(--ink)",
                    background: "var(--bg-elev)", border: "1px solid var(--line)",
                    borderRadius: 6, outline: "none",
                  }}
                >
                  {SOURCE_TYPES.map((s) => (
                    <option key={s.id} value={s.id}>{s.name}</option>
                  ))}
                </select>
              </div>
            </div>

            {/* Connection fields — labels swap per source */}
            <div className="grid grid-cols-1 md:grid-cols-3 gap-3">
              {sourceSpec.fields.slice(0, 2).map((f) => (
                <MonoInputField
                  key={f.key}
                  label={f.label}
                  icon={f.key === "host" ? <Database className="h-4 w-4" strokeWidth={1.25} /> : undefined}
                  value={fieldValue(conn, f.key)}
                  type={f.key === "port" ? "number" : f.key === "password" ? "password" : "text"}
                  onChange={(v) => setConn(updateConn(conn, f.key, v, sourceSpec.defaultPort))}
                  className={f.key === "host" && sourceSpec.fields.length > 2 ? "md:col-span-2" : undefined}
                />
              ))}
            </div>
            <div className="grid grid-cols-1 md:grid-cols-3 gap-3 mt-3">
              {sourceSpec.fields.slice(2).map((f) => (
                <MonoInputField
                  key={f.key}
                  label={f.label}
                  value={fieldValue(conn, f.key)}
                  type={f.key === "password" ? "password" : "text"}
                  onChange={(v) => setConn(updateConn(conn, f.key, v, sourceSpec.defaultPort))}
                />
              ))}
            </div>

            <div className="mt-3 flex items-center justify-between">
              <span className="mono" style={{ fontSize: 11.5, color: "var(--ink-4)" }}>
                {sourceSpec.name} · {conn.host}{conn.port ? `:${conn.port}` : ""}{conn.service ? `/${conn.service}` : ""}
              </span>
              <button onClick={onTest} disabled={testing || isBeta} style={btnSecondary}>
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
                          {/* When active the card bg is a hard-coded light pastel
                              (a.bg), so the name + description need a hard-coded
                              dark colour to stay readable in dark mode where
                              var(--ink) flips to near-white. */}
                          <span style={{
                            fontSize: 13.5, fontWeight: 500,
                            color: on ? "#0F1F2C" : "var(--ink-2)",
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
                          fontSize: 11.5,
                          color: on ? "#3B4A57" : "var(--ink-3)",
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
  background: "var(--brand-ink)", color: "var(--invert-fg)", border: "1px solid var(--brand-ink)",
  cursor: "pointer", transition: "background .15s, border-color .15s",
};

const btnSecondary: React.CSSProperties = {
  display: "inline-flex", alignItems: "center", gap: 8,
  fontFamily: "var(--font-sans)", fontSize: 13, fontWeight: 500, lineHeight: 1,
  padding: "8px 12px", borderRadius: "var(--r-md)",
  background: "var(--bg-elev)", color: "var(--ink)", border: "1px solid var(--line)",
  cursor: "pointer", transition: "background .15s, border-color .15s",
};

function fieldValue(conn: OracleConnection, key: string): string {
  switch (key) {
    case "host": return conn.host;
    case "port": return String(conn.port || "");
    case "service": return conn.service;
    case "user": return conn.user;
    case "password": return conn.password;
    default: return "";
  }
}


function updateConn(
  conn: OracleConnection,
  key: string,
  value: string,
  defaultPort: number,
): OracleConnection {
  switch (key) {
    case "host":     return { ...conn, host: value };
    case "port":     return { ...conn, port: Number(value || defaultPort) };
    case "service":  return { ...conn, service: value };
    case "user":     return { ...conn, user: value };
    case "password": return { ...conn, password: value };
    default:         return conn;
  }
}


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
