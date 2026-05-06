"use client";

import { useEffect, useMemo, useState } from "react";
import { useRouter } from "next/navigation";
import {
  Database, Folder, ArrowRight, Loader2, CheckCircle2, AlertCircle, Plug, Search,
} from "lucide-react";
import { api } from "@/lib/api";
import type { AgentName, OracleConnection, TestConnectionResponse } from "@/lib/types";

const AGENTS: { id: AgentName; name: string; desc: string }[] = [
  { id: "inventory", name: "Inventory",         desc: "Oracle introspection — tables, views, columns, FKs, audit log" },
  { id: "lineage",   name: "Lineage",           desc: "Column-level lineage from ETL XML pipelines and FK relationships" },
  { id: "usage",     name: "Usage",             desc: "Pipeline run history, success rates, undocumented executions" },
  { id: "summary",   name: "Executive summary", desc: "Synthesis: headline, findings, recommendations" },
];

export function SetupForm() {
  const router = useRouter();
  const [conn, setConn] = useState<OracleConnection | null>(null);
  const [bucket, setBucket] = useState("");
  const [prefix, setPrefix] = useState("");
  const [outputsPrefix, setOutputsPrefix] = useState<string | null>(null);
  const [active, setActive] = useState<AgentName[]>(["inventory", "lineage", "usage", "summary"]);
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
        Four agents introspect your live database, parse your ETL pipelines, and map column-level
        lineage end-to-end. Surfaces hot tables, broken pipelines, undocumented ETL, and dead weight —
        in minutes, not weeks.
      </p>

      <div
        className="grid gap-10"
        style={{ gridTemplateColumns: "1fr 360px", marginTop: 56, alignItems: "start" }}
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

          {/* Section 03 — Pipeline (numbered list) */}
          <section style={{ marginTop: 48 }}>
            <div className="eyebrow">03 · Pipeline</div>
            <h2 style={{ fontSize: 20, fontWeight: 500, margin: "8px 0 4px", letterSpacing: "-0.01em" }}>
              Run the full pipeline, or pick stages
            </h2>
            <p style={{ color: "var(--ink-3)", fontSize: 14, margin: "0 0 24px" }}>
              Each stage feeds the next. Results are auto-saved per run.
            </p>

            <ol style={{ listStyle: "none", padding: 0, margin: 0, position: "relative" }}>
              <div style={{ position: "absolute", left: 15, top: 24, bottom: 24, width: 1, background: "var(--line)" }} />
              {AGENTS.map((a, i) => {
                const on = active.includes(a.id);
                return (
                  <li
                    key={a.id}
                    style={{ position: "relative", display: "flex", gap: 18, padding: "14px 0", alignItems: "flex-start" }}
                  >
                    <div
                      style={{
                        width: 32, height: 32, borderRadius: 6,
                        border: `1px solid ${on ? "var(--brand-emerald)" : "var(--line)"}`,
                        background: on ? "var(--brand-emerald-100)" : "var(--bg-elev)",
                        color: on ? "var(--brand-emerald-700)" : "var(--ink-3)",
                        display: "flex", alignItems: "center", justifyContent: "center",
                        fontFamily: "var(--font-mono)", fontSize: 12, fontWeight: 600,
                        flexShrink: 0, zIndex: 1,
                      }}
                    >
                      {String(i + 1).padStart(2, "0")}
                    </div>
                    <div style={{ flex: 1, paddingTop: 4 }}>
                      <div style={{ display: "flex", justifyContent: "space-between", alignItems: "baseline" }}>
                        <div style={{ fontSize: 15, fontWeight: 500, color: "var(--ink)" }}>{a.name}</div>
                        <button
                          onClick={() => toggle(a.id)}
                          className="mono"
                          style={{
                            background: "transparent", border: 0, cursor: "pointer",
                            color: on ? "var(--ink-2)" : "var(--ink-3)",
                            fontSize: 12, padding: 4,
                          }}
                        >
                          {on ? "✓ included" : "skip"}
                        </button>
                      </div>
                      <div style={{ fontSize: 13.5, color: "var(--ink-3)", marginTop: 2, lineHeight: 1.5 }}>
                        {a.desc}
                      </div>
                    </div>
                  </li>
                );
              })}
            </ol>
          </section>
        </div>

        {/* ─── RIGHT column — sticky aside ───────────────────────── */}
        <aside style={{ position: "sticky", top: 96 }}>
          <div style={{ border: "1px solid var(--line)", borderRadius: 8, background: "var(--bg-elev)", overflow: "hidden" }}>
            <div style={{ padding: "16px 18px", borderBottom: "1px solid var(--line)" }}>
              <div className="eyebrow">Run summary</div>
              <div style={{ fontSize: 15, fontWeight: 500, marginTop: 6, color: "var(--ink)" }}>
                Streaming progress per agent
              </div>
            </div>
            <dl style={{ margin: 0, padding: "8px 18px" }}>
              <SummaryRow k="Database" v={`${conn.host}:${conn.port}/${conn.service}`} mono />
              <SummaryRow k="Bucket" v={bucket || "—"} mono />
              <SummaryRow k="Prefix" v={prefix || "(root)"} mono />
              <SummaryRow k="Stages" v={`${active.length} of ${AGENTS.length}`} mono />
              <SummaryRow k="Synthesis model" v="gemini-2.5-pro" mono />
            </dl>
            {error && (
              <div className="mx-4 my-2 px-3 py-2 text-xs" style={{ color: "var(--crit)", background: "var(--crit-bg)", borderRadius: 6 }}>
                {error}
              </div>
            )}
            <div style={{ padding: 16 }}>
              <button onClick={onRun} disabled={!canRun} style={{ ...btnPrimary, width: "100%", justifyContent: "center", padding: "12px 16px" }}>
                {submitting ? <Loader2 className="h-4 w-4 animate-spin" /> : null}
                {submitting ? "Starting…" : "Run analysis"}
                {!submitting && <ArrowRight className="h-3.5 w-3.5" strokeWidth={1.5} />}
              </button>
              <div className="mono" style={{ fontSize: 11, color: "var(--ink-4)", textAlign: "center", marginTop: 10 }}>
                ~ 3 min · Vertex AI · australia-southeast1
              </div>
            </div>
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

function SummaryRow({ k, v, mono }: { k: string; v: string; mono?: boolean }) {
  return (
    <div
      style={{
        display: "flex", justifyContent: "space-between",
        padding: "10px 0", borderBottom: "1px dashed var(--line)",
        fontSize: 13,
      }}
    >
      <dt style={{ color: "var(--ink-3)" }}>{k}</dt>
      <dd
        className={mono ? "mono" : ""}
        style={{
          margin: 0, textAlign: "right", maxWidth: "60%",
          overflow: "hidden", textOverflow: "ellipsis", whiteSpace: "nowrap",
          color: "var(--ink-2)", fontSize: mono ? 12.5 : 13,
        }}
        title={v}
      >
        {v}
      </dd>
    </div>
  );
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
