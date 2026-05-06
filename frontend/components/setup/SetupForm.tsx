"use client";

import { useEffect, useMemo, useState } from "react";
import { useRouter } from "next/navigation";
import {
  Database, GitBranch, Activity, Sparkles, ArrowRight, Loader2,
  CheckCircle2, AlertCircle, Plug, Folder,
} from "lucide-react";
import { Card, CardContent, CardDescription, CardHeader, CardTitle } from "@/components/ui/Card";
import { Button } from "@/components/ui/Button";
import { Switch } from "@/components/ui/Switch";
import { Badge } from "@/components/ui/Badge";
import { Input } from "@/components/ui/Input";
import { api } from "@/lib/api";
import type { AgentName, OracleConnection, TestConnectionResponse } from "@/lib/types";

const AGENTS: { id: AgentName; title: string; tagline: string; icon: React.ComponentType<{ className?: string }>; tint: string }[] = [
  {
    id: "inventory",
    title: "Inventory",
    tagline: "Live DB introspection — tables, columns, FKs, sizes, audit log",
    icon: Database,
    tint: "from-[#285294] to-[#7ebcf9]",
  },
  {
    id: "lineage",
    title: "Lineage",
    tagline: "Column-level lineage from ETL XML pipelines and FK relationships",
    icon: GitBranch,
    tint: "from-[#7ebcf9] to-[#00b4f0]",
  },
  {
    id: "usage",
    title: "Usage",
    tagline: "Pipeline run history, success rates, undocumented executions",
    icon: Activity,
    tint: "from-[#00b4f0] to-[#18c29c]",
  },
  {
    id: "summary",
    title: "Executive summary",
    tagline: "Gemini 2.5 Pro synthesis: headline, findings, recommendations",
    icon: Sparkles,
    tint: "from-[#ff6b47] to-[#f6b400]",
  },
];

export function SetupForm() {
  const router = useRouter();
  const [conn, setConn] = useState<OracleConnection | null>(null);
  const [bucket, setBucket] = useState("");
  const [prefix, setPrefix] = useState("");
  const [enabled, setEnabled] = useState<Record<AgentName, boolean>>({
    inventory: true, lineage: true, usage: true, summary: true,
  });
  const [submitting, setSubmitting] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const [testing, setTesting] = useState(false);
  const [testResult, setTestResult] = useState<TestConnectionResponse | null>(null);

  // Load demo defaults to pre-fill the form
  useEffect(() => {
    api.demoDefaults().then((d) => {
      setConn(d.oracle);
      setBucket(d.bucket);
      setPrefix(d.prefix);
    }).catch((e) => setError(String(e)));
  }, []);

  const selectedAgents = useMemo(
    () => (Object.entries(enabled) as [AgentName, boolean][]).filter(([, v]) => v).map(([k]) => k),
    [enabled],
  );

  const canRun = !!conn && selectedAgents.length > 0 && !submitting;

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
      const run = await api.createRun({ oracle: conn, bucket, prefix, agents: selectedAgents });
      router.push(`/runs/${run.id}`);
    } catch (e) {
      setError(String(e));
      setSubmitting(false);
    }
  };

  if (!conn) {
    return (
      <div className="flex items-center gap-3 text-[var(--color-fg-muted)]">
        <Loader2 className="h-4 w-4 animate-spin" /> Loading defaults…
      </div>
    );
  }

  return (
    <div className="grid grid-cols-1 lg:grid-cols-3 gap-6">
      <div className="lg:col-span-2 space-y-6">
        {/* ─── Oracle connection (primary) ────────────────────────── */}
        <Card>
          <CardHeader>
            <div className="flex items-center justify-between">
              <div>
                <CardTitle className="flex items-center gap-2">
                  <Plug className="h-4 w-4 text-[var(--color-cyan-accent)]" /> Oracle connection
                </CardTitle>
                <CardDescription>Live introspection — tables, FKs, ETL audit log. No extracts needed.</CardDescription>
              </div>
              <ConnTestPill result={testResult} testing={testing} />
            </div>
          </CardHeader>
          <CardContent className="space-y-3">
            <div className="grid grid-cols-1 md:grid-cols-3 gap-3">
              <Field label="Host" className="md:col-span-2">
                <Input value={conn.host} onChange={(e) => setConn({ ...conn, host: e.target.value })} />
              </Field>
              <Field label="Port">
                <Input type="number" value={conn.port} onChange={(e) => setConn({ ...conn, port: Number(e.target.value || 1521) })} />
              </Field>
            </div>
            <div className="grid grid-cols-1 md:grid-cols-3 gap-3">
              <Field label="Service">
                <Input value={conn.service} onChange={(e) => setConn({ ...conn, service: e.target.value })} />
              </Field>
              <Field label="User">
                <Input value={conn.user} onChange={(e) => setConn({ ...conn, user: e.target.value })} />
              </Field>
              <Field label="Password">
                <Input type="password" value={conn.password} onChange={(e) => setConn({ ...conn, password: e.target.value })} />
              </Field>
            </div>
            <div className="flex items-center justify-between">
              <div className="text-[11.5px] font-mono text-[var(--color-fg-subtle)]">
                {conn.host}:{conn.port}/{conn.service} as <span className="text-[var(--color-cyan-soft)]">{conn.user}</span>
              </div>
              <Button variant="secondary" size="sm" onClick={onTest} disabled={testing}>
                {testing ? <Loader2 className="h-3.5 w-3.5 animate-spin" /> : <Plug className="h-3.5 w-3.5" />}
                Test connection
              </Button>
            </div>
            {testResult && (
              <ConnTestPanel result={testResult} />
            )}
          </CardContent>
        </Card>

        {/* ─── ETL pipelines (optional) ───────────────────────────── */}
        <Card>
          <CardHeader>
            <CardTitle>ETL pipeline definitions <span className="text-[11px] text-[var(--color-fg-subtle)] font-normal ml-2">optional</span></CardTitle>
            <CardDescription>GCS bucket containing the XML pipelines for column-level lineage. Leave blank for FK-graph lineage only.</CardDescription>
          </CardHeader>
          <CardContent className="grid grid-cols-1 md:grid-cols-3 gap-3">
            <Field label="Bucket" className="md:col-span-2">
              <div className="relative">
                <Database className="h-4 w-4 absolute left-3 top-1/2 -translate-y-1/2 text-[var(--color-fg-subtle)]" />
                <Input value={bucket} onChange={(e) => setBucket(e.target.value)} className="pl-9" />
              </div>
            </Field>
            <Field label="Prefix">
              <div className="relative">
                <Folder className="h-4 w-4 absolute left-3 top-1/2 -translate-y-1/2 text-[var(--color-fg-subtle)]" />
                <Input value={prefix} onChange={(e) => setPrefix(e.target.value)} className="pl-9" />
              </div>
            </Field>
          </CardContent>
        </Card>

        {/* ─── Agents ─────────────────────────────────────────────── */}
        <Card>
          <CardHeader>
            <CardTitle>Agents</CardTitle>
            <CardDescription>Run the full pipeline, or pick agents to focus the analysis.</CardDescription>
          </CardHeader>
          <CardContent className="grid grid-cols-1 md:grid-cols-2 gap-3">
            {AGENTS.map((a) => (
              <AgentToggleCard
                key={a.id}
                agent={a}
                enabled={enabled[a.id]}
                onToggle={(v) => setEnabled((p) => ({ ...p, [a.id]: v }))}
              />
            ))}
          </CardContent>
        </Card>
      </div>

      <div className="space-y-6">
        <Card>
          <CardHeader>
            <CardTitle>Run</CardTitle>
            <CardDescription>Streaming progress per agent. Results auto-saved.</CardDescription>
          </CardHeader>
          <CardContent className="space-y-4">
            <div className="space-y-2.5 text-[12.5px]">
              <KV label="Database" value={`${conn.host}:${conn.port}/${conn.service}`} mono />
              <KV label="ETL bucket" value={bucket || "—"} mono />
              <KV label="Prefix" value={prefix || "(none)"} mono />
              <KV label="Agents" value={`${selectedAgents.length} of ${AGENTS.length}`} />
            </div>
            {error && (
              <div className="rounded-md border border-[rgba(244,71,107,0.4)] bg-[rgba(244,71,107,0.08)] text-[12px] text-[var(--color-rose)] p-2.5">
                {error}
              </div>
            )}
            <Button size="lg" className="w-full" disabled={!canRun} onClick={onRun}>
              {submitting ? <Loader2 className="h-4 w-4 animate-spin" /> : <Sparkles className="h-4 w-4" />}
              {submitting ? "Starting…" : "Run analysis"}
              <ArrowRight className="h-4 w-4" />
            </Button>
            <div className="text-[11px] text-[var(--color-fg-subtle)] text-center">
              Gemini 2.5 Flash for agents · 2.5 Pro for synthesis · Vertex AI
            </div>
          </CardContent>
        </Card>
      </div>
    </div>
  );
}

function Field({ label, className, children }: { label: string; className?: string; children: React.ReactNode }) {
  return (
    <div className={className}>
      <label className="text-[11px] uppercase tracking-wider text-[var(--color-fg-subtle)] font-medium block mb-1.5">{label}</label>
      {children}
    </div>
  );
}

function KV({ label, value, mono }: { label: string; value: string; mono?: boolean }) {
  return (
    <div className="flex items-center justify-between gap-3 border-b border-[var(--color-border-soft)] pb-2 last:border-0">
      <span className="text-[var(--color-fg-subtle)]">{label}</span>
      <span className={`text-white truncate max-w-[60%] ${mono ? "font-mono" : ""}`} title={value}>{value}</span>
    </div>
  );
}

function ConnTestPill({ result, testing }: { result: TestConnectionResponse | null; testing: boolean }) {
  if (testing) return <Badge variant="info"><Loader2 className="h-3 w-3 animate-spin" /> testing</Badge>;
  if (!result) return null;
  if (result.ok) return <Badge variant="ok"><CheckCircle2 className="h-3 w-3" /> connected</Badge>;
  return <Badge variant="crit"><AlertCircle className="h-3 w-3" /> failed</Badge>;
}

function ConnTestPanel({ result }: { result: TestConnectionResponse }) {
  if (result.ok) {
    return (
      <div className="rounded-md border border-[rgba(24,194,156,0.4)] bg-[rgba(24,194,156,0.08)] text-[12.5px] text-[var(--color-emerald)] p-3 flex items-start gap-2">
        <CheckCircle2 className="h-4 w-4 flex-shrink-0 mt-0.5" />
        <div>
          <div className="font-medium">Connected to <span className="font-mono">{result.schema_name}</span></div>
          <div className="text-[11.5px] text-[var(--color-fg-muted)] mt-0.5">
            {result.table_count} tables · {result.pipeline_runs ?? 0} pipelines tracked in audit log
          </div>
        </div>
      </div>
    );
  }
  return (
    <div className="rounded-md border border-[rgba(244,71,107,0.4)] bg-[rgba(244,71,107,0.08)] text-[12.5px] text-[var(--color-rose)] p-3 flex items-start gap-2">
      <AlertCircle className="h-4 w-4 flex-shrink-0 mt-0.5" />
      <div className="font-mono break-words">{result.error}</div>
    </div>
  );
}

function AgentToggleCard({
  agent, enabled, onToggle,
}: {
  agent: (typeof AGENTS)[number];
  enabled: boolean;
  onToggle: (v: boolean) => void;
}) {
  const Icon = agent.icon;
  return (
    <div
      className={`group relative rounded-lg border p-4 transition cursor-pointer ${
        enabled
          ? "border-[var(--color-navy-500)] bg-[var(--color-bg-elev-2)]/70"
          : "border-[var(--color-border-soft)] bg-[var(--color-bg-elev-1)]/40 hover:border-[var(--color-border)]"
      }`}
      onClick={() => onToggle(!enabled)}
    >
      <div className="flex items-start gap-3">
        <div className={`flex-shrink-0 h-9 w-9 rounded-md bg-gradient-to-br ${agent.tint} flex items-center justify-center shadow-md`}>
          <Icon className="h-4 w-4 text-white drop-shadow" />
        </div>
        <div className="flex-1 min-w-0">
          <div className="flex items-center justify-between gap-2">
            <div className="text-[13.5px] font-semibold text-white truncate">{agent.title}</div>
            <Switch checked={enabled} onCheckedChange={onToggle} />
          </div>
          <p className="mt-0.5 text-[12px] leading-relaxed text-[var(--color-fg-muted)]">{agent.tagline}</p>
        </div>
      </div>
    </div>
  );
}
