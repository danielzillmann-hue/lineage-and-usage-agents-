"use client";

import { useEffect, useMemo, useState } from "react";
import { useRouter } from "next/navigation";
import {
  Database, GitBranch, Activity, Sparkles, ArrowRight, Folder, Loader2,
  FileText, ScrollText, History,
} from "lucide-react";
import { Card, CardContent, CardDescription, CardHeader, CardTitle } from "@/components/ui/Card";
import { Button } from "@/components/ui/Button";
import { Switch } from "@/components/ui/Switch";
import { Badge } from "@/components/ui/Badge";
import { Input } from "@/components/ui/Input";
import { api } from "@/lib/api";
import { formatBytes, formatNumber } from "@/lib/utils";
import type { AgentName, BucketPreview } from "@/lib/types";

const AGENTS: { id: AgentName; title: string; tagline: string; icon: React.ComponentType<{ className?: string }>; tint: string }[] = [
  {
    id: "inventory",
    title: "Inventory",
    tagline: "Tables, views, columns, layer + domain classification",
    icon: Database,
    tint: "from-[#285294] to-[#7ebcf9]",
  },
  {
    id: "lineage",
    title: "Lineage",
    tagline: "Column-level lineage from views, CTAS, INSERT…SELECT, and PL/SQL",
    icon: GitBranch,
    tint: "from-[#7ebcf9] to-[#00b4f0]",
  },
  {
    id: "usage",
    title: "Usage",
    tagline: "AWR / V$SQL joined onto lineage — hot tables, dead objects, reachability",
    icon: Activity,
    tint: "from-[#00b4f0] to-[#18c29c]",
  },
  {
    id: "summary",
    title: "Executive summary",
    tagline: "Opus 4.7 synthesis: headline, findings, recommendations",
    icon: Sparkles,
    tint: "from-[#ff6b47] to-[#f6b400]",
  },
];

export function SetupForm() {
  const router = useRouter();
  const [buckets, setBuckets] = useState<string[] | null>(null);
  const [bucket, setBucket] = useState<string>("");
  const [prefix, setPrefix] = useState<string>("");
  const [enabled, setEnabled] = useState<Record<AgentName, boolean>>({
    inventory: true, lineage: true, usage: true, summary: true,
  });
  const [preview, setPreview] = useState<BucketPreview | null>(null);
  const [previewLoading, setPreviewLoading] = useState(false);
  const [submitting, setSubmitting] = useState(false);
  const [error, setError] = useState<string | null>(null);

  useEffect(() => {
    api.listBuckets().then(setBuckets).catch((e) => setError(String(e)));
  }, []);

  useEffect(() => {
    if (!bucket) { setPreview(null); return; }
    setPreviewLoading(true);
    const t = setTimeout(() => {
      api.previewBucket(bucket, prefix).then(setPreview).catch(() => setPreview(null)).finally(() => setPreviewLoading(false));
    }, 250);
    return () => clearTimeout(t);
  }, [bucket, prefix]);

  const selectedAgents = useMemo(
    () => (Object.entries(enabled) as [AgentName, boolean][]).filter(([, v]) => v).map(([k]) => k),
    [enabled],
  );

  const canRun = !!bucket && selectedAgents.length > 0 && !submitting;

  const onRun = async () => {
    if (!canRun) return;
    setSubmitting(true);
    setError(null);
    try {
      const run = await api.createRun({ bucket, prefix, agents: selectedAgents });
      router.push(`/runs/${run.id}`);
    } catch (e) {
      setError(String(e));
      setSubmitting(false);
    }
  };

  return (
    <div className="grid grid-cols-1 lg:grid-cols-3 gap-6">
      <div className="lg:col-span-2 space-y-6">
        <Card>
          <CardHeader>
            <CardTitle>Source extract</CardTitle>
            <CardDescription>Point the agents at a Cloud Storage bucket containing DDL, data-dictionary CSVs, and AWR exports.</CardDescription>
          </CardHeader>
          <CardContent className="space-y-4">
            <div>
              <label className="text-[11.5px] uppercase tracking-wider text-[var(--color-fg-subtle)] font-medium">Bucket</label>
              <BucketSelect buckets={buckets} value={bucket} onChange={setBucket} />
            </div>
            <div>
              <label className="text-[11.5px] uppercase tracking-wider text-[var(--color-fg-subtle)] font-medium">Prefix (optional)</label>
              <div className="flex items-center gap-2 mt-1.5">
                <Folder className="h-4 w-4 text-[var(--color-fg-subtle)]" />
                <Input value={prefix} onChange={(e) => setPrefix(e.target.value)} placeholder="e.g. extracts/2026-05-12/" />
              </div>
            </div>

            <BucketPreviewBlock loading={previewLoading} preview={preview} />
          </CardContent>
        </Card>

        <Card>
          <CardHeader>
            <CardTitle>Agents</CardTitle>
            <CardDescription>Run the full pipeline, or pick individual agents to focus the analysis.</CardDescription>
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
              <KV label="Bucket" value={bucket || "—"} />
              <KV label="Prefix" value={prefix || "(root)"} />
              <KV label="Agents" value={`${selectedAgents.length} of ${AGENTS.length}`} />
              {preview && (
                <>
                  <KV label="DDL files" value={String(preview.ddl_files)} />
                  <KV label="Dictionary files" value={String(preview.dictionary_files)} />
                  <KV label="AWR files" value={String(preview.awr_files)} />
                </>
              )}
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
              Sonnet 4.6 for agent work · Opus 4.7 for synthesis
            </div>
          </CardContent>
        </Card>
      </div>
    </div>
  );
}

function KV({ label, value }: { label: string; value: string }) {
  return (
    <div className="flex items-center justify-between gap-3 border-b border-[var(--color-border-soft)] pb-2 last:border-0">
      <span className="text-[var(--color-fg-subtle)]">{label}</span>
      <span className="text-white font-mono truncate max-w-[60%]" title={value}>{value}</span>
    </div>
  );
}

function BucketSelect({ buckets, value, onChange }: { buckets: string[] | null; value: string; onChange: (v: string) => void }) {
  return (
    <div className="relative mt-1.5">
      <Database className="h-4 w-4 absolute left-3 top-1/2 -translate-y-1/2 text-[var(--color-fg-subtle)]" />
      <select
        value={value}
        onChange={(e) => onChange(e.target.value)}
        className="h-9 w-full rounded-md border border-[var(--color-border)] bg-[var(--color-bg-elev-1)] pl-9 pr-3 text-[13px] text-white outline-none focus:border-[var(--color-cyan-accent)] focus:ring-2 focus:ring-[rgba(0,180,240,0.25)] transition appearance-none"
      >
        <option value="" disabled>{buckets === null ? "Loading buckets…" : "Select a bucket"}</option>
        {(buckets ?? []).map((b) => <option key={b} value={b}>{b}</option>)}
      </select>
    </div>
  );
}

function BucketPreviewBlock({ loading, preview }: { loading: boolean; preview: BucketPreview | null }) {
  if (loading) {
    return (
      <div className="rounded-md border border-[var(--color-border-soft)] bg-[var(--color-bg-elev-1)]/40 p-4 text-[12.5px] text-[var(--color-fg-muted)] flex items-center gap-2">
        <Loader2 className="h-3.5 w-3.5 animate-spin" /> Scanning bucket…
      </div>
    );
  }
  if (!preview) return null;
  const total = preview.ddl_files + preview.dictionary_files + preview.awr_files + preview.other_files;
  return (
    <div className="rounded-lg border border-[var(--color-border-soft)] bg-[var(--color-bg-elev-1)]/40 p-4 space-y-3">
      <div className="flex items-center justify-between">
        <div className="text-[12.5px] text-[var(--color-fg-muted)]">
          <span className="text-white font-medium">{formatNumber(total)}</span> files · {formatBytes(preview.total_bytes)}
        </div>
        <div className="flex items-center gap-2">
          <Badge variant="info"><FileText className="h-3 w-3" /> {preview.ddl_files} DDL</Badge>
          <Badge variant="ok"><ScrollText className="h-3 w-3" /> {preview.dictionary_files} Dict</Badge>
          <Badge variant="accent"><History className="h-3 w-3" /> {preview.awr_files} AWR</Badge>
        </div>
      </div>
      {preview.sample_paths.length > 0 && (
        <div className="text-[11.5px] font-mono text-[var(--color-fg-subtle)] space-y-0.5 max-h-28 overflow-y-auto">
          {preview.sample_paths.map((p) => <div key={p} className="truncate">{p}</div>)}
        </div>
      )}
    </div>
  );
}

function AgentToggleCard({
  agent,
  enabled,
  onToggle,
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
