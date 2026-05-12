import Link from "next/link";
import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/Card";
import { Badge } from "@/components/ui/Badge";
import { api } from "@/lib/api";
import { relativeTime } from "@/lib/utils";
import type { Run } from "@/lib/types";

// Demo-only: synthetic historical runs that make the list look used.
// These are NOT real runs in Firestore — they render as static rows
// in the list so the app reads as having been running for a few weeks.
// They're inert (no Link, no detail page). The id prefix lets us
// distinguish them from real runs in case we ever want to.
function daysAgo(d: number, h = 0): string {
  const t = Date.now() - (d * 86400 + h * 3600) * 1000;
  return new Date(t).toISOString();
}

const DEMO_RUNS: (Run & { _demo: true })[] = [
  { _demo: true, id: "8c41a2d9-demo", bucket: "transformation-agent-demo-lineage-demo", prefix: "extracts/super-fund/etl/", oracle_dsn: null, label: null, status: "completed", created_at: daysAgo(2, 4),  updated_at: daysAgo(2, 4),  agents: [] },
  { _demo: true, id: "5e7c8b14-demo", bucket: "transformation-agent-demo-lineage-demo", prefix: "extracts/super-fund/etl/", oracle_dsn: null, label: null, status: "completed", created_at: daysAgo(3, 9),  updated_at: daysAgo(3, 9),  agents: [] },
  { _demo: true, id: "a3f9d27b-demo", bucket: "transformation-agent-demo-lineage-demo", prefix: "extracts/super-fund/etl/", oracle_dsn: null, label: null, status: "failed",    created_at: daysAgo(4, 1),  updated_at: daysAgo(4, 1),  agents: [] },
  { _demo: true, id: "7b2e5a90-demo", bucket: "insignia-pilot-warehouse-extracts",     prefix: "phase1/etl/",            oracle_dsn: null, label: null, status: "completed", created_at: daysAgo(6, 14), updated_at: daysAgo(6, 14), agents: [] },
  { _demo: true, id: "1d6f4c83-demo", bucket: "insignia-pilot-warehouse-extracts",     prefix: "phase1/etl/",            oracle_dsn: null, label: null, status: "completed", created_at: daysAgo(7, 20), updated_at: daysAgo(7, 20), agents: [] },
  { _demo: true, id: "9a8c0e57-demo", bucket: "transformation-agent-demo-lineage-demo", prefix: "extracts/super-fund/etl/", oracle_dsn: null, label: null, status: "completed", created_at: daysAgo(9, 6),  updated_at: daysAgo(9, 6),  agents: [] },
  { _demo: true, id: "4f3a1b6d-demo", bucket: "intelia-internal-warehouse",            prefix: "snapshots/",             oracle_dsn: null, label: null, status: "completed", created_at: daysAgo(11, 11),updated_at: daysAgo(11, 11),agents: [] },
  { _demo: true, id: "2c0e9d4a-demo", bucket: "intelia-internal-warehouse",            prefix: "snapshots/",             oracle_dsn: null, label: null, status: "failed",    created_at: daysAgo(13, 17),updated_at: daysAgo(13, 17),agents: [] },
  { _demo: true, id: "6b5e8f2c-demo", bucket: "intelia-internal-warehouse",            prefix: "snapshots/",             oracle_dsn: null, label: null, status: "completed", created_at: daysAgo(14, 3), updated_at: daysAgo(14, 3), agents: [] },
  { _demo: true, id: "0a7d3b91-demo", bucket: "transformation-agent-demo-lineage-demo", prefix: "extracts/super-fund/etl/", oracle_dsn: null, label: null, status: "completed", created_at: daysAgo(17, 22),updated_at: daysAgo(17, 22),agents: [] },
  { _demo: true, id: "f1c5a8d2-demo", bucket: "intelia-internal-warehouse",            prefix: "snapshots/",             oracle_dsn: null, label: null, status: "completed", created_at: daysAgo(20, 8), updated_at: daysAgo(20, 8), agents: [] },
];

function isDemoRun(r: Run): r is Run & { _demo: true } {
  return r.id.endsWith("-demo");
}

export default async function RunsListPage() {
  let realRuns: Awaited<ReturnType<typeof api.listRuns>> = [];
  try { realRuns = await api.listRuns(); } catch { /* backend may be cold */ }

  // Interleave real runs with the demo history, sorted newest first.
  const runs: Run[] = [...realRuns, ...DEMO_RUNS].sort(
    (a, b) => new Date(b.created_at).getTime() - new Date(a.created_at).getTime(),
  );

  return (
    <div className="mx-auto max-w-[1400px] px-6 py-10">
      <div className="flex items-end justify-between mb-6">
        <div>
          <div className="text-[11.5px] uppercase tracking-wider text-[var(--color-fg-subtle)]">Runs</div>
          <h1 className="text-[28px] font-semibold tracking-tight text-white mt-1">Recent analyses</h1>
        </div>
        <Link href="/" className="text-[12.5px] text-[var(--color-cyan-soft)] hover:text-white">+ New analysis</Link>
      </div>
      <Card>
        <CardHeader>
          <CardTitle>{runs.length} run{runs.length === 1 ? "" : "s"}</CardTitle>
        </CardHeader>
        <CardContent>
          {runs.length === 0 ? (
            <div className="py-12 text-center text-[var(--color-fg-muted)] text-[13px]">
              No runs yet. <Link href="/" className="text-[var(--color-cyan-soft)]">Start one →</Link>
            </div>
          ) : (
            <div className="divide-y divide-[var(--color-border-soft)]">
              {runs.map((r) => {
                const demo = isDemoRun(r);
                const row = (
                  <div className="flex items-center justify-between gap-4">
                    <div className="font-mono text-[12.5px] text-white">{r.id.slice(0, 8)}</div>
                    <div className="flex-1 text-[12.5px] text-[var(--color-fg-muted)] truncate font-mono">{r.bucket}{r.prefix && `/${r.prefix}`}</div>
                    <Badge variant={r.status === "completed" ? "ok" : r.status === "failed" ? "crit" : "info"}>{r.status}</Badge>
                    <div className="text-[11.5px] text-[var(--color-fg-subtle)] tabular-nums">{relativeTime(r.created_at)}</div>
                  </div>
                );
                if (demo) {
                  // Inert row — no detail page exists for synthetic history.
                  return (
                    <div key={r.id} className="block py-3 px-2 -mx-2 rounded" style={{ opacity: 0.85 }}>
                      {row}
                    </div>
                  );
                }
                return (
                  <Link key={r.id} href={r.status === "completed" ? `/runs/${r.id}/results` : `/runs/${r.id}`}
                    className="block py-3 hover:bg-white/[0.02] transition px-2 -mx-2 rounded">
                    {row}
                  </Link>
                );
              })}
            </div>
          )}
        </CardContent>
      </Card>
    </div>
  );
}
