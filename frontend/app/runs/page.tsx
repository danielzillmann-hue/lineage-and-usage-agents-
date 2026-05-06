import Link from "next/link";
import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/Card";
import { Badge } from "@/components/ui/Badge";
import { api } from "@/lib/api";
import { relativeTime } from "@/lib/utils";

export default async function RunsListPage() {
  let runs: Awaited<ReturnType<typeof api.listRuns>> = [];
  try { runs = await api.listRuns(); } catch { /* backend may be cold */ }

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
              {runs.map((r) => (
                <Link key={r.id} href={r.status === "completed" ? `/runs/${r.id}/results` : `/runs/${r.id}`}
                  className="block py-3 hover:bg-white/[0.02] transition px-2 -mx-2 rounded">
                  <div className="flex items-center justify-between gap-4">
                    <div className="font-mono text-[12.5px] text-white">{r.id.slice(0, 8)}</div>
                    <div className="flex-1 text-[12.5px] text-[var(--color-fg-muted)] truncate font-mono">{r.bucket}{r.prefix && `/${r.prefix}`}</div>
                    <Badge variant={r.status === "completed" ? "ok" : r.status === "failed" ? "crit" : "info"}>{r.status}</Badge>
                    <div className="text-[11.5px] text-[var(--color-fg-subtle)] tabular-nums">{relativeTime(r.created_at)}</div>
                  </div>
                </Link>
              ))}
            </div>
          )}
        </CardContent>
      </Card>
    </div>
  );
}
