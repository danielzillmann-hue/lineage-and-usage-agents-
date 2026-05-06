"use client";

import { useEffect, useState } from "react";
import Link from "next/link";
import { ArrowLeft, Download, Loader2 } from "lucide-react";
import { Tabs, TabsContent, TabsList, TabsTrigger } from "@/components/ui/Tabs";
import { Button } from "@/components/ui/Button";
import { Badge } from "@/components/ui/Badge";
import { api } from "@/lib/api";
import { relativeTime } from "@/lib/utils";
import type { Run, RunResults } from "@/lib/types";

import { ExecutiveSummaryView } from "./ExecutiveSummaryView";
import { InventoryView } from "./InventoryView";
import { LineageView } from "./LineageView";
import { UsageView } from "./UsageView";
import { FindingsView } from "./FindingsView";

export function ResultsView({ runId }: { runId: string }) {
  const [run, setRun] = useState<Run | null>(null);
  const [results, setResults] = useState<RunResults | null>(null);
  const [tab, setTab] = useState("overview");

  useEffect(() => {
    api.getRun(runId).then(setRun).catch(() => {});
    api.getResults(runId).then(setResults).catch(() => {});
  }, [runId]);

  if (!run || !results) {
    return (
      <div className="flex items-center gap-3 text-[var(--color-fg-muted)]">
        <Loader2 className="h-4 w-4 animate-spin" />
        Loading results…
      </div>
    );
  }

  return (
    <div className="space-y-6">
      <div className="flex flex-col md:flex-row md:items-end md:justify-between gap-4">
        <div>
          <Link href={`/runs/${runId}`} className="inline-flex items-center gap-1 text-[12px] text-[var(--color-fg-subtle)] hover:text-white transition mb-2">
            <ArrowLeft className="h-3 w-3" /> Back to run
          </Link>
          <h1 className="text-[28px] font-semibold tracking-tight text-white text-balance">
            {results.summary?.headline ?? "Analysis complete"}
          </h1>
          <div className="mt-2 flex items-center gap-3 text-[12px] text-[var(--color-fg-muted)]">
            <Badge variant={run.status === "completed" ? "ok" : "warn"}>{run.status}</Badge>
            <span className="font-mono">{run.bucket}{run.prefix && `/${run.prefix}`}</span>
            <span>· {relativeTime(run.created_at)}</span>
          </div>
        </div>
        <div className="flex gap-2">
          <Button variant="secondary" size="md">
            <Download className="h-3.5 w-3.5" /> Export
          </Button>
        </div>
      </div>

      <Tabs value={tab} onValueChange={setTab} className="space-y-5">
        <TabsList>
          <TabsTrigger value="overview">Overview</TabsTrigger>
          <TabsTrigger value="inventory">Inventory</TabsTrigger>
          <TabsTrigger value="lineage">Lineage</TabsTrigger>
          <TabsTrigger value="usage">Usage</TabsTrigger>
          <TabsTrigger value="findings">
            Findings
            {results.summary && results.summary.findings.length > 0 && (
              <span className="ml-1.5 inline-flex h-4 min-w-4 px-1 items-center justify-center rounded-full bg-[var(--color-coral)] text-[10px] font-semibold text-white">
                {results.summary.findings.length}
              </span>
            )}
          </TabsTrigger>
        </TabsList>

        <TabsContent value="overview">
          <ExecutiveSummaryView results={results} />
        </TabsContent>
        <TabsContent value="inventory">
          <InventoryView inventory={results.inventory} />
        </TabsContent>
        <TabsContent value="lineage">
          <LineageView lineage={results.lineage} inventory={results.inventory} />
        </TabsContent>
        <TabsContent value="usage">
          <UsageView usage={results.usage} inventory={results.inventory} />
        </TabsContent>
        <TabsContent value="findings">
          <FindingsView findings={results.summary?.findings ?? []} />
        </TabsContent>
      </Tabs>
    </div>
  );
}
