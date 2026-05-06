"use client";

import { useEffect, useMemo, useRef, useState } from "react";
import dynamic from "next/dynamic";
import type cytoscape from "cytoscape";
import { Search, GitBranch, ArrowRightToLine, ArrowLeftToLine, Maximize2 } from "lucide-react";
import { Card, CardContent, CardHeader, CardTitle, CardDescription } from "@/components/ui/Card";
import { Badge } from "@/components/ui/Badge";
import { Input } from "@/components/ui/Input";
import { Button } from "@/components/ui/Button";
import type { Inventory, LineageGraph, Layer } from "@/lib/types";

const CytoscapeComponent = dynamic(() => import("react-cytoscapejs"), { ssr: false });

const LAYER_COLOR: Record<Layer, string> = {
  raw:         "#506aa3",
  staging:     "#7ebcf9",
  integration: "#00b4f0",
  reporting:   "#ff6b47",
  unknown:     "#6471a0",
};

const STYLESHEET: unknown[] = [
  {
    selector: "node",
    style: {
      label: "data(label)",
      color: "#e7ecf6",
      "font-size": "10px",
      "font-family": "var(--font-geist-mono), monospace",
      "text-valign": "center",
      "text-halign": "center",
      "text-outline-color": "#060b1a",
      "text-outline-width": 2,
      "border-width": 1,
      "border-color": "#1f2c5a",
      width: 64,
      height: 24,
      shape: "round-rectangle",
    },
  },
  { selector: 'node[layer = "raw"]',         style: { "background-color": LAYER_COLOR.raw } },
  { selector: 'node[layer = "staging"]',     style: { "background-color": LAYER_COLOR.staging, color: "#001520" } },
  { selector: 'node[layer = "integration"]', style: { "background-color": LAYER_COLOR.integration, color: "#001520" } },
  { selector: 'node[layer = "reporting"]',   style: { "background-color": LAYER_COLOR.reporting, color: "#1a0500" } },
  { selector: 'node[layer = "unknown"]',     style: { "background-color": LAYER_COLOR.unknown } },
  { selector: "node[?focused]",              style: { "border-color": "#00b4f0", "border-width": 2.5 } },
  {
    selector: "edge",
    style: {
      width: 1.2,
      "line-color": "#1f2c5a",
      "target-arrow-color": "#506aa3",
      "target-arrow-shape": "triangle-backcurve",
      "curve-style": "bezier",
      "arrow-scale": 0.9,
      label: "data(label)",
      "font-size": "9px",
      "font-family": "var(--font-geist-mono), monospace",
      color: "#7d8fbb",
      "text-background-color": "#0c1530",
      "text-background-opacity": 1,
      "text-background-padding": "2px",
    },
  },
];

export function LineageView({ lineage, inventory }: { lineage?: LineageGraph; inventory?: Inventory }) {
  const [focus, setFocus] = useState<string | null>(null);
  const [direction, setDirection] = useState<"both" | "upstream" | "downstream">("both");
  const [search, setSearch] = useState("");
  const cyRef = useRef<cytoscape.Core | null>(null);

  const layerByFqn = useMemo(() => {
    const m = new Map<string, Layer>();
    for (const t of inventory?.tables ?? []) m.set(`${t.schema_name}.${t.name}`, t.layer);
    return m;
  }, [inventory]);

  const elements = useMemo(() => {
    if (!lineage) return [];
    const tables = new Set<string>();
    for (const e of lineage.edges) {
      tables.add(e.source_fqn);
      tables.add(e.target_fqn);
    }
    let visible = new Set(tables);
    if (focus) {
      visible = traverse(focus, lineage, direction);
    } else if (search) {
      const q = search.toLowerCase();
      visible = new Set([...tables].filter((t) => t.toLowerCase().includes(q)));
    }
    const nodes = [...visible].map((fqn) => ({
      data: {
        id: fqn,
        label: fqn.split(".").pop()!,
        schema: fqn.split(".")[0],
        layer: layerByFqn.get(fqn) ?? "unknown",
        focused: fqn === focus,
      },
    }));
    const edges = lineage.edges
      .filter((e) => visible.has(e.source_fqn) && visible.has(e.target_fqn))
      // table-level dedup so the graph isn't column-spaghetti
      .reduce((acc: Map<string, { count: number; ops: Set<string> }>, e) => {
        const key = `${e.source_fqn}→${e.target_fqn}`;
        const cur = acc.get(key) ?? { count: 0, ops: new Set<string>() };
        cur.count += 1;
        cur.ops.add(e.operation);
        acc.set(key, cur);
        return acc;
      }, new Map());
    const edgeEls = [...edges.entries()].map(([key, v]) => {
      const [s, t] = key.split("→");
      return { data: { id: key, source: s, target: t, label: v.count > 1 ? `${v.count}` : "", op: [...v.ops].join(", ") } };
    });
    return [...nodes, ...edgeEls];
  }, [lineage, focus, direction, search, layerByFqn]);

  useEffect(() => {
    if (!cyRef.current) return;
    cyRef.current.layout({ name: "breadthfirst", directed: true, padding: 30, spacingFactor: 1.4 } as cytoscape.LayoutOptions).run();
  }, [elements]);

  if (!lineage) return <Card><CardContent className="py-16 text-center text-[var(--color-fg-muted)]">No lineage data.</CardContent></Card>;

  return (
    <div className="grid grid-cols-1 lg:grid-cols-4 gap-6">
      <div className="lg:col-span-1 space-y-4">
        <Card>
          <CardHeader>
            <CardTitle>Explore</CardTitle>
            <CardDescription>Click a node to focus on its upstream and downstream.</CardDescription>
          </CardHeader>
          <CardContent className="space-y-3">
            <div className="relative">
              <Search className="h-4 w-4 absolute left-3 top-1/2 -translate-y-1/2 text-[var(--color-fg-subtle)]" />
              <Input value={search} onChange={(e) => { setSearch(e.target.value); setFocus(null); }} placeholder="Search nodes…" className="pl-9" />
            </div>
            {focus && (
              <div className="rounded-md border border-[var(--color-border)] bg-[var(--color-bg-elev-1)]/60 p-3 space-y-2">
                <div className="text-[11px] uppercase tracking-wider text-[var(--color-fg-subtle)]">Focused</div>
                <div className="font-mono text-[12.5px] text-white break-all">{focus}</div>
                <div className="flex gap-2">
                  <Button size="sm" variant={direction === "upstream" ? "primary" : "outline"} onClick={() => setDirection("upstream")}>
                    <ArrowLeftToLine className="h-3 w-3" /> Upstream
                  </Button>
                  <Button size="sm" variant={direction === "downstream" ? "primary" : "outline"} onClick={() => setDirection("downstream")}>
                    <ArrowRightToLine className="h-3 w-3" /> Downstream
                  </Button>
                  <Button size="sm" variant={direction === "both" ? "primary" : "outline"} onClick={() => setDirection("both")}>Both</Button>
                </div>
                <Button size="sm" variant="ghost" onClick={() => setFocus(null)}>Clear focus</Button>
              </div>
            )}
            <div className="space-y-1.5 text-[11.5px]">
              {(["raw", "staging", "integration", "reporting", "unknown"] as Layer[]).map((l) => (
                <div key={l} className="flex items-center gap-2">
                  <span className="h-2.5 w-2.5 rounded-full" style={{ background: LAYER_COLOR[l] }} />
                  <span className="text-[var(--color-fg-muted)] capitalize">{l}</span>
                </div>
              ))}
            </div>
          </CardContent>
        </Card>

        <Card>
          <CardHeader><CardTitle>Stats</CardTitle></CardHeader>
          <CardContent className="space-y-2 text-[12.5px]">
            <Stat label="Edges" value={lineage.edges.length} />
            <Stat label="Tables in graph" value={new Set(lineage.edges.flatMap((e) => [e.source_fqn, e.target_fqn])).size} />
            <Stat label="Unresolved" value={lineage.unresolved.length} tint="text-[var(--color-amber)]" />
          </CardContent>
        </Card>
      </div>

      <Card className="lg:col-span-3 overflow-hidden">
        <CardHeader>
          <div className="flex items-center justify-between">
            <CardTitle className="flex items-center gap-2"><GitBranch className="h-4 w-4 text-[var(--color-cyan-accent)]" /> Lineage graph</CardTitle>
            <Badge variant="info">{elements.filter((e) => "label" in e.data).length} nodes</Badge>
          </div>
        </CardHeader>
        <CardContent className="p-0">
          <div className="relative h-[600px] bg-[radial-gradient(ellipse_at_center,rgba(40,82,148,0.10),transparent_70%)]">
            <CytoscapeComponent
              elements={elements as cytoscape.ElementDefinition[]}
              style={{ width: "100%", height: "100%" }}
              layout={{ name: "breadthfirst", directed: true, padding: 30, spacingFactor: 1.4 } as cytoscape.LayoutOptions}
              cy={(cy: cytoscape.Core) => {
                cyRef.current = cy;
                cy.removeListener("tap", "node");
                cy.on("tap", "node", (evt: cytoscape.EventObject) => setFocus(evt.target.id()));
              }}
              stylesheet={STYLESHEET}
              minZoom={0.2}
              maxZoom={2.5}
              wheelSensitivity={0.25}
            />
            <button
              onClick={() => cyRef.current?.fit(undefined, 60)}
              className="absolute top-3 right-3 inline-flex items-center gap-1 rounded-md border border-[var(--color-border)] bg-[var(--color-bg-elev-2)]/80 px-2 py-1 text-[11px] text-[var(--color-fg-muted)] hover:text-white hover:border-[var(--color-cyan-accent)] backdrop-blur"
            >
              <Maximize2 className="h-3 w-3" /> Fit
            </button>
          </div>
        </CardContent>
      </Card>
    </div>
  );
}

function Stat({ label, value, tint }: { label: string; value: number; tint?: string }) {
  return (
    <div className="flex items-center justify-between">
      <span className="text-[var(--color-fg-subtle)]">{label}</span>
      <span className={`font-mono ${tint ?? "text-white"} tabular-nums`}>{value}</span>
    </div>
  );
}

function traverse(start: string, graph: LineageGraph, direction: "upstream" | "downstream" | "both"): Set<string> {
  const visited = new Set<string>([start]);
  const queue = [start];
  while (queue.length) {
    const node = queue.shift()!;
    for (const e of graph.edges) {
      if ((direction === "downstream" || direction === "both") && e.source_fqn === node && !visited.has(e.target_fqn)) {
        visited.add(e.target_fqn); queue.push(e.target_fqn);
      }
      if ((direction === "upstream" || direction === "both") && e.target_fqn === node && !visited.has(e.source_fqn)) {
        visited.add(e.source_fqn); queue.push(e.source_fqn);
      }
    }
  }
  return visited;
}
