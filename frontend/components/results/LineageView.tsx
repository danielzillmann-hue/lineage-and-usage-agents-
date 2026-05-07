"use client";

import { useEffect, useMemo, useRef, useState } from "react";
import { Search, Maximize2, Minimize2, Image as ImageIcon, FileSpreadsheet, X, Filter } from "lucide-react";
import type { Inventory, Layer, LineageEdge, LineageGraph, Sensitivity } from "@/lib/types";

// Visual taxonomy — palette per node "kind" mirroring the user's Mermaid handoff.
//   raw      → blue   (source tables, the system of record)
//   staging  → orange (stg_* tables — pre-conformance)
//   core     → green  (integration / dimensional / fact tables)
//   pipeline → dark   (XML pipeline nodes — first-class)
//   output   → pink   (CSV / file outputs)
//   external → purple (CSVs flowing IN that aren't from the DB)
//   unknown  → grey
type NodeKind = "raw" | "staging" | "core" | "pipeline" | "output" | "external" | "unknown";

const KIND_STYLES: Record<NodeKind, { fill: string; stroke: string; text: string; dot: string }> = {
  raw:      { fill: "#E1F5FE", stroke: "#0288D1", text: "#01579B", dot: "#0288D1" },
  staging:  { fill: "#FFF3E0", stroke: "#F57C00", text: "#E65100", dot: "#F57C00" },
  core:     { fill: "#E8F5E9", stroke: "#388E3C", text: "#1B5E20", dot: "#388E3C" },
  pipeline: { fill: "#1F1F1F", stroke: "#1F1F1F", text: "#FFFFFF", dot: "#1F1F1F" },
  output:   { fill: "#FCE4EC", stroke: "#C2185B", text: "#880E4F", dot: "#C2185B" },
  external: { fill: "#EDE7F6", stroke: "#512DA8", text: "#311B92", dot: "#512DA8" },
  unknown:  { fill: "var(--bg-sunk)", stroke: "var(--line-strong)", text: "var(--ink-3)", dot: "var(--line-strong)" },
};

const KIND_LABEL: Record<NodeKind, string> = {
  raw: "Raw", staging: "Staging", core: "Core / Integration",
  pipeline: "Pipeline", output: "Output", external: "External", unknown: "Unknown",
};

// Layer (from inventory) → NodeKind. Helps the renderer show the same colours
// users see in the inventory grid.
const LAYER_TO_KIND: Record<Layer, NodeKind> = {
  raw: "raw", staging: "staging", integration: "core",
  reporting: "core", output: "output", unknown: "unknown",
};

export function LineageView({
  lineage, inventory, runId,
}: {
  lineage?: LineageGraph;
  inventory?: Inventory;
  runId?: string;
}) {
  const [selected, setSelected] = useState<string | null>(null);
  const [search, setSearch] = useState("");
  const [layers, setLayers] = useState<Record<Layer, boolean>>({
    raw: true, staging: true, integration: true, reporting: true, output: true, unknown: true,
  });
  const [pipelineFilter, setPipelineFilter] = useState<string | null>(null);
  const [fullscreen, setFullscreen] = useState(false);
  const [showEdgeLabels, setShowEdgeLabels] = useState(true);
  const [showSubgraphs, setShowSubgraphs] = useState(true);
  const svgRef = useRef<SVGSVGElement | null>(null);

  // ESC exits fullscreen
  useEffect(() => {
    if (!fullscreen) return;
    const onKey = (e: KeyboardEvent) => {
      if (e.key === "Escape") setFullscreen(false);
    };
    window.addEventListener("keydown", onKey);
    return () => window.removeEventListener("keydown", onKey);
  }, [fullscreen]);

  const inferLayer = useMemo(() => {
    const m = new Map<string, Layer>();
    for (const t of inventory?.tables ?? []) m.set(`${t.schema_name}.${t.name}`, t.layer);
    return (fqn: string): Layer => {
      if (m.has(fqn)) return m.get(fqn)!;
      if (fqn.startsWith("SOURCE.")) return "raw";
      if (fqn.startsWith("OUTPUTS.")) return "output";
      if (fqn.startsWith("PIPELINE.")) return "integration";
      return "unknown";
    };
  }, [inventory]);

  // Render-side classifier: pipelines are first-class; otherwise map layer → kind.
  const inferKind = (fqn: string): NodeKind => {
    if (fqn.startsWith("PIPELINE.")) return "pipeline";
    if (fqn.startsWith("OUTPUTS.")) return "output";
    const upper = fqn.toUpperCase();
    const last = upper.split(".").pop() ?? "";
    if (last.startsWith("STG_") || last.startsWith("STAGE_")) return "staging";
    if (last.startsWith("CORE_")) return "core";
    return LAYER_TO_KIND[inferLayer(fqn)] ?? "unknown";
  };

  // Column-level sensitivity lookup for CSV export and PII path detection
  const sensByCol = useMemo(() => {
    const m = new Map<string, Sensitivity>();
    for (const t of inventory?.tables ?? []) {
      const fqn = `${t.schema_name}.${t.name}`;
      for (const c of t.columns) {
        m.set(`${fqn}.${c.name}`.toUpperCase(), c.sensitivity);
        m.set(`SOURCE.${t.name}.${c.name}`.toUpperCase(), c.sensitivity);
        m.set(`OUTPUTS.${t.name}.${c.name}`.toUpperCase(), c.sensitivity);
      }
    }
    return m;
  }, [inventory]);

  const pipelineNames = useMemo(() => {
    return [...new Set(inventory?.pipelines.map((p) => p.name) ?? [])].sort();
  }, [inventory]);

  // Set of FQNs visible when a pipeline filter is active
  const pipelineFilterSet = useMemo(() => {
    if (!pipelineFilter || !inventory || !lineage) return null;
    const p = inventory.pipelines.find((pp) => pp.name === pipelineFilter);
    if (!p) return null;
    const set = new Set<string>();
    set.add(`PIPELINE.${pipelineFilter}`);
    // Source tables (SOURCE.<NAME>) the pipeline reads
    for (const src of p.source_tables) {
      set.add(`SOURCE.${src.toUpperCase()}`);
      // Some edges reference real schema names — accept those too
      const match = inventory.tables.find((t) => t.name.toUpperCase() === src.toUpperCase());
      if (match) set.add(`${match.schema_name}.${match.name}`);
    }
    // Output CSV
    if (p.output_csv) {
      const short = p.output_csv.replace(/\.csv$/i, "").toUpperCase();
      set.add(`OUTPUTS.${short}`);
    }
    // Walk the lineage to pull in step nodes belonging to this pipeline
    for (const e of lineage.edges) {
      const collapse = (fqn: string): string => {
        if (fqn.startsWith("PIPELINE.")) {
          const parts = fqn.split(".");
          return parts.length >= 2 ? `${parts[0]}.${parts[1]}` : fqn;
        }
        return fqn;
      };
      const cs = collapse(e.source_fqn);
      const ct = collapse(e.target_fqn);
      if (cs === `PIPELINE.${pipelineFilter}` || ct === `PIPELINE.${pipelineFilter}`) {
        set.add(cs);
        set.add(ct);
      }
    }
    return set;
  }, [pipelineFilter, inventory, lineage]);

  // Build node set (excluding intra-pipeline step nodes — we collapse those to keep the graph readable)
  const { nodes, edges } = useMemo(() => {
    const allEdges = lineage?.edges ?? [];
    // Collapse PIPELINE.<name>.<step> to PIPELINE.<name>
    const collapse = (fqn: string): string => {
      if (fqn.startsWith("PIPELINE.")) {
        const parts = fqn.split(".");
        return parts.length >= 2 ? `${parts[0]}.${parts[1]}` : fqn;
      }
      return fqn;
    };
    // When multiple column-level edges roll up between the same pair of
    // table/pipeline nodes, pick the most informative operation as the label.
    const opPriority: Record<string, number> = {
      aggregate: 100, join: 90, transform: 80, VIEW: 70, fk: 60,
      extract: 30, load: 20, pipeline: 10,
    };

    const nodeSet = new Set<string>();
    const edgeSet = new Map<string, { src: string; dst: string; operation: string }>();
    for (const e of allEdges) {
      const s = collapse(e.source_fqn);
      const t = collapse(e.target_fqn);
      if (s === t) continue;
      if (pipelineFilterSet) {
        if (!pipelineFilterSet.has(s) && !pipelineFilterSet.has(t)) continue;
      }
      nodeSet.add(s);
      nodeSet.add(t);
      const key = `${s}→${t}`;
      const cur = edgeSet.get(key);
      if (!cur || (opPriority[e.operation] ?? 0) > (opPriority[cur.operation] ?? 0)) {
        edgeSet.set(key, { src: s, dst: t, operation: e.operation });
      }
    }
    return {
      nodes: [...nodeSet],
      edges: [...edgeSet.values()],
    };
  }, [lineage, pipelineFilterSet]);

  // Topological depth layout — every node's column = its longest path from a
  // root. Lets chains of arbitrary length flow LR without being squashed into
  // 4 fixed columns; pipelines naturally sit between their inputs and outputs.
  const layout = useMemo(() => {
    // Fullscreen renders at native size: scale up the canvas so columns spread
    // out and labels are comfortably readable. Non-fullscreen scales to fit.
    const W = fullscreen ? 2200 : 1400;
    const adj = new Map<string, string[]>();   // node → downstream
    const rev = new Map<string, string[]>();   // node → upstream
    for (const e of edges) {
      if (!adj.has(e.src)) adj.set(e.src, []);
      adj.get(e.src)!.push(e.dst);
      if (!rev.has(e.dst)) rev.set(e.dst, []);
      rev.get(e.dst)!.push(e.src);
    }

    // Compute depth per node (longest path from any root). Memoised; cycle-safe.
    const depthCache: Record<string, number> = {};
    const visiting = new Set<string>();
    const depth = (n: string): number => {
      if (n in depthCache) return depthCache[n];
      if (visiting.has(n)) return 0; // cycle break
      visiting.add(n);
      const parents = rev.get(n) ?? [];
      let d = 0;
      if (parents.length === 0) d = 0;
      else d = 1 + Math.max(...parents.map((p) => depth(p)));
      visiting.delete(n);
      depthCache[n] = d;
      return d;
    };
    nodes.forEach((n) => depth(n));

    const byCol: Record<number, string[]> = {};
    for (const n of nodes) {
      const c = depthCache[n] ?? 0;
      (byCol[c] ??= []).push(n);
    }
    // Stable sort within column: kind first (raw → staging → core → output),
    // then alphabetical, so layouts feel predictable.
    const kindOrder: Record<NodeKind, number> = {
      external: 0, raw: 1, staging: 2, pipeline: 3, core: 4, output: 5, unknown: 6,
    };
    for (const c of Object.keys(byCol)) {
      byCol[Number(c)].sort((a, b) => {
        const ka = kindOrder[inferKind(a)];
        const kb = kindOrder[inferKind(b)];
        if (ka !== kb) return ka - kb;
        return a.localeCompare(b);
      });
    }

    const cols = Object.keys(byCol).map(Number).sort((a, b) => a - b);
    const colCount = cols.length || 1;
    const COL_PAD_LEFT = 110;
    const COL_PAD_RIGHT = 110;
    const usableW = W - COL_PAD_LEFT - COL_PAD_RIGHT;
    const colXs: number[] = cols.map((c, i) =>
      colCount === 1 ? W / 2 : COL_PAD_LEFT + (i * usableW) / (colCount - 1),
    );

    // Vertical spacing — bigger in fullscreen so labels are readable.
    const ROW_H = fullscreen ? 80 : 60;
    const TOP_PAD = 64;
    const tallest = Math.max(...cols.map((c) => byCol[c].length), 1);
    const H = Math.max(fullscreen ? 880 : 560, TOP_PAD + 40 + tallest * ROW_H);

    const pos: Record<string, { x: number; y: number; col: number }> = {};
    cols.forEach((c, i) => {
      const ns = byCol[c];
      const colHeight = (ns.length - 1) * ROW_H;
      const startY = TOP_PAD + (H - TOP_PAD * 2 - colHeight) / 2;
      ns.forEach((n, j) => {
        pos[n] = { x: colXs[i], y: startY + j * ROW_H, col: c };
      });
    });

    return { W, H, pos, colXs, cols };
  }, [nodes, edges, fullscreen]);

  // Filter logic
  const matches = (id: string) => !search || id.toLowerCase().includes(search.toLowerCase());
  const layerOk = (id: string) => {
    const l = normalizeLayer(inferLayer(id));
    return layers[l] !== false;
  };

  const focusSet = useMemo(() => {
    if (!selected) return null;
    // Full lineage: BFS upstream + downstream from the selected node so the
    // entire connected chain lights up, not just the immediate neighbours.
    const upstreamMap = new Map<string, string[]>();
    const downstreamMap = new Map<string, string[]>();
    for (const e of edges) {
      if (!downstreamMap.has(e.src)) downstreamMap.set(e.src, []);
      downstreamMap.get(e.src)!.push(e.dst);
      if (!upstreamMap.has(e.dst)) upstreamMap.set(e.dst, []);
      upstreamMap.get(e.dst)!.push(e.src);
    }
    const set = new Set<string>([selected]);
    const walk = (start: string, adjacency: Map<string, string[]>) => {
      const stack = [start];
      while (stack.length) {
        const node = stack.pop()!;
        for (const next of adjacency.get(node) ?? []) {
          if (set.has(next)) continue;
          set.add(next);
          stack.push(next);
        }
      }
    };
    walk(selected, upstreamMap);
    walk(selected, downstreamMap);
    return set;
  }, [selected, edges]);

  const upstream = useMemo(
    () => (selected ? edges.filter((e) => e.dst === selected).map((e) => e.src) : []),
    [selected, edges],
  );
  const downstream = useMemo(
    () => (selected ? edges.filter((e) => e.src === selected).map((e) => e.dst) : []),
    [selected, edges],
  );

  const sel = selected ? { id: selected, layer: normalizeLayer(inferLayer(selected)) } : null;

  // Subgraph detection — identify per-output chains and the nodes exclusive
  // to each one. Walk upstream from every output CSV to compute its ancestor
  // set; nodes that lead to exactly one output get grouped behind a labelled
  // bounding box. Shared nodes (like MEMBERS feeding many chains) stay
  // outside any subgraph.
  const subgraphs = useMemo(() => {
    if (!showSubgraphs) return [];
    const outputs = nodes.filter((n) => inferKind(n) === "output");
    if (outputs.length === 0) return [];
    const upstreamMap = new Map<string, string[]>();
    for (const e of edges) {
      if (!upstreamMap.has(e.dst)) upstreamMap.set(e.dst, []);
      upstreamMap.get(e.dst)!.push(e.src);
    }
    // For each node: which outputs it leads to (BFS upstream from each output).
    const reachByNode = new Map<string, Set<string>>();
    for (const out of outputs) {
      const seen = new Set<string>([out]);
      const stack = [out];
      while (stack.length) {
        const n = stack.pop()!;
        for (const up of upstreamMap.get(n) ?? []) {
          if (seen.has(up)) continue;
          seen.add(up);
          stack.push(up);
        }
      }
      for (const n of seen) {
        if (!reachByNode.has(n)) reachByNode.set(n, new Set());
        reachByNode.get(n)!.add(out);
      }
    }
    // Build groups: a node is exclusive to chain X if reachByNode(n) === {X}.
    const groups: { output: string; label: string; nodes: string[] }[] = [];
    for (const out of outputs) {
      const exclusive = nodes.filter((n) =>
        reachByNode.get(n)?.size === 1 && reachByNode.get(n)?.has(out),
      );
      if (exclusive.length < 2) continue; // skip trivial
      const label = out.replace(/^OUTPUTS\./, "").toLowerCase().replace(/_/g, " ");
      groups.push({ output: out, label: `${label} chain`, nodes: exclusive });
    }
    return groups;
  }, [nodes, edges, showSubgraphs]);

  if (!lineage) {
    return (
      <div className="px-8 py-16 text-center" style={{ color: "var(--ink-3)" }}>
        No lineage data — run the lineage agent to populate.
      </div>
    );
  }

  // When fullscreen the whole grid pops out of normal flow into a fixed overlay.
  // Critical: the parent gets explicit 100vh height and overflow:hidden so the
  // grid children — each with their own overflow-y:auto — actually scroll
  // instead of getting clipped at the viewport edge.
  const containerStyle: React.CSSProperties = fullscreen
    ? {
        position: "fixed", inset: 0, zIndex: 80,
        display: "grid",
        gridTemplateColumns: "320px 1fr 340px",
        gridTemplateRows: "100vh",
        height: "100vh",
        overflow: "hidden",
        background: "var(--bg)",
      }
    : {
        display: "grid", gridTemplateColumns: "320px 1fr 340px",
        minHeight: 720, background: "var(--bg)",
      };

  // Common grid-cell style that lets content scroll within its column.
  const cellMinHeight: React.CSSProperties = fullscreen ? { minHeight: 0 } : {};

  return (
    <div style={containerStyle}>
      {/* ─── Side rail ────────────────────────────────────────── */}
      <aside
        style={{
          borderRight: "1px solid var(--line)",
          padding: "24px 22px",
          overflowY: "auto",
          background: "var(--bg-elev)",
          ...cellMinHeight,
        }}
      >
        <div className="eyebrow">Explore</div>
        <div style={{ marginTop: 14, marginBottom: 18 }}>
          <div
            style={{
              display: "flex", alignItems: "center", gap: 8,
              padding: "8px 10px", border: "1px solid var(--line)",
              borderRadius: 6, background: "var(--bg)",
            }}
          >
            <Search className="h-3.5 w-3.5" strokeWidth={1.25} style={{ color: "var(--ink-3)" }} />
            <input
              className="mono"
              placeholder="search nodes…"
              value={search}
              onChange={(e) => setSearch(e.target.value)}
              style={{
                flex: 1, border: 0, outline: 0, background: "transparent",
                fontSize: 12.5, color: "var(--ink)",
              }}
            />
          </div>
        </div>

        {pipelineNames.length > 0 && (
          <>
            <div className="eyebrow" style={{ marginTop: 8 }}>Focus pipeline</div>
            <div style={{ marginTop: 12 }}>
              <select
                value={pipelineFilter ?? ""}
                onChange={(e) => setPipelineFilter(e.target.value || null)}
                className="mono"
                style={{
                  width: "100%", height: 32, padding: "0 8px",
                  fontSize: 12, color: "var(--ink)",
                  background: "var(--bg)", border: "1px solid var(--line)",
                  borderRadius: 6, outline: "none",
                }}
              >
                <option value="">— show all —</option>
                {pipelineNames.map((p) => (
                  <option key={p} value={p}>{p}</option>
                ))}
              </select>
              {pipelineFilter && (
                <button
                  onClick={() => setPipelineFilter(null)}
                  style={{
                    marginTop: 6, fontSize: 11,
                    background: "transparent", border: 0,
                    color: "var(--brand-emerald-700)", cursor: "pointer",
                    fontFamily: "var(--font-sans)", padding: 0,
                    display: "inline-flex", alignItems: "center", gap: 4,
                  }}
                >
                  <X className="h-3 w-3" strokeWidth={1.5} /> clear focus
                </button>
              )}
            </div>
          </>
        )}

        <div className="eyebrow" style={{ marginTop: 28 }}>Node types</div>
        <div style={{ marginTop: 12, display: "flex", flexDirection: "column", gap: 8 }}>
          {(["raw", "staging", "core", "pipeline", "output", "external", "unknown"] as NodeKind[]).map((kind) => {
            // Map kind back to one or more Layer ids for the existing layer-toggle state.
            // staging/core/pipeline/output/external all gate on layer presence; we keep the
            // simpler: toggling these hides nodes of that kind from the canvas.
            const ids: Layer[] = (
              kind === "raw" ? ["raw"] :
              kind === "staging" ? ["staging"] :
              kind === "core" ? ["integration", "reporting"] :
              kind === "output" ? ["output"] :
              kind === "pipeline" ? ["integration"] :
              kind === "external" ? ["unknown"] :
              ["unknown"]
            );
            const checked = ids.every((i) => layers[i] !== false);
            return (
              <label
                key={kind}
                style={{
                  display: "flex", alignItems: "center", gap: 10,
                  fontSize: 13, color: "var(--ink-2)", cursor: "pointer",
                }}
              >
                <input
                  type="checkbox"
                  checked={checked}
                  onChange={() => setLayers((l) => {
                    const next = { ...l };
                    for (const i of ids) next[i] = !checked;
                    return next;
                  })}
                  style={{ accentColor: KIND_STYLES[kind].stroke }}
                />
                <span
                  style={{
                    width: 10, height: 10, borderRadius: kind === "pipeline" ? 5 : 2,
                    background: KIND_STYLES[kind].fill,
                    border: `1px solid ${KIND_STYLES[kind].stroke}`,
                    flexShrink: 0,
                  }}
                />
                {KIND_LABEL[kind]}
              </label>
            );
          })}
        </div>

        <div className="eyebrow" style={{ marginTop: 28 }}>Display</div>
        <div style={{ marginTop: 12, display: "flex", flexDirection: "column", gap: 8 }}>
          <label style={{ display: "flex", alignItems: "center", gap: 10, fontSize: 13, color: "var(--ink-2)", cursor: "pointer" }}>
            <input
              type="checkbox"
              checked={showEdgeLabels}
              onChange={() => setShowEdgeLabels((v) => !v)}
              style={{ accentColor: "var(--brand-emerald)" }}
            />
            Edge labels
          </label>
          <label style={{ display: "flex", alignItems: "center", gap: 10, fontSize: 13, color: "var(--ink-2)", cursor: "pointer" }}>
            <input
              type="checkbox"
              checked={showSubgraphs}
              onChange={() => setShowSubgraphs((v) => !v)}
              style={{ accentColor: "var(--brand-emerald)" }}
            />
            Chain groupings
          </label>
        </div>

        <div className="eyebrow" style={{ marginTop: 28 }}>Stats</div>
        <dl style={{ margin: "12px 0 0", padding: 0 }}>
          <StatRow k="Edges" v={edges.length} />
          <StatRow k="Nodes" v={nodes.length} />
          <StatRow k="Unresolved" v={lineage.unresolved.length} />
        </dl>

        <div className="eyebrow" style={{ marginTop: 28 }}>Markers</div>
        <ul style={{ listStyle: "none", padding: 0, margin: "12px 0 0", fontSize: 13, color: "var(--ink-2)" }}>
          <li style={{ display: "flex", alignItems: "center", gap: 8, padding: "5px 0" }}>
            <span className="dot warn" style={{ width: 6, height: 6 }} /> warning
          </li>
          <li style={{ display: "flex", alignItems: "center", gap: 8, padding: "5px 0" }}>
            <span className="dot crit" style={{ width: 6, height: 6 }} /> dead object
          </li>
          <li style={{ display: "flex", alignItems: "center", gap: 8, padding: "5px 0" }}>
            <span className="dot muted" style={{ width: 6, height: 6 }} /> orphan
          </li>
        </ul>
      </aside>

      {/* ─── Graph canvas ────────────────────────────────────── */}
      <div style={{ position: "relative", padding: 20, borderRight: "1px solid var(--line)", ...cellMinHeight, overflow: fullscreen ? "auto" : undefined }}>
        <div style={{ position: "absolute", top: 28, left: 28, display: "flex", gap: 8, zIndex: 2 }}>
          <span
            className="mono"
            style={{
              display: "inline-flex", alignItems: "center", gap: 6,
              fontSize: 11.5, padding: "4px 8px",
              background: "var(--bg-sunk)", color: "var(--ink-2)",
              border: "1px solid var(--line)", borderRadius: 4,
              letterSpacing: "0.02em", textTransform: "uppercase",
            }}
          >
            <span className="dot ok" style={{ width: 6, height: 6 }} />
            {nodes.length} nodes · {edges.length} edges
          </span>
        </div>
        <div style={{ position: "absolute", top: 28, right: 28, display: "flex", gap: 8, zIndex: 2 }}>
          <button onClick={() => exportCsv(lineage, inferLayer, sensByCol, runId, pipelineFilter)} style={toolbarBtn} title="Export edges as CSV">
            <FileSpreadsheet className="h-3 w-3" strokeWidth={1.25} /> CSV
          </button>
          <button onClick={() => exportPng(svgRef.current, runId, pipelineFilter)} style={toolbarBtn} title="Export graph as PNG">
            <ImageIcon className="h-3 w-3" strokeWidth={1.25} /> PNG
          </button>
          <button onClick={() => setSelected(null)} style={toolbarBtn} title="Clear selection">
            Reset
          </button>
          <button onClick={() => setFullscreen((v) => !v)} style={toolbarBtn} title={fullscreen ? "Exit fullscreen (Esc)" : "Fullscreen"}>
            {fullscreen ? <Minimize2 className="h-3 w-3" strokeWidth={1.25} /> : <Maximize2 className="h-3 w-3" strokeWidth={1.25} />}
            {fullscreen ? " Exit" : " Fullscreen"}
          </button>
        </div>
        <div
          style={{
            height: "100%",
            minHeight: 600,
            border: "1px solid var(--line)",
            borderRadius: 8,
            background: "var(--bg-elev)",
            overflow: "hidden",
          }}
        >
          <GraphSVG
            svgRef={svgRef}
            W={layout.W}
            H={layout.H}
            nodes={nodes}
            edges={edges}
            pos={layout.pos}
            inferKind={inferKind}
            selected={selected}
            focusSet={focusSet}
            matches={matches}
            layerOk={layerOk}
            onSelect={(id) => setSelected((s) => (s === id ? null : id))}
            showEdgeLabels={showEdgeLabels}
            subgraphs={subgraphs}
            nativeSize={fullscreen}
          />
        </div>
      </div>

      {/* ─── Inspector ───────────────────────────────────────── */}
      <aside style={{ background: "var(--bg-elev)", padding: 24, overflowY: "auto", ...cellMinHeight }}>
        {sel ? (
          <>
            <div className="eyebrow">{KIND_LABEL[inferKind(sel.id)]}</div>
            <h3
              className="mono"
              style={{ fontSize: 15, fontWeight: 500, margin: "8px 0 4px", color: "var(--ink)", wordBreak: "break-all" }}
            >
              {sel.id}
            </h3>
            <div style={{ fontSize: 13, color: "var(--ink-3)" }}>
              {sel.id.split(".")[0]}
            </div>

            <div style={{ marginTop: 20 }}>
              <div className="eyebrow">Upstream ({upstream.length})</div>
              <ul style={{ listStyle: "none", padding: 0, margin: "10px 0 0" }}>
                {upstream.length === 0 && (
                  <li style={{ fontSize: 12, color: "var(--ink-4)", padding: "7px 0" }}>none — entry point</li>
                )}
                {upstream.map((id) => (
                  <li
                    key={id}
                    className="mono"
                    style={{
                      fontSize: 12, padding: "7px 10px", borderRadius: 4, marginBottom: 4,
                      background: "var(--bg-sunk)", color: "var(--ink-2)", cursor: "pointer",
                      wordBreak: "break-all",
                    }}
                    onClick={() => setSelected(id)}
                  >
                    ↑ {id}
                  </li>
                ))}
              </ul>
            </div>
            <div style={{ marginTop: 16 }}>
              <div className="eyebrow">Downstream ({downstream.length})</div>
              <ul style={{ listStyle: "none", padding: 0, margin: "10px 0 0" }}>
                {downstream.length === 0 && (
                  <li style={{ fontSize: 12, color: "var(--ink-4)", padding: "7px 0" }}>terminal node</li>
                )}
                {downstream.map((id) => (
                  <li
                    key={id}
                    className="mono"
                    style={{
                      fontSize: 12, padding: "7px 10px", borderRadius: 4, marginBottom: 4,
                      background: "var(--bg-sunk)", color: "var(--ink-2)", cursor: "pointer",
                      wordBreak: "break-all",
                    }}
                    onClick={() => setSelected(id)}
                  >
                    ↓ {id}
                  </li>
                ))}
              </ul>
            </div>
          </>
        ) : (
          <div style={{ color: "var(--ink-3)", fontSize: 13, lineHeight: 1.6 }}>
            Click any node to see its upstream and downstream.
          </div>
        )}
      </aside>
    </div>
  );
}

// ─── Toolbar / export helpers ───────────────────────────────────────────────

const toolbarBtn: React.CSSProperties = {
  display: "inline-flex", alignItems: "center", gap: 6,
  fontSize: 12.5, padding: "6px 10px",
  background: "var(--bg-elev)", color: "var(--ink)",
  border: "1px solid var(--line)", borderRadius: 6, cursor: "pointer",
  fontFamily: "var(--font-sans)",
};

function safeFilename(runId: string | undefined, pipelineFilter: string | null, ext: string): string {
  const id = (runId ?? "").slice(0, 8) || "lineage";
  const filter = pipelineFilter ? `-${pipelineFilter.replace(/[^A-Za-z0-9_-]/g, "_")}` : "";
  return `lineage-${id}${filter}.${ext}`;
}

function downloadBlob(blob: Blob, filename: string) {
  const url = URL.createObjectURL(blob);
  const a = document.createElement("a");
  a.href = url;
  a.download = filename;
  document.body.appendChild(a);
  a.click();
  document.body.removeChild(a);
  setTimeout(() => URL.revokeObjectURL(url), 5000);
}

function csvEscape(v: string | number | null | undefined): string {
  if (v === null || v === undefined) return "";
  const s = String(v);
  if (s.includes(",") || s.includes("\n") || s.includes('"')) {
    return `"${s.replace(/"/g, '""')}"`;
  }
  return s;
}

function exportCsv(
  lineage: LineageGraph,
  inferLayer: (fqn: string) => Layer,
  sensByCol: Map<string, Sensitivity>,
  runId: string | undefined,
  pipelineFilter: string | null,
) {
  const headers = [
    "source_object", "source_column", "source_layer", "source_sensitivity",
    "target_object", "target_column", "target_layer", "target_sensitivity",
    "operation", "transform_notes", "pipeline", "confidence", "is_pii_path",
  ];
  const lines: string[] = [headers.join(",")];

  // If a pipeline filter is active, restrict to edges produced by that pipeline OR
  // edges where source/target nodes belong to the same pipeline.
  const matchesFilter = (e: LineageEdge): boolean => {
    if (!pipelineFilter) return true;
    if ((e.origin_object ?? "").toUpperCase() === pipelineFilter.toUpperCase()) return true;
    if (e.source_fqn.startsWith(`PIPELINE.${pipelineFilter}`)) return true;
    if (e.target_fqn.startsWith(`PIPELINE.${pipelineFilter}`)) return true;
    return false;
  };

  for (const e of lineage.edges) {
    if (!matchesFilter(e)) continue;
    const srcLayer = inferLayer(e.source_fqn);
    const tgtLayer = inferLayer(e.target_fqn);
    const srcSens = sensByCol.get(`${e.source_fqn}.${e.source_column ?? ""}`.toUpperCase()) ?? "";
    const tgtSens = sensByCol.get(`${e.target_fqn}.${e.target_column ?? ""}`.toUpperCase()) ?? "";
    const isPii = srcSens === "pii" || tgtSens === "pii";
    lines.push([
      csvEscape(e.source_fqn),
      csvEscape(e.source_column),
      csvEscape(srcLayer),
      csvEscape(srcSens),
      csvEscape(e.target_fqn),
      csvEscape(e.target_column),
      csvEscape(tgtLayer),
      csvEscape(tgtSens),
      csvEscape(e.operation),
      csvEscape(e.transform),
      csvEscape(e.origin_object),
      csvEscape(e.confidence),
      csvEscape(isPii ? "true" : "false"),
    ].join(","));
  }
  const blob = new Blob([lines.join("\n") + "\n"], { type: "text/csv;charset=utf-8" });
  downloadBlob(blob, safeFilename(runId, pipelineFilter, "csv"));
}

function exportPng(svg: SVGSVGElement | null, runId: string | undefined, pipelineFilter: string | null) {
  if (!svg) return;
  const cloned = svg.cloneNode(true) as SVGSVGElement;
  // Inline a white background so the PNG isn't transparent
  const bgRect = document.createElementNS("http://www.w3.org/2000/svg", "rect");
  const vb = svg.viewBox.baseVal;
  bgRect.setAttribute("x", String(vb.x));
  bgRect.setAttribute("y", String(vb.y));
  bgRect.setAttribute("width", String(vb.width || svg.clientWidth));
  bgRect.setAttribute("height", String(vb.height || svg.clientHeight));
  bgRect.setAttribute("fill", "#FFFFFF");
  cloned.insertBefore(bgRect, cloned.firstChild);

  const xml = new XMLSerializer().serializeToString(cloned);
  const svgBlob = new Blob([xml], { type: "image/svg+xml;charset=utf-8" });
  const url = URL.createObjectURL(svgBlob);

  const img = new Image();
  img.onload = () => {
    const scale = 2; // 2x for high DPI
    const w = (vb.width || svg.clientWidth) * scale;
    const h = (vb.height || svg.clientHeight) * scale;
    const canvas = document.createElement("canvas");
    canvas.width = w;
    canvas.height = h;
    const ctx = canvas.getContext("2d");
    if (!ctx) return;
    ctx.fillStyle = "#FFFFFF";
    ctx.fillRect(0, 0, w, h);
    ctx.drawImage(img, 0, 0, w, h);
    canvas.toBlob((blob) => {
      if (blob) downloadBlob(blob, safeFilename(runId, pipelineFilter, "png"));
      URL.revokeObjectURL(url);
    }, "image/png");
  };
  img.onerror = () => URL.revokeObjectURL(url);
  img.src = url;
}


function StatRow({ k, v }: { k: string; v: number | string }) {
  return (
    <div
      style={{
        display: "flex", justifyContent: "space-between",
        padding: "9px 0", borderBottom: "1px dashed var(--line)",
        fontSize: 13,
      }}
    >
      <dt style={{ color: "var(--ink-3)" }}>{k}</dt>
      <dd className="mono" style={{ margin: 0, color: "var(--ink-2)", fontSize: 12.5 }}>{v}</dd>
    </div>
  );
}

function normalizeLayer(l: Layer): Layer {
  // Treat "output" nodes as the reporting column for layout purposes
  return l === "output" ? "reporting" : l;
}

// ─── SVG renderer ────────────────────────────────────────────────────────────

interface GraphSVGProps {
  svgRef?: React.MutableRefObject<SVGSVGElement | null>;
  W: number;
  H: number;
  nodes: string[];
  edges: { src: string; dst: string; operation: string }[];
  pos: Record<string, { x: number; y: number; col: number }>;
  inferKind: (id: string) => NodeKind;
  selected: string | null;
  focusSet: Set<string> | null;
  matches: (id: string) => boolean;
  layerOk: (id: string) => boolean;
  onSelect: (id: string) => void;
  showEdgeLabels: boolean;
  subgraphs: { output: string; label: string; nodes: string[] }[];
  /** When true, render at native pixel size (caller scrolls). When false,
   *  scale to fit the container via 100%/100% — the small-screen behaviour. */
  nativeSize?: boolean;
}

function GraphSVG({
  svgRef, W, H, nodes, edges, pos, inferKind, selected, focusSet, matches, layerOk, onSelect,
  showEdgeLabels, subgraphs, nativeSize = false,
}: GraphSVGProps) {
  // Compute bounding box per subgraph (with padding) so we can render the
  // chain banner behind the affected nodes.
  const subgraphBoxes = subgraphs.map((g) => {
    const ps = g.nodes.map((n) => pos[n]).filter(Boolean) as { x: number; y: number }[];
    if (ps.length === 0) return null;
    const xs = ps.map((p) => p.x);
    const ys = ps.map((p) => p.y);
    const padX = 105, padY = 28;
    const x = Math.min(...xs) - padX;
    const y = Math.min(...ys) - padY;
    const w = Math.max(...xs) - Math.min(...xs) + padX * 2;
    const h = Math.max(...ys) - Math.min(...ys) + padY * 2;
    return { ...g, x, y, w, h };
  }).filter(Boolean) as { output: string; label: string; nodes: string[]; x: number; y: number; w: number; h: number }[];

  // Operation → short edge label. Keep extract/load blank to reduce clutter.
  const labelFor = (op: string): string => {
    if (!op) return "";
    if (op === "extract" || op === "load" || op === "VIEW" || op === "pipeline") return "";
    if (op === "fk") return "FK";
    return op;
  };

  return (
    <svg
      ref={svgRef}
      xmlns="http://www.w3.org/2000/svg"
      viewBox={`0 0 ${W} ${H}`}
      width={nativeSize ? W : undefined}
      height={nativeSize ? H : undefined}
      style={{
        width: nativeSize ? `${W}px` : "100%",
        height: nativeSize ? `${H}px` : "100%",
        display: "block",
      }}
    >
      <defs>
        <pattern id="dotgrid" width="24" height="24" patternUnits="userSpaceOnUse">
          <circle cx="1" cy="1" r="0.6" fill="var(--line-strong)" opacity="0.5" />
        </pattern>
        <marker id="arrow" viewBox="0 0 10 10" refX="9" refY="5"
                markerWidth="7" markerHeight="7" orient="auto-start-reverse">
          <path d="M 0 0 L 10 5 L 0 10 z" fill="#6B7884" />
        </marker>
        <marker id="arrow-dim" viewBox="0 0 10 10" refX="9" refY="5"
                markerWidth="7" markerHeight="7" orient="auto-start-reverse">
          <path d="M 0 0 L 10 5 L 0 10 z" fill="var(--line)" />
        </marker>
      </defs>
      <rect width={W} height={H} fill="url(#dotgrid)" opacity="0.5" />

      {/* Subgraph banners — drawn behind everything so edges/nodes sit on top */}
      {subgraphBoxes.map((box, i) => (
        <g key={`sg-${i}`} style={{ pointerEvents: "none" }}>
          <rect
            x={box.x}
            y={box.y}
            width={box.w}
            height={box.h}
            rx={14}
            fill="#FFFFFF"
            stroke="#D4D1C7"
            strokeWidth={1.25}
            strokeDasharray="6 4"
            opacity={0.75}
          />
          <text
            x={box.x + 14}
            y={box.y + 18}
            style={{
              fontFamily: "var(--font-mono)",
              fontSize: 11,
              letterSpacing: "0.1em",
              textTransform: "uppercase",
              fill: "var(--ink-3)",
            }}
          >
            {box.label}
          </text>
        </g>
      ))}

      {/* Edges — orthogonal H-V-H with rounded line-joins and arrow heads */}
      {edges.map((e, i) => {
        const a = pos[e.src];
        const b = pos[e.dst];
        if (!a || !b) return null;
        const dim = !!selected && !(focusSet?.has(e.src) && focusSet?.has(e.dst));
        // Anchor points on each node's right/left edge (slight offset for
        // parallelograms — table-kind nodes have a 8px slant).
        const x1 = a.x + 88;
        const x2 = b.x - 88;
        const y1 = a.y;
        const y2 = b.y;
        const midX = x1 + (x2 - x1) * 0.5;
        // Elbow path with rounded corners via Q (quadratic) at the bends.
        const r = 8;
        const yDir = y2 > y1 ? 1 : y2 < y1 ? -1 : 0;
        let d: string;
        if (yDir === 0) {
          d = `M${x1},${y1} L${x2},${y2}`;
        } else {
          d = (
            `M${x1},${y1} ` +
            `L${midX - r},${y1} ` +
            `Q${midX},${y1} ${midX},${y1 + r * yDir} ` +
            `L${midX},${y2 - r * yDir} ` +
            `Q${midX},${y2} ${midX + r},${y2} ` +
            `L${x2},${y2}`
          );
        }
        const lbl = showEdgeLabels && !dim ? labelFor(e.operation) : "";
        return (
          <g key={i}>
            <path
              d={d}
              fill="none"
              stroke={dim ? "var(--line)" : "#6B7884"}
              strokeOpacity={dim ? 0.3 : 0.7}
              strokeWidth={1.25}
              strokeLinejoin="round"
              strokeLinecap="round"
              markerEnd={dim ? "url(#arrow-dim)" : "url(#arrow)"}
              style={{ transition: "opacity .2s, stroke .2s" }}
            />
            {lbl && (
              <g transform={`translate(${midX}, ${(y1 + y2) / 2})`}>
                <rect
                  x={-(lbl.length * 3.4 + 6)}
                  y={-7}
                  width={lbl.length * 6.8 + 12}
                  height={14}
                  rx={3}
                  fill="#FFFFFF"
                  stroke="var(--line)"
                  strokeWidth={0.75}
                />
                <text
                  textAnchor="middle"
                  y={4}
                  style={{
                    fontFamily: "var(--font-mono)",
                    fontSize: 9.5,
                    fill: "var(--ink-3)",
                    letterSpacing: "0.04em",
                  }}
                >
                  {lbl}
                </text>
              </g>
            )}
          </g>
        );
      })}

      {/* Nodes */}
      {nodes.map((id) => {
        const p = pos[id];
        if (!p) return null;
        const kind = inferKind(id);
        const c = KIND_STYLES[kind];
        const isMatch = matches(id);
        const isFocused = !selected || focusSet?.has(id);
        const dim = !isMatch || !isFocused || !layerOk(id);
        const isSel = selected === id;
        let label: string;
        if (kind === "pipeline") {
          label = id.replace(/^PIPELINE\./, "");
        } else if (id.startsWith("OUTPUTS.") || id.startsWith("SOURCE.")) {
          label = id.split(".").slice(1).join(".");
        } else {
          label = id.split(".").slice(-2).join(".");
        }

        const isPipeline = kind === "pipeline";
        const w = isPipeline ? 175 : 168;
        const h = 30;
        // Tables are rendered as parallelograms (data-flow symbol) — 8px slant.
        // Pipelines are rendered as rounded rectangles (process symbol).
        const slant = 8;
        const cx = p.x;
        const cy = p.y;
        const shape: React.ReactNode = isPipeline ? (
          <rect
            x={cx - w / 2}
            y={cy - h / 2}
            width={w}
            height={h}
            rx={15}
            fill={c.fill}
            stroke={isSel ? "#0FB37A" : c.stroke}
            strokeWidth={isSel ? 2 : 1.5}
          />
        ) : (
          <polygon
            points={[
              `${cx - w / 2 + slant},${cy - h / 2}`,
              `${cx + w / 2 + slant},${cy - h / 2}`,
              `${cx + w / 2 - slant},${cy + h / 2}`,
              `${cx - w / 2 - slant},${cy + h / 2}`,
            ].join(" ")}
            fill={c.fill}
            stroke={isSel ? "#0FB37A" : c.stroke}
            strokeWidth={isSel ? 2 : 1.5}
            strokeLinejoin="round"
          />
        );

        return (
          <g
            key={id}
            style={{ cursor: "pointer", opacity: dim ? 0.18 : 1, transition: "opacity .2s" }}
            onClick={() => onSelect(id)}
          >
            {shape}
            <text
              x={cx}
              y={cy + 4}
              textAnchor="middle"
              style={{
                fontFamily: "var(--font-mono)",
                fontSize: 10.5,
                fill: c.text,
                fontWeight: isPipeline ? 500 : 400,
              }}
            >
              {label.length > 24 ? label.slice(0, 23) + "…" : label}
            </text>
          </g>
        );
      })}
    </svg>
  );
}
