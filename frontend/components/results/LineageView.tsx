"use client";

import { useMemo, useState } from "react";
import { Search, Maximize2 } from "lucide-react";
import type { Inventory, Layer, LineageGraph } from "@/lib/types";

const LAYER_COLORS: Record<Layer, { fill: string; stroke: string; text: string; dot: string }> = {
  raw:         { fill: "var(--bg-elev)",       stroke: "var(--ink-3)",          text: "var(--ink-2)",          dot: "var(--ink-3)" },
  staging:     { fill: "var(--bg-elev)",       stroke: "var(--ink-3)",          text: "var(--ink-2)",          dot: "var(--ink-3)" },
  integration: { fill: "var(--brand-emerald-100)", stroke: "var(--brand-emerald)", text: "var(--brand-emerald-700)", dot: "var(--brand-emerald)" },
  reporting:   { fill: "var(--brand-ink)",     stroke: "var(--brand-ink)",      text: "#FFFFFF",               dot: "var(--brand-ink)" },
  output:      { fill: "var(--brand-ink)",     stroke: "var(--brand-ink)",      text: "#FFFFFF",               dot: "var(--brand-ink)" },
  unknown:     { fill: "var(--bg-sunk)",       stroke: "var(--line-strong)",    text: "var(--ink-3)",          dot: "var(--line-strong)" },
};

const COLUMNS: Layer[] = ["raw", "staging", "integration", "reporting"];
const COL_LABELS: Record<Layer, string> = {
  raw: "RAW", staging: "STAGING", integration: "INTEGRATION", reporting: "REPORTING",
  output: "OUTPUTS", unknown: "UNKNOWN",
};

export function LineageView({ lineage, inventory }: { lineage?: LineageGraph; inventory?: Inventory }) {
  const [selected, setSelected] = useState<string | null>(null);
  const [search, setSearch] = useState("");
  const [layers, setLayers] = useState<Record<Layer, boolean>>({
    raw: true, staging: true, integration: true, reporting: true, output: true, unknown: true,
  });

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

    const nodeSet = new Set<string>();
    const edgeSet = new Map<string, { src: string; dst: string }>();
    for (const e of allEdges) {
      const s = collapse(e.source_fqn);
      const t = collapse(e.target_fqn);
      if (s === t) continue;
      nodeSet.add(s);
      nodeSet.add(t);
      const key = `${s}→${t}`;
      if (!edgeSet.has(key)) edgeSet.set(key, { src: s, dst: t });
    }
    return {
      nodes: [...nodeSet],
      edges: [...edgeSet.values()],
    };
  }, [lineage]);

  // Group nodes by column (layer) and assign positions
  const layout = useMemo(() => {
    const W = 1200, H = 720;
    const colXs: number[] = [120, 460, 800, 1080];
    const usedLayers: Layer[] = COLUMNS.filter((l) =>
      nodes.some((n) => normalizeLayer(inferLayer(n)) === l),
    );
    const colXMap: Record<string, number> = {};
    usedLayers.forEach((l, i) => {
      const denom = Math.max(usedLayers.length - 1, 1);
      colXMap[l] = 120 + (i * (W - 240)) / denom;
    });

    const byLayer: Record<string, string[]> = {};
    for (const n of nodes) {
      const l = normalizeLayer(inferLayer(n));
      (byLayer[l] ??= []).push(n);
    }
    for (const l of Object.keys(byLayer)) byLayer[l].sort();

    const pos: Record<string, { x: number; y: number; layer: Layer }> = {};
    for (const [l, ns] of Object.entries(byLayer)) {
      const spacing = (H - 100) / Math.max(ns.length, 1);
      ns.forEach((n, i) => {
        pos[n] = { x: colXMap[l] ?? 120, y: 80 + i * spacing, layer: l as Layer };
      });
    }
    return { W, H, pos, colXs, usedLayers, colXMap };
  }, [nodes, inferLayer]);

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

  if (!lineage) {
    return (
      <div className="px-8 py-16 text-center" style={{ color: "var(--ink-3)" }}>
        No lineage data — run the lineage agent to populate.
      </div>
    );
  }

  return (
    <div
      style={{
        display: "grid",
        gridTemplateColumns: "320px 1fr 340px",
        minHeight: 720,
        background: "var(--bg)",
      }}
    >
      {/* ─── Side rail ────────────────────────────────────────── */}
      <aside
        style={{
          borderRight: "1px solid var(--line)",
          padding: "24px 22px",
          overflowY: "auto",
          background: "var(--bg-elev)",
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

        <div className="eyebrow" style={{ marginTop: 8 }}>Layers</div>
        <div style={{ marginTop: 12, display: "flex", flexDirection: "column", gap: 8 }}>
          {(["raw", "staging", "integration", "reporting", "unknown"] as Layer[]).map((id) => (
            <label
              key={id}
              style={{
                display: "flex", alignItems: "center", gap: 10,
                fontSize: 13, color: "var(--ink-2)", cursor: "pointer",
              }}
            >
              <input
                type="checkbox"
                checked={layers[id]}
                onChange={() => setLayers((l) => ({ ...l, [id]: !l[id] }))}
                style={{ accentColor: "var(--brand-emerald)" }}
              />
              <span
                style={{
                  width: 8, height: 8, borderRadius: 99,
                  background: LAYER_COLORS[id].dot, flexShrink: 0,
                }}
              />
              {id.charAt(0).toUpperCase() + id.slice(1)}
            </label>
          ))}
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
      <div style={{ position: "relative", padding: 20, borderRight: "1px solid var(--line)" }}>
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
          <button
            onClick={() => setSelected(null)}
            style={{
              display: "inline-flex", alignItems: "center", gap: 6,
              fontSize: 13, padding: "8px 12px",
              background: "var(--bg-elev)", color: "var(--ink)",
              border: "1px solid var(--line)", borderRadius: 6, cursor: "pointer",
            }}
          >
            <Maximize2 className="h-3 w-3" strokeWidth={1.25} /> Reset
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
            W={layout.W}
            H={layout.H}
            nodes={nodes}
            edges={edges}
            pos={layout.pos}
            colXs={Object.values(layout.colXMap)}
            colLabels={layout.usedLayers.map((l) => COL_LABELS[l])}
            inferLayer={(n) => normalizeLayer(inferLayer(n))}
            selected={selected}
            focusSet={focusSet}
            matches={matches}
            layerOk={layerOk}
            onSelect={(id) => setSelected((s) => (s === id ? null : id))}
          />
        </div>
      </div>

      {/* ─── Inspector ───────────────────────────────────────── */}
      <aside style={{ background: "var(--bg-elev)", padding: 24, overflowY: "auto" }}>
        {sel ? (
          <>
            <div className="eyebrow">{COL_LABELS[sel.layer] ?? sel.layer.toUpperCase()}</div>
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
  W: number;
  H: number;
  nodes: string[];
  edges: { src: string; dst: string }[];
  pos: Record<string, { x: number; y: number; layer: Layer }>;
  colXs: number[];
  colLabels: string[];
  inferLayer: (id: string) => Layer;
  selected: string | null;
  focusSet: Set<string> | null;
  matches: (id: string) => boolean;
  layerOk: (id: string) => boolean;
  onSelect: (id: string) => void;
}

function GraphSVG({
  W, H, nodes, edges, pos, colXs, colLabels, inferLayer, selected, focusSet, matches, layerOk, onSelect,
}: GraphSVGProps) {
  return (
    <svg viewBox={`0 0 ${W} ${H}`} style={{ width: "100%", height: "100%", display: "block" }}>
      <defs>
        <pattern id="dotgrid" width="24" height="24" patternUnits="userSpaceOnUse">
          <circle cx="1" cy="1" r="0.6" fill="var(--line-strong)" opacity="0.5" />
        </pattern>
      </defs>
      <rect width={W} height={H} fill="url(#dotgrid)" opacity="0.5" />

      {/* Column labels */}
      {colLabels.map((label, i) => (
        <text
          key={label}
          x={colXs[i]}
          y={32}
          textAnchor="middle"
          style={{
            fontFamily: "var(--font-mono)",
            fontSize: 10.5,
            letterSpacing: "0.12em",
            fill: "var(--ink-3)",
          }}
        >
          {label}
        </text>
      ))}

      {/* Edges */}
      {edges.map((e, i) => {
        const a = pos[e.src];
        const b = pos[e.dst];
        if (!a || !b) return null;
        const dim = !!selected && !(focusSet?.has(e.src) && focusSet?.has(e.dst));
        const x1 = a.x + 80, x2 = b.x - 80, y1 = a.y, y2 = b.y;
        const cx1 = x1 + (x2 - x1) * 0.5;
        const cx2 = x1 + (x2 - x1) * 0.5;
        return (
          <path
            key={i}
            d={`M${x1},${y1} C${cx1},${y1} ${cx2},${y2} ${x2},${y2}`}
            fill="none"
            stroke={dim ? "var(--line)" : "var(--ink-4)"}
            strokeOpacity={dim ? 0.3 : 0.55}
            strokeWidth={1}
            style={{ transition: "opacity .2s, stroke .2s" }}
          />
        );
      })}

      {/* Nodes */}
      {nodes.map((id) => {
        const p = pos[id];
        if (!p) return null;
        const layer = inferLayer(id);
        const c = LAYER_COLORS[layer];
        const isMatch = matches(id);
        const isFocused = !selected || focusSet?.has(id);
        const dim = !isMatch || !isFocused || !layerOk(id);
        const isSel = selected === id;
        const label = id.split(".").slice(-2).join(".");
        return (
          <g
            key={id}
            style={{ cursor: "pointer", opacity: dim ? 0.25 : 1, transition: "opacity .2s" }}
            onClick={() => onSelect(id)}
          >
            <rect
              x={p.x - 80}
              y={p.y - 14}
              width={160}
              height={28}
              rx={4}
              fill={c.fill}
              stroke={isSel ? "var(--brand-emerald)" : c.stroke}
              strokeWidth={isSel ? 1.5 : 1}
            />
            <text
              x={p.x}
              y={p.y + 4}
              textAnchor="middle"
              style={{ fontFamily: "var(--font-mono)", fontSize: 10.5, fill: c.text }}
            >
              {label.length > 22 ? label.slice(0, 21) + "…" : label}
            </text>
          </g>
        );
      })}
    </svg>
  );
}
