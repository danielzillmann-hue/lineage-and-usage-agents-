"use client";

import { useEffect, useMemo, useState } from "react";
import { Download, FileCode2, FolderTree, Play, RefreshCw, AlertTriangle } from "lucide-react";

import { api } from "@/lib/api";
import type { TransformManifestResponse } from "@/lib/api";

// ── Visual taxonomy for the file tree ─────────────────────────────────
//   primary    → green  (definitions/<pipeline>.sqlx — produced tables)
//   operations → orange (definitions/operations/*.sqlx — DML scripts)
//   sources    → blue   (definitions/sources.sqlx — declarations)
//   meta       → grey   (workflow_settings.yaml, README.md)
type FileKind = "primary" | "operations" | "sources" | "meta";

const KIND_STYLES: Record<FileKind, { fg: string; bg: string; border: string }> = {
  primary:    { fg: "#1B5E20", bg: "#E8F5E9", border: "#388E3C" },
  operations: { fg: "#E65100", bg: "#FFF3E0", border: "#F57C00" },
  sources:    { fg: "#01579B", bg: "#E1F5FE", border: "#0288D1" },
  meta:       { fg: "var(--ink-3)", bg: "var(--bg-sunk)", border: "var(--line)" },
};


function classifyFile(path: string): FileKind {
  if (path.startsWith("definitions/operations/")) return "operations";
  if (path === "definitions/sources.sqlx") return "sources";
  if (path.startsWith("definitions/")) return "primary";
  return "meta";
}


export function TransformView({ runId }: { runId: string }) {
  const [manifest, setManifest] = useState<TransformManifestResponse | null>(null);
  const [generating, setGenerating] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const [selectedPath, setSelectedPath] = useState<string | null>(null);
  const [fileContent, setFileContent] = useState<string>("");
  const [loadingFile, setLoadingFile] = useState(false);

  // Initial manifest fetch — 404 is expected if the user hasn't generated yet.
  useEffect(() => {
    api.transformManifest(runId)
      .then((m) => {
        setManifest(m);
        // Auto-select the first primary file
        const firstPrimary = m.files.find((p) =>
          p.startsWith("definitions/") && !p.startsWith("definitions/operations/") && p !== "definitions/sources.sqlx"
        );
        if (firstPrimary) setSelectedPath(firstPrimary);
      })
      .catch(() => {
        setManifest(null);
      });
  }, [runId]);

  // Load selected file content
  useEffect(() => {
    if (!selectedPath) return;
    setLoadingFile(true);
    api.transformReadFile(runId, selectedPath)
      .then(setFileContent)
      .catch((e) => setFileContent(`// failed to load: ${e.message}`))
      .finally(() => setLoadingFile(false));
  }, [runId, selectedPath]);

  const handleGenerate = async () => {
    setGenerating(true);
    setError(null);
    try {
      await api.transformGenerate(runId);
      // Refetch manifest after generation
      const m = await api.transformManifest(runId);
      setManifest(m);
      const firstPrimary = m.files.find((p) =>
        p.startsWith("definitions/") && !p.startsWith("definitions/operations/") && p !== "definitions/sources.sqlx"
      );
      if (firstPrimary) setSelectedPath(firstPrimary);
    } catch (e: unknown) {
      const msg = e instanceof Error ? e.message : String(e);
      setError(msg);
    } finally {
      setGenerating(false);
    }
  };

  // Group files by directory for tree rendering
  const fileGroups = useMemo(() => {
    if (!manifest) return [];
    const groups: Record<string, string[]> = {};
    for (const path of manifest.files) {
      const parts = path.split("/");
      const dir = parts.length > 1 ? parts.slice(0, -1).join("/") : "(root)";
      (groups[dir] ??= []).push(path);
    }
    return Object.entries(groups)
      .sort(([a], [b]) => a.localeCompare(b))
      .map(([dir, paths]) => ({ dir, paths: paths.sort() }));
  }, [manifest]);

  // ── Empty state ─────────────────────────────────────────────────────
  if (!manifest) {
    return (
      <div style={{ padding: "64px 32px", textAlign: "center", color: "var(--ink-3)" }}>
        <FileCode2 className="h-10 w-10 mx-auto mb-4" strokeWidth={1.25} style={{ opacity: 0.4 }} />
        <h3 style={{ fontSize: 18, fontWeight: 500, color: "var(--ink)", marginBottom: 8 }}>
          Generate Dataform SQLX
        </h3>
        <p style={{ fontSize: 14, maxWidth: 480, margin: "0 auto 24px" }}>
          Translate the Oracle pipelines analysed in this run into a deployable
          Dataform project (BigQuery SQLX with proper <code>{"${ref()}"}</code>{" "}
          syntax, source declarations, and post-load operations).
        </p>
        <button
          onClick={handleGenerate}
          disabled={generating}
          style={{
            display: "inline-flex", alignItems: "center", gap: 8,
            padding: "10px 20px", fontSize: 14, fontWeight: 500,
            background: generating ? "var(--ink-4)" : "var(--brand-emerald)",
            color: "#fff", border: 0, borderRadius: 6,
            cursor: generating ? "wait" : "pointer",
          }}
        >
          {generating
            ? <><RefreshCw className="h-4 w-4 animate-spin" strokeWidth={1.5} /> Generating…</>
            : <><Play className="h-4 w-4" strokeWidth={1.5} /> Generate Dataform project</>}
        </button>
        {error && (
          <div style={{
            marginTop: 24, padding: 12, background: "var(--crit-bg)",
            color: "var(--crit)", borderRadius: 6, fontSize: 13, maxWidth: 480,
            margin: "24px auto 0",
          }}>
            <AlertTriangle className="h-4 w-4 inline mr-2" strokeWidth={1.5} /> {error}
          </div>
        )}
      </div>
    );
  }

  // ── Generated state ─────────────────────────────────────────────────
  return (
    <div style={{ display: "grid", gridTemplateColumns: "320px 1fr", minHeight: 720, background: "var(--bg)" }}>
      {/* Left: file tree + summary */}
      <aside style={{
        borderRight: "1px solid var(--line)", padding: "24px 22px",
        overflowY: "auto", background: "var(--bg-elev)",
      }}>
        <div className="eyebrow">Project</div>
        <h3 style={{ fontSize: 15, fontWeight: 500, margin: "8px 0 14px", color: "var(--ink)" }}>
          {manifest.pipelines.length} pipelines · {manifest.files.length} files
        </h3>

        <div style={{ display: "flex", gap: 8, marginBottom: 22 }}>
          <a
            href={api.transformDownloadUrl(runId)}
            style={{
              display: "inline-flex", alignItems: "center", gap: 6,
              padding: "6px 12px", fontSize: 12.5, fontWeight: 500,
              background: "var(--brand-emerald)", color: "#fff",
              borderRadius: 6, textDecoration: "none",
            }}
          >
            <Download className="h-3.5 w-3.5" strokeWidth={1.5} /> Download zip
          </a>
          <button
            onClick={handleGenerate}
            disabled={generating}
            style={{
              padding: "6px 12px", fontSize: 12.5,
              background: "transparent", color: "var(--ink-2)",
              border: "1px solid var(--line)", borderRadius: 6, cursor: "pointer",
            }}
            title="Re-generate from latest pipeline XMLs"
          >
            <RefreshCw className={`h-3.5 w-3.5 inline mr-1 ${generating ? "animate-spin" : ""}`} strokeWidth={1.5} />
            Regenerate
          </button>
        </div>

        {manifest.warnings.length > 0 && (
          <div style={{
            padding: 10, marginBottom: 16, background: "var(--warn-bg)",
            borderRadius: 4, fontSize: 12, color: "var(--warn)",
          }}>
            <AlertTriangle className="h-3.5 w-3.5 inline mr-1" />
            {manifest.warnings.length} warning{manifest.warnings.length === 1 ? "" : "s"}
          </div>
        )}

        <div className="eyebrow">Files</div>
        <div style={{ marginTop: 12 }}>
          {fileGroups.map(({ dir, paths }) => (
            <div key={dir} style={{ marginBottom: 12 }}>
              <div className="mono" style={{
                fontSize: 11, color: "var(--ink-3)", padding: "4px 0",
                display: "flex", alignItems: "center", gap: 6,
              }}>
                <FolderTree className="h-3 w-3" strokeWidth={1.5} />
                {dir}
              </div>
              <ul style={{ listStyle: "none", padding: 0, margin: 0 }}>
                {paths.map((path) => {
                  const kind = classifyFile(path);
                  const c = KIND_STYLES[kind];
                  const leaf = path.split("/").pop();
                  const isSel = selectedPath === path;
                  return (
                    <li key={path}>
                      <button
                        onClick={() => setSelectedPath(path)}
                        className="mono"
                        style={{
                          display: "flex", alignItems: "center", gap: 6,
                          width: "100%", textAlign: "left",
                          padding: "5px 8px", fontSize: 12,
                          background: isSel ? c.bg : "transparent",
                          color: isSel ? c.fg : "var(--ink-2)",
                          border: 0, borderLeft: `2px solid ${isSel ? c.border : "transparent"}`,
                          borderRadius: 0, cursor: "pointer",
                          wordBreak: "break-all",
                        }}
                      >
                        <FileCode2 className="h-3 w-3 flex-shrink-0" strokeWidth={1.5} />
                        {leaf}
                      </button>
                    </li>
                  );
                })}
              </ul>
            </div>
          ))}
        </div>
      </aside>

      {/* Right: file content */}
      <main style={{ padding: "24px 32px", overflow: "auto" }}>
        {selectedPath ? (
          <>
            <div className="eyebrow">{classifyFile(selectedPath)}</div>
            <h3 className="mono" style={{
              fontSize: 14, fontWeight: 500, margin: "8px 0 16px",
              color: "var(--ink)", wordBreak: "break-all",
            }}>
              {selectedPath}
            </h3>
            <pre className="mono" style={{
              padding: 20, background: "var(--bg-elev)",
              border: "1px solid var(--line)", borderRadius: 6,
              fontSize: 12.5, lineHeight: 1.6,
              color: "var(--ink)", overflow: "auto",
              whiteSpace: "pre-wrap",
            }}>
              {loadingFile ? "Loading…" : fileContent}
            </pre>
          </>
        ) : (
          <div style={{ color: "var(--ink-3)", fontSize: 14, paddingTop: 80, textAlign: "center" }}>
            Select a file to view its contents.
          </div>
        )}
      </main>
    </div>
  );
}
