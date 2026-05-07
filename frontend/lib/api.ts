import type {
  BucketPreview, Column, ColumnAnnotationUpdate, DemoDefaults, OracleConnection,
  Run, RunRequest, RunResults, StreamEvent, TestConnectionResponse,
} from "./types";

const API_BASE = process.env.NEXT_PUBLIC_API_BASE ?? "http://localhost:8080";

async function jfetch<T>(path: string, init?: RequestInit): Promise<T> {
  const r = await fetch(`${API_BASE}${path}`, {
    ...init,
    headers: { "content-type": "application/json", ...(init?.headers ?? {}) },
    cache: "no-store",
  });
  if (!r.ok) throw new Error(`${r.status} ${r.statusText}: ${await r.text()}`);
  return r.json() as Promise<T>;
}

export const api = {
  listBuckets: () => jfetch<string[]>("/api/buckets"),
  previewBucket: (bucket: string, prefix = "") =>
    jfetch<BucketPreview>(`/api/buckets/${encodeURIComponent(bucket)}/preview?prefix=${encodeURIComponent(prefix)}`),

  demoDefaults: () => jfetch<DemoDefaults>("/api/demo-defaults"),
  testOracle: (conn: OracleConnection) =>
    jfetch<TestConnectionResponse>("/api/oracle/test", { method: "POST", body: JSON.stringify(conn) }),

  createRun: (req: RunRequest) =>
    jfetch<Run>("/api/runs", { method: "POST", body: JSON.stringify(req) }),
  listRuns: () => jfetch<Run[]>("/api/runs"),
  getRun: (id: string) => jfetch<Run>(`/api/runs/${id}`),
  getResults: (id: string) => jfetch<RunResults>(`/api/runs/${id}/results`),
  patchColumn: (id: string, update: ColumnAnnotationUpdate) =>
    jfetch<Column>(`/api/runs/${id}/columns`, { method: "PATCH", body: JSON.stringify(update) }),

  streamUrl: (id: string) => `${API_BASE}/api/runs/${id}/stream`,

  // ─── Transform tab ────────────────────────────────────────────────
  transformGenerate: (id: string) =>
    jfetch<TransformResponse>(`/api/runs/${id}/transform`, { method: "POST" }),
  transformManifest: (id: string) =>
    jfetch<TransformManifestResponse>(`/api/runs/${id}/transform`),
  transformListFiles: (id: string) =>
    jfetch<string[]>(`/api/runs/${id}/transform/files`),
  transformReadFile: async (id: string, path: string): Promise<string> => {
    const r = await fetch(`${API_BASE}/api/runs/${id}/transform/files/${path}`, { cache: "no-store" });
    if (!r.ok) throw new Error(`${r.status} ${r.statusText}`);
    return r.text();
  },
  transformDownloadUrl: (id: string) =>
    `${API_BASE}/api/runs/${id}/transform/download.zip`,
};

export type TransformResponse = {
  run_id: string;
  pipelines_generated: number;
  sources_declared: number;
  operations_generated: number;
  files: string[];
  warnings: string[];
};

export type TransformManifestResponse = {
  run_id: string;
  pipelines: string[];
  sources: string[];
  operations: string[];
  files: string[];
  warnings: string[];
  generated_at: string;
};

export function streamRun(
  id: string,
  onEvent: (e: StreamEvent) => void,
  onError?: (e: Event) => void,
): () => void {
  const es = new EventSource(api.streamUrl(id));
  const handler = (kind: StreamEvent["event"]) => (msg: MessageEvent) => {
    try {
      const parsed = JSON.parse(msg.data) as StreamEvent;
      onEvent(parsed);
    } catch {
      onEvent({ event: kind, message: msg.data, ts: new Date().toISOString() });
    }
  };
  ["status", "log", "thinking", "result", "error", "done"].forEach((kind) =>
    es.addEventListener(kind, handler(kind as StreamEvent["event"])),
  );
  if (onError) es.onerror = onError;
  es.addEventListener("done", () => es.close());
  return () => es.close();
}
