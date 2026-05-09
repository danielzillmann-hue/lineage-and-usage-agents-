"use client";

import { useEffect, useRef, useState } from "react";
import { Send, Loader2, Sparkles, Copy, Check } from "lucide-react";

import { api } from "@/lib/api";
import type { ChatTurn } from "@/lib/api";


const SUGGESTIONS = [
  "Which pipelines write to FACT_REGULATORY_AUDIT?",
  "Show the most complex pipeline by step count.",
  "Which raw tables aren't read by any pipeline?",
  "Summarise findings under 50 words for an exec.",
  "Which tables hold PII? Group by domain.",
  "What's the longest dependency chain?",
];


export function AskView({ runId }: { runId: string }) {
  const [history, setHistory] = useState<ChatTurn[]>([]);
  const [input, setInput] = useState("");
  const [thinking, setThinking] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const [copied, setCopied] = useState<number | null>(null);
  const scrollRef = useRef<HTMLDivElement>(null);

  // Auto-scroll on new messages
  useEffect(() => {
    if (scrollRef.current) {
      scrollRef.current.scrollTop = scrollRef.current.scrollHeight;
    }
  }, [history, thinking]);

  const send = async (q: string) => {
    if (!q.trim() || thinking) return;
    setError(null);
    const userTurn: ChatTurn = { role: "user", content: q.trim() };
    const newHistory = [...history, userTurn];
    setHistory(newHistory);
    setInput("");
    setThinking(true);
    try {
      const r = await api.chat(runId, { question: q, history });
      setHistory([...newHistory, { role: "assistant", content: r.answer }]);
    } catch (e: unknown) {
      setError(e instanceof Error ? e.message : String(e));
    } finally {
      setThinking(false);
    }
  };

  const copyAnswer = async (i: number, text: string) => {
    await navigator.clipboard.writeText(text);
    setCopied(i);
    setTimeout(() => setCopied(null), 1500);
  };

  return (
    <div style={{ maxWidth: 1100, margin: "0 auto", padding: "24px 32px 64px" }}>
      <div style={{ marginBottom: 20 }}>
        <h2 style={{ fontSize: 20, fontWeight: 500, color: "var(--ink)", margin: "0 0 6px" }}>
          Ask the agents
        </h2>
        <p style={{ fontSize: 13, color: "var(--ink-3)", lineHeight: 1.5, margin: 0 }}>
          Free-form questions about this run, answered from the agents&apos; outputs
          (inventory, lineage, usage, summary). Powered by Gemini 2.5 Pro — answers
          are grounded in the JSON you&apos;d see on the other tabs.
        </p>
      </div>

      {history.length === 0 && (
        <div style={{ marginBottom: 16 }}>
          <div className="eyebrow" style={{ marginBottom: 10 }}>Try one</div>
          <div style={{ display: "flex", flexWrap: "wrap", gap: 8 }}>
            {SUGGESTIONS.map((s) => (
              <button
                key={s}
                onClick={() => send(s)}
                style={{
                  fontSize: 12.5, padding: "6px 12px",
                  background: "var(--bg-elev)", color: "var(--ink-2)",
                  border: "1px solid var(--line)", borderRadius: 99,
                  cursor: "pointer",
                }}
              >
                {s}
              </button>
            ))}
          </div>
        </div>
      )}

      <div
        ref={scrollRef}
        style={{
          minHeight: 380,
          maxHeight: "calc(100vh - 320px)",
          overflowY: "auto",
          border: "1px solid var(--line)", borderRadius: 8,
          background: "var(--bg-elev)",
          padding: 24,
          display: "flex", flexDirection: "column", gap: 20,
        }}
      >
        {history.length === 0 && !thinking && (
          <div style={{ color: "var(--ink-3)", fontSize: 13, textAlign: "center", padding: "60px 0" }}>
            No conversation yet — ask anything about the analysis above.
          </div>
        )}
        {history.map((turn, i) => (
          <div
            key={i}
            style={{
              display: "flex",
              flexDirection: turn.role === "user" ? "row-reverse" : "row",
              gap: 12,
              alignItems: "flex-start",
            }}
          >
            <div
              style={{
                width: 28, height: 28, borderRadius: 14, flexShrink: 0,
                background: turn.role === "user" ? "var(--brand-emerald)" : "var(--invert-bg)",
                color: turn.role === "user" ? "#FFFFFF" : "var(--invert-fg)",
                display: "flex", alignItems: "center", justifyContent: "center",
                fontSize: 11, fontWeight: 600,
              }}
            >
              {turn.role === "user" ? "You" : <Sparkles className="h-3.5 w-3.5" strokeWidth={1.5} />}
            </div>
            <div
              style={{
                flex: 1, maxWidth: "80%",
                padding: "10px 14px",
                background: turn.role === "user" ? "var(--brand-emerald)" : "var(--bg)",
                color: turn.role === "user" ? "#FFFFFF" : "var(--ink)",
                border: turn.role === "user" ? "none" : "1px solid var(--line)",
                borderRadius: 8,
                fontSize: 13.5, lineHeight: 1.55,
                whiteSpace: "pre-wrap",
                wordBreak: "break-word",
                position: "relative",
              }}
            >
              {turn.content}
              {turn.role === "assistant" && (
                <button
                  onClick={() => copyAnswer(i, turn.content)}
                  title="Copy answer"
                  style={{
                    position: "absolute", top: 6, right: 6,
                    background: "transparent", border: 0,
                    color: "var(--ink-3)", cursor: "pointer",
                    padding: 4,
                  }}
                >
                  {copied === i
                    ? <Check className="h-3 w-3" strokeWidth={2} />
                    : <Copy className="h-3 w-3" strokeWidth={1.5} />}
                </button>
              )}
            </div>
          </div>
        ))}
        {thinking && (
          <div style={{ display: "flex", gap: 12, alignItems: "center", color: "var(--ink-3)", fontSize: 13 }}>
            <div
              style={{
                width: 28, height: 28, borderRadius: 14,
                background: "var(--invert-bg)", color: "var(--invert-fg)",
                display: "flex", alignItems: "center", justifyContent: "center",
              }}
            >
              <Sparkles className="h-3.5 w-3.5" strokeWidth={1.5} />
            </div>
            <Loader2 className="h-4 w-4 animate-spin" strokeWidth={1.5} />
            <span>Thinking…</span>
          </div>
        )}
      </div>

      {error && (
        <div style={{
          marginTop: 12, padding: 10, background: "var(--crit-bg)",
          color: "var(--crit)", borderRadius: 6, fontSize: 12.5,
        }}>
          {error}
        </div>
      )}

      <div style={{
        marginTop: 16, display: "flex", gap: 8,
      }}>
        <input
          value={input}
          onChange={(e) => setInput(e.target.value)}
          onKeyDown={(e) => { if (e.key === "Enter") send(input); }}
          placeholder="Ask anything about this run…"
          disabled={thinking}
          style={{
            flex: 1, padding: "12px 16px", fontSize: 14,
            background: "var(--bg-elev)", color: "var(--ink)",
            border: "1px solid var(--line)", borderRadius: 8,
            outline: "none",
            fontFamily: "var(--font-sans)",
          }}
        />
        <button
          onClick={() => send(input)}
          disabled={thinking || !input.trim()}
          style={{
            padding: "0 18px", fontSize: 13, fontWeight: 500,
            background: thinking || !input.trim() ? "var(--ink-4)" : "var(--brand-emerald)",
            color: "#fff", border: 0, borderRadius: 8,
            cursor: thinking || !input.trim() ? "not-allowed" : "pointer",
            display: "inline-flex", alignItems: "center", gap: 6,
          }}
        >
          <Send className="h-4 w-4" strokeWidth={1.5} /> Send
        </button>
      </div>
    </div>
  );
}
