"use client";

import Link from "next/link";
import { usePathname } from "next/navigation";
import { Wordmark } from "./Brand";

export function SiteHeader() {
  const pathname = usePathname();
  const tab: "new" | "runs" =
    pathname?.startsWith("/runs") ? "runs" : "new";

  return (
    <header
      className="border-b"
      style={{
        height: 64,
        padding: "0 32px",
        display: "flex",
        alignItems: "center",
        justifyContent: "space-between",
        borderColor: "var(--line)",
        background: "var(--bg-elev)",
      }}
    >
      <Link href="/" className="flex items-center gap-8 no-underline">
        <Wordmark size={18} />
        <span style={{ width: 1, height: 22, background: "var(--line)" }} />
        <span style={{ fontSize: 13.5, color: "var(--ink-2)" }}>
          Lineage &amp; Usage Agents
        </span>
      </Link>
      <nav style={{ display: "flex", gap: 28, alignItems: "center", fontSize: 14, color: "var(--ink-2)" }}>
        <Link
          href="/"
          style={{
            color: tab === "new" ? "var(--ink)" : "var(--ink-2)",
            fontWeight: tab === "new" ? 500 : 400,
            textDecoration: "none",
          }}
        >
          New analysis
        </Link>
        <Link
          href="/runs"
          style={{
            color: tab === "runs" ? "var(--ink)" : "var(--ink-2)",
            fontWeight: tab === "runs" ? 500 : 400,
            textDecoration: "none",
          }}
        >
          Runs
        </Link>
        <span style={{ color: "var(--ink-2)" }}>Docs</span>
      </nav>
    </header>
  );
}
