import type { Metadata } from "next";
import { Geist, Geist_Mono } from "next/font/google";
import Link from "next/link";
import "./globals.css";

const geistSans = Geist({ variable: "--font-geist-sans", subsets: ["latin"] });
const geistMono = Geist_Mono({ variable: "--font-geist-mono", subsets: ["latin"] });

export const metadata: Metadata = {
  title: "Lineage and Usage Agents — Insignia Financial",
  description:
    "Multi-agent analysis of an Oracle data warehouse: schema inventory, lineage graph, and usage analytics.",
};

export default function RootLayout({ children }: { children: React.ReactNode }) {
  return (
    <html lang="en" className={`${geistSans.variable} ${geistMono.variable} h-full antialiased`}>
      <body className="min-h-full flex flex-col">
        <header className="sticky top-0 z-40 border-b border-[var(--color-border)] backdrop-blur-md bg-[rgba(6,11,26,0.7)]">
          <div className="mx-auto max-w-[1400px] px-6 h-14 flex items-center justify-between">
            <Link href="/" className="flex items-center gap-3 group">
              <span className="relative inline-flex h-8 w-8 items-center justify-center rounded-md bg-gradient-to-br from-[var(--color-navy-500)] to-[var(--color-cyan-accent)] shadow-md">
                <span className="absolute inset-[2px] rounded-[5px] bg-[var(--color-bg)]" />
                <span className="relative font-mono text-[10px] font-bold tracking-tight text-[var(--color-cyan-soft)]">IF</span>
              </span>
              <div className="leading-tight">
                <div className="text-[13px] font-semibold tracking-tight text-white">Lineage &amp; Usage Agents</div>
                <div className="text-[10.5px] text-[var(--color-fg-muted)]">Insignia Financial · technical preview</div>
              </div>
            </Link>
            <nav className="hidden md:flex items-center gap-1 text-[12px]">
              <Link href="/" className="px-3 py-1.5 rounded-md text-[var(--color-fg-muted)] hover:text-white hover:bg-white/5 transition">New analysis</Link>
              <Link href="/runs" className="px-3 py-1.5 rounded-md text-[var(--color-fg-muted)] hover:text-white hover:bg-white/5 transition">Runs</Link>
            </nav>
            <div className="text-[11px] text-[var(--color-fg-subtle)] font-mono">
              made by <span className="text-[var(--color-cyan-soft)]">intelia</span>
            </div>
          </div>
        </header>
        <main className="flex-1">{children}</main>
        <footer className="border-t border-[var(--color-border-soft)] mt-auto">
          <div className="mx-auto max-w-[1400px] px-6 py-6 flex flex-col md:flex-row items-center justify-between gap-3 text-[11.5px] text-[var(--color-fg-subtle)]">
            <div>© 2026 Intelia · Demonstration build for Insignia Financial</div>
            <div className="flex items-center gap-4">
              <span>Cloud Run · australia-southeast1</span>
              <span className="font-mono">v0.1.0</span>
            </div>
          </div>
        </footer>
      </body>
    </html>
  );
}
