import { SetupForm } from "@/components/setup/SetupForm";

export default function HomePage() {
  return (
    <div className="relative">
      <div className="absolute inset-0 bg-grid pointer-events-none [mask-image:radial-gradient(ellipse_at_top,black_20%,transparent_70%)]" />
      <section className="relative mx-auto max-w-[1400px] px-6 pt-16 pb-10">
        <div className="max-w-3xl">
          <div className="inline-flex items-center gap-2 rounded-full border border-[var(--color-border)] bg-[var(--color-bg-elev-1)]/60 px-3 py-1 text-[11px] text-[var(--color-fg-muted)] mb-6">
            <span className="relative flex h-1.5 w-1.5">
              <span className="absolute inline-flex h-full w-full rounded-full bg-[var(--color-cyan-accent)] opacity-75 animate-ping" />
              <span className="relative inline-flex h-1.5 w-1.5 rounded-full bg-[var(--color-cyan-accent)]" />
            </span>
            Multi-agent · Oracle warehouse · Lineage &amp; usage
          </div>
          <h1 className="text-[44px] md:text-[56px] font-semibold tracking-[-0.02em] leading-[1.05] gradient-text text-balance">
            Map your warehouse end-to-end. Find what&apos;s actually used.
          </h1>
          <p className="mt-5 max-w-2xl text-[15px] leading-relaxed text-[var(--color-fg-muted)] text-balance">
            Three agents read your DDL, data dictionary, and AWR exports — then produce a column-level
            lineage graph, a usage map, and a findings list with the kind of detail your platform team
            can act on tomorrow.
          </p>
        </div>
      </section>
      <section className="relative mx-auto max-w-[1400px] px-6 pb-24">
        <SetupForm />
      </section>
    </div>
  );
}
