import { cn } from "@/lib/utils";

export function Progress({ value, className, indeterminate }: { value?: number; className?: string; indeterminate?: boolean }) {
  return (
    <div className={cn("h-1.5 w-full overflow-hidden rounded-full bg-[var(--color-bg-elev-2)]", className)}>
      {indeterminate ? (
        <div className="h-full w-1/3 shimmer rounded-full" />
      ) : (
        <div
          className="h-full rounded-full bg-gradient-to-r from-[var(--color-navy-500)] via-[var(--color-cyan-soft)] to-[var(--color-cyan-accent)] transition-[width] duration-500"
          style={{ width: `${Math.max(0, Math.min(100, value ?? 0))}%` }}
        />
      )}
    </div>
  );
}
