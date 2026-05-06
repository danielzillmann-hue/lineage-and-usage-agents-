import { cn } from "@/lib/utils";

export function Progress({
  value, className, indeterminate,
}: { value?: number; className?: string; indeterminate?: boolean }) {
  return (
    <div
      className={cn("h-1.5 w-full overflow-hidden", className)}
      style={{
        background: "var(--bg-sunk)",
        border: "1px solid var(--line)",
        borderRadius: 99,
      }}
    >
      <div
        style={{
          width: indeterminate ? "30%" : `${Math.max(0, Math.min(100, value ?? 0))}%`,
          height: "100%",
          background: "var(--brand-emerald)",
          borderRadius: 99,
          transition: "width 0.5s",
          animation: indeterminate ? "shimmer 1.5s linear infinite" : "none",
        }}
      />
    </div>
  );
}
