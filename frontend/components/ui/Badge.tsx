import * as React from "react";
import { cva, type VariantProps } from "class-variance-authority";
import { cn } from "@/lib/utils";

const badge = cva(
  "inline-flex items-center gap-1.5 rounded px-2 py-0.5",
  {
    variants: {
      variant: {
        neutral: "",
        info:    "",
        ok:      "",
        warn:    "",
        crit:    "",
        accent:  "",
      },
    },
    defaultVariants: { variant: "neutral" },
  },
);

const STYLES: Record<string, React.CSSProperties> = {
  neutral: { background: "var(--bg-sunk)", color: "var(--ink-2)", border: "1px solid var(--line)" },
  info:    { background: "rgba(46,111,180,0.10)", color: "var(--info)", border: "1px solid rgba(46,111,180,0.25)" },
  ok:      { background: "var(--brand-emerald-100)", color: "var(--brand-emerald-700)", border: "1px solid rgba(15,179,122,0.3)" },
  warn:    { background: "var(--warn-bg)", color: "var(--warn)", border: "1px solid rgba(199,123,10,0.3)" },
  crit:    { background: "var(--crit-bg)", color: "var(--crit)", border: "1px solid rgba(192,54,44,0.3)" },
  accent:  { background: "var(--brand-emerald-100)", color: "var(--brand-emerald-700)", border: "1px solid rgba(15,179,122,0.3)" },
};

export interface BadgeProps
  extends React.HTMLAttributes<HTMLSpanElement>,
    VariantProps<typeof badge> {}

export function Badge({ className, variant, style, ...props }: BadgeProps) {
  return (
    <span
      className={cn(badge({ variant }), className)}
      style={{
        fontFamily: "var(--font-mono)", fontSize: 11, lineHeight: 1, fontWeight: 500,
        letterSpacing: "0.02em",
        ...STYLES[variant ?? "neutral"],
        ...style,
      }}
      {...props}
    />
  );
}
