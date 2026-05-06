import * as React from "react";
import { cva, type VariantProps } from "class-variance-authority";
import { cn } from "@/lib/utils";

const badge = cva(
  "inline-flex items-center gap-1.5 rounded-full px-2.5 py-0.5 text-[11px] font-medium border",
  {
    variants: {
      variant: {
        neutral: "border-[var(--color-border)] bg-[var(--color-bg-elev-2)] text-[var(--color-fg-muted)]",
        info:    "border-[rgba(0,180,240,0.35)] bg-[rgba(0,180,240,0.10)] text-[var(--color-cyan-soft)]",
        ok:      "border-[rgba(24,194,156,0.35)] bg-[rgba(24,194,156,0.10)] text-[var(--color-emerald)]",
        warn:    "border-[rgba(246,180,0,0.35)] bg-[rgba(246,180,0,0.08)] text-[var(--color-amber)]",
        crit:    "border-[rgba(244,71,107,0.40)] bg-[rgba(244,71,107,0.08)] text-[var(--color-rose)]",
        accent:  "border-[rgba(255,107,71,0.35)] bg-[rgba(255,107,71,0.08)] text-[var(--color-coral)]",
      },
    },
    defaultVariants: { variant: "neutral" },
  },
);

export interface BadgeProps
  extends React.HTMLAttributes<HTMLSpanElement>,
    VariantProps<typeof badge> {}

export function Badge({ className, variant, ...props }: BadgeProps) {
  return <span className={cn(badge({ variant }), className)} {...props} />;
}
