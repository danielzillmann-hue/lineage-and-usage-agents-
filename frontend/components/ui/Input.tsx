import * as React from "react";
import { cn } from "@/lib/utils";

export const Input = React.forwardRef<HTMLInputElement, React.InputHTMLAttributes<HTMLInputElement>>(
  ({ className, ...props }, ref) => (
    <input
      ref={ref}
      className={cn(
        "flex h-9 w-full rounded-md border border-[var(--color-border)] bg-[var(--color-bg-elev-1)] px-3 text-[13px] text-white placeholder:text-[var(--color-fg-subtle)] outline-none focus:border-[var(--color-cyan-accent)] focus:ring-2 focus:ring-[rgba(0,180,240,0.25)] transition",
        className,
      )}
      {...props}
    />
  ),
);
Input.displayName = "Input";
