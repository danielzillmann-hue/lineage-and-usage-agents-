import * as React from "react";
import { cn } from "@/lib/utils";

export const Input = React.forwardRef<HTMLInputElement, React.InputHTMLAttributes<HTMLInputElement>>(
  ({ className, style, ...props }, ref) => (
    <input
      ref={ref}
      className={cn("w-full outline-none transition", className)}
      style={{
        height: 36,
        padding: "8px 12px",
        fontSize: 13,
        fontFamily: "var(--font-sans)",
        color: "var(--ink)",
        background: "var(--bg-elev)",
        border: "1px solid var(--line)",
        borderRadius: "var(--r-md)",
        ...style,
      }}
      onFocus={(e) => { e.currentTarget.style.borderColor = "var(--brand-emerald)"; }}
      onBlur={(e) => { e.currentTarget.style.borderColor = "var(--line)"; }}
      {...props}
    />
  ),
);
Input.displayName = "Input";
