import * as React from "react";
import { cva, type VariantProps } from "class-variance-authority";
import { cn } from "@/lib/utils";

const button = cva(
  "inline-flex items-center justify-center gap-2 font-medium transition-colors disabled:opacity-50 disabled:cursor-not-allowed select-none cursor-pointer",
  {
    variants: {
      variant: {
        primary: "",
        secondary: "",
        ghost: "",
        outline: "",
        destructive: "",
      },
      size: {
        sm: "",
        md: "",
        lg: "",
        icon: "",
      },
    },
    defaultVariants: { variant: "primary", size: "md" },
  },
);

const VARIANTS: Record<string, React.CSSProperties> = {
  primary:     { background: "var(--brand-ink)", color: "#FFFFFF", border: "1px solid var(--brand-ink)" },
  secondary:   { background: "var(--bg-elev)", color: "var(--ink)", border: "1px solid var(--line)" },
  ghost:       { background: "transparent", color: "var(--ink-2)", border: "1px solid transparent" },
  outline:     { background: "transparent", color: "var(--ink)", border: "1px solid var(--line)" },
  destructive: { background: "var(--crit)", color: "#FFFFFF", border: "1px solid var(--crit)" },
};

const SIZES: Record<string, React.CSSProperties> = {
  sm:   { fontSize: 12, padding: "6px 10px", borderRadius: "var(--r-md)", height: 28 },
  md:   { fontSize: 14, padding: "8px 14px", borderRadius: "var(--r-md)", height: 36 },
  lg:   { fontSize: 14, padding: "10px 18px", borderRadius: "var(--r-md)", height: 42, fontWeight: 500 },
  icon: { padding: 0, borderRadius: "var(--r-md)", height: 36, width: 36 },
};

export interface ButtonProps
  extends React.ButtonHTMLAttributes<HTMLButtonElement>,
    VariantProps<typeof button> {}

export const Button = React.forwardRef<HTMLButtonElement, ButtonProps>(
  ({ className, variant, size, style, ...props }, ref) => (
    <button
      ref={ref}
      className={cn(button({ variant, size }), className)}
      style={{
        fontFamily: "var(--font-sans)",
        ...VARIANTS[variant ?? "primary"],
        ...SIZES[size ?? "md"],
        ...style,
      }}
      {...props}
    />
  ),
);
Button.displayName = "Button";
