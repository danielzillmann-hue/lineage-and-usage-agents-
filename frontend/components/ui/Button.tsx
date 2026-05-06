import * as React from "react";
import { cva, type VariantProps } from "class-variance-authority";
import { cn } from "@/lib/utils";

const button = cva(
  "inline-flex items-center justify-center gap-2 rounded-md font-medium transition-all focus-visible:outline-none focus-visible:ring-2 focus-visible:ring-[var(--color-cyan-accent)] disabled:opacity-50 disabled:cursor-not-allowed select-none",
  {
    variants: {
      variant: {
        primary:
          "bg-gradient-to-b from-[var(--color-cyan-accent)] to-[#0099d4] text-[#001520] shadow-[0_8px_24px_-8px_rgba(0,180,240,0.6)] hover:brightness-110 active:brightness-95",
        secondary:
          "bg-[var(--color-bg-elev-2)] text-white border border-[var(--color-border)] hover:bg-[var(--color-bg-elev-3)] hover:border-[var(--color-navy-500)]",
        ghost:
          "text-[var(--color-fg-muted)] hover:text-white hover:bg-white/5",
        outline:
          "border border-[var(--color-border)] text-white hover:border-[var(--color-cyan-accent)] hover:text-[var(--color-cyan-soft)]",
        destructive:
          "bg-[var(--color-rose)] text-white hover:brightness-110",
      },
      size: {
        sm: "h-8 px-3 text-[12px]",
        md: "h-9 px-4 text-[13px]",
        lg: "h-11 px-6 text-[14px]",
        icon: "h-9 w-9",
      },
    },
    defaultVariants: { variant: "primary", size: "md" },
  },
);

export interface ButtonProps
  extends React.ButtonHTMLAttributes<HTMLButtonElement>,
    VariantProps<typeof button> {}

export const Button = React.forwardRef<HTMLButtonElement, ButtonProps>(
  ({ className, variant, size, ...props }, ref) => (
    <button ref={ref} className={cn(button({ variant, size }), className)} {...props} />
  ),
);
Button.displayName = "Button";
