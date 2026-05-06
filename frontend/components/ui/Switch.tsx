"use client";
import * as React from "react";
import { cn } from "@/lib/utils";

interface SwitchProps {
  checked: boolean;
  onCheckedChange: (next: boolean) => void;
  disabled?: boolean;
  id?: string;
  className?: string;
}

export function Switch({ checked, onCheckedChange, disabled, id, className }: SwitchProps) {
  return (
    <button
      type="button"
      role="switch"
      id={id}
      aria-checked={checked}
      disabled={disabled}
      onClick={() => onCheckedChange(!checked)}
      className={cn("relative inline-flex h-5 w-9 items-center rounded-full transition-colors", className)}
      style={{
        background: checked ? "var(--brand-emerald)" : "var(--bg-sunk)",
        border: `1px solid ${checked ? "var(--brand-emerald-700)" : "var(--line)"}`,
        cursor: disabled ? "not-allowed" : "pointer",
        opacity: disabled ? 0.5 : 1,
      }}
    >
      <span
        className={cn("inline-block h-3.5 w-3.5 rounded-full transition-transform")}
        style={{
          background: "#FFFFFF",
          transform: `translateX(${checked ? "18px" : "3px"})`,
          boxShadow: "0 1px 2px rgba(0,0,0,0.15)",
        }}
      />
    </button>
  );
}
