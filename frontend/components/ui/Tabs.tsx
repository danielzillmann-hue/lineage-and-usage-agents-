"use client";
import * as React from "react";
import { cn } from "@/lib/utils";

interface TabsContextValue {
  value: string;
  setValue: (v: string) => void;
}
const TabsCtx = React.createContext<TabsContextValue | null>(null);

export function Tabs({
  value: controlled,
  defaultValue,
  onValueChange,
  className,
  children,
}: {
  value?: string;
  defaultValue?: string;
  onValueChange?: (v: string) => void;
  className?: string;
  children: React.ReactNode;
}) {
  const [internal, setInternal] = React.useState(defaultValue ?? "");
  const value = controlled ?? internal;
  const setValue = (v: string) => {
    if (controlled === undefined) setInternal(v);
    onValueChange?.(v);
  };
  return (
    <TabsCtx.Provider value={{ value, setValue }}>
      <div className={className}>{children}</div>
    </TabsCtx.Provider>
  );
}

export function TabsList({ className, children }: { className?: string; children: React.ReactNode }) {
  return (
    <nav role="tablist" className={cn("inline-flex items-center gap-1", className)}>
      {children}
    </nav>
  );
}

export function TabsTrigger({
  value, className, children,
}: { value: string; className?: string; children: React.ReactNode }) {
  const ctx = React.useContext(TabsCtx)!;
  const active = ctx.value === value;
  return (
    <button
      role="tab"
      aria-selected={active}
      onClick={() => ctx.setValue(value)}
      className={cn("transition-colors cursor-pointer", className)}
      style={{
        background: "transparent",
        border: 0,
        borderBottom: `2px solid ${active ? "var(--brand-ink)" : "transparent"}`,
        padding: "12px 14px",
        fontSize: 14,
        fontWeight: active ? 500 : 400,
        color: active ? "var(--ink)" : "var(--ink-3)",
        marginBottom: -1,
        fontFamily: "var(--font-sans)",
      }}
    >
      {children}
    </button>
  );
}

export function TabsContent({
  value, className, children,
}: { value: string; className?: string; children: React.ReactNode }) {
  const ctx = React.useContext(TabsCtx)!;
  if (ctx.value !== value) return null;
  return <div className={className}>{children}</div>;
}
