"use client";

import { useEffect, useState } from "react";
import { Sun, Moon, Monitor } from "lucide-react";
import { applyTheme, readStoredChoice, type ThemeChoice, type Theme, resolveTheme } from "@/lib/theme";

const ORDER: ThemeChoice[] = ["light", "dark", "system"];

const META: Record<ThemeChoice, { label: string; Icon: typeof Sun }> = {
  light:  { label: "Light",  Icon: Sun },
  dark:   { label: "Dark",   Icon: Moon },
  system: { label: "System", Icon: Monitor },
};

/**
 * Three-state toggle: Light → Dark → System → Light. Stored choice is
 * read on mount; before that we render a skeleton so server and client
 * markup match. The `<head>` init script has already set the visible
 * theme — this component just controls the *choice*.
 */
export function ThemeToggle() {
  const [mounted, setMounted] = useState(false);
  const [choice, setChoice] = useState<ThemeChoice>("system");
  const [resolved, setResolved] = useState<Theme>("light");

  useEffect(() => {
    const c = readStoredChoice();
    setChoice(c);
    setResolved(resolveTheme(c));
    setMounted(true);
  }, []);

  // If the user picked "system", follow OS-level changes.
  useEffect(() => {
    if (!mounted || choice !== "system" || typeof window === "undefined") return;
    const mq = window.matchMedia("(prefers-color-scheme: dark)");
    const onChange = () => {
      const next = applyTheme("system");
      setResolved(next);
    };
    mq.addEventListener("change", onChange);
    return () => mq.removeEventListener("change", onChange);
  }, [choice, mounted]);

  const cycle = () => {
    const idx = ORDER.indexOf(choice);
    const next = ORDER[(idx + 1) % ORDER.length];
    setChoice(next);
    setResolved(applyTheme(next));
  };

  // Always render the same DOM shape on server + first client paint, so
  // Next.js hydration doesn't complain. The icon swap only happens after
  // mount.
  const { Icon, label } = META[mounted ? choice : "system"];
  const ariaLabel = mounted
    ? `Theme: ${label}${choice === "system" ? ` (${resolved})` : ""}. Click to change.`
    : "Theme";

  return (
    <button
      type="button"
      onClick={cycle}
      aria-label={ariaLabel}
      title={ariaLabel}
      style={{
        display: "inline-flex",
        alignItems: "center",
        justifyContent: "center",
        width: 32,
        height: 32,
        borderRadius: 99,
        background: "transparent",
        border: "1px solid var(--line)",
        color: "var(--ink-2)",
        cursor: "pointer",
        transition: "background 120ms ease, color 120ms ease, border-color 120ms ease",
      }}
      onMouseEnter={(e) => {
        e.currentTarget.style.background = "var(--bg-elev)";
        e.currentTarget.style.color = "var(--ink)";
      }}
      onMouseLeave={(e) => {
        e.currentTarget.style.background = "transparent";
        e.currentTarget.style.color = "var(--ink-2)";
      }}
    >
      <Icon className="h-4 w-4" />
    </button>
  );
}
