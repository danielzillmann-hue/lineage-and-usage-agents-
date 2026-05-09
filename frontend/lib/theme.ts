// Theme switching: light/dark + system preference.
//
// We set `data-theme="dark"` on the `<html>` element and let CSS custom
// properties in `globals.css` swap. The init script in `app/layout.tsx`
// runs synchronously in `<head>` so the page never flashes the wrong
// theme.

export type Theme = "light" | "dark";
export type ThemeChoice = Theme | "system";

export const THEME_STORAGE_KEY = "ila-theme";

/**
 * Inline script string that runs in `<head>` before any React code, so
 * the correct `data-theme` attribute is set before the first paint.
 *
 * Read order: explicit user choice in localStorage > system preference.
 */
export const themeInitScript = `
(function() {
  try {
    var stored = localStorage.getItem(${JSON.stringify(THEME_STORAGE_KEY)});
    var theme;
    if (stored === "light" || stored === "dark") {
      theme = stored;
    } else {
      theme = window.matchMedia("(prefers-color-scheme: dark)").matches ? "dark" : "light";
    }
    document.documentElement.setAttribute("data-theme", theme);
    document.documentElement.style.colorScheme = theme;
  } catch (_) {
    // localStorage unavailable (private mode, etc.) — leave default.
  }
})();
`.trim();

/** Read the user's stored preference, or null if they haven't picked one. */
export function readStoredChoice(): ThemeChoice {
  if (typeof window === "undefined") return "system";
  try {
    const v = window.localStorage.getItem(THEME_STORAGE_KEY);
    if (v === "light" || v === "dark") return v;
  } catch { /* ignore */ }
  return "system";
}

/** Resolve a choice to the concrete theme to apply right now. */
export function resolveTheme(choice: ThemeChoice): Theme {
  if (choice === "system") {
    if (typeof window === "undefined") return "light";
    return window.matchMedia("(prefers-color-scheme: dark)").matches ? "dark" : "light";
  }
  return choice;
}

/** Apply a theme to the document and persist the choice. */
export function applyTheme(choice: ThemeChoice): Theme {
  const theme = resolveTheme(choice);
  if (typeof document !== "undefined") {
    document.documentElement.setAttribute("data-theme", theme);
    document.documentElement.style.colorScheme = theme;
  }
  if (typeof window !== "undefined") {
    try {
      if (choice === "system") {
        window.localStorage.removeItem(THEME_STORAGE_KEY);
      } else {
        window.localStorage.setItem(THEME_STORAGE_KEY, choice);
      }
    } catch { /* ignore */ }
  }
  return theme;
}
