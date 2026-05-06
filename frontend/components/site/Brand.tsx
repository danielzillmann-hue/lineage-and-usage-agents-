/**
 * intelia logomark + wordmark, per design handoff.
 * Three-circle abstract mark in emerald palette, "intelia" in Source Serif 4.
 */

interface InteliaLogoProps {
  size?: number;
  mono?: boolean;
}

export function InteliaLogo({ size = 22, mono = false }: InteliaLogoProps) {
  return (
    <svg
      width={size * 1.2}
      height={size}
      viewBox="0 0 48 40"
      fill="none"
      aria-hidden="true"
    >
      <circle cx="11" cy="14" r="9" fill={mono ? "currentColor" : "var(--brand-emerald)"} />
      <circle cx="29" cy="9"  r="5" fill={mono ? "currentColor" : "var(--brand-emerald-700)"} opacity="0.85" />
      <circle cx="22" cy="29" r="7" fill={mono ? "currentColor" : "var(--brand-mint)"}        opacity="0.8" />
    </svg>
  );
}

interface WordmarkProps {
  size?: number;
}

export function Wordmark({ size = 18 }: WordmarkProps) {
  return (
    <span className="inline-flex items-center gap-2">
      <InteliaLogo size={size} />
      <span
        className="serif"
        style={{
          fontWeight: 500,
          fontSize: size,
          letterSpacing: "-0.01em",
          color: "var(--brand-ink)",
        }}
      >
        intelia
      </span>
    </span>
  );
}
