import * as React from "react";
import { cn } from "@/lib/utils";

export function Card({ className, ...props }: React.HTMLAttributes<HTMLDivElement>) {
  return (
    <div
      className={cn("rounded-lg overflow-hidden", className)}
      style={{
        border: "1px solid var(--line)",
        background: "var(--bg-elev)",
      }}
      {...props}
    />
  );
}

export function CardHeader({ className, ...props }: React.HTMLAttributes<HTMLDivElement>) {
  return (
    <div
      className={cn("p-5 pb-3", className)}
      style={{ borderBottom: "1px solid var(--line)" }}
      {...props}
    />
  );
}

export function CardTitle({ className, ...props }: React.HTMLAttributes<HTMLHeadingElement>) {
  return (
    <h3
      className={cn(className)}
      style={{ fontSize: 15, fontWeight: 500, letterSpacing: "-0.01em", color: "var(--ink)", margin: 0 }}
      {...props}
    />
  );
}

export function CardDescription({ className, ...props }: React.HTMLAttributes<HTMLParagraphElement>) {
  return (
    <p
      className={cn(className)}
      style={{ fontSize: 13, color: "var(--ink-3)", marginTop: 4, lineHeight: 1.5 }}
      {...props}
    />
  );
}

export function CardContent({ className, ...props }: React.HTMLAttributes<HTMLDivElement>) {
  return <div className={cn("p-5", className)} {...props} />;
}

export function CardFooter({ className, ...props }: React.HTMLAttributes<HTMLDivElement>) {
  return (
    <div
      className={cn("px-5 py-3 flex items-center", className)}
      style={{ borderTop: "1px solid var(--line)" }}
      {...props}
    />
  );
}
