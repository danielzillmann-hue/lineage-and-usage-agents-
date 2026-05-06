import type { NextConfig } from "next";

const nextConfig: NextConfig = {
  // Cloud Run friendly — emit a standalone server with a minimal node_modules.
  output: "standalone",
};

export default nextConfig;
