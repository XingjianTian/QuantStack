import react from "@vitejs/plugin-react";
import { fileURLToPath } from "node:url";
import { loadEnv } from "vite";
import { defineConfig } from "vitest/config";

export default defineConfig(({ mode }) => {
  const env = loadEnv(mode, fileURLToPath(new URL("../..", import.meta.url)), "");
  return {
    plugins: [react()],
    server: {
      host: env.QUANT_WEB_DEV_HOST || "127.0.0.1",
      port: Number(env.QUANT_NAUTILUS_DEV_PORT || 5187),
      proxy: {
        "/api": {
          target: env.QUANT_DEV_API_URL || "http://127.0.0.1:8900",
          headers: env.QUANT_API_KEY ? { "X-Quant-API-Key": env.QUANT_API_KEY } : undefined,
          changeOrigin: true,
          rewrite: (path) => path.replace(/^\/api/, ""),
        },
      },
    },
    test: {
      environment: "jsdom",
      setupFiles: "./src/test-setup.ts",
    },
  };
});
