import { defineConfig } from "vite";
import react from "@vitejs/plugin-react";

const backendTarget = "http://localhost:8000";

export default defineConfig({
  plugins: [react()],
  server: {
    host: "0.0.0.0",
    port: 5173,
    hmr: {
      protocol: "ws",
      host: "127.0.0.1",
      port: 5173,
      clientPort: 5173,
    },
    proxy: {
      "/api": {
        target: backendTarget,
        changeOrigin: true,
      },
      "/api/graph": {
        target: backendTarget,
        changeOrigin: true,
      },
      "/execution": {
        target: backendTarget,
        changeOrigin: true,
      },
      "^/graph/": {
        target: backendTarget,
        changeOrigin: true,
      },
      "/ws": {
        target: backendTarget,
        ws: true,
        changeOrigin: true,
      },
    },
  },
});
