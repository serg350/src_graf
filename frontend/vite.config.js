import { defineConfig } from "vite";
import react from "@vitejs/plugin-react";

const backendTarget = "http://localhost:8000";

export default defineConfig({
  plugins: [react()],
  server: {
    host: "0.0.0.0",
    port: 5173,
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
    },
  },
});
