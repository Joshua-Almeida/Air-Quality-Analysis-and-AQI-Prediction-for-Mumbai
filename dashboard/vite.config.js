import { defineConfig } from "vite";
import react from "@vitejs/plugin-react";

// During development, /api/* is forwarded to the FastAPI server on port 8077.
// 127.0.0.1 (not "localhost") so Windows never resolves it to another IPv6 service.
export default defineConfig({
  plugins: [react()],
  server: {
    port: 5173,
    proxy: { "/api": "http://127.0.0.1:8077" },
  },
});
