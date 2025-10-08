import { defineConfig } from "vite";

export default defineConfig({
  root: "./",
  server: {
    proxy: {
      "/api": {
        target: "http://127.0.0.1:33277",
        changeOrigin: true,
        secure: false,
      },
      "/stream": {
        target: "http://127.0.0.1:33277",
        changeOrigin: true,
        secure: false,
      },
    },
  },
  build: {
    outDir: "../internal/server/dist",
    emptyOutDir: false,
  },
});
