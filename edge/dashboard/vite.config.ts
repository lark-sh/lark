import { defineConfig } from 'vite';
import react from '@vitejs/plugin-react';
import tailwindcss from '@tailwindcss/vite';

// All admin assets live under /admin/ in production so the SPA can coexist
// with the wire-protocol routes on the same hostname. Dev mode proxies
// /admin/api/* to the local lark-edge HTTP server so cookies and JSON
// requests work without CORS gymnastics.
export default defineConfig({
  base: '/admin/',
  plugins: [react(), tailwindcss()],
  server: {
    port: 5173,
    proxy: {
      '/admin/api': {
        target: 'http://localhost:8080',
        changeOrigin: false,
      },
    },
  },
  build: {
    outDir: 'dist',
    emptyOutDir: true,
    sourcemap: false,
  },
});
