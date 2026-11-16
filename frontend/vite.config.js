import { defineConfig } from 'vite'
import vue from '@vitejs/plugin-vue'

// В dev-режиме запросы /api проксируются на API-шлюз (cmd/gateway).
export default defineConfig({
  plugins: [vue()],
  server: {
    port: 5173,
    proxy: {
      '/api': process.env.GATEWAY_URL || 'http://localhost:8080',
    },
  },
})
