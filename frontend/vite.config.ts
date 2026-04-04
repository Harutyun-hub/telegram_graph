import { defineConfig } from 'vite'
import path from 'path'
import tailwindcss from '@tailwindcss/vite'
import react from '@vitejs/plugin-react'

export default defineConfig({
  plugins: [
    // The React and Tailwind plugins are both required for Make, even if
    // Tailwind is not being actively used – do not remove them
    react(),
    tailwindcss(),
  ],
  resolve: {
    alias: {
      // Alias @ to the src directory
      '@': path.resolve(__dirname, './src'),
    },
  },

  server: {
    host: '127.0.0.1',
    port: 5174,
    proxy: {
      '/api': {
        target: 'http://127.0.0.1:8001',
        changeOrigin: true,
      },
    },
  },

  // File types to support raw imports. Never add .css, .tsx, or .ts files to this.
  assetsInclude: ['**/*.svg', '**/*.csv'],

  build: {
    rollupOptions: {
      output: {
        manualChunks(id) {
          if (id.includes('node_modules/html2canvas')) {
            return 'vendor-html2canvas';
          }
          if (
            id.includes('node_modules/recharts') ||
            id.includes('node_modules/d3-') ||
            id.includes('node_modules/internmap')
          ) {
            return 'vendor-charts';
          }
          if (id.includes('node_modules/lucide-react')) {
            return 'vendor-icons';
          }
          if (id.includes('/src/app/components/widgets/ExecutiveGlance')) {
            return 'dashboard-pulse';
          }
          if (id.includes('/src/app/components/widgets/StrategicWidgets')) {
            return 'dashboard-strategic';
          }
          if (id.includes('/src/app/components/widgets/BehavioralWidgets')) {
            return 'dashboard-behavioral';
          }
          if (id.includes('/src/app/components/widgets/NetworkWidgets')) {
            return 'dashboard-network';
          }
          if (id.includes('/src/app/components/widgets/PsychographicWidgets')) {
            return 'dashboard-psychographic';
          }
          if (id.includes('/src/app/components/widgets/PredictiveWidgets')) {
            return 'dashboard-predictive';
          }
          if (id.includes('/src/app/components/widgets/ActionableWidgets')) {
            return 'dashboard-actionable';
          }
          if (id.includes('/src/app/components/widgets/ComparativeWidgets')) {
            return 'dashboard-comparative';
          }
          return undefined;
        },
      },
    },
  },
})
