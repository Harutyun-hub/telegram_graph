import { Monitor } from 'lucide-react';
import { useLanguage } from '@/app/contexts/LanguageContext';
import { GraphDashboard } from '@/app/graph/GraphDashboard';

export function GraphPage() {
  const { lang } = useLanguage();
  const ru = lang === 'ru';

  return (
    <div className="h-[calc(100dvh-56px)] md:h-[calc(100dvh-64px)]">
      <div className="md:hidden flex flex-col items-center justify-center h-full bg-gray-50 px-8 text-center">
        <div className="w-20 h-20 rounded-2xl bg-slate-100 flex items-center justify-center mb-6">
          <Monitor className="w-10 h-10 text-slate-400" />
        </div>
        <h2 className="text-gray-800 mb-2" style={{ fontSize: '1.25rem', fontWeight: 600 }}>
          {ru ? 'Только для ПК' : 'Desktop Only'}
        </h2>
        <p className="text-gray-500 text-sm leading-relaxed max-w-xs">
          {ru
            ? 'Графовая визуализация пока доступна на большом экране.'
            : 'Graph visualization is currently optimized for desktop screens.'}
        </p>
      </div>

      <div className="hidden md:block h-full">
        <GraphDashboard />
      </div>
    </div>
  );
}
