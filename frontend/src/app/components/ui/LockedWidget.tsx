import { Lock } from 'lucide-react';
import { useLanguage } from '../../contexts/LanguageContext';

interface LockedWidgetProps {
  title: string;
  minDays?: number;
  reason: 'minimum_window' | 'not_connected';
}

export function LockedWidget({ title, minDays = 15, reason }: LockedWidgetProps) {
  const { lang } = useLanguage();
  const ru = lang === 'ru';
  const message = reason === 'minimum_window'
    ? (ru ? `Доступно от ${minDays} дней общего окна.` : `Available from a shared ${minDays}-day window.`)
    : (ru ? 'Этот виджет ещё не подключён к общему фильтру дат.' : 'This widget is not connected to the shared date filter yet.');

  return (
    <div className="bg-white rounded-xl border border-dashed border-gray-300 p-6">
      <h3 className="text-gray-900 mb-3" style={{ fontSize: '1.05rem' }}>
        {title}
      </h3>
      <div className="flex flex-col items-center justify-center py-10 text-center">
        <div className="w-10 h-10 rounded-full bg-gray-100 flex items-center justify-center mb-3">
          <Lock className="w-5 h-5 text-gray-400" />
        </div>
        <p className="text-sm text-gray-600">{message}</p>
        <p className="text-xs text-gray-400 mt-1">
          {ru ? 'Фильтр в шапке остаётся источником правды для активных карточек.' : 'The header filter remains the source of truth for active cards.'}
        </p>
      </div>
    </div>
  );
}
