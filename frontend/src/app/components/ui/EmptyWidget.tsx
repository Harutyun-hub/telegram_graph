import { Inbox } from 'lucide-react';
import { useLanguage } from '../../contexts/LanguageContext';

interface EmptyWidgetProps {
  title?: string;
  message?: string;
  compact?: boolean;
}

export function EmptyWidget({ title, message, compact = false }: EmptyWidgetProps) {
  const { lang } = useLanguage();
  const ru = lang === 'ru';

  return (
    <div className={`bg-white rounded-xl border border-gray-200 ${compact ? 'p-4' : 'p-6'}`}>
      {title && (
        <h3 className="text-gray-900 mb-3" style={{ fontSize: '1.05rem' }}>
          {title}
        </h3>
      )}
      <div className={`flex flex-col items-center justify-center text-gray-400 ${compact ? 'py-6' : 'py-10'}`}>
        <Inbox className="w-8 h-8 mb-2 text-gray-300" />
        <p className="text-sm text-gray-500">
          {message ?? (ru ? 'Данные пока отсутствуют' : 'No data available yet')}
        </p>
        <p className="text-xs text-gray-400 mt-1">
          {ru ? 'Подключите источник данных для отображения' : 'Connect a data source to populate'}
        </p>
      </div>
    </div>
  );
}
