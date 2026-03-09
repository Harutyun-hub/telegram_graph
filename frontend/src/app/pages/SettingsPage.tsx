import { useState } from 'react';
import { Bell, User, Globe, Send, CheckCircle2, AlertCircle, Loader2, ExternalLink } from 'lucide-react';
import { useLanguage } from '../contexts/LanguageContext';

function TelegramIcon({ className }: { className?: string }) {
  return (
    <svg className={className} viewBox="0 0 24 24" fill="currentColor">
      <path d="M11.944 0A12 12 0 0 0 0 12a12 12 0 0 0 12 12 12 12 0 0 0 12-12A12 12 0 0 0 12 0a12 12 0 0 0-.056 0zm4.962 7.224c.1-.002.321.023.465.14a.506.506 0 0 1 .171.325c.016.093.036.306.02.472-.18 1.898-.962 6.502-1.36 8.627-.168.9-.499 1.201-.82 1.23-.696.065-1.225-.46-1.9-.902-1.056-.693-1.653-1.124-2.678-1.8-1.185-.78-.417-1.21.258-1.91.177-.184 3.247-2.977 3.307-3.23.007-.032.014-.15-.056-.212s-.174-.041-.249-.024c-.106.024-1.793 1.14-5.061 3.345-.48.33-.913.49-1.302.48-.428-.008-1.252-.241-1.865-.44-.752-.245-1.349-.374-1.297-.789.027-.216.325-.437.893-.663 3.498-1.524 5.83-2.529 6.998-3.014 3.332-1.386 4.025-1.627 4.476-1.635z" />
    </svg>
  );
}

type TelegramStatus = 'idle' | 'connecting' | 'connected' | 'error';

export function SettingsPage() {
  const { lang } = useLanguage();
  const ru = lang === 'ru';

  // Telegram connection state
  const [tgUsername, setTgUsername] = useState('');
  const [tgStatus, setTgStatus] = useState<TelegramStatus>('idle');
  const [tgConnected, setTgConnected] = useState(false);
  const [tgConnectedName, setTgConnectedName] = useState('');

  // Notification toggles
  const [notifRedAlert, setNotifRedAlert] = useState(true);
  const [notifDaily, setNotifDaily] = useState(true);
  const [notifTopics, setNotifTopics] = useState(false);
  const [tgRedAlert, setTgRedAlert] = useState(true);
  const [tgDaily, setTgDaily] = useState(true);
  const [tgTopics, setTgTopics] = useState(false);

  const handleTelegramConnect = async () => {
    const raw = tgUsername.trim().replace(/^@/, '');
    if (!raw) return;
    setTgStatus('connecting');
    // Simulate verification delay
    await new Promise(r => setTimeout(r, 1800));
    // Mock success
    setTgStatus('connected');
    setTgConnected(true);
    setTgConnectedName('@' + raw);
  };

  const handleTelegramDisconnect = () => {
    setTgConnected(false);
    setTgConnectedName('');
    setTgUsername('');
    setTgStatus('idle');
  };

  return (
    <div className="p-4 md:p-8">
      <div className="mb-5 md:mb-6">
        <h1 className="text-2xl text-gray-900" style={{ fontWeight: 700 }}>{ru ? 'Настройки' : 'Settings'}</h1>
        <p className="text-gray-500 text-sm mt-1">
          {ru ? 'Управление аккаунтом и настройками платформы' : 'Manage your account and platform preferences'}
        </p>
      </div>

      <div className="max-w-4xl space-y-5 md:space-y-6">

        {/* Profile Settings */}
        <div className="bg-white rounded-xl border border-gray-200 p-6">
          <div className="flex items-center gap-3 mb-6">
            <div className="w-10 h-10 bg-blue-100 rounded-lg flex items-center justify-center">
              <User className="w-5 h-5 text-blue-600" />
            </div>
            <div>
              <h2 className="text-gray-900" style={{ fontSize: '1.05rem', fontWeight: 600 }}>
                {ru ? 'Настройки профиля' : 'Profile Settings'}
              </h2>
              <p className="text-sm text-gray-500">
                {ru ? 'Управление личными данными' : 'Manage your personal information'}
              </p>
            </div>
          </div>

          <div className="space-y-4">
            <div>
              <label className="block text-sm text-gray-700 mb-2" style={{ fontWeight: 500 }}>
                {ru ? 'Полное имя' : 'Full Name'}
              </label>
              <input
                type="text"
                defaultValue="Admin User"
                className="w-full px-4 py-2 border border-gray-300 rounded-lg focus:outline-none focus:ring-2 focus:ring-blue-500 text-sm"
              />
            </div>
            <div>
              <label className="block text-sm text-gray-700 mb-2" style={{ fontWeight: 500 }}>
                {ru ? 'Электронная почта' : 'Email'}
              </label>
              <input
                type="email"
                defaultValue="admin@armentel.io"
                className="w-full px-4 py-2 border border-gray-300 rounded-lg focus:outline-none focus:ring-2 focus:ring-blue-500 text-sm"
              />
            </div>
            <div>
              <label className="block text-sm text-gray-700 mb-2" style={{ fontWeight: 500 }}>
                {ru ? 'Роль' : 'Role'}
              </label>
              <select className="w-full px-4 py-2 border border-gray-300 rounded-lg focus:outline-none focus:ring-2 focus:ring-blue-500 text-sm">
                <option>{ru ? 'Руководитель' : 'Executive Monitor'}</option>
                <option>{ru ? 'Аналитик разведки' : 'Intelligence Analyst'}</option>
                <option>{ru ? 'Эксперт-исследователь' : 'Expert Researcher'}</option>
              </select>
            </div>
          </div>
        </div>

        {/* Notification Settings */}
        <div className="bg-white rounded-xl border border-gray-200 p-6">
          <div className="flex items-center gap-3 mb-6">
            <div className="w-10 h-10 bg-purple-100 rounded-lg flex items-center justify-center">
              <Bell className="w-5 h-5 text-purple-600" />
            </div>
            <div>
              <h2 className="text-gray-900" style={{ fontSize: '1.05rem', fontWeight: 600 }}>
                {ru ? 'Уведомления' : 'Notifications'}
              </h2>
              <p className="text-sm text-gray-500">
                {ru ? 'Настройка предпочтений оповещений' : 'Configure alert preferences'}
              </p>
            </div>
          </div>

          {/* In-app notifications */}
          <div className="mb-5">
            <p className="text-xs text-gray-500 uppercase tracking-wider mb-3" style={{ fontWeight: 600 }}>
              {ru ? 'Уведомления в приложении' : 'In-app notifications'}
            </p>
            <div className="space-y-1">
              {[
                {
                  label: ru ? 'Критические оповещения' : 'Red Alert Notifications',
                  desc: ru ? 'Уведомления о критических разведывательных сигналах' : 'Get notified of critical intelligence alerts',
                  checked: notifRedAlert, onChange: setNotifRedAlert,
                },
                {
                  label: ru ? 'Ежедневная сводка' : 'Daily Briefing',
                  desc: ru ? 'Получайте ежедневный разведывательный дайджест' : 'Receive daily intelligence summary',
                  checked: notifDaily, onChange: setNotifDaily,
                },
                {
                  label: ru ? 'Оповещения по темам' : 'Topic Alerts',
                  desc: ru ? 'Уведомления при резком росте отслеживаемых тем' : 'Alert when tracked topics spike',
                  checked: notifTopics, onChange: setNotifTopics,
                },
              ].map((item) => (
                <label key={item.label} className="flex items-center justify-between p-3 rounded-lg hover:bg-gray-50 cursor-pointer">
                  <div>
                    <p className="text-sm text-gray-900" style={{ fontWeight: 500 }}>{item.label}</p>
                    <p className="text-xs text-gray-500">{item.desc}</p>
                  </div>
                  <button
                    onClick={() => item.onChange(!item.checked)}
                    className={`relative inline-flex h-5 w-9 items-center rounded-full transition-colors duration-200 focus:outline-none ${item.checked ? 'bg-blue-600' : 'bg-gray-200'}`}
                  >
                    <span className={`inline-block h-3.5 w-3.5 transform rounded-full bg-white shadow transition-transform duration-200 ${item.checked ? 'translate-x-4' : 'translate-x-1'}`} />
                  </button>
                </label>
              ))}
            </div>
          </div>

          {/* Divider */}
          <div className="border-t border-gray-100 mb-5" />

          {/* Telegram Notifications */}
          <div>
            <div className="flex items-center gap-2 mb-1">
              <TelegramIcon className="w-4 h-4 text-sky-500" />
              <p className="text-xs text-gray-500 uppercase tracking-wider" style={{ fontWeight: 600 }}>
                {ru ? 'Уведомления в Telegram' : 'Telegram notifications'}
              </p>
            </div>
            <p className="text-xs text-gray-400 mb-4 ml-6">
              {ru
                ? 'Получайте разведывательные оповещения напрямую в Telegram-чат'
                : 'Receive intelligence alerts directly in your Telegram chat'}
            </p>

            {/* Connect Telegram */}
            {!tgConnected ? (
              <div className="rounded-xl border border-sky-100 bg-sky-50 p-4">
                <div className="flex items-start gap-3 mb-4">
                  <div className="w-9 h-9 rounded-xl flex items-center justify-center flex-shrink-0"
                    style={{ background: 'linear-gradient(135deg, #0ea5e9, #0284c7)' }}>
                    <TelegramIcon className="w-4 h-4 text-white" />
                  </div>
                  <div>
                    <p className="text-sm text-gray-900" style={{ fontWeight: 600 }}>
                      {ru ? 'Подключить Telegram' : 'Connect Telegram'}
                    </p>
                    <p className="text-xs text-gray-500 mt-0.5">
                      {ru
                        ? 'Введите ваш Telegram username, чтобы начать получать уведомления'
                        : 'Enter your Telegram username to start receiving notifications'}
                    </p>
                  </div>
                </div>

                <div className="flex gap-2">
                  <div className="relative flex-1">
                    <span className="absolute left-3 top-1/2 -translate-y-1/2 text-gray-400 text-sm select-none">@</span>
                    <input
                      type="text"
                      value={tgUsername}
                      onChange={e => setTgUsername(e.target.value.replace(/^@/, ''))}
                      onKeyDown={e => { if (e.key === 'Enter') handleTelegramConnect(); }}
                      placeholder={ru ? 'ваш_username' : 'your_username'}
                      disabled={tgStatus === 'connecting'}
                      className="w-full pl-7 pr-3 py-2 text-sm border border-sky-200 rounded-lg bg-white focus:outline-none focus:ring-2 focus:ring-sky-400 disabled:opacity-50"
                    />
                  </div>
                  <button
                    onClick={handleTelegramConnect}
                    disabled={!tgUsername.trim() || tgStatus === 'connecting'}
                    className="px-4 py-2 rounded-lg text-sm text-white flex items-center gap-2 transition-all disabled:opacity-50"
                    style={{ background: 'linear-gradient(135deg, #0ea5e9, #0284c7)' }}
                  >
                    {tgStatus === 'connecting' ? (
                      <><Loader2 className="w-3.5 h-3.5 animate-spin" />{ru ? 'Подключение...' : 'Connecting...'}</>
                    ) : (
                      <><Send className="w-3.5 h-3.5" />{ru ? 'Подключить' : 'Connect'}</>
                    )}
                  </button>
                </div>

                {tgStatus === 'error' && (
                  <div className="mt-3 flex items-center gap-2 text-xs text-red-600">
                    <AlertCircle className="w-3.5 h-3.5" />
                    {ru ? 'Не удалось найти аккаунт. Проверьте username и повторите.' : 'Account not found. Check the username and try again.'}
                  </div>
                )}

                <div className="mt-3 flex items-start gap-2 text-xs text-gray-400">
                  <ExternalLink className="w-3 h-3 mt-0.5 flex-shrink-0" />
                  <span>
                    {ru
                      ? <>Затем откройте <span className="text-sky-600 font-medium">@ArmentelBot</span> в Telegram и нажмите <span className="font-medium">Start</span> для активации</>
                      : <>Then open <span className="text-sky-600 font-medium">@ArmentelBot</span> in Telegram and tap <span className="font-medium">Start</span> to activate</>
                    }
                  </span>
                </div>
              </div>
            ) : (
              /* Connected state */
              <div className="rounded-xl border border-emerald-200 bg-emerald-50 p-4">
                <div className="flex items-center justify-between mb-4">
                  <div className="flex items-center gap-3">
                    <div className="w-9 h-9 rounded-xl flex items-center justify-center flex-shrink-0"
                      style={{ background: 'linear-gradient(135deg, #0ea5e9, #0284c7)' }}>
                      <TelegramIcon className="w-4 h-4 text-white" />
                    </div>
                    <div>
                      <div className="flex items-center gap-2">
                        <p className="text-sm text-gray-900" style={{ fontWeight: 600 }}>{tgConnectedName}</p>
                        <span className="flex items-center gap-1 text-xs text-emerald-700 bg-emerald-100 border border-emerald-200 px-2 py-0.5 rounded-full" style={{ fontWeight: 500 }}>
                          <CheckCircle2 className="w-3 h-3" />
                          {ru ? 'Подключено' : 'Connected'}
                        </span>
                      </div>
                      <p className="text-xs text-gray-500 mt-0.5">
                        {ru ? 'Уведомления в Telegram активны' : 'Telegram notifications are active'}
                      </p>
                    </div>
                  </div>
                  <button
                    onClick={handleTelegramDisconnect}
                    className="text-xs text-gray-400 hover:text-red-500 transition-colors"
                    style={{ fontWeight: 500 }}
                  >
                    {ru ? 'Отключить' : 'Disconnect'}
                  </button>
                </div>

                {/* Telegram notification toggles */}
                <div className="bg-white rounded-lg border border-emerald-100 divide-y divide-gray-50">
                  {[
                    {
                      label: ru ? 'Критические оповещения' : 'Red Alert Notifications',
                      desc: ru ? 'Мгновенно в Telegram при критических сигналах' : 'Instant Telegram message on critical alerts',
                      checked: tgRedAlert, onChange: setTgRedAlert,
                    },
                    {
                      label: ru ? 'Ежедневная сводка' : 'Daily Briefing',
                      desc: ru ? 'Утренний дайджест в 08:00' : 'Morning digest at 08:00',
                      checked: tgDaily, onChange: setTgDaily,
                    },
                    {
                      label: ru ? 'Оповещения по темам' : 'Topic Alerts',
                      desc: ru ? 'При резком росте отслеживаемых тем' : 'When tracked topics spike',
                      checked: tgTopics, onChange: setTgTopics,
                    },
                  ].map((item) => (
                    <label key={item.label} className="flex items-center justify-between px-3 py-2.5 cursor-pointer hover:bg-gray-50 transition-colors">
                      <div>
                        <p className="text-xs text-gray-900" style={{ fontWeight: 500 }}>{item.label}</p>
                        <p className="text-xs text-gray-400">{item.desc}</p>
                      </div>
                      <button
                        onClick={() => item.onChange(!item.checked)}
                        className={`relative inline-flex h-5 w-9 items-center rounded-full transition-colors duration-200 focus:outline-none ${item.checked ? 'bg-sky-500' : 'bg-gray-200'}`}
                      >
                        <span className={`inline-block h-3.5 w-3.5 transform rounded-full bg-white shadow transition-transform duration-200 ${item.checked ? 'translate-x-4' : 'translate-x-1'}`} />
                      </button>
                    </label>
                  ))}
                </div>
              </div>
            )}
          </div>
        </div>

        {/* Save Button */}
        <div className="flex justify-end gap-3">
          <button className="px-6 py-2.5 border border-gray-300 rounded-lg text-gray-700 hover:bg-gray-50 transition-colors text-sm">
            {ru ? 'Отмена' : 'Cancel'}
          </button>
          <button className="px-6 py-2.5 text-white rounded-lg transition-colors text-sm" style={{ fontWeight: 500, background: 'linear-gradient(135deg, #1a56db, #1e3a8a)' }}>
            {ru ? 'Сохранить изменения' : 'Save Changes'}
          </button>
        </div>
      </div>
    </div>
  );
}