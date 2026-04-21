import { useLocation, useNavigate, Outlet, Link } from 'react-router';
import { motion, AnimatePresence } from 'motion/react';
import {
  LayoutDashboard, Share2, Settings, Search, Bell, User, Hash, Radio,
  Calendar, Users, LogOut, ChevronRight, AlertTriangle, TrendingUp,
  Newspaper, CheckCheck, X, Shield, Menu, Globe, Sparkles, ChevronUp,
  PlusCircle, Loader2, Megaphone,
} from 'lucide-react';
import { LogoIcon } from '@/app/components/Logo';
import { useState, useRef, useEffect, useCallback } from 'react';
import { useLanguage } from '../contexts/LanguageContext';
import type { Lang } from '../contexts/LanguageContext';
import { useData } from '../contexts/DataContext';
import { useDashboardDateRange } from '../contexts/DashboardDateRangeContext';
import { useAuth } from '../contexts/AuthContext';
import { AIAssistant } from '../components/AIAssistant';
import {
  differenceInDaysInclusive,
  type DashboardDatePresetId,
} from '../utils/dashboardDateRange';

// ─── Helpers ─────────────────────────────────────────────────────
function formatDisplayDate(dateStr: string, lang: Lang) {
  const d = new Date(dateStr + 'T00:00:00');
  return d.toLocaleDateString(lang === 'ru' ? 'ru-RU' : 'en-US', { month: 'short', day: 'numeric', year: 'numeric' });
}
function useIsMobile(bp = 768) {
  const [mobile, setMobile] = useState(false);
  useEffect(() => {
    const check = () => setMobile(window.innerWidth < bp);
    check();
    window.addEventListener('resize', check);
    return () => window.removeEventListener('resize', check);
  }, [bp]);
  return mobile;
}

type NavLeafItem = {
  type: 'item';
  label: string;
  path: string;
  icon: React.ElementType;
};

type NavGroupItem = {
  type: 'group';
  label: string;
  path: string;
  icon: React.ElementType;
  children: NavLeafItem[];
};

type NavDividerItem = {
  type: 'divider';
  id: string;
};

type SidebarNavItem = NavLeafItem | NavGroupItem | NavDividerItem;

// ─── Notification data ───────────────────────────────────────────
type NotifType = 'alert' | 'briefing' | 'trend' | 'system';
interface Notification {
  id: number; type: NotifType; read: boolean;
  titleEn: string; titleRu: string; descEn: string; descRu: string; time: string;
}
const INITIAL_NOTIFS: Notification[] = [
  { id: 1, type: 'alert', read: false, titleEn: 'Red Alert: Sentiment Spike', titleRu: 'Красный сигнал: всплеск настроений', descEn: '"Housing & Rent" hit -78% negative — 3× daily average.', descRu: '«Жильё» достигло -78% негатива — в 3× выше нормы.', time: '2 min ago' },
  { id: 2, type: 'briefing', read: false, titleEn: 'Daily Briefing Ready', titleRu: 'Сводка готова', descEn: 'Morning intelligence digest for Feb 25 is available.', descRu: 'Утренний дайджест за 25 фев готов.', time: '1 hr ago' },
  { id: 3, type: 'trend', read: false, titleEn: 'Topic Spike: Armenian Language', titleRu: 'Рост: Армянский язык', descEn: 'Mentions surged +55% across 12 channels in 6 hours.', descRu: 'Упоминания +55% в 12 каналах за 6 часов.', time: '3 hr ago' },
  { id: 4, type: 'trend', read: true, titleEn: 'New Channel Detected', titleRu: 'Обнаружен новый канал', descEn: '"Yerevan Nomads Hub" added 1,240 members.', descRu: '«Yerevan Nomads Hub» набрал 1 240 участников.', time: '5 hr ago' },
  { id: 5, type: 'system', read: true, titleEn: 'Pipeline Completed', titleRu: 'Пайплайн завершён', descEn: 'GPT-4o-mini processed 14,382 comments.', descRu: 'GPT-4o-mini обработал 14 382 комментария.', time: 'Yesterday' },
];
const notifMeta: Record<NotifType, { icon: React.ElementType; bg: string; color: string }> = {
  alert:    { icon: AlertTriangle, bg: 'bg-red-100',    color: 'text-red-600' },
  briefing: { icon: Newspaper,     bg: 'bg-blue-100',   color: 'text-blue-600' },
  trend:    { icon: TrendingUp,    bg: 'bg-violet-100', color: 'text-violet-600' },
  system:   { icon: Shield,        bg: 'bg-gray-100',   color: 'text-gray-500' },
};

// ─── Notification list ───────────────────────────────────────────
function NotifList({ notifications, ru, onMarkRead, onDismiss, onMarkAll, onClose, onGoSettings }: {
  notifications: Notification[]; ru: boolean;
  onMarkRead: (id: number) => void;
  onDismiss: (id: number, e: React.MouseEvent) => void;
  onMarkAll: () => void; onClose: () => void; onGoSettings: () => void;
}) {
  const unread = notifications.filter(n => !n.read).length;
  return (
    <div className="flex flex-col h-full">
      <div className="flex items-center justify-between px-4 py-3 border-b border-gray-100 flex-shrink-0">
        <div className="flex items-center gap-2">
          <span className="text-gray-900 text-sm" style={{ fontWeight: 600 }}>{ru ? 'Уведомления' : 'Notifications'}</span>
          {unread > 0 && <span className="bg-red-100 text-red-600 text-xs px-1.5 py-0.5 rounded-full" style={{ fontWeight: 600 }}>{unread} {ru ? 'новых' : 'new'}</span>}
        </div>
        {unread > 0 && (
          <button onClick={onMarkAll} className="flex items-center gap-1 text-xs text-blue-600" style={{ fontWeight: 500 }}>
            <CheckCheck className="w-3.5 h-3.5" />{ru ? 'Прочитать все' : 'Mark all read'}
          </button>
        )}
      </div>
      <div className="flex-1 overflow-y-auto divide-y divide-gray-50">
        {notifications.length === 0 ? (
          <div className="px-4 py-12 text-center">
            <Bell className="w-8 h-8 text-gray-200 mx-auto mb-2" />
            <p className="text-sm text-gray-400">{ru ? 'Нет уведомлений' : 'No notifications'}</p>
          </div>
        ) : notifications.map(n => {
          const meta = notifMeta[n.type]; const Icon = meta.icon;
          return (
            <motion.div key={n.id} layout initial={{ opacity: 0, x: -10 }} animate={{ opacity: 1, x: 0 }} exit={{ opacity: 0, height: 0 }}
              onClick={() => onMarkRead(n.id)}
              className={`flex items-start gap-3 px-4 py-3 cursor-pointer active:bg-gray-100 transition-colors ${!n.read ? 'bg-slate-50' : 'bg-white'}`}>
              <div className={`w-8 h-8 rounded-lg flex items-center justify-center flex-shrink-0 mt-0.5 ${meta.bg}`}>
                <Icon className={`w-4 h-4 ${meta.color}`} />
              </div>
              <div className="flex-1 min-w-0">
                <div className="flex items-start justify-between gap-2">
                  <p className="text-sm text-gray-900 leading-snug" style={!n.read ? { fontWeight: 600 } : {}}>{ru ? n.titleRu : n.titleEn}</p>
                  <button onClick={e => onDismiss(n.id, e)} className="flex-shrink-0 p-0.5 text-gray-300 hover:text-gray-500 rounded"><X className="w-3.5 h-3.5" /></button>
                </div>
                <p className="text-xs text-gray-500 mt-0.5">{ru ? n.descRu : n.descEn}</p>
                <div className="flex items-center gap-2 mt-1">
                  <span className="text-xs text-gray-400">{n.time}</span>
                  {!n.read && <span className="w-1.5 h-1.5 bg-blue-500 rounded-full" />}
                </div>
              </div>
            </motion.div>
          );
        })}
      </div>
      <div className="border-t border-gray-100 px-4 py-2.5 flex-shrink-0">
        <button onClick={() => { onGoSettings(); onClose(); }}
          className="flex items-center justify-center gap-1.5 w-full text-xs text-gray-500 hover:text-blue-600 transition-colors" style={{ fontWeight: 500 }}>
          {ru ? 'Настройки уведомлений' : 'Notification settings'}<ChevronRight className="w-3.5 h-3.5" />
        </button>
      </div>
    </div>
  );
}

// ─── Account menu ────────────────────────────────────────────────
function AccountMenu({ ru, onSettings, onLogout, onClose }: {
  ru: boolean; onSettings: () => void; onLogout: () => void; onClose: () => void;
}) {
  return (
    <div className="flex flex-col">
      <div className="px-4 py-4 border-b border-gray-100 bg-gradient-to-br from-slate-50 to-blue-50">
        <div className="flex items-center gap-3">
          <div className="w-12 h-12 rounded-full flex items-center justify-center flex-shrink-0" style={{ background: 'linear-gradient(135deg, #1a56db, #1e3a8a)' }}>
            <User className="w-6 h-6 text-white" />
          </div>
          <div>
            <p className="text-gray-900" style={{ fontWeight: 600 }}>Admin</p>
            <span className="inline-flex items-center gap-1 mt-1 text-xs text-blue-700 bg-blue-100 border border-blue-200 px-2 py-0.5 rounded-full" style={{ fontWeight: 500 }}>
              <Shield className="w-3 h-3" />{ru ? 'Администратор' : 'Administrator'}
            </span>
          </div>
        </div>
      </div>
      <div className="py-1.5">
        <button onClick={() => { onSettings(); onClose(); }} className="w-full flex items-center gap-3 px-4 py-3 text-sm text-gray-700 hover:bg-gray-50 active:bg-gray-100 transition-colors">
          <div className="w-8 h-8 rounded-lg bg-gray-100 flex items-center justify-center"><Settings className="w-4 h-4 text-gray-500" /></div>
          <span>{ru ? 'Настройки' : 'Settings'}</span>
          <ChevronRight className="w-4 h-4 text-gray-300 ml-auto" />
        </button>
        <button onClick={() => { onSettings(); onClose(); }} className="w-full flex items-center gap-3 px-4 py-3 text-sm text-gray-700 hover:bg-gray-50 active:bg-gray-100 transition-colors">
          <div className="w-8 h-8 rounded-lg bg-gray-100 flex items-center justify-center"><User className="w-4 h-4 text-gray-500" /></div>
          <span>{ru ? 'Профиль' : 'Edit Profile'}</span>
          <ChevronRight className="w-4 h-4 text-gray-300 ml-auto" />
        </button>
      </div>
      <div className="border-t border-gray-100 py-1.5">
        <button onClick={onLogout} className="w-full flex items-center gap-3 px-4 py-3 text-sm text-red-600 hover:bg-red-50 active:bg-red-100 transition-colors">
          <div className="w-8 h-8 rounded-lg bg-red-100 flex items-center justify-center"><LogOut className="w-4 h-4 text-red-500" /></div>
          <span style={{ fontWeight: 500 }}>{ru ? 'Выйти' : 'Log Out'}</span>
        </button>
      </div>
    </div>
  );
}

// ─── Backdrop ────────────────────────────────────────────────────
function Backdrop({ onClick }: { onClick: () => void }) {
  return (
    <motion.div className="fixed inset-0 z-40 bg-black/50 backdrop-blur-sm"
      initial={{ opacity: 0 }} animate={{ opacity: 1 }} exit={{ opacity: 0 }}
      transition={{ duration: 0.2 }} onClick={onClick} />
  );
}

// ─── Bell icon ───────────────────────────────────────────────────
function BellIcon({ count }: { count: number }) {
  return (
    <>
      <Bell className="w-5 h-5 text-gray-600" />
      {count > 0 && (
        <span className="absolute top-1 right-1 w-4 h-4 bg-red-500 text-white rounded-full flex items-center justify-center"
          style={{ fontSize: '9px', fontWeight: 700 }}>{count}</span>
      )}
    </>
  );
}

// ═══════════════════════════════════════════════════════════════
// Main Layout
// ═══════════════════════════════════════════════════════════════
export function AdminLayout() {
  const location = useLocation();
  const navigate = useNavigate();
  const { logout } = useAuth();
  const { lang, setLang } = useLanguage();
  const {
    loading,
    isRefreshing,
    hasLiveData,
    error,
    displayRange,
    refresh,
  } = useData();
  const { range, ready, trustedEndDate, freshness, setPreset, setCustomRange } = useDashboardDateRange();
  const ru = lang === 'ru';
  const isMobile = useIsMobile();
  const isGraphRoute = location.pathname.startsWith('/graph');
  const isSocialRoute = location.pathname.startsWith('/social');
  // UI state
  const [showDatePicker, setShowDatePicker] = useState(false);
  const [showMobileDatePicker, setShowMobileDatePicker] = useState(false);
  const [showNotifications, setShowNotifications] = useState(false);
  const [showAccount, setShowAccount] = useState(false);
  const [drawerOpen, setDrawerOpen] = useState(false);
  const [showAI, setShowAI] = useState(false);
  const [showScrollTop, setShowScrollTop] = useState(false);
  const [draftFrom, setDraftFrom] = useState(range.from);
  const [draftTo, setDraftTo] = useState(range.to);

  const [notifications, setNotifications] = useState<Notification[]>(INITIAL_NOTIFS);

  const datePickerRef = useRef<HTMLDivElement>(null);
  const notifRef = useRef<HTMLDivElement>(null);
  const accountRef = useRef<HTMLDivElement>(null);
  const mainRef = useRef<HTMLElement>(null);

  const QUICK_RANGES: Array<{ id: Exclude<DashboardDatePresetId, 'custom'>; label: string }> = [
    { id: 'today', label: ru ? 'Сегодня' : 'Today' },
    { id: 'yesterday', label: ru ? 'Вчера' : 'Yesterday' },
    { id: 'last_3_days', label: ru ? 'Последние 3 дня' : 'Last 3 days' },
    { id: 'last_7_days', label: ru ? 'Последние 7 дней' : 'Last 7 days' },
    { id: 'last_15_days', label: ru ? 'Последние 15 дней' : 'Last 15 days' },
    { id: 'last_30_days', label: ru ? 'Последние 30 дней' : 'Last 30 days' },
    { id: 'last_3_months', label: ru ? 'Последние 3 месяца' : 'Last 3 months' },
    { id: 'last_6_months', label: ru ? 'Последние 6 месяцев' : 'Last 6 months' },
  ];
  const displayedRangeDays = displayRange?.days ?? range.days;
  const showsPreviousSnapshot = isRefreshing && displayedRangeDays !== range.days;

  useEffect(() => {
    setDraftFrom(range.from);
    setDraftTo(range.to);
  }, [range.from, range.to]);

  // All sidebar nav items (for drawer + desktop sidebar)
  const navItems: SidebarNavItem[] = [
    {
      type: 'group',
      label: ru ? 'Дашборд' : 'Dashboard',
      path: '/',
      icon: LayoutDashboard,
      children: [
        { type: 'item', label: ru ? 'Темы' : 'Topics', path: '/topics', icon: Hash },
      ],
    },
    {
      type: 'group',
      label: ru ? 'Social Media' : 'Social Media',
      path: '/social',
      icon: Megaphone,
      children: [
        { type: 'item', label: ru ? 'Темы Social' : 'Social Topics', path: '/social/topics', icon: Hash },
      ],
    },
    { type: 'item', label: ru ? 'Каналы' : 'Channels', path: '/channels', icon: Radio },
    { type: 'item', label: ru ? 'Аудитория' : 'Audience', path: '/audience', icon: Users },
    { type: 'item', label: ru ? 'Граф связей' : 'Graph', path: '/graph', icon: Share2 },
    { type: 'divider', id: 'primary-secondary-divider' },
    { type: 'item', label: ru ? 'ИИ Агент' : 'AI Agent', path: '/agent', icon: Sparkles },
    { type: 'item', label: ru ? 'Источники' : 'Sources', path: '/sources', icon: PlusCircle },
    { type: 'item', label: ru ? 'Админ' : 'Admin', path: '/admin', icon: Shield },
    { type: 'item', label: ru ? 'Настройки' : 'Settings', path: '/settings', icon: Settings },
  ];

  const flatNavItems = navItems.flatMap((item) => {
    if (item.type === 'divider') return [];
    if (item.type === 'group') return [item, ...item.children];
    return [item];
  });

  // Mobile bottom nav: Home, Topics, [AI center], Channels, Audience
  const mobileNavItems = [
    { label: ru ? 'Дашборд' : 'Home', path: '/', icon: LayoutDashboard },
    { label: ru ? 'Темы' : 'Topics', path: '/topics', icon: Hash },
  ];
  const mobileNavItemsRight = [
    { label: ru ? 'Каналы' : 'Channels', path: '/channels', icon: Radio },
    { label: ru ? 'Аудитория' : 'Audience', path: '/audience', icon: Users },
  ];

  const isNavItemActive = useCallback((path: string) => {
    if (path === '/') {
      return location.pathname === '/';
    }
    if (path === '/social') {
      return location.pathname === '/social' || location.pathname.startsWith('/social/ops');
    }
    return location.pathname === path || location.pathname.startsWith(`${path}/`);
  }, [location.pathname]);

  const currentPage = flatNavItems.find((item) => isNavItemActive(item.path));

  // Close desktop dropdowns on outside click
  useEffect(() => {
    function handle(e: MouseEvent) {
      if (datePickerRef.current && !datePickerRef.current.contains(e.target as Node)) setShowDatePicker(false);
      if (!isMobile) {
        if (notifRef.current && !notifRef.current.contains(e.target as Node)) setShowNotifications(false);
        if (accountRef.current && !accountRef.current.contains(e.target as Node)) setShowAccount(false);
      }
    }
    document.addEventListener('mousedown', handle);
    return () => document.removeEventListener('mousedown', handle);
  }, [isMobile]);

  // Body scroll lock for mobile overlays
  useEffect(() => {
    const locked = drawerOpen || (isMobile && (showNotifications || showAccount || showAI || showMobileDatePicker));
    document.body.style.overflow = locked ? 'hidden' : '';
    return () => { document.body.style.overflow = ''; };
  }, [drawerOpen, isMobile, showNotifications, showAccount, showAI, showMobileDatePicker]);

  // Scroll-to-top tracker
  useEffect(() => {
    const el = mainRef.current;
    if (!el) return;
    const handler = () => setShowScrollTop(el.scrollTop > 250);
    el.addEventListener('scroll', handler, { passive: true });
    return () => el.removeEventListener('scroll', handler);
  }, []);

  const scrollToTop = useCallback(() => {
    mainRef.current?.scrollTo({ top: 0, behavior: 'smooth' });
  }, []);

  const unreadCount = notifications.filter(n => !n.read).length;
  const markAllRead = useCallback(() => setNotifications(p => p.map(n => ({ ...n, read: true }))), []);
  const markRead = useCallback((id: number) => setNotifications(p => p.map(n => n.id === id ? { ...n, read: true } : n)), []);
  const dismissNotif = useCallback((id: number, e: React.MouseEvent) => { e.stopPropagation(); setNotifications(p => p.filter(n => n.id !== id)); }, []);
  const handleLogout = useCallback(() => {
    void logout();
    setShowAccount(false);
    setDrawerOpen(false);
    navigate('/login', { replace: true });
  }, [logout, navigate]);
  const goSettings = useCallback(() => navigate('/settings'), [navigate]);

  const activeRange = QUICK_RANGES.find((preset) => preset.id === range.presetId);
  const draftDays = differenceInDaysInclusive(draftFrom, draftTo);
  const springConfig = { type: 'spring', damping: 30, stiffness: 300 };
  const applyCustomRange = useCallback(() => {
    setCustomRange(draftFrom, draftTo);
    setShowDatePicker(false);
    setShowMobileDatePicker(false);
  }, [draftFrom, draftTo, setCustomRange]);
  const dateHelperText = isSocialRoute
    ? (ru
      ? 'Диапазон общий для Telegram и Social. Актуальность Social показана на самой странице.'
      : 'This date range is shared across Telegram and Social. Social freshness is shown on the page itself.')
    : (freshness?.trustedEndLabel || (ru ? `Надёжные данные до ${formatDisplayDate(trustedEndDate, lang)}` : `Trusted data through ${formatDisplayDate(trustedEndDate, lang)}`));

  // Date picker content (shared between desktop dropdown and mobile sheet)
  const DatePickerContent = () => (
    <div className="flex gap-4 p-4">
      <div className="w-[140px] border-r border-gray-100 pr-4 space-y-1">
        <p className="text-xs text-gray-400 mb-2" style={{ fontWeight: 600 }}>{ru ? 'Быстрый выбор' : 'Quick Select'}</p>
        {QUICK_RANGES.map((preset) => {
          const isA = preset.id === range.presetId;
          return (
            <button key={preset.id} onClick={() => setPreset(preset.id)}
              className={`w-full text-left px-2.5 py-1.5 rounded-lg text-xs transition-colors ${isA ? 'bg-blue-50 text-blue-700' : 'text-gray-600 hover:bg-gray-50'}`}
              style={isA ? { fontWeight: 500 } : {}}>
              {preset.label}
            </button>
          );
        })}
      </div>
      <div className="flex-1 space-y-3">
        <p className="text-xs text-gray-400" style={{ fontWeight: 600 }}>{ru ? 'Произвольный период' : 'Custom Range'}</p>
        <div className="space-y-2">
          <div>
            <label className="text-xs text-gray-500 mb-1 block">{ru ? 'С' : 'From'}</label>
            <input type="date" value={draftFrom} onChange={e => setDraftFrom(e.target.value)} max={draftTo}
              className="w-full px-3 py-2 border border-gray-200 rounded-lg text-sm focus:outline-none focus:ring-2 focus:ring-blue-500" />
          </div>
          <div>
            <label className="text-xs text-gray-500 mb-1 block">{ru ? 'По' : 'To'}</label>
            <input type="date" value={draftTo} onChange={e => setDraftTo(e.target.value)} min={draftFrom} max={trustedEndDate}
              className="w-full px-3 py-2 border border-gray-200 rounded-lg text-sm focus:outline-none focus:ring-2 focus:ring-blue-500" />
          </div>
        </div>
        <div className="flex items-center justify-between pt-2 border-t border-gray-100">
          <span className="text-xs text-gray-400">{ru ? `${draftDays} дней` : `${draftDays} days`}</span>
          <button onClick={applyCustomRange}
            className="px-4 py-1.5 text-white text-xs rounded-lg transition-colors" style={{ fontWeight: 500, background: 'linear-gradient(135deg, #1a56db, #1e3a8a)' }}>
            {ru ? 'Применить' : 'Apply'}
          </button>
        </div>
        <p className="text-[11px] text-gray-400">{dateHelperText}</p>
      </div>
    </div>
  );

  return (
    <div className="flex h-[100dvh] bg-gray-50 overflow-hidden">

      {/* ════════════════════════════════════════════
          DESKTOP SIDEBAR
      ════════════════════════════════════════════ */}
      <aside className={`hidden md:flex bg-slate-800 flex-col shrink-0 transition-[width] duration-200 ${isGraphRoute ? 'w-20' : 'w-64'}`}>
        <div className={`border-b border-slate-700 ${isGraphRoute ? 'px-3 py-5' : 'p-6'}`}>
          <div className={`flex items-center ${isGraphRoute ? 'justify-center' : 'gap-3'}`}>
            <LogoIcon size={36} />
            {!isGraphRoute && (
              <div>
                <h1 className="text-white text-sm" style={{ fontWeight: 600 }}>Радар Общины</h1>
                <p className="text-slate-400 text-xs">{ru ? 'Платформа' : 'Community Platform'}</p>
              </div>
            )}
          </div>
        </div>
        <nav className={`flex-1 overflow-y-auto ${isGraphRoute ? 'px-2 py-4 space-y-2' : 'px-4 py-6 space-y-1'}`}>
          {!isGraphRoute && (
            <div className="text-slate-400 text-xs uppercase tracking-wider mb-3 px-3" style={{ fontWeight: 700 }}>
              {ru ? 'Меню' : 'General'}
            </div>
          )}
          {navItems.map(item => {
            if (item.type === 'divider') {
              return <div key={item.id} className={`my-3 border-t border-slate-700/80 ${isGraphRoute ? 'mx-1' : 'mx-3'}`} />;
            }

            if (item.type === 'group') {
              const isParentActive = isNavItemActive(item.path) || item.children.some((child) => isNavItemActive(child.path));
              const ParentIcon = item.icon;

              return (
                <div key={item.path} className={isGraphRoute ? 'space-y-2' : 'space-y-1'}>
                  <Link
                    to={item.path}
                    title={item.label}
                    className={`flex rounded-lg transition-colors ${isGraphRoute ? 'justify-center px-0 py-3' : 'items-center gap-3 px-3 py-2.5'} ${isParentActive ? 'bg-slate-700 text-white' : 'text-slate-300 hover:bg-slate-700/50 hover:text-white'}`}
                  >
                    <ParentIcon className="w-5 h-5 flex-shrink-0" />
                    {!isGraphRoute && (
                      <span className="text-sm" style={{ fontWeight: 500 }}>{item.label}</span>
                    )}
                  </Link>
                  {!isGraphRoute && (
                    <div className="space-y-1">
                      {item.children.map((child) => {
                        const isChildActive = isNavItemActive(child.path);
                        const ChildIcon = child.icon;
                        return (
                          <Link
                            key={child.path}
                            to={child.path}
                            title={child.label}
                            className={`ml-4 flex items-center gap-3 rounded-lg px-3 py-2.5 transition-colors ${isChildActive ? 'bg-slate-700 text-white' : 'text-slate-300 hover:bg-slate-700/50 hover:text-white'}`}
                          >
                            <ChildIcon className="w-5 h-5 flex-shrink-0" />
                            <span className="text-sm" style={{ fontWeight: 500 }}>{child.label}</span>
                          </Link>
                        );
                      })}
                    </div>
                  )}
                </div>
              );
            }

            const isActive = isNavItemActive(item.path);
            const Icon = item.icon;
            return (
              <Link key={item.path} to={item.path}
                title={item.label}
                className={`flex rounded-lg transition-colors ${isGraphRoute ? 'justify-center px-0 py-3' : 'items-center gap-3 px-3 py-2.5'} ${isActive ? 'bg-slate-700 text-white' : 'text-slate-300 hover:bg-slate-700/50 hover:text-white'}`}>
                <Icon className="w-5 h-5 flex-shrink-0" />
                {!isGraphRoute && (
                  <span className="text-sm" style={{ fontWeight: 500 }}>{item.label}</span>
                )}
              </Link>
            );
          })}
        </nav>
        <div className={`border-t border-slate-700 ${isGraphRoute ? 'px-2 py-3' : 'p-4'}`}>
          <div className={`flex ${isGraphRoute ? 'justify-center px-0 py-2' : 'items-center gap-3 px-3 py-2'}`}>
            <div className="w-8 h-8 rounded-full flex items-center justify-center" style={{ background: 'linear-gradient(135deg, #1a56db, #1e3a8a)' }}>
              <User className="w-4 h-4 text-white" />
            </div>
            {!isGraphRoute && (
              <div className="flex-1 min-w-0">
                <p className="text-white text-sm" style={{ fontWeight: 500 }}>Admin</p>
                <p className="text-slate-400 text-xs truncate">{ru ? 'Администратор' : 'Administrator'}</p>
              </div>
            )}
          </div>
        </div>
      </aside>

      {/* Main column */}
      <div className="flex-1 flex flex-col min-w-0 overflow-hidden">

        {/* ════════════════════════════════════════════
            DESKTOP HEADER
        ════════════════════════════════════════════ */}
        <header className={`hidden md:flex items-center justify-between bg-white border-b border-gray-200 flex-shrink-0 ${isGraphRoute ? 'h-16 px-5 py-3' : 'px-8 py-4'}`}>
          {/* Search */}
          <div className={`flex-1 ${isGraphRoute ? 'max-w-[320px]' : 'max-w-md'}`}>
            <div className="relative">
              <Search className={`absolute left-3 top-1/2 -translate-y-1/2 text-gray-400 ${isGraphRoute ? 'w-4 h-4' : 'w-5 h-5'}`} />
              <input type="text"
                placeholder={ru ? 'Поиск по темам, каналам, участникам...' : 'Search topics, channels, members...'}
                className={`w-full border border-gray-300 rounded-lg focus:outline-none focus:ring-2 focus:ring-blue-500 ${isGraphRoute ? 'pl-9 pr-3 py-1.5 text-[13px]' : 'pl-10 pr-4 py-2 text-sm'}`} />
            </div>
          </div>

          {/* Date picker */}
          <div className={`relative ${isGraphRoute ? 'ml-3' : 'ml-4'}`} ref={datePickerRef}>
            <button onClick={() => setShowDatePicker(!showDatePicker)}
              title={isRefreshing ? (ru ? 'Обновляем дашборд для выбранного периода' : 'Updating dashboard for selected range') : undefined}
              className={`flex items-center gap-2 rounded-lg border transition-colors ${isGraphRoute ? 'px-3 py-1.5 text-[13px]' : 'px-3 py-2 text-sm'} ${showDatePicker ? 'border-blue-400 bg-blue-50 text-blue-700' : 'border-gray-300 bg-white text-gray-700 hover:bg-gray-50'}`}>
              <Calendar className={isGraphRoute ? 'w-3.5 h-3.5' : 'w-4 h-4'} />
              {activeRange ? activeRange.label : `${formatDisplayDate(range.from, lang)} — ${formatDisplayDate(range.to, lang)}`}
              {isRefreshing && (
                <Loader2
                  className="w-4 h-4 animate-spin text-blue-600"
                  aria-label={ru ? 'Обновление дашборда' : 'Updating dashboard'}
                />
              )}
            </button>
            {showDatePicker && (
              <div className="absolute right-0 top-full mt-2 w-[420px] bg-white border border-gray-200 rounded-xl shadow-xl z-50 overflow-hidden">
                <DatePickerContent />
              </div>
            )}
            <p className={`mt-1 text-gray-400 text-right ${isGraphRoute ? 'text-[10px]' : 'text-[11px]'}`}>
              {dateHelperText}
            </p>
          </div>

          {/* Right controls */}
          <div className={`flex items-center ml-4 ${isGraphRoute ? 'gap-2' : 'gap-3'}`}>
            {/* Language toggle */}
            <div className="flex items-center rounded-lg border border-gray-200 overflow-hidden">
              {(['en', 'ru'] as Lang[]).map(l => (
                <button key={l} onClick={() => setLang(l)}
                  className={`${isGraphRoute ? 'px-2.5 py-1' : 'px-3 py-1.5'} text-xs transition-colors ${lang === l ? 'bg-slate-800 text-white' : 'bg-white text-gray-500 hover:bg-gray-50'}`}
                  style={{ fontWeight: 600 }}>
                  {l === 'en' ? 'EN' : 'РУ'}
                </button>
              ))}
            </div>

            {/* Desktop Notification Bell */}
            <div className="relative" ref={notifRef}>
              <button onClick={() => { setShowNotifications(v => !v); setShowAccount(false); }}
                className={`relative p-2 rounded-lg transition-colors ${showNotifications ? 'bg-gray-100' : 'hover:bg-gray-100'}`}>
                <BellIcon count={unreadCount} />
              </button>
              <AnimatePresence>
                {showNotifications && !isMobile && (
                  <motion.div initial={{ opacity: 0, y: 6 }} animate={{ opacity: 1, y: 0 }} exit={{ opacity: 0, y: 6 }}
                    transition={{ duration: 0.15 }}
                    className="absolute right-0 top-full mt-2 w-[380px] bg-white border border-gray-200 rounded-xl shadow-2xl z-50 overflow-hidden"
                    style={{ maxHeight: '480px' }}>
                    <NotifList notifications={notifications} ru={ru}
                      onMarkRead={markRead} onDismiss={dismissNotif} onMarkAll={markAllRead}
                      onClose={() => setShowNotifications(false)} onGoSettings={goSettings} />
                  </motion.div>
                )}
              </AnimatePresence>
            </div>

            {/* Desktop Account */}
            <div className="relative" ref={accountRef}>
              <button onClick={() => { setShowAccount(v => !v); setShowNotifications(false); }}
                className={`flex items-center rounded-lg transition-colors ${isGraphRoute ? 'gap-1.5 px-1.5 py-1' : 'gap-2 px-2 py-1.5'} ${showAccount ? 'bg-gray-100' : 'hover:bg-gray-100'}`}>
                <div className="w-8 h-8 rounded-full flex items-center justify-center" style={{ background: 'linear-gradient(135deg, #1a56db, #1e3a8a)' }}>
                  <User className="w-4 h-4 text-white" />
                </div>
                {!isGraphRoute && (
                  <div className="text-left">
                    <p className="text-sm text-gray-900" style={{ fontWeight: 600 }}>Admin</p>
                    <p className="text-xs text-gray-500">{ru ? 'Администратор' : 'Administrator'}</p>
                  </div>
                )}
              </button>
              <AnimatePresence>
                {showAccount && !isMobile && (
                  <motion.div initial={{ opacity: 0, y: 6 }} animate={{ opacity: 1, y: 0 }} exit={{ opacity: 0, y: 6 }}
                    transition={{ duration: 0.15 }}
                    className="absolute right-0 top-full mt-2 w-[260px] bg-white border border-gray-200 rounded-xl shadow-2xl z-50 overflow-hidden">
                    <AccountMenu ru={ru} onSettings={goSettings} onLogout={handleLogout} onClose={() => setShowAccount(false)} />
                  </motion.div>
                )}
              </AnimatePresence>
            </div>
          </div>
        </header>

        {/* ════════════════════════════════════════════
            MOBILE TOP BAR
        ════════════════════════════════════════════ */}
        <header className="md:hidden flex items-center justify-between px-3 bg-white border-b border-gray-200 flex-shrink-0 relative"
          style={{ height: '56px' }}>
          {/* Left: Hamburger */}
          <button onClick={() => setDrawerOpen(true)}
            className="p-2 rounded-xl hover:bg-gray-100 active:bg-gray-200 transition-colors flex-shrink-0">
            <Menu className="w-5 h-5 text-gray-700" />
          </button>

          {/* Center: Logo + Page title (absolute centered) */}
          <div className="flex items-center gap-2 absolute left-1/2 -translate-x-1/2 pointer-events-none">
            <LogoIcon size={22} />
            <span className="text-sm text-gray-900 whitespace-nowrap" style={{ fontWeight: 600 }}>
              {currentPage?.label ?? 'Armenian Intel'}
            </span>
          </div>

          {/* Right: Date + Bell + Avatar */}
          <div className="flex items-center gap-1 flex-shrink-0">
            {/* Date filter */}
            <button
              onClick={() => { setShowMobileDatePicker(v => !v); setShowNotifications(false); setShowAccount(false); }}
              title={isRefreshing ? (ru ? 'Обновляем дашборд для выбранного периода' : 'Updating dashboard for selected range') : undefined}
              className={`relative p-2 rounded-xl transition-colors ${showMobileDatePicker ? 'bg-blue-50' : 'hover:bg-gray-100 active:bg-gray-200'}`}
            >
              <Calendar className={`w-5 h-5 ${showMobileDatePicker ? 'text-blue-600' : 'text-gray-600'}`} />
              {isRefreshing ? (
                <Loader2
                  className="absolute -top-0.5 -right-0.5 w-3.5 h-3.5 animate-spin text-blue-600 bg-white rounded-full"
                  aria-label={ru ? 'Обновление дашборда' : 'Updating dashboard'}
                />
              ) : ready && range.presetId !== 'last_15_days' && (
                <span className="absolute top-1.5 right-1.5 w-1.5 h-1.5 bg-blue-500 rounded-full" />
              )}
            </button>
            {/* Bell */}
            <button onClick={() => { setShowNotifications(v => !v); setShowAccount(false); setShowMobileDatePicker(false); }}
              className="relative p-2 rounded-xl hover:bg-gray-100 active:bg-gray-200 transition-colors">
              <BellIcon count={unreadCount} />
            </button>
            {/* Avatar */}
            <button onClick={() => { setShowAccount(v => !v); setShowNotifications(false); setShowMobileDatePicker(false); }}
              className="w-8 h-8 rounded-full flex items-center justify-center active:opacity-80 transition-opacity ml-0.5" style={{ background: 'linear-gradient(135deg, #1a56db, #1e3a8a)' }}>
              <User className="w-4 h-4 text-white" />
            </button>
          </div>
        </header>

        {/* ════════════════════════════════════════════
            MAIN CONTENT
        ════════════════════════════════════════════ */}
        <main ref={mainRef} className="flex-1 overflow-y-auto overflow-x-hidden">
          <div className="pb-20 md:pb-0">
            {!hasLiveData && loading ? (
              <div className="min-h-[60vh] flex flex-col items-center justify-center px-6 text-center">
                <div className="w-9 h-9 rounded-full border-2 border-blue-200 border-t-blue-600 animate-spin mb-3" />
                <p className="text-sm text-gray-700" style={{ fontWeight: 500 }}>
                  {ru ? 'Загружаем данные панели…' : 'Loading dashboard data...'}
                </p>
                <p className="text-xs text-gray-500 mt-1">
                  {ru ? 'Показываем только реальные данные без мок-значений' : 'Showing only real backend data, no mock placeholders'}
                </p>
              </div>
            ) : !hasLiveData && error ? (
              <div className="min-h-[60vh] flex flex-col items-center justify-center px-6 text-center">
                <p className="text-sm text-red-600" style={{ fontWeight: 500 }}>
                  {ru ? 'Не удалось загрузить данные' : 'Failed to load dashboard data'}
                </p>
                <p className="text-xs text-gray-500 mt-1">{error}</p>
                <button
                  onClick={refresh}
                  className="mt-3 px-3 py-1.5 rounded-lg bg-blue-600 text-white text-xs hover:bg-blue-700 transition-colors"
                >
                  {ru ? 'Повторить' : 'Retry'}
                </button>
              </div>
            ) : (
              <>
                {hasLiveData && isRefreshing && (
                  <div className="px-4 md:px-6 pt-4">
                    <div className="rounded-xl border border-slate-200 bg-white px-4 py-3 shadow-sm">
                      <div className="flex items-center gap-3">
                        <Loader2 className="w-4 h-4 animate-spin text-blue-600 flex-shrink-0" />
                        <div className="min-w-0">
                          <p className="text-sm text-slate-900" style={{ fontWeight: 600 }}>
                            {ru ? 'Обновляем данные панели…' : 'Refreshing dashboard data...'}
                          </p>
                          <p className="text-xs text-slate-500 mt-0.5">
                            {showsPreviousSnapshot
                              ? (
                                ru
                                  ? `На экране пока остаётся снимок за ${displayedRangeDays} дн., пока загружается диапазон ${range.days} дн.`
                                  : `Showing the current ${displayedRangeDays}-day snapshot while the selected ${range.days}-day range loads.`
                              )
                              : (
                                ru
                                  ? 'Предыдущий снимок остаётся на экране, пока новый диапазон загружается.'
                                  : 'The previous snapshot stays visible while the new range is loading.'
                              )}
                          </p>
                        </div>
                      </div>
                    </div>
                  </div>
                )}
                <Outlet />
              </>
            )}
          </div>
        </main>

        {/* ════════════════════════════════════════════
            MOBILE BOTTOM NAV (4 pages + AI)
        ════════════════════════════════════════════ */}
        <nav className="md:hidden flex-shrink-0 flex items-stretch bg-white border-t border-gray-200"
          style={{ height: '64px', paddingBottom: 'env(safe-area-inset-bottom)' }}>

          {/* 4 regular nav tabs */}
          {mobileNavItems.map(item => {
            const isActive = item.path === '/' ? location.pathname === '/' : location.pathname.startsWith(item.path);
            const Icon = item.icon;
            return (
              <Link key={item.path} to={item.path}
                className="flex-1 flex flex-col items-center justify-center gap-0.5 relative transition-colors active:bg-gray-50">
                {isActive && (
                  <motion.div layoutId="bottomNavIndicator"
                    className="absolute top-0 left-1/2 -translate-x-1/2 w-8 h-0.5 rounded-full"
                    style={{ background: 'linear-gradient(90deg, #1a56db, #1e3a8a)' }}
                    transition={{ type: 'spring', damping: 30, stiffness: 300 }} />
                )}
                <motion.div animate={{ scale: isActive ? 1.1 : 1, y: isActive ? -1 : 0 }}
                  transition={{ type: 'spring', damping: 20, stiffness: 300 }}>
                  <Icon className={`w-5 h-5 transition-colors ${isActive ? 'text-blue-600' : 'text-gray-400'}`} />
                </motion.div>
                <span className={`transition-colors ${isActive ? 'text-blue-600' : 'text-gray-400'}`}
                  style={{ fontSize: '10px', fontWeight: isActive ? 600 : 400 }}>
                  {item.label}
                </span>
              </Link>
            );
          })}

          {/* AI tab */}
          <button
            onClick={() => setShowAI(v => !v)}
            className="flex-1 flex flex-col items-center justify-center gap-0.5 relative active:bg-gray-50 transition-colors">
            {showAI && (
              <motion.div layoutId="bottomNavIndicator"
                className="absolute top-0 left-1/2 -translate-x-1/2 w-8 h-0.5 bg-violet-500 rounded-full"
                transition={{ type: 'spring', damping: 30, stiffness: 300 }} />
            )}
            <motion.div animate={{ scale: showAI ? 1.1 : 1, y: showAI ? -1 : 0 }}
              transition={{ type: 'spring', damping: 20, stiffness: 300 }}
              className={`w-8 h-8 rounded-xl flex items-center justify-center transition-all ${showAI
                ? 'bg-gradient-to-br from-violet-500 to-purple-600 shadow-md'
                : 'bg-violet-50'
              }`}>
              <Sparkles className={`w-4 h-4 transition-colors ${showAI ? 'text-white' : 'text-violet-500'}`} />
            </motion.div>
            <span className={`transition-colors ${showAI ? 'text-violet-600' : 'text-gray-400'}`}
              style={{ fontSize: '10px', fontWeight: showAI ? 600 : 400 }}>
              {ru ? 'ИИ' : 'AI'}
            </span>
          </button>

          {/* 4 regular nav tabs */}
          {mobileNavItemsRight.map(item => {
            const isActive = item.path === '/' ? location.pathname === '/' : location.pathname.startsWith(item.path);
            const Icon = item.icon;
            return (
              <Link key={item.path} to={item.path}
                className="flex-1 flex flex-col items-center justify-center gap-0.5 relative transition-colors active:bg-gray-50">
                {isActive && (
                  <motion.div layoutId="bottomNavIndicator"
                    className="absolute top-0 left-1/2 -translate-x-1/2 w-8 h-0.5 rounded-full"
                    style={{ background: 'linear-gradient(90deg, #1a56db, #1e3a8a)' }}
                    transition={{ type: 'spring', damping: 30, stiffness: 300 }} />
                )}
                <motion.div animate={{ scale: isActive ? 1.1 : 1, y: isActive ? -1 : 0 }}
                  transition={{ type: 'spring', damping: 20, stiffness: 300 }}>
                  <Icon className={`w-5 h-5 transition-colors ${isActive ? 'text-blue-600' : 'text-gray-400'}`} />
                </motion.div>
                <span className={`transition-colors ${isActive ? 'text-blue-600' : 'text-gray-400'}`}
                  style={{ fontSize: '10px', fontWeight: isActive ? 600 : 400 }}>
                  {item.label}
                </span>
              </Link>
            );
          })}
        </nav>
      </div>

      {/* ════════════════════════════════════════════
          MOBILE DRAWER
      ════════════════════════════════════════════ */}
      <AnimatePresence>
        {drawerOpen && (
          <>
            <Backdrop onClick={() => setDrawerOpen(false)} />
            <motion.aside
              className="fixed top-0 left-0 bottom-0 z-50 w-72 bg-slate-800 flex flex-col overflow-hidden md:hidden"
              initial={{ x: '-100%' }} animate={{ x: 0 }} exit={{ x: '-100%' }}
              transition={springConfig}>
              <div className="flex items-center justify-between p-5 border-b border-slate-700">
                <div className="flex items-center gap-3">
                  <LogoIcon size={32} />
                  <div>
                    <p className="text-white text-sm" style={{ fontWeight: 600 }}>Armenian Intel</p>
                    <p className="text-slate-400 text-xs">{ru ? 'Платформа' : 'Community Platform'}</p>
                  </div>
                </div>
                <button onClick={() => setDrawerOpen(false)}
                  className="p-1.5 rounded-lg hover:bg-slate-700 text-slate-400 hover:text-white transition-colors">
                  <X className="w-5 h-5" />
                </button>
              </div>
              {/* Profile */}
              <div className="px-4 py-4 border-b border-slate-700">
                <div className="flex items-center gap-3">
                  <div className="w-10 h-10 rounded-full flex items-center justify-center" style={{ background: 'linear-gradient(135deg, #1a56db, #1e3a8a)' }}>
                    <User className="w-5 h-5 text-white" />
                  </div>
                  <div>
                    <p className="text-white text-sm" style={{ fontWeight: 600 }}>Admin</p>
                    <p className="text-slate-400 text-xs">{ru ? 'Администратор' : 'Administrator'}</p>
                  </div>
                </div>
              </div>
              {/* Nav */}
              <nav className="flex-1 overflow-y-auto px-3 py-4 space-y-1">
                <div className="text-slate-500 text-xs uppercase tracking-wider mb-3 px-3" style={{ fontWeight: 700 }}>
                  {ru ? 'Меню' : 'Navigation'}
                </div>
                {navItems.map(item => {
                  if (item.type === 'divider') {
                    return <div key={item.id} className="my-3 mx-3 border-t border-slate-700/80" />;
                  }

                  if (item.type === 'group') {
                    const isParentActive = isNavItemActive(item.path) || item.children.some((child) => isNavItemActive(child.path));
                    const ParentIcon = item.icon;

                    return (
                      <div key={item.path} className="space-y-1">
                        <Link
                          to={item.path}
                          onClick={() => setDrawerOpen(false)}
                          className={`flex items-center gap-3 px-3 py-3 rounded-xl transition-colors ${isParentActive ? 'text-white' : 'text-slate-300 hover:bg-slate-700/60 hover:text-white'}`}
                          style={isParentActive ? { background: 'linear-gradient(135deg, #1a56db, #1e3a8a)' } : {}}
                        >
                          <ParentIcon className="w-5 h-5 flex-shrink-0" />
                          <span className="text-sm" style={{ fontWeight: 500 }}>{item.label}</span>
                          {isParentActive && <motion.div layoutId={`drawerActive-${item.path}`} className="ml-auto w-1.5 h-1.5 rounded-full bg-white/70" />}
                        </Link>
                        <div className="space-y-1">
                          {item.children.map((child) => {
                            const isChildActive = isNavItemActive(child.path);
                            const ChildIcon = child.icon;
                            return (
                              <Link
                                key={child.path}
                                to={child.path}
                                onClick={() => setDrawerOpen(false)}
                                className={`ml-4 flex items-center gap-3 px-3 py-3 rounded-xl transition-colors ${isChildActive ? 'text-white' : 'text-slate-300 hover:bg-slate-700/60 hover:text-white'}`}
                                style={isChildActive ? { background: 'linear-gradient(135deg, #1a56db, #1e3a8a)' } : {}}
                              >
                                <ChildIcon className="w-5 h-5 flex-shrink-0" />
                                <span className="text-sm" style={{ fontWeight: 500 }}>{child.label}</span>
                                {isChildActive && <motion.div layoutId={`drawerActive-${child.path}`} className="ml-auto w-1.5 h-1.5 rounded-full bg-white/70" />}
                              </Link>
                            );
                          })}
                        </div>
                      </div>
                    );
                  }

                  const isActive = isNavItemActive(item.path);
                  const Icon = item.icon;
                  return (
                    <Link key={item.path} to={item.path} onClick={() => setDrawerOpen(false)}
                      className={`flex items-center gap-3 px-3 py-3 rounded-xl transition-colors ${isActive ? 'text-white' : 'text-slate-300 hover:bg-slate-700/60 hover:text-white'}`}
                      style={isActive ? { background: 'linear-gradient(135deg, #1a56db, #1e3a8a)' } : {}}>
                      <Icon className="w-5 h-5 flex-shrink-0" />
                      <span className="text-sm" style={{ fontWeight: 500 }}>{item.label}</span>
                      {isActive && <motion.div layoutId={`drawerActive-${item.path}`} className="ml-auto w-1.5 h-1.5 rounded-full bg-white/70" />}
                    </Link>
                  );
                })}
              </nav>
              {/* Language + Logout */}
              <div className="p-4 border-t border-slate-700 space-y-3">
                <div className="flex items-center gap-3 px-1">
                  <Globe className="w-4 h-4 text-slate-400 flex-shrink-0" />
                  <div className="flex items-center rounded-lg border border-slate-600 overflow-hidden flex-1">
                    {(['en', 'ru'] as Lang[]).map(l => (
                      <button key={l} onClick={() => setLang(l)}
                        className={`flex-1 py-2 text-xs transition-colors ${lang === l ? 'bg-slate-600 text-white' : 'bg-transparent text-slate-400 hover:text-white'}`}
                        style={{ fontWeight: 600 }}>
                        {l === 'en' ? 'EN' : 'РУ'}
                      </button>
                    ))}
                  </div>
                </div>
                <button onClick={handleLogout}
                  className="w-full flex items-center gap-3 px-3 py-3 rounded-xl text-red-400 hover:bg-red-500/10 hover:text-red-300 transition-colors">
                  <LogOut className="w-5 h-5" />
                  <span className="text-sm" style={{ fontWeight: 500 }}>{ru ? 'Выйти' : 'Log Out'}</span>
                </button>
              </div>
            </motion.aside>
          </>
        )}
      </AnimatePresence>

      {/* ════════════════════════════════════════════
          MOBILE DATE PICKER BOTTOM SHEET
      ════════════════════════════════════════════ */}
      <AnimatePresence>
        {showMobileDatePicker && isMobile && (
          <>
            <Backdrop onClick={() => setShowMobileDatePicker(false)} />
            <motion.div
              className="fixed bottom-0 left-0 right-0 z-50 bg-white rounded-t-2xl overflow-hidden md:hidden"
              initial={{ y: '100%' }} animate={{ y: 0 }} exit={{ y: '100%' }}
              transition={springConfig}>
              {/* Handle */}
              <div className="pt-3 pb-1 flex justify-center">
                <div className="w-10 h-1 bg-gray-300 rounded-full" />
              </div>
              {/* Header */}
              <div className="flex items-center justify-between px-4 py-3 border-b border-gray-100">
                <div className="flex items-center gap-2">
                  <Calendar className="w-4 h-4 text-blue-600" />
                  <span className="text-sm text-gray-900" style={{ fontWeight: 600 }}>
                    {ru ? 'Период анализа' : 'Date Range'}
                  </span>
                </div>
                <span className="text-xs text-blue-600 bg-blue-50 px-2 py-0.5 rounded-full" style={{ fontWeight: 500 }}>
                  {activeRange ? activeRange.label : `${range.days}d`}
                </span>
              </div>
              <DatePickerContent />
            </motion.div>
          </>
        )}
      </AnimatePresence>

      {/* ════════════════════════════════════════════
          MOBILE NOTIFICATION BOTTOM SHEET
      ════════════════════════════════════════════ */}
      <AnimatePresence>
        {showNotifications && isMobile && (
          <>
            <Backdrop onClick={() => setShowNotifications(false)} />
            <motion.div
              className="fixed bottom-0 left-0 right-0 z-50 bg-white rounded-t-2xl overflow-hidden flex flex-col md:hidden"
              style={{ maxHeight: '82dvh' }}
              initial={{ y: '100%' }} animate={{ y: 0 }} exit={{ y: '100%' }}
              transition={springConfig}>
              <div className="flex-shrink-0 pt-3 pb-1 flex justify-center">
                <div className="w-10 h-1 bg-gray-300 rounded-full" />
              </div>
              <NotifList notifications={notifications} ru={ru}
                onMarkRead={markRead} onDismiss={dismissNotif} onMarkAll={markAllRead}
                onClose={() => setShowNotifications(false)} onGoSettings={goSettings} />
            </motion.div>
          </>
        )}
      </AnimatePresence>

      {/* ════════════════════════════════════════════
          MOBILE ACCOUNT BOTTOM SHEET
      ════════════════════════════════════════════ */}
      <AnimatePresence>
        {showAccount && isMobile && (
          <>
            <Backdrop onClick={() => setShowAccount(false)} />
            <motion.div
              className="fixed bottom-0 left-0 right-0 z-50 bg-white rounded-t-2xl overflow-hidden md:hidden"
              initial={{ y: '100%' }} animate={{ y: 0 }} exit={{ y: '100%' }}
              transition={springConfig}>
              <div className="pt-3 pb-1 flex justify-center">
                <div className="w-10 h-1 bg-gray-300 rounded-full" />
              </div>
              <AccountMenu ru={ru} onSettings={goSettings} onLogout={handleLogout}
                onClose={() => setShowAccount(false)} />
            </motion.div>
          </>
        )}
      </AnimatePresence>

      {/* ════════════════════════════════════════════
          SCROLL TO TOP BUTTON
      ════════════════════════════════════════════ */}
      <AnimatePresence>
        {showScrollTop && (
          <motion.button
            onClick={scrollToTop}
            initial={{ opacity: 0, scale: 0.7, y: 8 }}
            animate={{ opacity: 1, scale: 1, y: 0 }}
            exit={{ opacity: 0, scale: 0.7, y: 8 }}
            transition={{ type: 'spring', damping: 20, stiffness: 300 }}
            whileHover={{ scale: 1.08 }}
            whileTap={{ scale: 0.93 }}
            className="fixed z-30 flex items-center justify-center rounded-2xl shadow-lg transition-shadow hover:shadow-xl"
            style={{
              right: '1rem',
              bottom: 'calc(64px + 0.75rem)', // just above mobile bottom nav
              width: '40px',
              height: '40px',
              background: 'linear-gradient(135deg, #1a56db, #1e3a8a)',
              boxShadow: '0 4px 16px rgba(26,86,219,0.35)',
            }}
            title={ru ? 'Наверх' : 'Back to top'}
          >
            <ChevronUp className="w-5 h-5 text-white" />
          </motion.button>
        )}
      </AnimatePresence>

      {/* AI Assistant (desktop floating + mobile controlled sheet) */}
      <AIAssistant mobileOpen={showAI} onMobileClose={() => setShowAI(false)} />
    </div>
  );
}
