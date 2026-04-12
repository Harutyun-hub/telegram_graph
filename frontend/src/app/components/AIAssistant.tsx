import { useState, useRef, useEffect } from 'react';
import { Sparkles, X, Send, RotateCcw, ChevronDown } from 'lucide-react';
import { motion, AnimatePresence } from 'motion/react';
import { useLocation } from 'react-router';
import { useLanguage } from '../contexts/LanguageContext';
import { aiHelperChat, getAiHelperHistory, resetAiHelper } from '../services/api';

interface Message {
  id: string;
  role: 'user' | 'assistant';
  text: string;
  timestamp: Date;
}

const SUGGESTED_PROMPTS_EN = [
  'What are the top community concerns this week?',
  'Which topics need urgent attention?',
  'Who are the most influential voices?',
  'Summarize housing sentiment trends',
];

const SUGGESTED_PROMPTS_RU = [
  'Какие главные проблемы сообщества на этой неделе?',
  'Какие темы требуют срочного внимания?',
  'Кто самые влиятельные участники?',
  'Сводка настроений по жилью',
];
const MAX_MESSAGE_CHARS = 2000;
const AI_CHAT_SESSION_STORAGE_KEY = 'radar.ai-chat.session.v1';

function generateAiChatSessionId() {
  const randomPart = typeof crypto !== 'undefined' && typeof crypto.randomUUID === 'function'
    ? crypto.randomUUID()
    : `${Date.now().toString(36)}-${Math.random().toString(36).slice(2, 12)}`;
  return `web_${randomPart.replace(/[^A-Za-z0-9_-]/g, '_')}`;
}

function getOrCreateAiChatSessionId() {
  if (typeof window === 'undefined') {
    return generateAiChatSessionId();
  }
  const existing = window.localStorage.getItem(AI_CHAT_SESSION_STORAGE_KEY);
  if (existing && /^[A-Za-z0-9_-]{8,64}$/.test(existing)) {
    return existing;
  }
  const next = generateAiChatSessionId();
  window.localStorage.setItem(AI_CHAT_SESSION_STORAGE_KEY, next);
  return next;
}

function persistAiChatSessionId(value: string) {
  if (typeof window !== 'undefined') {
    window.localStorage.setItem(AI_CHAT_SESSION_STORAGE_KEY, value);
  }
}

function parseTimestamp(value: string | Date | undefined) {
  if (value instanceof Date) {
    return value;
  }
  if (typeof value === 'string') {
    const parsed = new Date(value);
    if (!Number.isNaN(parsed.getTime())) {
      return parsed;
    }
  }
  return new Date();
}

function renderMarkdown(text: string) {
  return text.split('\n').map((line, i) => {
    const parts = line.split(/\*\*(.*?)\*\*/g);
    return (
      <span key={i} className="block">
        {parts.map((part, j) =>
          j % 2 === 1
            ? <strong key={j} className="text-gray-900" style={{ fontWeight: 600 }}>{part}</strong>
            : part
        )}
      </span>
    );
  });
}

// ─── Chat content (shared between mobile sheet and desktop panel) ──
function ChatContent({
  messages, typing, suggested, input, setInput, onSend, onReset, onClose, ru, inputRef, bottomRef,
  remainingChars, busy,
}: {
  messages: Message[]; typing: boolean; suggested: string[];
  input: string; setInput: (v: string) => void;
  onSend: (text: string) => void; onReset: () => void; onClose: () => void;
  ru: boolean; inputRef: React.RefObject<HTMLInputElement | null>; bottomRef: React.RefObject<HTMLDivElement | null>;
  remainingChars: number; busy: boolean;
}) {
  const inputTooLong = remainingChars < 0;
  return (
    <>
      {/* Header */}
      <div className="flex items-center justify-between px-4 py-3 flex-shrink-0"
        style={{ background: 'linear-gradient(135deg, #7c3aed, #6d28d9)' }}>
        <div className="flex items-center gap-3">
          <div className="w-8 h-8 rounded-xl bg-white/20 flex items-center justify-center">
            <Sparkles className="w-4 h-4 text-white" />
          </div>
          <div>
            <p className="text-white text-sm" style={{ fontWeight: 600 }}>
              {ru ? 'ИИ-аналитик' : 'AI Analyst'}
            </p>
            <div className="flex items-center gap-1.5">
              <span className="w-1.5 h-1.5 bg-emerald-400 rounded-full animate-pulse" />
              <p className="text-purple-200 text-xs">{ru ? 'Живые данные · онлайн' : 'Live data · online'}</p>
            </div>
          </div>
        </div>
        <div className="flex items-center gap-1">
          <button onClick={onReset} disabled={busy}
            className="p-1.5 rounded-lg hover:bg-white/10 text-white/70 hover:text-white transition-colors"
            title={ru ? 'Очистить чат' : 'Clear chat'}>
            <RotateCcw className="w-3.5 h-3.5" />
          </button>
          <button onClick={onClose}
            className="p-1.5 rounded-lg hover:bg-white/10 text-white/70 hover:text-white transition-colors">
            <X className="w-4 h-4" />
          </button>
        </div>
      </div>

      {/* Messages */}
      <div className="flex-1 overflow-y-auto bg-gray-50 px-4 py-4 space-y-3">
        {messages.map(msg => (
          <div key={msg.id} className={`flex ${msg.role === 'user' ? 'justify-end' : 'justify-start'}`}>
            {msg.role === 'assistant' && (
              <div className="w-6 h-6 rounded-lg flex-shrink-0 mr-2 mt-0.5 flex items-center justify-center"
                style={{ background: 'linear-gradient(135deg, #8b5cf6, #7c3aed)' }}>
                <Sparkles className="w-3 h-3 text-white" />
              </div>
            )}
            <div className={`max-w-[82%] rounded-2xl px-3.5 py-2.5 text-xs leading-relaxed ${
              msg.role === 'user'
                ? 'text-white rounded-br-sm'
                : 'bg-white border border-gray-100 text-gray-700 rounded-bl-sm shadow-sm'
            }`}
              style={msg.role === 'user' ? { background: 'linear-gradient(135deg, #8b5cf6, #7c3aed)' } : {}}>
              {msg.role === 'assistant'
                ? <div className="space-y-0.5">{renderMarkdown(msg.text)}</div>
                : msg.text}
            </div>
          </div>
        ))}

        {/* Typing */}
        {typing && (
          <div className="flex justify-start">
            <div className="w-6 h-6 rounded-lg flex-shrink-0 mr-2 mt-0.5 flex items-center justify-center"
              style={{ background: 'linear-gradient(135deg, #8b5cf6, #7c3aed)' }}>
              <Sparkles className="w-3 h-3 text-white" />
            </div>
            <div className="bg-white border border-gray-100 rounded-2xl rounded-bl-sm px-3.5 py-3 shadow-sm flex items-center gap-1">
              <span className="w-1.5 h-1.5 bg-violet-400 rounded-full animate-bounce" style={{ animationDelay: '0ms' }} />
              <span className="w-1.5 h-1.5 bg-violet-400 rounded-full animate-bounce" style={{ animationDelay: '150ms' }} />
              <span className="w-1.5 h-1.5 bg-violet-400 rounded-full animate-bounce" style={{ animationDelay: '300ms' }} />
            </div>
          </div>
        )}

        {/* Suggested prompts */}
        {messages.filter(m => m.role === 'user').length === 0 && !typing && (
          <div className="space-y-2 pt-2">
            <p className="text-xs text-gray-400 text-center" style={{ fontWeight: 500 }}>
              {ru ? 'Попробуйте спросить:' : 'Try asking:'}
            </p>
            {suggested.map(prompt => (
              <button key={prompt} onClick={() => onSend(prompt)}
                className="w-full text-left text-xs bg-white border border-gray-200 rounded-xl px-3.5 py-2.5 text-gray-600 hover:border-violet-300 hover:bg-violet-50 hover:text-violet-700 transition-colors shadow-sm">
                {prompt}
              </button>
            ))}
          </div>
        )}

        <div ref={bottomRef} />
      </div>

      {/* Input */}
      <div className="flex-shrink-0 bg-white border-t border-gray-100 px-3 py-3">
        <div className="flex items-center gap-2 bg-gray-50 rounded-xl border border-gray-200 px-3 py-2 focus-within:border-violet-400 focus-within:ring-2 focus-within:ring-violet-100 transition-all">
          <input ref={inputRef} type="text" value={input} maxLength={MAX_MESSAGE_CHARS}
            onChange={e => setInput(e.target.value)}
            onKeyDown={e => { if (e.key === 'Enter' && !e.shiftKey) { e.preventDefault(); onSend(input); } }}
            placeholder={ru ? 'Спросите о трендах, темах...' : 'Ask about trends, topics...'}
            className="flex-1 bg-transparent text-sm text-gray-800 placeholder-gray-400 focus:outline-none"
            disabled={busy} />
          <button onClick={() => onSend(input)} disabled={!input.trim() || inputTooLong || busy}
            className="w-8 h-8 rounded-lg flex items-center justify-center transition-all disabled:opacity-30"
            style={{ background: input.trim() ? 'linear-gradient(135deg, #8b5cf6, #7c3aed)' : '#e5e7eb' }}>
            <Send className="w-3.5 h-3.5 text-white" />
          </button>
        </div>
        <div className="mt-2 flex items-center justify-between px-1">
          <p className={`text-[11px] ${inputTooLong ? 'text-rose-500' : 'text-gray-400'}`}>
            {ru
              ? `Символы: ${input.length}/${MAX_MESSAGE_CHARS}`
              : `Characters: ${input.length}/${MAX_MESSAGE_CHARS}`}
          </p>
          {inputTooLong && (
            <p className="text-[11px] text-rose-500">
              {ru ? 'Сократите сообщение перед отправкой.' : 'Shorten your message before sending.'}
            </p>
          )}
        </div>
      </div>
    </>
  );
}

// ─── Main Export ─────────────────────────────────────────────────
interface AIAssistantProps {
  /** controlled from outside (mobile bottom-nav tab) */
  mobileOpen?: boolean;
  onMobileClose?: () => void;
}

export function AIAssistant({ mobileOpen, onMobileClose }: AIAssistantProps = {}) {
  const location = useLocation();
  const { lang } = useLanguage();
  const ru = lang === 'ru';
  const [desktopOpen, setDesktopOpen] = useState(false);
  const [input, setInput] = useState('');
  const [messages, setMessages] = useState<Message[]>([]);
  const [typing, setTyping] = useState(false);
  const [historyLoaded, setHistoryLoaded] = useState(false);
  const [resetting, setResetting] = useState(false);
  const [sessionId, setSessionId] = useState(() => getOrCreateAiChatSessionId());
  const bottomRef = useRef<HTMLDivElement>(null);
  const inputRef = useRef<HTMLInputElement>(null);
  const historyRequestedRef = useRef(false);

  const suggested = ru ? SUGGESTED_PROMPTS_RU : SUGGESTED_PROMPTS_EN;
  const remainingChars = MAX_MESSAGE_CHARS - input.length;
  const busy = typing || resetting;

  const initMessages = (resetId: string) => [{
    id: resetId,
    role: 'assistant' as const,
    text: ru
      ? 'Привет! Я ваш ИИ-аналитик армянского сообщества. Спросите меня о трендах, настроениях, ключевых темах или участниках.'
      : "Hi! I'm your Armenian community AI analyst. Ask me about trends, sentiment, key topics, or community voices.",
    timestamp: new Date(),
  }];

  useEffect(() => {
    const isOpen = desktopOpen || mobileOpen;
    if (!isOpen) {
      return;
    }
    if (messages.length === 0) {
      setMessages(initMessages('welcome'));
    }
    if (!historyLoaded && !historyRequestedRef.current) {
      let cancelled = false;
      historyRequestedRef.current = true;
      void (async () => {
        try {
          const history = await getAiHelperHistory(sessionId);
          if (cancelled) {
            return;
          }
          if (history.length > 0) {
            setMessages(history.map((message, index) => ({
              id: `history-${index}`,
              role: message.role,
              text: message.text,
              timestamp: parseTimestamp(message.timestamp),
            })));
          }
        } catch (error: any) {
          if (cancelled) {
            return;
          }
          setMessages(prev => {
            const base = prev.length > 0 ? prev : initMessages('welcome-history-error');
            return [
              ...base,
              {
                id: `history-error-${Date.now()}`,
                role: 'assistant',
                text: ru
                  ? `Не удалось загрузить историю помощника.\n\n${String(error?.message || 'Неизвестная ошибка.')}`
                  : `I couldn't load the helper history.\n\n${String(error?.message || 'Unknown error.')}`,
                timestamp: new Date(),
              },
            ];
          });
        } finally {
          if (!cancelled) {
            setHistoryLoaded(true);
          }
        }
      })();
      return () => {
        cancelled = true;
        historyRequestedRef.current = false;
      };
    }
    const timer = window.setTimeout(() => inputRef.current?.focus(), 150);
    return () => window.clearTimeout(timer);
  }, [desktopOpen, mobileOpen, historyLoaded, messages.length, ru, sessionId]);

  useEffect(() => {
    bottomRef.current?.scrollIntoView({ behavior: 'smooth' });
  }, [messages, typing, resetting]);

  const sendMessage = async (text: string) => {
    const trimmed = text.trim();
    if (!trimmed || remainingChars < 0 || busy) return;
    setMessages(prev => [...prev, { id: Date.now().toString(), role: 'user', text: trimmed, timestamp: new Date() }]);
    setInput('');
    setTyping(true);
    try {
      const response = await aiHelperChat(trimmed, sessionId);
      setMessages(prev => [...prev, {
        id: (Date.now() + 1).toString(),
        role: 'assistant',
        text: String(response?.text || (ru ? 'Не удалось получить ответ.' : 'No answer was returned.')),
        timestamp: parseTimestamp(response?.timestamp),
      }]);
      setHistoryLoaded(true);
    } catch (error: any) {
      setMessages(prev => [...prev, {
        id: (Date.now() + 1).toString(),
        role: 'assistant',
        text: ru
          ? `Не удалось получить ответ от помощника.\n\n${String(error?.message || 'Неизвестная ошибка.')}`
          : `I couldn't reach the AI helper.\n\n${String(error?.message || 'Unknown error.')}`,
        timestamp: new Date(),
      }]);
    } finally {
      setTyping(false);
    }
  };

  const handleReset = async () => {
    if (resetting) {
      return;
    }
    setResetting(true);
    try {
      await resetAiHelper(sessionId);
      const nextSessionId = generateAiChatSessionId();
      persistAiChatSessionId(nextSessionId);
      historyRequestedRef.current = false;
      setSessionId(nextSessionId);
      setMessages(initMessages('welcome-reset'));
      setHistoryLoaded(false);
    } catch (error: any) {
      setMessages(prev => [
        ...prev,
        {
          id: `reset-error-${Date.now()}`,
          role: 'assistant',
          text: ru
            ? `Не удалось сбросить сессию помощника.\n\n${String(error?.message || 'Неизвестная ошибка.')}`
            : `I couldn't reset the helper session.\n\n${String(error?.message || 'Unknown error.')}`,
          timestamp: new Date(),
        },
      ]);
    }
    setInput('');
    setResetting(false);
  };

  const springConfig = { type: 'spring', damping: 30, stiffness: 300 };

  const chatProps = {
    messages, typing, suggested, input, setInput,
    onSend: sendMessage, onReset: handleReset, ru, inputRef, bottomRef,
    remainingChars, busy,
  };

  if (location.pathname.startsWith('/graph')) {
    return null;
  }

  return (
    <>
      {/* ── DESKTOP FLOATING BUTTON (hidden on mobile) ── */}
      <button
        onClick={() => setDesktopOpen(o => !o)}
        className="hidden md:flex fixed bottom-6 right-6 z-50 w-14 h-14 rounded-2xl shadow-xl items-center justify-center transition-all duration-200 hover:scale-105 active:scale-95"
        style={{
          background: desktopOpen
            ? 'linear-gradient(135deg, #7c3aed, #6d28d9)'
            : 'linear-gradient(135deg, #8b5cf6, #7c3aed)',
          boxShadow: '0 8px 32px rgba(139,92,246,0.45), 0 2px 8px rgba(0,0,0,0.15)',
        }}
        title={ru ? 'ИИ-ассистент' : 'AI Assistant'}
      >
        {desktopOpen
          ? <ChevronDown className="w-6 h-6 text-white" />
          : <Sparkles className="w-6 h-6 text-white" />
        }
        {!desktopOpen && (
          <span className="absolute inset-0 rounded-2xl animate-ping opacity-20"
            style={{ background: 'linear-gradient(135deg, #8b5cf6, #7c3aed)' }} />
        )}
      </button>

      {/* ── DESKTOP CHAT PANEL ── */}
      <AnimatePresence>
        {desktopOpen && (
          <motion.div
            initial={{ opacity: 0, y: 16, scale: 0.97 }}
            animate={{ opacity: 1, y: 0, scale: 1 }}
            exit={{ opacity: 0, y: 16, scale: 0.97 }}
            transition={{ type: 'spring', damping: 25, stiffness: 280 }}
            className="hidden md:flex fixed bottom-24 right-6 z-50 w-[380px] rounded-2xl overflow-hidden flex-col"
            style={{
              height: '520px',
              boxShadow: '0 24px 64px rgba(139,92,246,0.22), 0 4px 24px rgba(0,0,0,0.12)',
              border: '1px solid rgba(139,92,246,0.18)',
            }}
          >
            <ChatContent {...chatProps} onClose={() => setDesktopOpen(false)} />
          </motion.div>
        )}
      </AnimatePresence>

      {/* ── MOBILE CHAT BOTTOM SHEET ── */}
      <AnimatePresence>
        {mobileOpen && (
          <>
            <motion.div
              className="md:hidden fixed inset-0 z-40 bg-black/50 backdrop-blur-sm"
              initial={{ opacity: 0 }} animate={{ opacity: 1 }} exit={{ opacity: 0 }}
              transition={{ duration: 0.2 }}
              onClick={onMobileClose}
            />
            <motion.div
              className="md:hidden fixed inset-x-0 bottom-0 z-50 rounded-t-2xl overflow-hidden flex flex-col"
              style={{
                top: '64px', // below mobile top bar
                background: 'white',
                boxShadow: '0 -8px 40px rgba(139,92,246,0.18)',
              }}
              initial={{ y: '100%' }} animate={{ y: 0 }} exit={{ y: '100%' }}
              transition={springConfig}
            >
              {/* Drag handle */}
              <div className="pt-2.5 pb-1 flex justify-center flex-shrink-0">
                <div className="w-10 h-1 bg-gray-300 rounded-full" />
              </div>
              <ChatContent {...chatProps} onClose={onMobileClose!} />
            </motion.div>
          </>
        )}
      </AnimatePresence>
    </>
  );
}
