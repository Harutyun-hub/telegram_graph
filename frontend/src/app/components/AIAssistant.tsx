import { useState, useRef, useEffect } from 'react';
import { Sparkles, X, Send, RotateCcw, ChevronDown } from 'lucide-react';
import { motion, AnimatePresence } from 'motion/react';
import { useLanguage } from '../contexts/LanguageContext';

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

const MOCK_RESPONSES_EN: Record<string, string> = {
  default: `Based on the current data from **14,238 analyzed messages**, here's what stands out:\n\n• **Housing affordability** remains the #1 topic (+89% spike)\n• **Education inquiries** are up 125% — mostly international schools\n• **New member onboarding** questions repeat daily across 3 channels\n\nWould you like a deeper breakdown of any of these areas?`,
  housing: `**Housing Sentiment Analysis** (last 7 days)\n\nOverall tone: 62% frustrated, 28% neutral, 10% positive\n\nTop pain points:\n1. Rental prices up 30% YoY near Cascade district\n2. Short-term vs long-term lease confusion\n3. Deposit disputes with landlords\n\nRecommendation: Pin a "Tenant Rights in Armenia" guide — it would reduce 40+ daily repetitive questions.`,
  topics: `**Topics Requiring Urgent Attention** (by volume + urgency score)\n\n🔴 **Housing crisis** — 342 mentions, frustration index 8.2/10\n🟡 **Banking access** — 156 mentions, confusion index 7.8/10\n🟡 **School enrollment deadlines** — 89 mentions, anxiety index 7.1/10\n\nThe banking cluster has grown 95% this week — likely triggered by new Ameriabank policy changes.`,
  voices: `**Key Community Voices** (by influence + helpfulness)\n\n1. **Алексей (IT_Alex_AM)** — Help score 95/100, 28 posts/week\n   Expertise: Tax optimization, IT jobs, coworking\n\n2. **Марина (Marina_Yerevan)** — Help score 92/100, 22 posts/week\n   Expertise: Schools, kids activities, pediatricians\n\n3. **Дима (relocate_dm)** — Help score 88/100, 35 posts/week\n   Expertise: Documents, banking, apartment hunting\n\nConsider featuring these members in a "Community Experts" pinned post.`,
};

const MOCK_RESPONSES_RU: Record<string, string> = {
  default: `На основе текущих данных из **14 238 проанализированных сообщений**, вот ключевые выводы:\n\n• **Доступность жилья** остаётся темой №1 (рост +89%)\n• **Запросы по образованию** выросли на 125% — преимущественно международные школы\n• **Вопросы новичков** повторяются ежедневно в 3 каналах\n\nХотите более детальный анализ по любой из этих тем?`,
  housing: `**Анализ настроений по жилью** (последние 7 дней)\n\nОбщий тон: 62% раздражённые, 28% нейтральные, 10% позитивные\n\nГлавные проблемы:\n1. Аренда выросла на 30% г/г в районе Каскада\n2. Путаница с краткосрочной и долгосрочной арендой\n3. Споры с арендодателями по залогу\n\nРекомендация: Закрепите руководство «Права арендатора в Армении» — это сократит 40+ ежедневных повторных вопросов.`,
  topics: `**Темы, требующие срочного внимания** (по объёму + индексу срочности)\n\n🔴 **Жилищный кризис** — 342 упоминания, индекс раздражённости 8.2/10\n🟡 **Доступ к банкам** — 156 упоминаний, индекс растерянности 7.8/10\n🟡 **Дедлайны зачисления в школы** — 89 упоминаний, индекс тревожности 7.1/10\n\nБанковский кластер вырос на 95% за неделю — вероятно, из-за изменений в политике Ameriabank.`,
  voices: `**Ключевые голоса сообщества** (по влиятельности + полезности)\n\n1. **Алексей (IT_Alex_AM)** — рейтинг помощи 95/100, 28 публ./нед.\n   Экспертиза: налоговая оптимизация, IT-вакансии, коворкинги\n\n2. **Марина (Marina_Yerevan)** — рейтинг помощи 92/100, 22 публ./нед.\n   Экспертиза: школы, детский досуг, педиатры\n\n3. **Дима (relocate_dm)** — рейтинг помощи 88/100, 35 публ./нед.\n   Экспертиза: документы, банки, поиск жилья\n\nРассмотрите возможность создания закреплённого поста «Эксперты сообщества» с этими участниками.`,
};

function getMockResponse(text: string, ru: boolean): string {
  const lower = text.toLowerCase();
  const responses = ru ? MOCK_RESPONSES_RU : MOCK_RESPONSES_EN;
  if (lower.includes('hous') || lower.includes('жил') || lower.includes('аренд')) return responses.housing;
  if (lower.includes('topic') || lower.includes('тем') || lower.includes('urgent') || lower.includes('сроч')) return responses.topics;
  if (lower.includes('voice') || lower.includes('голос') || lower.includes('influenc') || lower.includes('влиятел')) return responses.voices;
  return responses.default;
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
}: {
  messages: Message[]; typing: boolean; suggested: string[];
  input: string; setInput: (v: string) => void;
  onSend: (text: string) => void; onReset: () => void; onClose: () => void;
  ru: boolean; inputRef: React.RefObject<HTMLInputElement | null>; bottomRef: React.RefObject<HTMLDivElement | null>;
}) {
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
              <p className="text-purple-200 text-xs">{ru ? 'GPT-4o-mini · онлайн' : 'GPT-4o-mini · online'}</p>
            </div>
          </div>
        </div>
        <div className="flex items-center gap-1">
          <button onClick={onReset}
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
          <input ref={inputRef} type="text" value={input} onChange={e => setInput(e.target.value)}
            onKeyDown={e => { if (e.key === 'Enter' && !e.shiftKey) { e.preventDefault(); onSend(input); } }}
            placeholder={ru ? 'Спросите о трендах, темах...' : 'Ask about trends, topics...'}
            className="flex-1 bg-transparent text-sm text-gray-800 placeholder-gray-400 focus:outline-none" />
          <button onClick={() => onSend(input)} disabled={!input.trim()}
            className="w-8 h-8 rounded-lg flex items-center justify-center transition-all disabled:opacity-30"
            style={{ background: input.trim() ? 'linear-gradient(135deg, #8b5cf6, #7c3aed)' : '#e5e7eb' }}>
            <Send className="w-3.5 h-3.5 text-white" />
          </button>
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
  const { lang } = useLanguage();
  const ru = lang === 'ru';
  const [desktopOpen, setDesktopOpen] = useState(false);
  const [input, setInput] = useState('');
  const [messages, setMessages] = useState<Message[]>([]);
  const [typing, setTyping] = useState(false);
  const bottomRef = useRef<HTMLDivElement>(null);
  const inputRef = useRef<HTMLInputElement>(null);

  const suggested = ru ? SUGGESTED_PROMPTS_RU : SUGGESTED_PROMPTS_EN;

  const initMessages = (resetId: string) => [{
    id: resetId,
    role: 'assistant' as const,
    text: ru
      ? 'Привет! Я ваш ИИ-аналитик армянского сообщества. Спросите меня о трендах, настроениях, ключевых темах или участниках.'
      : "Hi! I'm your Armenian community AI analyst. Ask me about trends, sentiment, key topics, or community voices.",
    timestamp: new Date(),
  }];

  // Initialize when any panel opens
  useEffect(() => {
    if ((desktopOpen || mobileOpen) && messages.length === 0) {
      setMessages(initMessages('welcome'));
    }
    if (desktopOpen || mobileOpen) {
      setTimeout(() => inputRef.current?.focus(), 150);
    }
  }, [desktopOpen, mobileOpen, messages.length]);

  useEffect(() => {
    bottomRef.current?.scrollIntoView({ behavior: 'smooth' });
  }, [messages, typing]);

  const sendMessage = async (text: string) => {
    if (!text.trim()) return;
    setMessages(prev => [...prev, { id: Date.now().toString(), role: 'user', text: text.trim(), timestamp: new Date() }]);
    setInput('');
    setTyping(true);
    await new Promise(r => setTimeout(r, 900 + Math.random() * 600));
    setTyping(false);
    setMessages(prev => [...prev, {
      id: (Date.now() + 1).toString(),
      role: 'assistant',
      text: getMockResponse(text, ru),
      timestamp: new Date(),
    }]);
  };

  const handleReset = () => {
    setMessages(initMessages('welcome-reset'));
    setInput('');
  };

  const springConfig = { type: 'spring', damping: 30, stiffness: 300 };

  const chatProps = {
    messages, typing, suggested, input, setInput,
    onSend: sendMessage, onReset: handleReset, ru, inputRef, bottomRef,
  };

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