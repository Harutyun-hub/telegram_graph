import { useEffect, useMemo, useState } from 'react';
import { Sparkles, ChevronRight, MessageSquare, Info, Loader2, Send, Bot } from 'lucide-react';
import { askAI, type AIClientFilters, type AIEvidenceItem, type AIDataScope } from '@/app/graph/services/api';
import { NodeInspector } from '@/app/graph/components/NodeInspector';

type SidebarTab = 'chat' | 'inspector';

interface ChatMessage {
  id: string;
  role: 'user' | 'assistant';
  text: string;
  timestamp: string;
  confidence?: number;
  evidence?: AIEvidenceItem[];
  applied?: boolean;
  responseMode?: 'aura' | 'gemini' | 'fallback';
  model?: string;
  intent?: string;
  runtimeNote?: string;
  dataScope?: AIDataScope;
}

interface AISidebarProps {
  filters?: AIClientFilters;
  onApplyFilters: (filters: AIClientFilters) => void;
  selectedNode?: any;
  isCollapsed?: boolean;
  onCollapsedChange?: (collapsed: boolean) => void;
  onCloseInspector: () => void;
}

export function AISidebar({
  filters,
  onApplyFilters,
  selectedNode,
  isCollapsed: externalCollapsed,
  onCollapsedChange,
  onCloseInspector,
}: AISidebarProps) {
  const [internalCollapsed, setInternalCollapsed] = useState(true);
  const [activeTab, setActiveTab] = useState<SidebarTab>('chat');
  const [messages, setMessages] = useState<ChatMessage[]>([]);
  const [query, setQuery] = useState('');
  const [loading, setLoading] = useState(false);

  const messageCount = messages.length;

  const placeholder = useMemo(() => {
    return "Ask anything like: 'Who is talking about cashback most?'";
  }, []);

  const isCollapsed = externalCollapsed ?? internalCollapsed;

  useEffect(() => {
    if (externalCollapsed == null) return;
    setInternalCollapsed(externalCollapsed);
  }, [externalCollapsed]);

  const setCollapsed = (collapsed: boolean) => {
    if (externalCollapsed == null) {
      setInternalCollapsed(collapsed);
    }
    onCollapsedChange?.(collapsed);
  };

  const submitQuery = async (e: React.FormEvent) => {
    e.preventDefault();
    const trimmed = query.trim();
    if (!trimmed || loading) return;

    const userMessage: ChatMessage = {
      id: `user-${Date.now()}`,
      role: 'user',
      text: trimmed,
      timestamp: new Date().toISOString(),
    };
    setMessages((prev) => [...prev, userMessage]);
    setQuery('');
    setLoading(true);

    try {
      const response = await askAI(trimmed, { filters });
      if (response.graphInstruction?.mode === 'filter_patch') {
        onApplyFilters(response.graphInstruction.filters);
      }

      const assistantMessage: ChatMessage = {
        id: `assistant-${Date.now()}`,
        role: 'assistant',
        text: response.answer,
        timestamp: response.timestamp,
        confidence: response.confidence,
        evidence: response.evidence,
        applied: Boolean(response.graphInstruction),
        responseMode: response.responseMode,
        model: response.model,
        intent: response.intent,
        runtimeNote: response.runtimeNote,
        dataScope: response.dataScope,
      };
      setMessages((prev) => [...prev, assistantMessage]);
    } catch (error: any) {
      const assistantMessage: ChatMessage = {
        id: `assistant-error-${Date.now()}`,
        role: 'assistant',
        text: error?.message || 'Failed to answer this question right now.',
        timestamp: new Date().toISOString(),
      };
      setMessages((prev) => [...prev, assistantMessage]);
    } finally {
      setLoading(false);
    }
  };

  return (
    <div
      className={`absolute right-4 top-4 bottom-4 bg-slate-950/40 backdrop-blur-xl border border-white/10 rounded-2xl shadow-2xl flex flex-col z-40 overflow-hidden transition-all duration-300 ${
        isCollapsed ? 'w-14' : 'w-[296px]'
      }`}
    >
      {!isCollapsed && (
        <button
          onClick={() => setCollapsed(true)}
          className="absolute top-4 right-4 w-8 h-8 rounded-lg bg-white/5 hover:bg-white/10 border border-white/10 flex items-center justify-center transition-colors z-50"
          title="Collapse AI assistant"
        >
          <ChevronRight className="w-4 h-4 text-white/70" />
        </button>
      )}

      {isCollapsed ? (
        <div className="flex flex-col items-center justify-center h-full gap-4 py-6">
          <button
            onClick={() => setCollapsed(false)}
            className="w-10 h-10 rounded-lg bg-cyan-500/20 hover:bg-cyan-500/30 border border-cyan-500/30 flex items-center justify-center transition-colors"
            title="Expand AI assistant"
          >
            <Sparkles className="w-5 h-5 text-cyan-400" />
          </button>
          {messageCount > 0 && (
            <div className="w-6 h-6 rounded-full bg-cyan-500 flex items-center justify-center">
              <span className="text-white text-xs font-bold">{messageCount}</span>
            </div>
          )}
        </div>
      ) : (
        <>
          <div className="px-6 py-4 border-b border-white/10">
            <div className="flex items-center gap-2">
              <Sparkles className="w-5 h-5 text-cyan-400" />
              <h2 className="text-white/90 font-semibold">AI Copilot</h2>
            </div>
            <p className="text-white/50 text-xs mt-1">
              Ask questions and get graph-backed answers
            </p>
          </div>

          <div className="px-3 py-3 border-b border-white/10 flex gap-2">
            <button
              onClick={() => setActiveTab('chat')}
              className={`flex-1 px-3 py-2 rounded-lg text-xs font-medium border transition-colors ${
                activeTab === 'chat'
                  ? 'bg-cyan-500/20 border-cyan-500/40 text-cyan-200'
                  : 'bg-white/5 border-white/10 text-white/60 hover:bg-white/10'
              }`}
            >
              <span className="inline-flex items-center gap-1.5">
                <MessageSquare className="w-3.5 h-3.5" />
                AI Chat
              </span>
            </button>
            <button
              onClick={() => setActiveTab('inspector')}
              className={`flex-1 px-3 py-2 rounded-lg text-xs font-medium border transition-colors ${
                activeTab === 'inspector'
                  ? 'bg-cyan-500/20 border-cyan-500/40 text-cyan-200'
                  : 'bg-white/5 border-white/10 text-white/60 hover:bg-white/10'
              }`}
            >
              <span className="inline-flex items-center gap-1.5">
                <Info className="w-3.5 h-3.5" />
                Inspector
              </span>
            </button>
          </div>

          {activeTab === 'chat' ? (
            <>
              <div className="flex-1 overflow-y-auto px-4 py-4 space-y-3">
                {messages.length === 0 ? (
                  <div className="bg-white/5 border border-white/10 rounded-xl p-4">
                    <p className="text-white/70 text-sm leading-relaxed">
                      {placeholder}
                    </p>
                  </div>
                ) : (
                  messages.map((message) => (
                    <div
                      key={message.id}
                      className={`rounded-xl border p-3 ${
                        message.role === 'user'
                          ? 'bg-cyan-500/10 border-cyan-500/30'
                          : 'bg-white/5 border-white/10'
                      }`}
                    >
                      <div className="flex items-center justify-between gap-2 mb-2">
                        <span className={`text-[11px] font-medium uppercase tracking-wide ${message.role === 'user' ? 'text-cyan-300' : 'text-white/60'}`}>
                          {message.role === 'user' ? 'You' : 'AI Graph Expert'}
                        </span>
                        <div className="flex items-center gap-1.5 flex-wrap justify-end">
                          {message.responseMode && (
                            <span className={`text-[10px] px-2 py-0.5 rounded-full border ${
                              message.responseMode === 'aura'
                                ? 'bg-cyan-500/20 border-cyan-500/30 text-cyan-200'
                                : message.responseMode === 'gemini'
                                  ? 'bg-indigo-500/20 border-indigo-500/30 text-indigo-200'
                                  : 'bg-amber-500/20 border-amber-500/30 text-amber-200'
                            }`}>
                              {message.responseMode === 'aura'
                                ? 'Aura Agent'
                                : message.responseMode === 'gemini'
                                  ? (message.model || 'Gemini')
                                  : 'Fallback'}
                            </span>
                          )}
                          {message.confidence != null && (
                            <span className="text-[10px] px-2 py-0.5 rounded-full bg-emerald-500/20 border border-emerald-500/30 text-emerald-200">
                              Confidence {Math.round(message.confidence)}%
                            </span>
                          )}
                          {message.dataScope && (
                            <span className={`text-[10px] px-2 py-0.5 rounded-full border ${
                              message.dataScope === 'full_db'
                                ? 'bg-sky-500/20 border-sky-500/30 text-sky-200'
                                : 'bg-white/10 border-white/20 text-white/75'
                            }`}>
                              {message.dataScope === 'full_db' ? 'Scope: Full DB' : 'Scope: Current View'}
                            </span>
                          )}
                        </div>
                      </div>
                      <p className="text-sm text-white/85 leading-relaxed whitespace-pre-wrap">{message.text}</p>
                      {message.evidence && message.evidence.length > 0 && (
                        <div className="mt-3 space-y-1.5">
                          {message.evidence.map((item, idx) => (
                            <div key={`${message.id}-e-${idx}`} className="text-[11px] text-white/65">
                              <span className="text-cyan-300">{item.label}:</span> {item.value}
                            </div>
                          ))}
                        </div>
                      )}
                      {message.runtimeNote && (
                        <div className="mt-2 text-[10px] text-amber-200/90 bg-amber-500/10 border border-amber-500/30 rounded-md px-2 py-1.5">
                          Runtime note: {message.runtimeNote}
                        </div>
                      )}
                      {message.applied && (
                        <div className="mt-3 text-[10px] text-cyan-200 bg-cyan-500/15 border border-cyan-500/30 rounded-md px-2 py-1 inline-flex items-center gap-1.5">
                          <Bot className="w-3 h-3" />
                          Applied to graph view
                        </div>
                      )}
                    </div>
                  ))
                )}
              </div>

              <form onSubmit={submitQuery} className="border-t border-white/10 px-4 py-3">
                <div className="flex items-center gap-2">
                  <input
                    value={query}
                    onChange={(e) => setQuery(e.target.value)}
                    placeholder="Ask the graph expert..."
                    className="flex-1 px-3 py-2.5 rounded-lg bg-white/5 border border-white/10 text-sm text-white/90 placeholder:text-white/40 focus:outline-none focus:border-cyan-500/40"
                    disabled={loading}
                  />
                  <button
                    type="submit"
                    disabled={loading || !query.trim()}
                    className="w-10 h-10 rounded-lg bg-cyan-500/20 hover:bg-cyan-500/30 border border-cyan-500/30 disabled:opacity-50 disabled:cursor-not-allowed flex items-center justify-center"
                  >
                    {loading ? <Loader2 className="w-4 h-4 text-cyan-300 animate-spin" /> : <Send className="w-4 h-4 text-cyan-300" />}
                  </button>
                </div>
              </form>
            </>
          ) : (
            <div className="flex-1 min-h-0">
              {selectedNode ? (
                <NodeInspector node={selectedNode} filters={filters} onClose={onCloseInspector} embedded />
              ) : (
                <div className="h-full px-5 py-5 text-white/60 text-sm leading-relaxed">
                  Select a node in the graph to inspect details.
                </div>
              )}
            </div>
          )}
        </>
      )}
    </div>
  );
}
