import { useState, useRef, useEffect, useCallback } from 'react';
import { useLanguage } from '../contexts/LanguageContext';
import {
  Sparkles, Plus, Trash2, Upload, Link2, Send, FileText,
  Loader2, AlertCircle, CheckCircle2, ChevronDown, X,
  BookOpen, MessageSquare, FolderOpen,
} from 'lucide-react';
import {
  kbListCollections, kbCreateCollection, kbDeleteCollection,
  kbUploadDocument, kbAddUrl, kbListDocuments, kbDeleteDocument, kbAsk,
  type KBCollection, type KBDocument, type KBAskResult,
} from '../services/api';

// ─── Types ────────────────────────────────────────────────────────────────────

interface ChatMessage {
  id: string;
  role: 'user' | 'assistant';
  content: string;
  citations?: KBAskResult['citations'];
  confidence?: KBAskResult['confidence'];
  caveat?: string;
  loading?: boolean;
}

// ─── i18n ─────────────────────────────────────────────────────────────────────

const T = {
  title:         { en: 'AI Agent', ru: 'ИИ Агент' },
  subtitle:      { en: 'Knowledge Base', ru: 'База знаний' },
  newCollection: { en: 'New Collection', ru: 'Новая база' },
  collections:   { en: 'Collections', ru: 'Коллекции' },
  noCollections: { en: 'No collections yet', ru: 'Нет коллекций' },
  createFirst:   { en: 'Create your first collection to get started.', ru: 'Создайте первую коллекцию.' },
  collName:      { en: 'Collection name', ru: 'Название коллекции' },
  collDesc:      { en: 'Description (optional)', ru: 'Описание (опционально)' },
  create:        { en: 'Create', ru: 'Создать' },
  cancel:        { en: 'Cancel', ru: 'Отмена' },
  delete:        { en: 'Delete', ru: 'Удалить' },
  documents:     { en: 'Documents', ru: 'Документы' },
  noDocuments:   { en: 'No documents yet', ru: 'Нет документов' },
  uploadHint:    { en: 'Upload PDF, DOCX, TXT or add a URL', ru: 'Загрузите PDF, DOCX, TXT или добавьте URL' },
  addUrl:        { en: 'Add URL', ru: 'Добавить URL' },
  urlPlaceholder:{ en: 'https://...', ru: 'https://...' },
  add:           { en: 'Add', ru: 'Добавить' },
  chat:          { en: 'Ask your documents', ru: 'Спросить базу знаний' },
  placeholder:   { en: 'Ask a question about your documents…', ru: 'Задайте вопрос по вашим документам…' },
  selectCollection:{ en: 'Select a collection to start chatting', ru: 'Выберите коллекцию для начала' },
  sources:       { en: 'Sources', ru: 'Источники' },
  confidence:    { en: 'Confidence', ru: 'Уверенность' },
  uploading:     { en: 'Uploading…', ru: 'Загрузка…' },
  indexing:      { en: 'Indexing…', ru: 'Индексирование…' },
  indexed:       { en: 'Indexed', ru: 'Проиндексировано' },
  chunks:        { en: 'chunks', ru: 'чанков' },
  errorUpload:   { en: 'Upload failed', ru: 'Ошибка загрузки' },
  confirmDelete: { en: 'Delete this collection and all its documents?', ru: 'Удалить коллекцию и все документы?' },
  selectHint:    { en: 'Select or create a collection on the left to start.', ru: 'Выберите или создайте коллекцию слева.' },
} as const;

function t(key: keyof typeof T, ru: boolean): string {
  return T[key][ru ? 'ru' : 'en'];
}

// ─── Sub-components ───────────────────────────────────────────────────────────

function ConfidencePill({ level }: { level: KBAskResult['confidence'] }) {
  const colors: Record<string, string> = {
    high: 'bg-emerald-100 text-emerald-700',
    medium: 'bg-amber-100 text-amber-700',
    low_confidence: 'bg-red-100 text-red-600',
  };
  return (
    <span className={`inline-flex items-center gap-1 text-xs font-medium px-2 py-0.5 rounded-full ${colors[level] ?? colors.medium}`}>
      {level === 'high' && <CheckCircle2 className="w-3 h-3" />}
      {level === 'low_confidence' && <AlertCircle className="w-3 h-3" />}
      {level}
    </span>
  );
}

// ─── Main Page ────────────────────────────────────────────────────────────────

export function AgentPage() {
  const { lang } = useLanguage();
  const ru = lang === 'ru';

  // Collections
  const [collections, setCollections] = useState<KBCollection[]>([]);
  const [collectionsLoading, setCollectionsLoading] = useState(true);
  const [selectedCollection, setSelectedCollection] = useState<string | null>(null);
  const [showNewForm, setShowNewForm] = useState(false);
  const [newName, setNewName] = useState('');
  const [newDesc, setNewDesc] = useState('');
  const [creating, setCreating] = useState(false);

  // Documents
  const [documents, setDocuments] = useState<KBDocument[]>([]);
  const [docsLoading, setDocsLoading] = useState(false);
  const [uploadStatus, setUploadStatus] = useState<Record<string, string>>({});

  // URL ingestion
  const [urlValue, setUrlValue] = useState('');
  const [addingUrl, setAddingUrl] = useState(false);

  // Chat
  const [messages, setMessages] = useState<ChatMessage[]>([]);
  const [input, setInput] = useState('');
  const [asking, setAsking] = useState(false);
  const chatEndRef = useRef<HTMLDivElement>(null);
  const fileInputRef = useRef<HTMLInputElement>(null);

  // ── Load collections ──────────────────────────────────────────────
  const loadCollections = useCallback(async () => {
    setCollectionsLoading(true);
    try {
      const { collections: cols } = await kbListCollections();
      setCollections(cols);
    } catch {
      setCollections([]);
    } finally {
      setCollectionsLoading(false);
    }
  }, []);

  useEffect(() => { loadCollections(); }, [loadCollections]);

  // ── Load documents when collection changes ────────────────────────
  useEffect(() => {
    if (!selectedCollection) { setDocuments([]); return; }
    setDocsLoading(true);
    kbListDocuments(selectedCollection)
      .then(({ documents: docs }) => setDocuments(docs))
      .catch(() => setDocuments([]))
      .finally(() => setDocsLoading(false));
  }, [selectedCollection]);

  // ── Auto-scroll chat ──────────────────────────────────────────────
  useEffect(() => {
    chatEndRef.current?.scrollIntoView({ behavior: 'smooth' });
  }, [messages]);

  // ── Create collection ─────────────────────────────────────────────
  async function handleCreateCollection(e: React.FormEvent) {
    e.preventDefault();
    if (!newName.trim()) return;
    setCreating(true);
    try {
      await kbCreateCollection(newName.trim(), newDesc.trim());
      setNewName(''); setNewDesc(''); setShowNewForm(false);
      await loadCollections();
      setSelectedCollection(newName.trim());
    } catch (err: any) {
      alert(err.message);
    } finally {
      setCreating(false);
    }
  }

  // ── Delete collection ─────────────────────────────────────────────
  async function handleDeleteCollection(name: string) {
    if (!window.confirm(t('confirmDelete', ru))) return;
    await kbDeleteCollection(name);
    if (selectedCollection === name) { setSelectedCollection(null); setMessages([]); }
    await loadCollections();
  }

  // ── File upload ───────────────────────────────────────────────────
  async function handleFileUpload(files: FileList | null) {
    if (!files || !selectedCollection) return;
    for (const file of Array.from(files)) {
      const key = `${file.name}-${Date.now()}`;
      setUploadStatus(s => ({ ...s, [key]: 'uploading' }));
      try {
        const result = await kbUploadDocument(selectedCollection, file);
        setUploadStatus(s => ({ ...s, [key]: `indexed:${result.chunk_count}` }));
        const { documents: docs } = await kbListDocuments(selectedCollection);
        setDocuments(docs);
        await loadCollections();
      } catch (err: any) {
        setUploadStatus(s => ({ ...s, [key]: `error:${err.message}` }));
      }
    }
  }

  // ── Add URL ───────────────────────────────────────────────────────
  async function handleAddUrl(e: React.FormEvent) {
    e.preventDefault();
    if (!urlValue.trim() || !selectedCollection) return;
    setAddingUrl(true);
    try {
      await kbAddUrl(selectedCollection, urlValue.trim());
      setUrlValue('');
      const { documents: docs } = await kbListDocuments(selectedCollection);
      setDocuments(docs);
      await loadCollections();
    } catch (err: any) {
      alert(err.message);
    } finally {
      setAddingUrl(false);
    }
  }

  // ── Delete document ───────────────────────────────────────────────
  async function handleDeleteDoc(doc: KBDocument) {
    if (!selectedCollection) return;
    await kbDeleteDocument(selectedCollection, doc.doc_id);
    setDocuments(d => d.filter(x => x.doc_id !== doc.doc_id));
    await loadCollections();
  }

  // ── Ask question ──────────────────────────────────────────────────
  async function handleAsk(e: React.FormEvent) {
    e.preventDefault();
    if (!input.trim() || !selectedCollection || asking) return;
    const question = input.trim();
    setInput('');
    const userMsg: ChatMessage = { id: Date.now().toString(), role: 'user', content: question };
    const loadingMsg: ChatMessage = { id: `${Date.now()}-a`, role: 'assistant', content: '', loading: true };
    setMessages(m => [...m, userMsg, loadingMsg]);
    setAsking(true);
    try {
      const result = await kbAsk(selectedCollection, question);
      setMessages(m => m.map(msg =>
        msg.id === loadingMsg.id
          ? { ...msg, content: result.answer, citations: result.citations,
              confidence: result.confidence, caveat: result.caveat, loading: false }
          : msg
      ));
    } catch (err: any) {
      setMessages(m => m.map(msg =>
        msg.id === loadingMsg.id
          ? { ...msg, content: `Error: ${err.message}`, loading: false }
          : msg
      ));
    } finally {
      setAsking(false);
    }
  }

  // ─── Render ───────────────────────────────────────────────────────────────

  return (
    <div className="flex flex-col h-full bg-gray-50 overflow-hidden">
      {/* Header */}
      <div className="flex items-center justify-between px-6 py-4 bg-white border-b border-gray-200 shrink-0">
        <div className="flex items-center gap-3">
          <div className="w-8 h-8 rounded-lg bg-indigo-600 flex items-center justify-center">
            <Sparkles className="w-4 h-4 text-white" />
          </div>
          <div>
            <h1 className="text-lg font-semibold text-gray-900">{t('title', ru)}</h1>
            <p className="text-xs text-gray-500">{t('subtitle', ru)}</p>
          </div>
        </div>
      </div>

      {/* Three-panel layout */}
      <div className="flex flex-1 overflow-hidden">

        {/* ── LEFT: Collections sidebar ── */}
        <aside className="w-56 shrink-0 bg-white border-r border-gray-200 flex flex-col overflow-hidden">
          <div className="flex items-center justify-between px-4 py-3 border-b border-gray-100">
            <span className="text-xs font-semibold text-gray-500 uppercase tracking-wider">
              {t('collections', ru)}
            </span>
            <button
              onClick={() => setShowNewForm(v => !v)}
              className="w-6 h-6 flex items-center justify-center rounded-md text-gray-400 hover:bg-gray-100 hover:text-indigo-600 transition-colors"
              title={t('newCollection', ru)}
            >
              <Plus className="w-4 h-4" />
            </button>
          </div>

          {/* New collection form */}
          {showNewForm && (
            <form onSubmit={handleCreateCollection} className="p-3 border-b border-gray-100 space-y-2 bg-indigo-50">
              <input
                autoFocus
                value={newName}
                onChange={e => setNewName(e.target.value)}
                placeholder={t('collName', ru)}
                className="w-full text-sm px-2 py-1.5 border border-gray-300 rounded-md focus:outline-none focus:ring-1 focus:ring-indigo-500"
                maxLength={80}
              />
              <input
                value={newDesc}
                onChange={e => setNewDesc(e.target.value)}
                placeholder={t('collDesc', ru)}
                className="w-full text-sm px-2 py-1.5 border border-gray-300 rounded-md focus:outline-none focus:ring-1 focus:ring-indigo-500"
                maxLength={300}
              />
              <div className="flex gap-2">
                <button
                  type="submit"
                  disabled={creating || !newName.trim()}
                  className="flex-1 text-xs font-medium px-2 py-1.5 bg-indigo-600 text-white rounded-md hover:bg-indigo-700 disabled:opacity-50 transition-colors"
                >
                  {creating ? <Loader2 className="w-3 h-3 animate-spin mx-auto" /> : t('create', ru)}
                </button>
                <button
                  type="button"
                  onClick={() => setShowNewForm(false)}
                  className="text-xs px-2 py-1.5 text-gray-600 hover:bg-gray-200 rounded-md transition-colors"
                >
                  {t('cancel', ru)}
                </button>
              </div>
            </form>
          )}

          {/* Collection list */}
          <div className="flex-1 overflow-y-auto py-2">
            {collectionsLoading ? (
              <div className="flex justify-center py-6">
                <Loader2 className="w-5 h-5 text-gray-300 animate-spin" />
              </div>
            ) : collections.length === 0 ? (
              <div className="px-4 py-6 text-center">
                <BookOpen className="w-7 h-7 text-gray-300 mx-auto mb-2" />
                <p className="text-xs text-gray-500">{t('noCollections', ru)}</p>
                <p className="text-xs text-gray-400 mt-1">{t('createFirst', ru)}</p>
              </div>
            ) : (
              collections.map(col => (
                <div
                  key={col.name}
                  onClick={() => { setSelectedCollection(col.name); setMessages([]); }}
                  className={`group flex items-center gap-2 mx-2 px-3 py-2 rounded-lg cursor-pointer transition-colors ${
                    selectedCollection === col.name
                      ? 'bg-indigo-50 text-indigo-700'
                      : 'text-gray-700 hover:bg-gray-50'
                  }`}
                >
                  <FolderOpen className={`w-4 h-4 shrink-0 ${selectedCollection === col.name ? 'text-indigo-500' : 'text-gray-400'}`} />
                  <div className="flex-1 min-w-0">
                    <p className="text-sm font-medium truncate">{col.name}</p>
                    <p className="text-xs text-gray-400">{col.doc_count} docs · {col.chunk_count} chunks</p>
                  </div>
                  <button
                    onClick={e => { e.stopPropagation(); handleDeleteCollection(col.name); }}
                    className="opacity-0 group-hover:opacity-100 w-5 h-5 flex items-center justify-center text-gray-400 hover:text-red-500 transition-all"
                  >
                    <Trash2 className="w-3 h-3" />
                  </button>
                </div>
              ))
            )}
          </div>
        </aside>

        {/* ── CENTER: Chat ── */}
        <main className="flex-1 flex flex-col overflow-hidden">
          {!selectedCollection ? (
            <div className="flex-1 flex items-center justify-center text-center p-8">
              <div>
                <MessageSquare className="w-12 h-12 text-gray-200 mx-auto mb-3" />
                <p className="text-sm text-gray-500">{t('selectHint', ru)}</p>
              </div>
            </div>
          ) : (
            <>
              {/* Chat messages */}
              <div className="flex-1 overflow-y-auto p-4 space-y-4">
                {messages.length === 0 && (
                  <div className="flex items-center justify-center h-full">
                    <div className="text-center">
                      <Sparkles className="w-10 h-10 text-indigo-200 mx-auto mb-3" />
                      <p className="text-sm font-medium text-gray-600">{t('chat', ru)}</p>
                      <p className="text-xs text-gray-400 mt-1">
                        {selectedCollection} · {documents.length} {t('documents', ru).toLowerCase()}
                      </p>
                    </div>
                  </div>
                )}
                {messages.map(msg => (
                  <div key={msg.id} className={`flex ${msg.role === 'user' ? 'justify-end' : 'justify-start'}`}>
                    <div className={`max-w-[80%] ${msg.role === 'user' ? 'order-1' : ''}`}>
                      <div className={`rounded-2xl px-4 py-3 text-sm leading-relaxed ${
                        msg.role === 'user'
                          ? 'bg-indigo-600 text-white rounded-br-sm'
                          : 'bg-white border border-gray-200 text-gray-800 rounded-bl-sm shadow-sm'
                      }`}>
                        {msg.loading ? (
                          <div className="flex items-center gap-2 text-gray-400">
                            <Loader2 className="w-4 h-4 animate-spin" />
                            <span className="text-xs">Thinking…</span>
                          </div>
                        ) : (
                          <p className="whitespace-pre-wrap">{msg.content}</p>
                        )}
                      </div>

                      {/* Citations + confidence */}
                      {msg.role === 'assistant' && !msg.loading && msg.citations && msg.citations.length > 0 && (
                        <div className="mt-2 flex flex-wrap gap-1.5 items-center">
                          {msg.confidence && <ConfidencePill level={msg.confidence} />}
                          {msg.citations.map((c, i) => (
                            <span
                              key={i}
                              className="inline-flex items-center gap-1 text-xs text-gray-500 bg-gray-100 px-2 py-0.5 rounded-full"
                            >
                              <FileText className="w-3 h-3" />
                              {c.doc_title}{c.page ? ` p.${c.page}` : ''}
                            </span>
                          ))}
                        </div>
                      )}
                      {msg.caveat && (
                        <p className="mt-1.5 text-xs text-amber-600 flex items-center gap-1">
                          <AlertCircle className="w-3 h-3 shrink-0" />{msg.caveat}
                        </p>
                      )}
                    </div>
                  </div>
                ))}
                <div ref={chatEndRef} />
              </div>

              {/* Chat input */}
              <div className="shrink-0 px-4 pb-4 pt-2 bg-gray-50 border-t border-gray-200">
                <form onSubmit={handleAsk} className="flex gap-2">
                  <input
                    value={input}
                    onChange={e => setInput(e.target.value)}
                    placeholder={t('placeholder', ru)}
                    disabled={asking}
                    className="flex-1 text-sm px-4 py-2.5 bg-white border border-gray-300 rounded-xl focus:outline-none focus:ring-2 focus:ring-indigo-500 disabled:opacity-60"
                  />
                  <button
                    type="submit"
                    disabled={asking || !input.trim()}
                    className="px-4 py-2.5 bg-indigo-600 text-white rounded-xl hover:bg-indigo-700 disabled:opacity-50 transition-colors"
                  >
                    {asking ? <Loader2 className="w-4 h-4 animate-spin" /> : <Send className="w-4 h-4" />}
                  </button>
                </form>
              </div>
            </>
          )}
        </main>

        {/* ── RIGHT: Documents panel ── */}
        {selectedCollection && (
          <aside className="w-72 shrink-0 bg-white border-l border-gray-200 flex flex-col overflow-hidden">
            <div className="flex items-center justify-between px-4 py-3 border-b border-gray-100">
              <span className="text-xs font-semibold text-gray-500 uppercase tracking-wider">
                {t('documents', ru)}
              </span>
              <span className="text-xs text-gray-400">{documents.length}</span>
            </div>

            {/* Upload zone */}
            <div
              className="mx-3 mt-3 p-4 border-2 border-dashed border-gray-200 rounded-xl text-center cursor-pointer hover:border-indigo-400 hover:bg-indigo-50 transition-colors"
              onClick={() => fileInputRef.current?.click()}
              onDragOver={e => { e.preventDefault(); e.currentTarget.classList.add('border-indigo-400', 'bg-indigo-50'); }}
              onDragLeave={e => e.currentTarget.classList.remove('border-indigo-400', 'bg-indigo-50')}
              onDrop={e => { e.preventDefault(); e.currentTarget.classList.remove('border-indigo-400', 'bg-indigo-50'); handleFileUpload(e.dataTransfer.files); }}
            >
              <Upload className="w-6 h-6 text-gray-300 mx-auto mb-1.5" />
              <p className="text-xs text-gray-500 font-medium">{t('uploadHint', ru)}</p>
              <p className="text-xs text-gray-400 mt-0.5">PDF · DOCX · TXT · MD</p>
              <input
                ref={fileInputRef}
                type="file"
                multiple
                accept=".pdf,.docx,.txt,.md"
                className="hidden"
                onChange={e => handleFileUpload(e.target.files)}
              />
            </div>

            {/* URL input */}
            <form onSubmit={handleAddUrl} className="flex gap-2 px-3 mt-2">
              <input
                value={urlValue}
                onChange={e => setUrlValue(e.target.value)}
                placeholder={t('urlPlaceholder', ru)}
                className="flex-1 text-xs px-2.5 py-1.5 border border-gray-300 rounded-lg focus:outline-none focus:ring-1 focus:ring-indigo-500"
              />
              <button
                type="submit"
                disabled={addingUrl || !urlValue.trim()}
                className="px-2.5 py-1.5 bg-indigo-600 text-white text-xs rounded-lg hover:bg-indigo-700 disabled:opacity-50 transition-colors"
              >
                {addingUrl ? <Loader2 className="w-3 h-3 animate-spin" /> : <Link2 className="w-3 h-3" />}
              </button>
            </form>

            {/* Upload status indicators */}
            {Object.entries(uploadStatus).map(([key, status]) => (
              <div key={key} className="mx-3 mt-1.5 flex items-center gap-2 text-xs px-2.5 py-1.5 rounded-lg bg-gray-50">
                {status === 'uploading' && <Loader2 className="w-3 h-3 text-indigo-500 animate-spin shrink-0" />}
                {status.startsWith('indexed') && <CheckCircle2 className="w-3 h-3 text-emerald-500 shrink-0" />}
                {status.startsWith('error') && <AlertCircle className="w-3 h-3 text-red-400 shrink-0" />}
                <span className="truncate text-gray-600">
                  {status === 'uploading' && t('uploading', ru)}
                  {status.startsWith('indexed') && `${t('indexed', ru)} · ${status.split(':')[1]} ${t('chunks', ru)}`}
                  {status.startsWith('error') && `${t('errorUpload', ru)}: ${status.split(':').slice(1).join(':')}`}
                </span>
              </div>
            ))}

            {/* Document list */}
            <div className="flex-1 overflow-y-auto mt-2 px-2 pb-2 space-y-1">
              {docsLoading ? (
                <div className="flex justify-center py-4">
                  <Loader2 className="w-4 h-4 text-gray-300 animate-spin" />
                </div>
              ) : documents.length === 0 ? (
                <div className="text-center py-6 text-xs text-gray-400">
                  {t('noDocuments', ru)}
                </div>
              ) : (
                documents.map(doc => (
                  <div
                    key={doc.doc_id}
                    className="group flex items-start gap-2 px-2.5 py-2 rounded-lg hover:bg-gray-50 transition-colors"
                  >
                    <FileText className="w-4 h-4 text-indigo-400 shrink-0 mt-0.5" />
                    <div className="flex-1 min-w-0">
                      <p className="text-xs font-medium text-gray-700 truncate">{doc.doc_title}</p>
                      <p className="text-xs text-gray-400">{doc.chunk_count} {t('chunks', ru)} · {doc.source_type}</p>
                    </div>
                    <button
                      onClick={() => handleDeleteDoc(doc)}
                      className="opacity-0 group-hover:opacity-100 w-5 h-5 flex items-center justify-center text-gray-400 hover:text-red-500 transition-all"
                    >
                      <X className="w-3 h-3" />
                    </button>
                  </div>
                ))
              )}
            </div>
          </aside>
        )}
      </div>
    </div>
  );
}
