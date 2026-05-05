import { useEffect, useState } from 'react';
import type { ReactNode } from 'react';
import { Brain, LayoutDashboard, Shield, SlidersHorizontal, CheckCircle2, AlertCircle, Loader2, Pencil, RotateCcw, X, ChevronDown } from 'lucide-react';
import { useLanguage } from '../contexts/LanguageContext';
import { useAdminConfig } from '../contexts/AdminConfigContext';
import {
  ADMIN_PROMPT_DEFINITIONS,
  ADMIN_PROMPT_GROUPS,
  ADMIN_TIERS,
  ADMIN_WIDGET_DEFINITIONS,
} from '../admin/catalog';
import type { AdminRuntimeConfig, AdminWidgetSetting } from '../types/admin';

function ToggleButton({ checked, onClick }: { checked: boolean; onClick: () => void }) {
  return (
    <button
      type="button"
      onClick={(event) => {
        event.preventDefault();
        event.stopPropagation();
        onClick();
      }}
      className={`relative inline-flex h-5 w-9 items-center rounded-full transition-colors duration-200 focus:outline-none ${checked ? 'bg-blue-600' : 'bg-gray-200'}`}
    >
      <span
        className={`inline-block h-3.5 w-3.5 transform rounded-full bg-white shadow transition-transform duration-200 ${checked ? 'translate-x-4' : 'translate-x-1'}`}
      />
    </button>
  );
}

function cloneWidgetDraft(value: Record<string, AdminWidgetSetting>) {
  return Object.fromEntries(
    Object.entries(value || {}).map(([key, item]) => [key, { enabled: Boolean(item?.enabled) }]),
  );
}

function clonePromptDraft(value: Record<string, string>, defaults: Record<string, string> = {}) {
  const draft = { ...(defaults || {}), ...(value || {}) };
  ADMIN_PROMPT_DEFINITIONS.forEach((prompt) => {
    if (draft[prompt.key] == null) {
      draft[prompt.key] = '';
    }
  });
  return draft;
}

function cloneRuntimeDraft(value: AdminRuntimeConfig) {
  return { ...value, analysisLensIds: [...(value.analysisLensIds || ['finance_markets'])] };
}

function SaveNotice({
  status,
  successLabel,
  error,
  loadingLabel,
}: {
  status: 'idle' | 'saving' | 'success' | 'error';
  successLabel: string;
  error: string | null;
  loadingLabel: string;
}) {
  if (status === 'saving') {
    return (
      <div className="flex items-center gap-2 text-sm text-blue-600">
        <Loader2 className="w-4 h-4 animate-spin" />
        <span>{loadingLabel}</span>
      </div>
    );
  }

  if (status === 'success') {
    return (
      <div className="flex items-center gap-2 text-sm text-emerald-600">
        <CheckCircle2 className="w-4 h-4" />
        <span>{successLabel}</span>
      </div>
    );
  }

  if (status === 'error' && error) {
    return (
      <div className="flex items-center gap-2 text-sm text-red-600">
        <AlertCircle className="w-4 h-4" />
        <span>{error}</span>
      </div>
    );
  }

  return null;
}

function CollapsibleSection({
  id,
  title,
  description,
  badge,
  open,
  onToggle,
  children,
}: {
  id: string;
  title: string;
  description?: string;
  badge?: string;
  open: boolean;
  onToggle: () => void;
  children: ReactNode;
}) {
  return (
    <div className="rounded-lg border border-gray-100 bg-gray-50/60">
      <button
        type="button"
        aria-expanded={open}
        aria-controls={`${id}-content`}
        onClick={onToggle}
        className="flex w-full items-center justify-between gap-3 px-3 py-3 text-left focus:outline-none focus:ring-2 focus:ring-blue-500"
      >
        <span className="min-w-0">
          <span className="flex flex-wrap items-center gap-2">
            <span className="text-xs uppercase tracking-wider text-gray-500" style={{ fontWeight: 600 }}>
              {title}
            </span>
            {badge && (
              <span className="inline-flex items-center rounded-full border border-blue-200 bg-blue-50 px-2 py-0.5 text-[11px] text-blue-700">
                {badge}
              </span>
            )}
          </span>
          {description && (
            <span className="mt-1 block text-xs text-gray-500">
              {description}
            </span>
          )}
        </span>
        <ChevronDown className={`h-4 w-4 flex-shrink-0 text-gray-400 transition-transform ${open ? 'rotate-180' : ''}`} />
      </button>
      {open && (
        <div id={`${id}-content`} className="border-t border-gray-100 bg-white px-3 py-3">
          {children}
        </div>
      )}
    </div>
  );
}

export function AdminPage() {
  const { lang } = useLanguage();
  const { config, loading, saving, error, updateConfig } = useAdminConfig();
  const ru = lang === 'ru';

  const [widgetDraft, setWidgetDraft] = useState<Record<string, AdminWidgetSetting>>(() => cloneWidgetDraft(config.widgets));
  const [promptDraft, setPromptDraft] = useState<Record<string, string>>(() => clonePromptDraft(config.prompts, config.promptDefaults));
  const [runtimeDraft, setRuntimeDraft] = useState<AdminRuntimeConfig>(() => cloneRuntimeDraft(config.runtime));
  const [widgetStatus, setWidgetStatus] = useState<'idle' | 'saving' | 'success' | 'error'>('idle');
  const [promptStatus, setPromptStatus] = useState<'idle' | 'saving' | 'success' | 'error'>('idle');
  const [runtimeStatus, setRuntimeStatus] = useState<'idle' | 'saving' | 'success' | 'error'>('idle');
  const [editablePrompts, setEditablePrompts] = useState<Record<string, boolean>>({});
  const [openSections, setOpenSections] = useState<Record<string, boolean>>({
    'widgets:pulse': true,
    'prompts:extraction': true,
    'runtime:lens': true,
  });
  const [sectionError, setSectionError] = useState<{ widgets: string | null; prompts: string | null; runtime: string | null }>({
    widgets: null,
    prompts: null,
    runtime: null,
  });

  useEffect(() => {
    setWidgetDraft(cloneWidgetDraft(config.widgets));
    setPromptDraft(clonePromptDraft(config.prompts, config.promptDefaults));
    setRuntimeDraft(cloneRuntimeDraft(config.runtime));
    setEditablePrompts({});
  }, [config]);

  const widgetChangedCount = ADMIN_WIDGET_DEFINITIONS.reduce((count, widget) => {
    const draftEnabled = widgetDraft[widget.id]?.enabled ?? true;
    const savedEnabled = config.widgets[widget.id]?.enabled ?? true;
    return draftEnabled !== savedEnabled ? count + 1 : count;
  }, 0);
  const widgetsDirty = widgetChangedCount > 0;

  const promptDefaults = config.promptDefaults || {};
  const savedPromptDraft = clonePromptDraft(config.prompts, promptDefaults);
  const promptsDirty = JSON.stringify(promptDraft) !== JSON.stringify(savedPromptDraft);

  const isSectionOpen = (key: string) => Boolean(openSections[key]);
  const toggleSection = (key: string) => {
    setOpenSections((prev) => ({ ...prev, [key]: !prev[key] }));
  };

  const runtimeKeys: (keyof AdminRuntimeConfig)[] = [
    'openaiModel',
    'questionBriefsModel',
    'behavioralBriefsModel',
    'opportunityBriefsModel',
    'topicOverviewsModel',
    'questionBriefsPromptVersion',
    'behavioralBriefsPromptVersion',
    'opportunityBriefsPromptVersion',
    'topicOverviewsPromptVersion',
    'topicOverviewsRefreshMinutes',
    'aiPostPromptStyle',
    'analysisLensIds',
    'featureQuestionBriefsAi',
    'featureBehavioralBriefsAi',
    'featureOpportunityBriefsAi',
    'featureTopicOverviewsAi',
  ];

  const runtimeDirty = runtimeKeys.some((key) => {
    const draftValue = runtimeDraft[key];
    const savedValue = config.runtime[key];
    if (Array.isArray(draftValue) || Array.isArray(savedValue)) {
      return JSON.stringify(draftValue || []) !== JSON.stringify(savedValue || []);
    }
    return draftValue !== savedValue;
  });
  const lensCatalog = config.analysisLensCatalog || [];
  const selectedLensIds = runtimeDraft.analysisLensIds || ['finance_markets'];
  const lensSelectionValid = selectedLensIds.length >= 1 && selectedLensIds.length <= 3;
  const lensSourceLabel = config.analysisLensSelectionSource === 'operator'
    ? (ru ? 'Выбрано оператором' : 'Operator-selected')
    : (ru ? 'Стандарт по умолчанию' : 'Seeded default');
  const toggleAnalysisLens = (lensId: string) => {
    setRuntimeDraft((prev) => {
      const current = prev.analysisLensIds || [];
      const enabled = current.includes(lensId);
      if (enabled) {
        return { ...prev, analysisLensIds: current.filter((id) => id !== lensId) };
      }
      if (current.length >= 3) {
        return prev;
      }
      return { ...prev, analysisLensIds: [...current, lensId] };
    });
  };

  const runtimeFields = [
    {
      key: 'openaiModel' as const,
      labelEn: 'Default extraction model',
      labelRu: 'Модель извлечения по умолчанию',
      descriptionEn: 'Used by the main extraction pipeline.',
      descriptionRu: 'Используется основным пайплайном извлечения.',
    },
    {
      key: 'questionBriefsModel' as const,
      labelEn: 'Question briefs model',
      labelRu: 'Модель карточек вопросов',
      descriptionEn: 'Overrides question-card AI calls.',
      descriptionRu: 'Переопределяет AI-вызовы карточек вопросов.',
    },
    {
      key: 'behavioralBriefsModel' as const,
      labelEn: 'Behavioral briefs model',
      labelRu: 'Модель поведенческих карточек',
      descriptionEn: 'Used by problem, service-gap, and urgency cards.',
      descriptionRu: 'Используется для проблем, сервисных пробелов и срочности.',
    },
    {
      key: 'opportunityBriefsModel' as const,
      labelEn: 'Opportunity briefs model',
      labelRu: 'Модель карточек возможностей',
      descriptionEn: 'Used by business opportunity AI cards.',
      descriptionRu: 'Используется карточками AI-бизнес-возможностей.',
    },
    {
      key: 'topicOverviewsModel' as const,
      labelEn: 'Topic overviews model',
      labelRu: 'Модель обзоров тем',
      descriptionEn: 'Used by the AI overview on the topic detail page.',
      descriptionRu: 'Используется AI-обзором на странице темы.',
    },
    {
      key: 'questionBriefsPromptVersion' as const,
      labelEn: 'Question prompt version',
      labelRu: 'Версия промпта вопросов',
      descriptionEn: 'Fingerprint label for question brief snapshots.',
      descriptionRu: 'Версия для отпечатка снапшотов карточек вопросов.',
    },
    {
      key: 'behavioralBriefsPromptVersion' as const,
      labelEn: 'Behavioral prompt version',
      labelRu: 'Версия поведенческого промпта',
      descriptionEn: 'Fingerprint label for behavioral card snapshots.',
      descriptionRu: 'Версия для отпечатка поведенческих карточек.',
    },
    {
      key: 'opportunityBriefsPromptVersion' as const,
      labelEn: 'Opportunity prompt version',
      labelRu: 'Версия промпта возможностей',
      descriptionEn: 'Fingerprint label for opportunity-card snapshots.',
      descriptionRu: 'Версия для отпечатка карточек возможностей.',
    },
    {
      key: 'topicOverviewsPromptVersion' as const,
      labelEn: 'Topic overview prompt version',
      labelRu: 'Версия промпта обзоров тем',
      descriptionEn: 'Fingerprint label for topic-overview snapshots.',
      descriptionRu: 'Версия для отпечатка снапшотов обзоров тем.',
    },
    {
      key: 'topicOverviewsRefreshMinutes' as const,
      labelEn: 'Topic overview refresh interval (minutes)',
      labelRu: 'Интервал обновления обзоров тем (минуты)',
      descriptionEn: 'Used when the backend starts the topic-overview materializer schedule.',
      descriptionRu: 'Используется при запуске расписания materializer-а обзоров тем на backend.',
    },
  ];

  const runtimeToggles = [
    {
      key: 'featureQuestionBriefsAi' as const,
      labelEn: 'Enable Question Briefs AI',
      labelRu: 'Включить AI для карточек вопросов',
      descriptionEn: 'Controls AI generation for question-card flows.',
      descriptionRu: 'Управляет AI-генерацией карточек вопросов.',
    },
    {
      key: 'featureBehavioralBriefsAi' as const,
      labelEn: 'Enable Behavioral Briefs AI',
      labelRu: 'Включить AI для поведенческих карточек',
      descriptionEn: 'Controls AI generation for problem and service-gap cards.',
      descriptionRu: 'Управляет AI-генерацией карточек проблем и сервисных пробелов.',
    },
    {
      key: 'featureOpportunityBriefsAi' as const,
      labelEn: 'Enable Opportunity Briefs AI',
      labelRu: 'Включить AI для карточек возможностей',
      descriptionEn: 'Controls AI generation for business opportunity cards.',
      descriptionRu: 'Управляет AI-генерацией карточек бизнес-возможностей.',
    },
    {
      key: 'featureTopicOverviewsAi' as const,
      labelEn: 'Enable Topic Overview AI',
      labelRu: 'Включить AI для обзоров тем',
      descriptionEn: 'Controls background AI generation for topic detail overview cards.',
      descriptionRu: 'Управляет фоновой AI-генерацией карточек обзора на странице темы.',
    },
  ];

  const setPromptEditable = (key: string, editable: boolean) => {
    setEditablePrompts((prev) => ({ ...prev, [key]: editable }));
  };

  const handleSectionSave = async (
    section: 'widgets' | 'prompts' | 'runtime',
    patch: Parameters<typeof updateConfig>[0],
  ) => {
    const setStatus = section === 'widgets' ? setWidgetStatus : section === 'prompts' ? setPromptStatus : setRuntimeStatus;
    setStatus('saving');
    setSectionError((prev) => ({ ...prev, [section]: null }));

    try {
      await updateConfig(patch);
      if (section === 'prompts') {
        setEditablePrompts({});
      }
      setStatus('success');
      window.setTimeout(() => {
        setStatus('idle');
      }, 1800);
    } catch (err: any) {
      setStatus('error');
      setSectionError((prev) => ({ ...prev, [section]: err?.message ?? 'Failed to save changes' }));
    }
  };

  return (
    <div className="p-4 md:p-8">
      <div className="mb-5 md:mb-6">
        <h1 className="text-2xl text-gray-900" style={{ fontWeight: 700 }}>{ru ? 'Admin' : 'Admin'}</h1>
        <p className="text-gray-500 text-sm mt-1">
          {ru
            ? 'Управление виджетами дашборда, AI-промптами и ключевыми runtime-настройками'
            : 'Manage dashboard widgets, AI prompts, and key runtime settings'}
        </p>
      </div>

      <div className="max-w-4xl space-y-5 md:space-y-6">
        {error && (
          <div className="rounded-xl border border-red-200 bg-red-50 px-4 py-3 text-sm text-red-700">
            {error}
          </div>
        )}

        <div className="bg-white rounded-xl border border-gray-200 p-6">
          <div className="flex items-center gap-3 mb-6">
            <div className="w-10 h-10 bg-blue-100 rounded-lg flex items-center justify-center">
              <LayoutDashboard className="w-5 h-5 text-blue-600" />
            </div>
            <div>
              <h2 className="text-gray-900" style={{ fontSize: '1.05rem', fontWeight: 600 }}>
                {ru ? 'Виджеты дашборда' : 'Dashboard Widgets'}
              </h2>
              <p className="text-sm text-gray-500">
                {ru ? 'Включайте и выключайте виджеты без изменений в коде' : 'Show or hide dashboard widgets without code changes'}
              </p>
            </div>
          </div>

          <div className="space-y-5">
            {ADMIN_TIERS.map((tier) => {
              const tierWidgets = ADMIN_WIDGET_DEFINITIONS.filter((widget) => widget.tierId === tier.id);
              const sectionKey = `widgets:${tier.id}`;
              return (
                <CollapsibleSection
                  key={tier.id}
                  id={sectionKey}
                  title={ru ? tier.labelRu : tier.labelEn}
                  badge={`${tierWidgets.length}`}
                  open={isSectionOpen(sectionKey)}
                  onToggle={() => toggleSection(sectionKey)}
                >
                  <div className="space-y-1">
                    {tierWidgets.map((widget) => (
                      <div key={widget.id} className="flex items-center justify-between p-3 rounded-lg hover:bg-gray-50">
                        <div>
                          <p className="text-sm text-gray-900" style={{ fontWeight: 500 }}>
                            {ru ? widget.labelRu : widget.labelEn}
                          </p>
                          <p className="text-xs text-gray-500">
                            {ru ? 'Если выключено, виджет не показывается на дашборде.' : 'If disabled, the widget stays hidden on the dashboard.'}
                          </p>
                        </div>
                        <ToggleButton
                          checked={widgetDraft[widget.id]?.enabled ?? true}
                          onClick={() => {
                            setWidgetStatus('idle');
                            setSectionError((prev) => ({ ...prev, widgets: null }));
                            setWidgetDraft((prev) => ({
                              ...prev,
                              [widget.id]: { enabled: !(prev[widget.id]?.enabled ?? true) },
                            }));
                          }}
                        />
                      </div>
                    ))}
                  </div>
                </CollapsibleSection>
              );
            })}
          </div>

          <div className="flex items-center justify-between gap-3 mt-6 pt-4 border-t border-gray-100">
            <SaveNotice
              status={widgetStatus}
              successLabel={ru ? 'Настройки виджетов сохранены' : 'Widget settings saved'}
              error={sectionError.widgets}
              loadingLabel={ru ? 'Сохраняем виджеты...' : 'Saving widget settings...'}
            />
            <button
              type="button"
              disabled={!widgetsDirty || saving}
              onClick={() => handleSectionSave('widgets', { widgets: widgetDraft })}
              className="px-4 py-2 rounded-lg text-sm text-white transition-all disabled:opacity-50"
              style={{ background: 'linear-gradient(135deg, #1d4ed8, #1e40af)' }}
            >
              {ru ? `Сохранить виджеты${widgetChangedCount ? ` (${widgetChangedCount})` : ''}` : `Save widgets${widgetChangedCount ? ` (${widgetChangedCount})` : ''}`}
            </button>
          </div>
        </div>

        <div className="bg-white rounded-xl border border-gray-200 p-6">
          <div className="flex items-center gap-3 mb-6">
            <div className="w-10 h-10 bg-purple-100 rounded-lg flex items-center justify-center">
              <Brain className="w-5 h-5 text-purple-600" />
            </div>
            <div>
              <h2 className="text-gray-900" style={{ fontSize: '1.05rem', fontWeight: 600 }}>
                {ru ? 'AI-промпты' : 'AI Prompts'}
              </h2>
              <p className="text-sm text-gray-500">
                {ru ? 'Редактируйте текущие промпты для логики и AI-виджетов' : 'Edit the current prompts used by AI logic and widgets'}
              </p>
            </div>
          </div>

          <div className="space-y-6">
            {ADMIN_PROMPT_GROUPS.map((group) => {
              const prompts = ADMIN_PROMPT_DEFINITIONS.filter((prompt) => prompt.groupId === group.id);
              const sectionKey = `prompts:${group.id}`;
              return (
                <CollapsibleSection
                  key={group.id}
                  id={sectionKey}
                  title={ru ? group.labelRu : group.labelEn}
                  description={ru ? group.descriptionRu : group.descriptionEn}
                  badge={ru ? group.badgeRu : group.badgeEn}
                  open={isSectionOpen(sectionKey)}
                  onToggle={() => toggleSection(sectionKey)}
                >
                  <div className="space-y-4">
                    {prompts.map((prompt) => (
                      <div key={prompt.key}>
                        <div className="flex items-start justify-between gap-3 mb-2">
                          <div>
                            <label className="block text-sm text-gray-900" style={{ fontWeight: 500 }}>
                              {ru ? prompt.labelRu : prompt.labelEn}
                            </label>
                          </div>
                          <div className="flex items-center gap-2">
                            <button
                              type="button"
                              onClick={() => {
                                setPromptStatus('idle');
                                setSectionError((prev) => ({ ...prev, prompts: null }));
                                setPromptEditable(prompt.key, true);
                              }}
                              className="inline-flex items-center gap-1 rounded-lg border border-gray-200 bg-white px-3 py-1.5 text-xs text-gray-700 hover:bg-gray-50"
                            >
                              <Pencil className="w-3.5 h-3.5" />
                              {ru ? 'Редактировать' : 'Edit'}
                            </button>
                            <button
                              type="button"
                              onClick={() => {
                                setPromptDraft((prev) => ({ ...prev, [prompt.key]: savedPromptDraft[prompt.key] ?? '' }));
                                setPromptEditable(prompt.key, false);
                                setPromptStatus('idle');
                                setSectionError((prev) => ({ ...prev, prompts: null }));
                              }}
                              className="inline-flex items-center gap-1 rounded-lg border border-gray-200 bg-white px-3 py-1.5 text-xs text-gray-700 hover:bg-gray-50"
                            >
                              <X className="w-3.5 h-3.5" />
                              {ru ? 'Отменить' : 'Cancel'}
                            </button>
                            <button
                              type="button"
                              onClick={() => {
                                setPromptDraft((prev) => ({ ...prev, [prompt.key]: promptDefaults[prompt.key] ?? savedPromptDraft[prompt.key] ?? '' }));
                                setPromptEditable(prompt.key, true);
                                setPromptStatus('idle');
                                setSectionError((prev) => ({ ...prev, prompts: null }));
                              }}
                              className="inline-flex items-center gap-1 rounded-lg border border-gray-200 bg-white px-3 py-1.5 text-xs text-gray-700 hover:bg-gray-50"
                            >
                              <RotateCcw className="w-3.5 h-3.5" />
                              {ru ? 'Сбросить по умолчанию' : 'Reset to default'}
                            </button>
                          </div>
                        </div>
                        <p className="text-xs text-gray-500 mb-2">
                          {ru ? prompt.descriptionRu : prompt.descriptionEn}
                        </p>
                        <textarea
                          value={promptDraft[prompt.key] ?? ''}
                          onChange={(event) => {
                            if (!editablePrompts[prompt.key]) {
                              return;
                            }
                            setPromptStatus('idle');
                            setSectionError((prev) => ({ ...prev, prompts: null }));
                            const nextValue = event.target.value;
                            setPromptDraft((prev) => ({ ...prev, [prompt.key]: nextValue }));
                          }}
                          rows={6}
                          readOnly={!editablePrompts[prompt.key]}
                          className={`w-full px-4 py-3 border rounded-lg text-sm resize-y ${editablePrompts[prompt.key]
                            ? 'border-gray-300 bg-white focus:outline-none focus:ring-2 focus:ring-blue-500'
                            : 'border-gray-200 bg-gray-50 text-gray-700 focus:outline-none'}`}
                        />
                        {!editablePrompts[prompt.key] && (
                          <p className="text-[11px] text-gray-400 mt-2">
                            {ru ? 'Нажмите «Редактировать», чтобы изменить этот промпт.' : 'Click Edit before changing this prompt.'}
                          </p>
                        )}
                      </div>
                    ))}
                  </div>
                </CollapsibleSection>
              );
            })}
          </div>

          <div className="flex items-center justify-between gap-3 mt-6 pt-4 border-t border-gray-100">
            <SaveNotice
              status={promptStatus}
              successLabel={ru ? 'Промпты сохранены' : 'Prompts saved'}
              error={sectionError.prompts}
              loadingLabel={ru ? 'Сохраняем промпты...' : 'Saving prompts...'}
            />
            <button
              type="button"
              disabled={!promptsDirty || saving || loading}
              onClick={() => handleSectionSave('prompts', { prompts: promptDraft })}
              className="px-4 py-2 rounded-lg text-sm text-white transition-all disabled:opacity-50"
              style={{ background: 'linear-gradient(135deg, #7c3aed, #6d28d9)' }}
            >
              {ru ? 'Сохранить промпты' : 'Save prompts'}
            </button>
          </div>
        </div>

        <div className="bg-white rounded-xl border border-gray-200 p-6">
          <div className="flex items-center gap-3 mb-6">
            <div className="w-10 h-10 bg-emerald-100 rounded-lg flex items-center justify-center">
              <SlidersHorizontal className="w-5 h-5 text-emerald-600" />
            </div>
            <div>
              <h2 className="text-gray-900" style={{ fontSize: '1.05rem', fontWeight: 600 }}>
                {ru ? 'Runtime AI Settings' : 'Runtime AI Settings'}
              </h2>
              <p className="text-sm text-gray-500">
                {ru ? 'Безопасные настройки моделей, версий промптов и feature flags' : 'Safe model, prompt-version, and feature-flag overrides'}
              </p>
            </div>
          </div>

          <div className="space-y-4">
            <CollapsibleSection
              id="runtime:lens"
              title={ru ? 'Global AI Lens' : 'Global AI Lens'}
              description={ru
                ? 'Выберите 1-3 линзы анализа. Изменения применяются только к новым AI-анализам.'
                : 'Select 1-3 analysis lenses. Lens changes apply to new analyses only.'}
              badge={`${lensSourceLabel} · ${selectedLensIds.length}/3`}
              open={isSectionOpen('runtime:lens')}
              onToggle={() => toggleSection('runtime:lens')}
            >
              <div className="grid gap-3 md:grid-cols-3">
                {lensCatalog.map((lens) => {
                  const checked = selectedLensIds.includes(lens.id);
                  const disabled = !checked && selectedLensIds.length >= 3;
                  return (
                    <button
                      key={lens.id}
                      type="button"
                      disabled={disabled}
                      onClick={() => toggleAnalysisLens(lens.id)}
                      className={`rounded-lg border bg-white p-3 text-left transition-all focus:outline-none focus:ring-2 focus:ring-emerald-500 ${
                        checked ? 'border-emerald-400 shadow-sm' : 'border-gray-200 hover:border-emerald-200'
                      } ${disabled ? 'cursor-not-allowed opacity-50' : ''}`}
                    >
                      <div className="flex items-center justify-between gap-2">
                        <span className="text-sm text-gray-900" style={{ fontWeight: 600 }}>
                          {lens.name}
                        </span>
                        <span className={`h-4 w-4 rounded-full border ${checked ? 'border-emerald-500 bg-emerald-500' : 'border-gray-300'}`} />
                      </div>
                      <p className="mt-2 line-clamp-3 text-xs text-gray-500">
                        {lens.objective}
                      </p>
                    </button>
                  );
                })}
              </div>

              {!lensSelectionValid && (
                <p className="mt-3 text-xs text-red-600">
                  {ru ? 'Нужно выбрать хотя бы одну линзу.' : 'Select at least one lens.'}
                </p>
              )}
            </CollapsibleSection>

            <CollapsibleSection
              id="runtime:models"
              title={ru ? 'Модели и версии промптов' : 'Models and prompt versions'}
              open={isSectionOpen('runtime:models')}
              onToggle={() => toggleSection('runtime:models')}
            >
              <div className="space-y-4">
                {runtimeFields.map((field) => (
                  <div key={field.key}>
                    <label className="block text-sm text-gray-700 mb-2" style={{ fontWeight: 500 }}>
                      {ru ? field.labelRu : field.labelEn}
                    </label>
                    <p className="text-xs text-gray-500 mb-2">
                      {ru ? field.descriptionRu : field.descriptionEn}
                    </p>
                    <input
                      type="text"
                      value={runtimeDraft[field.key]}
                      onChange={(event) => {
                        const value = event.target.value;
                        setRuntimeDraft((prev) => ({ ...prev, [field.key]: value }));
                      }}
                      className="w-full px-4 py-2 border border-gray-300 rounded-lg focus:outline-none focus:ring-2 focus:ring-blue-500 text-sm"
                    />
                  </div>
                ))}

                <div>
                  <label className="block text-sm text-gray-700 mb-2" style={{ fontWeight: 500 }}>
                    {ru ? 'Стиль промпта постов' : 'Post prompt style'}
                  </label>
                  <select
                    value={runtimeDraft.aiPostPromptStyle}
                    onChange={(event) => {
                      const value = event.target.value === 'full' ? 'full' : 'compact';
                      setRuntimeDraft((prev) => ({ ...prev, aiPostPromptStyle: value }));
                    }}
                    className="w-full px-4 py-2 border border-gray-300 rounded-lg focus:outline-none focus:ring-2 focus:ring-blue-500 text-sm"
                  >
                    <option value="compact">{ru ? 'Компактный' : 'Compact'}</option>
                    <option value="full">{ru ? 'Полный' : 'Full'}</option>
                  </select>
                </div>
              </div>
            </CollapsibleSection>

            <CollapsibleSection
              id="runtime:features"
              title={ru ? 'AI feature flags' : 'AI feature flags'}
              open={isSectionOpen('runtime:features')}
              onToggle={() => toggleSection('runtime:features')}
            >
              <div className="space-y-1">
                {runtimeToggles.map((toggle) => (
                  <div key={toggle.key} className="flex items-center justify-between p-3 rounded-lg hover:bg-gray-50">
                    <div>
                      <p className="text-sm text-gray-900" style={{ fontWeight: 500 }}>
                        {ru ? toggle.labelRu : toggle.labelEn}
                      </p>
                      <p className="text-xs text-gray-500">
                        {ru ? toggle.descriptionRu : toggle.descriptionEn}
                      </p>
                    </div>
                    <ToggleButton
                      checked={runtimeDraft[toggle.key]}
                      onClick={() => {
                        setRuntimeDraft((prev) => ({ ...prev, [toggle.key]: !prev[toggle.key] }));
                      }}
                    />
                  </div>
                ))}
              </div>
            </CollapsibleSection>
          </div>

          <div className="flex items-center justify-between gap-3 mt-6 pt-4 border-t border-gray-100">
            <SaveNotice
              status={runtimeStatus}
              successLabel={ru ? 'Runtime-настройки сохранены' : 'Runtime settings saved'}
              error={sectionError.runtime}
              loadingLabel={ru ? 'Сохраняем runtime-настройки...' : 'Saving runtime settings...'}
            />
            <button
              type="button"
              disabled={!runtimeDirty || saving || loading || !lensSelectionValid}
              onClick={() => handleSectionSave('runtime', { runtime: runtimeDraft })}
              className="px-4 py-2 rounded-lg text-sm text-white transition-all disabled:opacity-50"
              style={{ background: 'linear-gradient(135deg, #059669, #047857)' }}
            >
              {ru ? 'Сохранить runtime' : 'Save runtime'}
            </button>
          </div>
        </div>

        {loading && (
          <div className="flex items-center gap-2 text-sm text-gray-500">
            <Loader2 className="w-4 h-4 animate-spin" />
            <span>{ru ? 'Загружаем admin-настройки...' : 'Loading admin settings...'}</span>
          </div>
        )}
      </div>

      <div className="mt-8 flex items-center gap-2 text-xs text-gray-400">
        <Shield className="w-3.5 h-3.5" />
        <span>
          {ru
            ? 'Страница доступна всем пользователям временно, пока роль admin не добавлена.'
            : 'This page is temporarily visible to all users until admin-role access is added.'}
        </span>
      </div>
    </div>
  );
}
