import { useEffect, useMemo, useRef, useState } from 'react'
import {
  AlertCircle,
  Brain,
  CheckCircle2,
  Clock,
  Database,
  MoreVertical,
  Network,
  Pencil,
  Plus,
  RefreshCw,
  Search,
} from 'lucide-react'

import { apiFetch } from '../../services/api'

type SocialSourceRow = {
  id: string
  entity_id: string
  company_id?: string | null
  company_name: string
  company_website?: string | null
  platform: 'facebook' | 'instagram' | 'google' | 'tiktok'
  source_kind: 'facebook_page' | 'meta_ads' | 'instagram_profile' | 'google_domain' | 'tiktok_profile'
  display_url: string | null
  account_external_id: string | null
  is_active: boolean
  health_status: 'unknown' | 'healthy' | 'invalid_identifier' | 'provider_404' | 'rate_limited' | 'auth_error' | 'network_error'
  last_collected_at: string | null
  last_error: string | null
  metadata: Record<string, unknown>
}

type SocialSourceListResponse = {
  count: number
  items: SocialSourceRow[]
}

type SocialCompanySourcesPayload = {
  company_name: string
  website?: string
  sources: {
    facebook_page?: string
    instagram_profile?: string
    meta_ads?: string
    google_domain?: string
  }
}

type SocialCompanySourcesResponse = {
  action: 'created' | 'updated'
  company: {
    id: string
    name: string
    website: string | null
  }
  entity: {
    id: string
    company_id: string
    name: string
  }
  items: SocialSourceRow[]
}

type CompanySourcesInitialValues = {
  companyId?: string | null
  entityId?: string | null
  companyName: string
  website: string
  facebookPage: string
  instagramProfile: string
  metaAds: string
  googleDomain: string
}

type SocialSourceUpdateResponse = {
  item: SocialSourceRow
}

type SocialRuntimeStatus = {
  status: 'active' | 'stopped'
  is_active: boolean
  interval_minutes: number
  running_now: boolean
  last_run_started_at: string | null
  last_run_finished_at: string | null
  last_success_at: string | null
  next_run_at: string | null
  last_error: string | null
  last_result?: {
    accounts_total?: number
    accounts_processed?: number
    activities_collected?: number
    activities_analyzed?: number
    activities_graph_synced?: number
    collect_failures?: number
    analysis_failures?: number
    graph_failures?: number
  } | null
  run_history?: Array<{
    finished_at: string | null
    accounts_processed: number
    activities_collected: number
    activities_analyzed: number
    activities_graph_synced: number
    collect_failures: number
    analysis_failures: number
    graph_failures: number
  }>
}

type SocialSourceStatus = 'active' | 'paused' | 'invalid_identifier' | 'provider_404' | 'rate_limited' | 'auth_error' | 'network_error' | 'error'
type SocialPlatformFilter = 'all' | 'facebook' | 'instagram' | 'google'

const socialStatusConfig: Record<SocialSourceStatus, { labelEn: string; labelRu: string; bg: string; text: string; dot: string }> = {
  active: { labelEn: 'Active', labelRu: 'Активен', bg: 'bg-emerald-50', text: 'text-emerald-700', dot: 'bg-emerald-500' },
  paused: { labelEn: 'Paused', labelRu: 'Пауза', bg: 'bg-amber-50', text: 'text-amber-700', dot: 'bg-amber-500' },
  invalid_identifier: { labelEn: 'Invalid source', labelRu: 'Неверный источник', bg: 'bg-red-50', text: 'text-red-700', dot: 'bg-red-500' },
  provider_404: { labelEn: 'Not found', labelRu: 'Не найдено', bg: 'bg-red-50', text: 'text-red-700', dot: 'bg-red-500' },
  rate_limited: { labelEn: 'Rate limited', labelRu: 'Лимит запросов', bg: 'bg-orange-50', text: 'text-orange-700', dot: 'bg-orange-500' },
  auth_error: { labelEn: 'Auth issue', labelRu: 'Ошибка доступа', bg: 'bg-rose-50', text: 'text-rose-700', dot: 'bg-rose-500' },
  network_error: { labelEn: 'Network issue', labelRu: 'Сетевая ошибка', bg: 'bg-red-50', text: 'text-red-700', dot: 'bg-red-500' },
  error: { labelEn: 'Error', labelRu: 'Ошибка', bg: 'bg-red-50', text: 'text-red-700', dot: 'bg-red-500' },
}

function socialRowStatus(item: SocialSourceRow): SocialSourceStatus {
  if (!item.is_active) return 'paused'
  if (item.health_status !== 'unknown' && item.health_status !== 'healthy') return item.health_status
  if (item.last_error) return 'error'
  return 'active'
}

function isFailingStatus(status: SocialSourceStatus): boolean {
  return !['active', 'paused'].includes(status)
}

function relativeTime(iso: string | null, ru: boolean): string {
  if (!iso) return ru ? 'Никогда' : 'Never'
  const date = new Date(iso)
  const diffMs = Date.now() - date.getTime()
  if (!Number.isFinite(diffMs) || diffMs < 0) return ru ? 'Только что' : 'Just now'
  const mins = Math.floor(diffMs / 60000)
  if (mins < 1) return ru ? 'Только что' : 'Just now'
  if (mins < 60) return ru ? `${mins} мин назад` : `${mins} min ago`
  const hours = Math.floor(mins / 60)
  if (hours < 24) return ru ? `${hours} ч назад` : `${hours}h ago`
  const days = Math.floor(hours / 24)
  return ru ? `${days} д назад` : `${days}d ago`
}

function formatDateTime(iso: string | null, ru: boolean): string {
  if (!iso) return '—'
  const date = new Date(iso)
  if (!Number.isFinite(date.getTime())) return '—'
  return date.toLocaleString(ru ? 'ru-RU' : 'en-US', {
    year: 'numeric',
    month: 'short',
    day: '2-digit',
    hour: '2-digit',
    minute: '2-digit',
  })
}

function lastResultLabel(lastResult: SocialRuntimeStatus['last_result'], runtime: SocialRuntimeStatus | null, ru: boolean) {
  if (lastResult) {
    const collected = lastResult.activities_collected ?? 0
    const analyzed = lastResult.activities_analyzed ?? 0
    const synced = lastResult.activities_graph_synced ?? 0
    return ru
      ? `${collected} собрано · ${analyzed} AI · ${synced} Neo4j`
      : `${collected} collected · ${analyzed} AI · ${synced} Neo4j`
  }
  if (runtime?.last_run_started_at || runtime?.is_active) {
    return ru ? 'Ожидаем завершённый цикл' : 'Awaiting completed cycle'
  }
  return ru ? 'Нет данных' : 'No data'
}

async function requestJson<T>(path: string, init?: RequestInit): Promise<T> {
  const normalizedPath = path.startsWith('/api/') ? path.slice(4) : path
  return apiFetch<T>(normalizedPath, {
    ...init,
    includeUserAuth: true,
  })
}

function SocialStatusBadge({ status, ru }: { status: SocialSourceStatus; ru: boolean }) {
  const cfg = socialStatusConfig[status]
  return (
    <span className={`inline-flex items-center gap-1.5 px-2 py-0.5 rounded-full text-xs ${cfg.bg} ${cfg.text}`} style={{ fontWeight: 500 }}>
      <span className={`w-1.5 h-1.5 rounded-full ${cfg.dot}`} />
      {ru ? cfg.labelRu : cfg.labelEn}
    </span>
  )
}

function sourceTypeLabel(sourceKind: SocialSourceRow['source_kind'], ru: boolean): string {
  switch (sourceKind) {
    case 'facebook_page':
      return ru ? 'Страница' : 'Page'
    case 'meta_ads':
      return ru ? 'Meta Ads' : 'Meta Ads'
    case 'instagram_profile':
      return ru ? 'Профиль' : 'Profile'
    case 'google_domain':
      return ru ? 'Домен' : 'Domain'
    case 'tiktok_profile':
      return ru ? 'Профиль' : 'Profile'
    default:
      return sourceKind
  }
}

function platformLabel(platform: SocialSourceRow['platform'] | SocialPlatformFilter, ru: boolean): string {
  if (platform === 'all') {
    return ru ? 'Все сети' : 'All platforms'
  }
  switch (platform) {
    case 'facebook':
      return 'Facebook'
    case 'instagram':
      return 'Instagram'
    case 'google':
      return 'Google'
    case 'tiktok':
      return 'TikTok'
    default:
      return ru ? 'Соцсеть' : 'Social'
  }
}

function platformBadgeTone(platform: SocialSourceRow['platform']) {
  switch (platform) {
    case 'facebook':
      return { bg: 'from-blue-600 to-blue-800', label: 'f' }
    case 'instagram':
      return { bg: 'from-pink-500 to-orange-500', label: 'I' }
    case 'google':
      return { bg: 'from-slate-700 to-slate-900', label: 'G' }
    case 'tiktok':
      return { bg: 'from-slate-800 to-black', label: 'T' }
    default:
      return { bg: 'from-slate-500 to-slate-700', label: 'S' }
  }
}

function SourceBadge({ platform }: { platform: SocialSourceRow['platform'] }) {
  const tone = platformBadgeTone(platform)
  return (
    <div
      className={`w-9 h-9 rounded-full flex items-center justify-center flex-shrink-0 text-white bg-gradient-to-br ${tone.bg}`}
      style={{ fontWeight: 700 }}
    >
      {tone.label}
    </div>
  )
}

function emptyCompanySources(): CompanySourcesInitialValues {
  return {
    companyId: null,
    entityId: null,
    companyName: '',
    website: '',
    facebookPage: '',
    instagramProfile: '',
    metaAds: '',
    googleDomain: '',
  }
}

function sourceValueCount(values: CompanySourcesInitialValues): number {
  return [
    values.facebookPage,
    values.instagramProfile,
    values.metaAds,
    values.googleDomain,
  ].filter((value) => value.trim()).length
}

function sourceFieldErrors(values: CompanySourcesInitialValues, ru: boolean): Partial<Record<keyof CompanySourcesInitialValues, string>> {
  const errors: Partial<Record<keyof CompanySourcesInitialValues, string>> = {}
  if (!values.companyName.trim()) {
    errors.companyName = ru ? 'Введите название компании' : 'Enter the company name'
  }
  if (!sourceValueCount(values)) {
    errors.googleDomain = ru ? 'Добавьте хотя бы один источник' : 'Add at least one scraping source'
  }
  if (values.website.trim() && !/^[a-z]+:\/\/[^\s.]+\.[^\s]+$/i.test(values.website.trim()) && !/^[^\s.]+\.[^\s]+$/i.test(values.website.trim())) {
    errors.website = ru ? 'Введите сайт, например https://www.xyz.com' : 'Enter a website like https://www.xyz.com'
  }
  if (values.facebookPage.trim()) {
    const facebook = values.facebookPage.trim()
    const isPageId = /^[0-9]{5,255}$/.test(facebook.replace(/\s+/g, ''))
    const isUrl = /(^|\/\/)(www\.|m\.)?(facebook|fb)\.com\//i.test(facebook)
    if (!isPageId && !isUrl) {
      errors.facebookPage = ru ? 'Введите публичную Facebook Page URL или page ID' : 'Enter a public Facebook Page URL or page ID'
    }
  }
  if (values.instagramProfile.trim()) {
    const instagram = values.instagramProfile.trim()
    const isHandle = /^@?[A-Za-z0-9._]{1,30}$/.test(instagram)
    const isUrl = /(^|\/\/)(www\.)?instagram\.com\/[A-Za-z0-9._]{1,30}/i.test(instagram)
    if (!isHandle && !isUrl) {
      errors.instagramProfile = ru ? 'Введите Instagram URL или @handle' : 'Enter an Instagram URL or @handle'
    }
  }
  if (values.metaAds.trim() && !/^[0-9]{5,255}$/.test(values.metaAds.replace(/\s+/g, ''))) {
    errors.metaAds = ru ? 'Введите только числовой Meta Ads ID' : 'Enter only the numeric Meta Ads ID'
  }
  if (values.googleDomain.trim() && !/^[a-z]+:\/\/[^\s.]+\.[^\s]+$/i.test(values.googleDomain.trim()) && !/^[^\s.]+\.[^\s]+$/i.test(values.googleDomain.trim())) {
    errors.googleDomain = ru ? 'Введите домен, например xyz.com' : 'Enter a domain like xyz.com'
  }
  return errors
}

function CompanySourcesModal({
  open,
  ru,
  onClose,
  onSubmit,
  initialValues,
}: {
  open: boolean
  ru: boolean
  onClose: () => void
  onSubmit: (payload: SocialCompanySourcesPayload, companyId?: string | null) => Promise<void>
  initialValues?: CompanySourcesInitialValues | null
}) {
  const inputRef = useRef<HTMLInputElement>(null)
  const [values, setValues] = useState<CompanySourcesInitialValues>(emptyCompanySources)
  const [errors, setErrors] = useState<Partial<Record<keyof CompanySourcesInitialValues, string>>>({})
  const [saving, setSaving] = useState(false)
  const isEditing = !!initialValues?.companyId

  useEffect(() => {
    if (!open) return
    setValues(initialValues || emptyCompanySources())
    setSaving(false)
    setErrors({})
    setTimeout(() => inputRef.current?.focus(), 80)
  }, [open, initialValues])

  const setField = (field: keyof CompanySourcesInitialValues, value: string) => {
    setValues((current) => ({ ...current, [field]: value }))
    setErrors((current) => ({ ...current, [field]: undefined }))
  }

  const validate = () => {
    const nextErrors = sourceFieldErrors(values, ru)
    setErrors(nextErrors)
    return Object.values(nextErrors).every((message) => !message)
  }

  const count = sourceValueCount(values)
  const preview = values.companyName.trim() && count > 0 && Object.values(errors).every((message) => !message)
    ? ru
      ? `Будет сохранено ${count} источников для ${values.companyName.trim()}`
      : `Will save ${count} sources under ${values.companyName.trim()}`
    : null

  const fieldClass = (field: keyof CompanySourcesInitialValues) =>
    `w-full px-3.5 py-2.5 border rounded-lg text-sm focus:outline-none focus:ring-2 focus:ring-blue-500 transition-all ${
      errors[field] ? 'border-red-300 bg-red-50/40' : 'border-gray-200'
    }`

  const FieldError = ({ field }: { field: keyof CompanySourcesInitialValues }) =>
    errors[field] ? <p className="mt-1 text-xs text-red-600">{errors[field]}</p> : null

  const helperTextClass = 'mt-1 text-[11px] leading-4 text-gray-500'

  const FieldRow = ({
    label,
    field,
    placeholder,
    helper,
    autoFocus = false,
  }: {
    label: string
    field: keyof CompanySourcesInitialValues
    placeholder: string
    helper?: string
    autoFocus?: boolean
  }) => (
    <div className="grid grid-cols-1 gap-2 rounded-xl border border-gray-100 bg-white p-3 md:grid-cols-[180px_minmax(0,1fr)] md:items-start md:gap-4">
      <label className="pt-0.5 text-xs text-gray-600 md:pt-3" style={{ fontWeight: 500 }}>
        {label}
      </label>
      <div className="min-w-0">
        <input
          ref={autoFocus ? inputRef : undefined}
          type="text"
          value={values[field]}
          onChange={(event) => setField(field, event.target.value)}
          onBlur={validate}
          placeholder={placeholder}
          className={fieldClass(field)}
        />
        {helper ? <p className={helperTextClass}>{helper}</p> : null}
        <FieldError field={field} />
      </div>
    </div>
  )

  if (!open) return null

  return (
    <div className="fixed inset-0 z-50 flex items-center justify-center p-4">
      <div className="absolute inset-0 bg-black/50 backdrop-blur-sm" onClick={onClose} />
      <div className="relative bg-white rounded-2xl shadow-2xl w-full max-w-2xl overflow-hidden">
        <div className="flex items-center justify-between px-6 py-4 border-b border-gray-100">
          <div className="flex items-center gap-3">
            <div
              className="w-9 h-9 rounded-xl flex items-center justify-center text-white"
              style={{ background: 'linear-gradient(135deg, #2563eb, #1d4ed8)', fontWeight: 700 }}
            >
              +
            </div>
            <div>
              <h3 className="text-gray-900" style={{ fontSize: '1rem', fontWeight: 600 }}>
                {isEditing ? (ru ? 'Редактировать источники компании' : 'Edit company sources') : (ru ? 'Добавить компанию и источники' : 'Add company sources')}
              </h3>
              <p className="text-xs text-gray-500 mt-0.5">
                {ru
                  ? 'Одна компания может иметь Facebook, Instagram, Meta Ads и Google источники'
                  : 'One company can have Facebook, Instagram, Meta Ads, and Google sources'}
              </p>
            </div>
          </div>
          <button onClick={onClose} className="p-1.5 rounded-lg hover:bg-gray-100 transition-colors">
            <span className="sr-only">{ru ? 'Закрыть' : 'Close'}</span>
            ×
          </button>
        </div>

        <div className="px-6 py-5 space-y-4 max-h-[70vh] overflow-y-auto">
          <div className="space-y-3">
            <FieldRow
              label={ru ? 'Название компании' : 'Company name'}
              field="companyName"
              placeholder="XYZ"
              autoFocus
            />
            <FieldRow
              label={ru ? 'Сайт компании' : 'Website'}
              field="website"
              placeholder="https://www.xyz.com"
              helper={ru ? 'Используется как основная идентичность компании. Нормализуем до xyz.com.' : 'Used as the main company identity. We will normalize it to xyz.com.'}
            />
            <FieldRow
              label="Facebook Page URL"
              field="facebookPage"
              placeholder="https://www.facebook.com/xyz"
              helper={ru ? 'Вставьте публичную Facebook page URL или page ID.' : 'Paste the public Facebook page URL or page ID.'}
            />
            <FieldRow
              label={ru ? 'Instagram URL или handle' : 'Instagram URL or handle'}
              field="instagramProfile"
              placeholder="https://www.instagram.com/xyz_global"
              helper={ru ? 'Можно вставить URL или @xyz_global. Мы автоматически выделим handle.' : 'You can paste a URL or @xyz_global. We will extract the handle automatically.'}
            />
            <FieldRow
              label="Meta Ads ID"
              field="metaAds"
              placeholder="123456789012"
              helper={ru ? 'Числовой Meta/Facebook Ad Library page ID. Не вставляйте URL.' : 'Numeric Meta/Facebook Ad Library page ID. Do not paste a URL here.'}
            />
            <FieldRow
              label={ru ? 'Google Ads домен' : 'Google Ads domain'}
              field="googleDomain"
              placeholder="xyz.com"
              helper={ru ? 'Домен компании для Google Ads Transparency. https:// не нужен.' : 'Company domain used for Google Ads Transparency results. No https:// needed.'}
            />
          </div>

          <div className={`border rounded-xl p-3.5 text-xs ${preview ? 'bg-blue-50 border-blue-100 text-blue-700' : 'bg-gray-50 border-gray-200 text-gray-500'}`}>
            {preview ||
              (ru
                ? 'Заполните название компании и хотя бы один источник. Источники будут сохранены под одной компанией.'
                : 'Enter the company name and at least one source. Sources will be saved under one company.')}
          </div>
        </div>

        <div className="flex items-center justify-end gap-3 px-6 py-4 border-t border-gray-100 bg-gray-50/50">
          <button
            onClick={onClose}
            className="px-4 py-2 text-sm text-gray-600 hover:text-gray-800 hover:bg-gray-100 rounded-lg transition-colors"
            style={{ fontWeight: 500 }}
          >
            {ru ? 'Отмена' : 'Cancel'}
          </button>
          <button
            onClick={async () => {
              if (!validate()) return
              setSaving(true)
              try {
                await onSubmit(
                  {
                    company_name: values.companyName.trim(),
                    website: values.website.trim() || undefined,
                    sources: {
                      facebook_page: values.facebookPage.trim() || undefined,
                      instagram_profile: values.instagramProfile.trim() || undefined,
                      meta_ads: values.metaAds.trim() || undefined,
                      google_domain: values.googleDomain.trim() || undefined,
                    },
                  },
                  values.companyId,
                )
                onClose()
              } catch (err: any) {
                setErrors({ googleDomain: String(err?.message || (ru ? 'Ошибка сохранения' : 'Failed to save sources')) })
              } finally {
                setSaving(false)
              }
            }}
            disabled={saving}
            className="px-5 py-2 rounded-lg text-sm text-white transition-all disabled:opacity-40 disabled:cursor-not-allowed"
            style={{ fontWeight: 500, background: 'linear-gradient(135deg, #2563eb, #1d4ed8)' }}
          >
            <span className="flex items-center gap-1.5">
              {saving ? <RefreshCw className="w-4 h-4 animate-spin" /> : <Plus className="w-4 h-4" />}
              {isEditing ? (ru ? 'Сохранить источники' : 'Save sources') : (ru ? 'Добавить компанию' : 'Add company')}
            </span>
          </button>
        </div>
      </div>
    </div>
  )
}

function SocialRowActions({
  item,
  ru,
  onToggleActive,
  onEditCompany,
}: {
  item: SocialSourceRow
  ru: boolean
  onToggleActive: (id: string, isActive: boolean) => Promise<void>
  onEditCompany: (item: SocialSourceRow) => void
}) {
  const [open, setOpen] = useState(false)
  const ref = useRef<HTMLDivElement>(null)

  useEffect(() => {
    function handle(event: MouseEvent) {
      if (ref.current && !ref.current.contains(event.target as Node)) setOpen(false)
    }
    document.addEventListener('mousedown', handle)
    return () => document.removeEventListener('mousedown', handle)
  }, [])

  return (
    <div className="relative" ref={ref}>
      <button onClick={() => setOpen(!open)} className={`p-1.5 rounded-lg transition-colors ${open ? 'bg-gray-100' : 'hover:bg-gray-100'}`}>
        <MoreVertical className="w-4 h-4 text-gray-400" />
      </button>
      {open && (
        <div className="absolute right-0 top-full mt-1 w-56 bg-white border border-gray-200 rounded-xl shadow-xl z-30 py-1 overflow-hidden">
          <button
            onClick={() => {
              onEditCompany(item)
              setOpen(false)
            }}
            className="w-full flex items-center gap-2.5 px-3 py-2 text-xs text-gray-700 hover:bg-gray-50 transition-colors"
          >
            <Pencil className="w-3.5 h-3.5 text-blue-500" />
            {ru ? 'Редактировать компанию' : 'Edit company sources'}
          </button>
          <button
            onClick={async () => {
              await onToggleActive(item.id, !item.is_active)
              setOpen(false)
            }}
            className="w-full flex items-center gap-2.5 px-3 py-2 text-xs text-gray-700 hover:bg-gray-50 transition-colors"
          >
            {item.is_active ? (
              <>
                <Clock className="w-3.5 h-3.5 text-amber-500" />
                {ru ? 'Остановить источник' : 'Pause source'}
              </>
            ) : (
              <>
                <CheckCircle2 className="w-3.5 h-3.5 text-emerald-500" />
                {ru ? 'Активировать источник' : 'Activate source'}
              </>
            )}
          </button>
        </div>
      )}
    </div>
  )
}

function stripProtocol(value: string | null | undefined): string {
  return String(value || '').replace(/^https?:\/\//i, '').replace(/^www\./i, '').replace(/\/$/, '')
}

function companyInitialValuesForItem(seed: SocialSourceRow, rows: SocialSourceRow[]): CompanySourcesInitialValues {
  const group = rows.filter((item) => item.entity_id === seed.entity_id)
  const initial = emptyCompanySources()
  initial.companyId = seed.company_id || null
  initial.entityId = seed.entity_id
  initial.companyName = seed.company_name || ''
  initial.website = seed.company_website || ''
  for (const item of group) {
    if (item.source_kind === 'facebook_page') {
      initial.facebookPage = item.display_url || ''
    } else if (item.source_kind === 'instagram_profile') {
      initial.instagramProfile = item.display_url || ''
    } else if (item.source_kind === 'meta_ads') {
      initial.metaAds = item.account_external_id || ''
    } else if (item.source_kind === 'google_domain') {
      initial.googleDomain = stripProtocol(item.display_url) || item.account_external_id || ''
    }
  }
  return initial
}

export function SocialSourcesSection({
  ru,
  addModalOpen,
  onCloseAddModal,
}: {
  ru: boolean
  addModalOpen: boolean
  onCloseAddModal: () => void
}) {
  const [items, setItems] = useState<SocialSourceRow[]>([])
  const [search, setSearch] = useState('')
  const [platformFilter, setPlatformFilter] = useState<SocialPlatformFilter>('all')
  const [statusFilter, setStatusFilter] = useState<'all' | SocialSourceStatus>('all')
  const [loading, setLoading] = useState(true)
  const [busy, setBusy] = useState(false)
  const [error, setError] = useState<string | null>(null)
  const [runtime, setRuntime] = useState<SocialRuntimeStatus | null>(null)
  const [runtimeLoading, setRuntimeLoading] = useState(true)
  const [runtimeBusy, setRuntimeBusy] = useState(false)
  const [runtimeError, setRuntimeError] = useState<string | null>(null)
  const [intervalInput, setIntervalInput] = useState('360')
  const [editingCompany, setEditingCompany] = useState<CompanySourcesInitialValues | null>(null)

  const loadSources = async (quiet = false) => {
    if (!quiet) setLoading(true)
    setError(null)
    try {
      const response = await requestJson<SocialSourceListResponse>('/api/sources/social')
      setItems(response.items || [])
    } catch (err: any) {
      setError(String(err?.message || 'Failed to load social sources'))
    } finally {
      if (!quiet) setLoading(false)
    }
  }

  const loadRuntime = async (quiet = false) => {
    if (!quiet) setRuntimeLoading(true)
    try {
      const response = await requestJson<SocialRuntimeStatus>('/api/social/runtime/status')
      setRuntime(response)
      setIntervalInput(String(response.interval_minutes))
      setRuntimeError(null)
    } catch (err: any) {
      setRuntimeError(String(err?.message || 'Failed to load social runtime'))
    } finally {
      if (!quiet) setRuntimeLoading(false)
    }
  }

  useEffect(() => {
    void loadSources()
    void loadRuntime()
    const timer = window.setInterval(() => {
      void loadSources(true)
      void loadRuntime(true)
    }, 10000)
    return () => window.clearInterval(timer)
  }, [])

  const filtered = useMemo(() => {
    return items.filter((item) => {
      const query = search.trim().toLowerCase()
      const itemStatus = socialRowStatus(item)
      const matchesPlatform = platformFilter === 'all' || item.platform === platformFilter
      const matchesSearch =
        !query ||
        item.company_name.toLowerCase().includes(query) ||
        (item.display_url || '').toLowerCase().includes(query) ||
        (item.account_external_id || '').toLowerCase().includes(query)
      const matchesStatus =
        statusFilter === 'all'
          ? true
          : statusFilter === 'error'
            ? isFailingStatus(itemStatus)
            : itemStatus === statusFilter
      return matchesPlatform && matchesSearch && matchesStatus
    })
  }, [items, platformFilter, search, statusFilter])

  const activeCount = items.filter((item) => item.is_active).length
  const healthyCount = items.filter((item) => item.health_status === 'healthy').length
  const failingCount = items.filter((item) => isFailingStatus(socialRowStatus(item))).length
  const lastResult = runtime?.last_result

  const saveSchedulerInterval = async () => {
    const parsed = Number(intervalInput)
    if (!Number.isFinite(parsed) || parsed < 1) {
      setRuntimeError(ru ? 'Интервал должен быть больше 0' : 'Interval must be greater than 0')
      return
    }

    setRuntimeBusy(true)
    setRuntimeError(null)
    try {
      const response = await requestJson<SocialRuntimeStatus>('/api/social/runtime', {
        method: 'PATCH',
        body: JSON.stringify({ interval_minutes: Math.floor(parsed) }),
      })
      setRuntime(response)
      setIntervalInput(String(response.interval_minutes))
    } catch (err: any) {
      setRuntimeError(String(err?.message || 'Failed to update scheduler interval'))
    } finally {
      setRuntimeBusy(false)
    }
  }

  const startRuntime = async () => {
    setRuntimeBusy(true)
    setRuntimeError(null)
    try {
      const response = await requestJson<SocialRuntimeStatus>('/api/social/runtime/start', { method: 'POST' })
      setRuntime(response)
    } catch (err: any) {
      setRuntimeError(String(err?.message || 'Failed to start social runtime'))
    } finally {
      setRuntimeBusy(false)
    }
  }

  const stopRuntime = async () => {
    setRuntimeBusy(true)
    setRuntimeError(null)
    try {
      const response = await requestJson<SocialRuntimeStatus>('/api/social/runtime/stop', { method: 'POST' })
      setRuntime(response)
    } catch (err: any) {
      setRuntimeError(String(err?.message || 'Failed to stop social runtime'))
    } finally {
      setRuntimeBusy(false)
    }
  }

  const runNow = async () => {
    setRuntimeBusy(true)
    setRuntimeError(null)
    try {
      const response = await requestJson<SocialRuntimeStatus>('/api/social/runtime/run-once', { method: 'POST' })
      setRuntime(response)
      setTimeout(() => {
        void loadRuntime(true)
        void loadSources(true)
      }, 1200)
    } catch (err: any) {
      setRuntimeError(String(err?.message || 'Failed to run social runtime'))
    } finally {
      setRuntimeBusy(false)
    }
  }

  const handleSaveCompanySources = async (payload: SocialCompanySourcesPayload, companyId?: string | null) => {
    setBusy(true)
    setError(null)
    try {
      const path = companyId ? `/api/sources/social/company/${companyId}` : '/api/sources/social/company'
      await requestJson<SocialCompanySourcesResponse>(path, {
        method: companyId ? 'PATCH' : 'POST',
        body: JSON.stringify(payload),
      })
      await loadSources(true)
      setEditingCompany(null)
    } catch (err) {
      throw err
    } finally {
      setBusy(false)
    }
  }

  const setSourceActive = async (id: string, isActive: boolean) => {
    setBusy(true)
    setError(null)
    try {
      await requestJson<SocialSourceUpdateResponse>(`/api/sources/social/${id}`, {
        method: 'PATCH',
        body: JSON.stringify({ is_active: isActive }),
      })
      await loadSources(true)
    } catch (err: any) {
      setError(String(err?.message || 'Update failed'))
    } finally {
      setBusy(false)
    }
  }

  const editCompanySources = (item: SocialSourceRow) => {
    setEditingCompany(companyInitialValuesForItem(item, items))
  }

  const closeCompanySourcesModal = () => {
    setEditingCompany(null)
    onCloseAddModal()
  }

  return (
    <>
      {error && (
        <div className="mb-4 flex items-center gap-2 rounded-xl border border-red-200 bg-red-50 px-3 py-2 text-sm text-red-700">
          <AlertCircle className="w-4 h-4" />
          <span>{error}</span>
        </div>
      )}

      <div className="grid grid-cols-2 md:grid-cols-4 gap-3 mb-6">
        <div className="bg-white rounded-xl border border-gray-200 p-4">
          <div className="flex items-center gap-2 mb-1">
            <Database className="w-4 h-4 text-blue-600" />
            <span className="text-xs text-gray-500">{ru ? 'Всего источников' : 'Total Sources'}</span>
          </div>
          <span className="text-xl text-gray-900" style={{ fontWeight: 600 }}>{items.length}</span>
        </div>
        <div className="bg-white rounded-xl border border-gray-200 p-4">
          <div className="flex items-center gap-2 mb-1">
            <CheckCircle2 className="w-4 h-4 text-emerald-600" />
            <span className="text-xs text-gray-500">{ru ? 'Активных' : 'Active'}</span>
          </div>
          <span className="text-xl text-gray-900" style={{ fontWeight: 600 }}>{activeCount}</span>
        </div>
        <div className="bg-white rounded-xl border border-gray-200 p-4">
          <div className="flex items-center gap-2 mb-1">
            <CheckCircle2 className="w-4 h-4 text-cyan-600" />
            <span className="text-xs text-gray-500">{ru ? 'Здоровых' : 'Healthy'}</span>
          </div>
          <span className="text-xl text-gray-900" style={{ fontWeight: 600 }}>{healthyCount}</span>
        </div>
        <div className="bg-white rounded-xl border border-gray-200 p-4">
          <div className="flex items-center gap-2 mb-1">
            <AlertCircle className="w-4 h-4 text-amber-600" />
            <span className="text-xs text-gray-500">{ru ? 'С ошибками' : 'Failing'}</span>
          </div>
          <span className="text-xl text-gray-900" style={{ fontWeight: 600 }}>{failingCount}</span>
        </div>
      </div>

      <div className="bg-white rounded-xl border border-gray-200 p-4 md:p-5 mb-6">
        <div className="flex items-start justify-between gap-4 flex-wrap">
          <div>
            <h2 className="text-gray-900" style={{ fontSize: '1rem', fontWeight: 600 }}>
              {ru ? 'Планировщик social scraping' : 'Social Scheduler'}
            </h2>
            <p className="text-xs text-gray-500 mt-1">
              {ru
                ? 'Управление social worker для сбора, AI-анализа и Neo4j синхронизации'
                : 'Control the social worker for collection, AI analysis, and Neo4j sync'}
            </p>
          </div>
          <div className={`inline-flex items-center gap-2 px-2.5 py-1 rounded-full text-xs ${runtime?.is_active ? 'bg-emerald-50 text-emerald-700' : 'bg-gray-100 text-gray-600'}`} style={{ fontWeight: 500 }}>
            <span className={`w-2 h-2 rounded-full ${runtime?.is_active ? 'bg-emerald-500' : 'bg-gray-400'}`} />
            {runtime?.is_active ? (ru ? 'Активен' : 'Active') : (ru ? 'Остановлен' : 'Stopped')}
            {runtime?.running_now ? ` · ${ru ? 'идет запуск' : 'running'}` : ''}
          </div>
        </div>

        {runtimeError && (
          <div className="mt-3 flex items-center gap-2 rounded-lg border border-red-200 bg-red-50 px-3 py-2 text-xs text-red-700">
            <AlertCircle className="w-3.5 h-3.5" />
            <span>{runtimeError}</span>
          </div>
        )}

        <div className="mt-4 grid grid-cols-1 lg:grid-cols-2 gap-4">
          <div className="border border-gray-100 rounded-lg p-3 bg-gray-50/50">
            <label className="text-xs text-gray-500 block mb-1.5" style={{ fontWeight: 500 }}>
              {ru ? 'Интервал запуска (минуты)' : 'Run interval (minutes)'}
            </label>
            <div className="flex items-center gap-2">
              <input
                type="number"
                min={1}
                value={intervalInput}
                onChange={(event) => setIntervalInput(event.target.value)}
                className="w-28 px-3 py-2 border border-gray-200 rounded-lg text-sm focus:outline-none focus:ring-2 focus:ring-blue-500"
              />
              <button
                onClick={saveSchedulerInterval}
                disabled={runtimeBusy || runtimeLoading}
                className="px-3 py-2 rounded-lg text-xs text-white bg-slate-800 hover:bg-slate-700 disabled:opacity-50 transition-colors"
                style={{ fontWeight: 500 }}
              >
                {ru ? 'Сохранить' : 'Save'}
              </button>
              <button
                onClick={() => {
                  void loadRuntime()
                  void loadSources()
                }}
                disabled={runtimeBusy}
                className="px-2.5 py-2 rounded-lg text-xs text-gray-600 bg-gray-100 hover:bg-gray-200 disabled:opacity-50 transition-colors"
              >
                <RefreshCw className={`w-3.5 h-3.5 ${runtimeLoading ? 'animate-spin' : ''}`} />
              </button>
            </div>
          </div>

          <div className="border border-gray-100 rounded-lg p-3 bg-gray-50/50">
            <div className="flex items-center gap-2 flex-wrap">
              <button
                onClick={startRuntime}
                disabled={runtimeBusy || runtimeLoading || !!runtime?.is_active}
                className="px-3 py-2 rounded-lg text-xs text-white bg-emerald-600 hover:bg-emerald-700 disabled:opacity-50 transition-colors"
                style={{ fontWeight: 500 }}
              >
                {ru ? 'Запустить' : 'Start'}
              </button>
              <button
                onClick={stopRuntime}
                disabled={runtimeBusy || runtimeLoading || !runtime?.is_active}
                className="px-3 py-2 rounded-lg text-xs text-white bg-amber-500 hover:bg-amber-600 disabled:opacity-50 transition-colors"
                style={{ fontWeight: 500 }}
              >
                {ru ? 'Остановить' : 'Stop'}
              </button>
              <button
                onClick={runNow}
                disabled={runtimeBusy || runtimeLoading || runtime?.running_now}
                className="px-3 py-2 rounded-lg text-xs text-white bg-blue-600 hover:bg-blue-700 disabled:opacity-50 transition-colors"
                style={{ fontWeight: 500 }}
              >
                {runtime?.running_now ? (ru ? 'Выполняется...' : 'Running...') : (ru ? 'Запустить сейчас' : 'Run now')}
              </button>
            </div>
          </div>
        </div>

        <div className="mt-3 grid grid-cols-1 md:grid-cols-3 gap-3 text-xs text-gray-500">
          <div className="border border-gray-100 rounded-lg px-3 py-2 bg-white">
            <span className="text-gray-400">{ru ? 'Последний запуск:' : 'Last run:'}</span>
            <div className="text-gray-700 mt-0.5" style={{ fontWeight: 500 }}>
              {formatDateTime(runtime?.last_run_started_at || null, ru)}
            </div>
          </div>
          <div className="border border-gray-100 rounded-lg px-3 py-2 bg-white">
            <span className="text-gray-400">{ru ? 'Следующий запуск:' : 'Next run:'}</span>
            <div className="text-gray-700 mt-0.5" style={{ fontWeight: 500 }}>
              {formatDateTime(runtime?.next_run_at || null, ru)}
            </div>
          </div>
          <div className="border border-gray-100 rounded-lg px-3 py-2 bg-white">
            <span className="text-gray-400">{ru ? 'Последний результат:' : 'Last result:'}</span>
            <div className="text-gray-700 mt-0.5" style={{ fontWeight: 500 }}>
              {lastResultLabel(runtime?.last_result, runtime, ru)}
            </div>
          </div>
        </div>

        <div className="mt-4 border border-gray-100 rounded-xl p-3 md:p-4 bg-gray-50/40">
          <div className="flex items-center justify-between gap-3 flex-wrap">
            <div>
              <h3 className="text-gray-900" style={{ fontSize: '0.95rem', fontWeight: 600 }}>
                {ru ? 'Последний цикл обработки' : 'Last Processing Cycle'}
              </h3>
              <p className="text-xs text-gray-500 mt-0.5">
                {ru
                  ? 'Сколько данных собрано, обработано AI и синхронизировано в Neo4j'
                  : 'How much data was collected, AI-processed, and synced to Neo4j'}
              </p>
            </div>
            <div className="text-xs text-gray-500">
              {ru ? 'Успешный цикл:' : 'Last success:'} {formatDateTime(runtime?.last_success_at || null, ru)}
            </div>
          </div>

          <div className="mt-3 grid grid-cols-1 md:grid-cols-2 lg:grid-cols-4 gap-3">
            <div className="rounded-lg border border-gray-100 bg-white p-3">
              <div className="flex items-center gap-2">
                <Database className="w-4 h-4 text-blue-600" />
                <span className="text-xs text-gray-500">{ru ? 'Собрано' : 'Collected'}</span>
              </div>
              <div className="mt-2 text-sm text-gray-900" style={{ fontWeight: 700 }}>
                {(lastResult?.activities_collected ?? 0).toLocaleString()}
              </div>
              <div className="mt-1 text-[11px] text-gray-500">
                {(lastResult?.accounts_processed ?? 0).toLocaleString()} {ru ? 'источников обработано' : 'sources processed'}
              </div>
            </div>

            <div className="rounded-lg border border-gray-100 bg-white p-3">
              <div className="flex items-center gap-2">
                <Brain className="w-4 h-4 text-violet-600" />
                <span className="text-xs text-gray-500">{ru ? 'AI обработка' : 'AI Processed'}</span>
              </div>
              <div className="mt-2 text-sm text-gray-900" style={{ fontWeight: 700 }}>
                {(lastResult?.activities_analyzed ?? 0).toLocaleString()}
              </div>
              <div className="mt-1 text-[11px] text-gray-500">
                {(lastResult?.analysis_failures ?? 0).toLocaleString()} {ru ? 'ошибок' : 'failures'}
              </div>
            </div>

            <div className="rounded-lg border border-gray-100 bg-white p-3">
              <div className="flex items-center gap-2">
                <Network className="w-4 h-4 text-cyan-600" />
                <span className="text-xs text-gray-500">{ru ? 'Neo4j синхронизация' : 'Neo4j Synced'}</span>
              </div>
              <div className="mt-2 text-sm text-gray-900" style={{ fontWeight: 700 }}>
                {(lastResult?.activities_graph_synced ?? 0).toLocaleString()}
              </div>
              <div className="mt-1 text-[11px] text-gray-500">
                {(lastResult?.graph_failures ?? 0).toLocaleString()} {ru ? 'ошибок' : 'failures'}
              </div>
            </div>

            <div className="rounded-lg border border-gray-100 bg-white p-3">
              <div className="flex items-center gap-2">
                <Clock className="w-4 h-4 text-amber-600" />
                <span className="text-xs text-gray-500">{ru ? 'Collect ошибки' : 'Collect Failures'}</span>
              </div>
              <div className="mt-2 text-sm text-gray-900" style={{ fontWeight: 700 }}>
                {(lastResult?.collect_failures ?? 0).toLocaleString()}
              </div>
              <div className="mt-1 text-[11px] text-gray-500">
                {(lastResult?.accounts_total ?? 0).toLocaleString()} {ru ? 'источников в цикле' : 'sources in cycle'}
              </div>
            </div>
          </div>
        </div>

        {!!runtime?.last_error && (
          <div className="mt-3 rounded-lg border border-red-200 bg-red-50 px-3 py-2 text-xs text-red-700">
            <span style={{ fontWeight: 500 }}>{ru ? 'Ошибка runtime:' : 'Runtime error:'}</span> {runtime.last_error}
          </div>
        )}
      </div>

      <div className="bg-white rounded-xl border border-gray-200 overflow-hidden">
        <div className="flex items-center justify-between gap-3 px-4 py-3 border-b border-gray-100 flex-wrap">
          <div className="relative flex-1 min-w-[200px] max-w-sm">
            <Search className="absolute left-3 top-1/2 -translate-y-1/2 w-4 h-4 text-gray-400" />
            <input
              type="text"
              value={search}
              onChange={(event) => setSearch(event.target.value)}
              placeholder={ru ? 'Поиск по компании или URL...' : 'Search by company or URL...'}
              className="w-full pl-9 pr-4 py-2 border border-gray-200 rounded-lg text-sm focus:outline-none focus:ring-2 focus:ring-blue-500 focus:border-blue-500"
            />
          </div>

          <div className="flex items-center gap-1 flex-wrap">
            {(['all', 'facebook', 'instagram', 'google'] as const).map((platform) => (
              <button
                key={platform}
                onClick={() => setPlatformFilter(platform)}
                className={`px-3 py-1.5 rounded-lg text-xs transition-colors ${
                  platformFilter === platform ? 'bg-blue-600 text-white' : 'bg-blue-50 text-blue-700 hover:bg-blue-100'
                }`}
                style={{ fontWeight: 500 }}
              >
                {platform === 'all' ? (ru ? 'Все сети' : 'All platforms') : platformLabel(platform, ru)}
              </button>
            ))}
          </div>

          <div className="flex items-center gap-1 flex-wrap">
            {(['all', 'active', 'paused', 'error'] as const).map((status) => (
              <button
                key={status}
                onClick={() => setStatusFilter(status)}
                className={`px-3 py-1.5 rounded-lg text-xs transition-colors ${
                  statusFilter === status ? 'bg-slate-800 text-white' : 'bg-gray-100 text-gray-500 hover:bg-gray-200'
                }`}
                style={{ fontWeight: 500 }}
              >
                {status === 'all'
                  ? ru
                    ? 'Все'
                    : 'All'
                  : status === 'active'
                    ? ru
                      ? 'Активные'
                      : 'Active'
                    : status === 'paused'
                      ? ru
                        ? 'На паузе'
                        : 'Paused'
                      : ru
                        ? 'Ошибки'
                        : 'Errors'}
              </button>
            ))}
          </div>

          <div className="flex items-center gap-2">
            <span className="text-xs text-gray-400">
              {filtered.length} {ru ? 'из' : 'of'} {items.length}
            </span>
          </div>
        </div>

        <div className="overflow-x-auto">
          <table className="w-full">
            <thead>
              <tr className="border-b border-gray-100">
                <th className="text-left text-xs text-gray-500 px-3 py-3" style={{ fontWeight: 500 }}>
                  {ru ? 'Источник' : 'Source'}
                </th>
                <th className="text-left text-xs text-gray-500 px-3 py-3 hidden md:table-cell" style={{ fontWeight: 500 }}>
                  {ru ? 'Платформа' : 'Platform'}
                </th>
                <th className="text-center text-xs text-gray-500 px-3 py-3" style={{ fontWeight: 500 }}>
                  {ru ? 'Статус' : 'Status'}
                </th>
                <th className="text-right text-xs text-gray-500 px-3 py-3 hidden md:table-cell" style={{ fontWeight: 500 }}>
                  {ru ? 'Обновлено' : 'Updated'}
                </th>
                <th className="w-10 px-3 py-3" />
              </tr>
            </thead>
            <tbody>
              {loading ? (
                <tr>
                  <td colSpan={5} className="text-center py-16">
                    <RefreshCw className="w-6 h-6 text-gray-300 mx-auto mb-2 animate-spin" />
                    <p className="text-sm text-gray-500">{ru ? 'Загрузка social источников...' : 'Loading social sources...'}</p>
                  </td>
                </tr>
              ) : filtered.length === 0 ? (
                <tr>
                  <td colSpan={5} className="text-center py-16">
                    <Database className="w-8 h-8 text-gray-200 mx-auto mb-2" />
                    <p className="text-sm text-gray-500">
                      {search ? (ru ? 'Ничего не найдено' : 'No sources found') : (ru ? 'Нет добавленных social источников' : 'No social sources added yet')}
                    </p>
                  </td>
                </tr>
              ) : (
                filtered.map((item) => {
                  const status = socialRowStatus(item)
                  return (
                    <tr key={item.id} className="border-b border-gray-50 transition-colors hover:bg-gray-50">
                      <td className="px-3 py-3">
                        <div className="flex items-center gap-3">
                          <SourceBadge platform={item.platform} />
                          <div className="min-w-0">
                            <span className="text-sm text-gray-900 block truncate" style={{ fontWeight: 500 }}>{item.company_name}</span>
                            <span className="text-xs text-gray-400 block truncate">{item.display_url || item.account_external_id || '—'}</span>
                            <div className="mt-1 flex items-center gap-2">
                              <span className="inline-flex items-center rounded-full bg-slate-100 px-2 py-0.5 text-[11px] text-slate-600">
                                {sourceTypeLabel(item.source_kind, ru)}
                              </span>
                            </div>
                            {item.last_error && (
                              <span className="text-xs text-red-500 block truncate max-w-[340px]">{item.last_error}</span>
                            )}
                          </div>
                        </div>
                      </td>
                      <td className="px-3 py-3 hidden md:table-cell">
                        <span className="inline-flex items-center gap-1.5 rounded-full bg-blue-50 px-2.5 py-1 text-xs text-blue-700" style={{ fontWeight: 500 }}>
                          {platformLabel(item.platform, ru)}
                        </span>
                      </td>
                      <td className="px-3 py-3 text-center">
                        <SocialStatusBadge status={status} ru={ru} />
                      </td>
                      <td className="px-3 py-3 text-right hidden md:table-cell">
                        <div className="flex items-center justify-end gap-1.5 text-xs text-gray-400">
                          {item.is_active ? <RefreshCw className="w-3 h-3 text-emerald-400" /> : <Clock className="w-3 h-3 text-amber-400" />}
                          <span>{relativeTime(item.last_collected_at, ru)}</span>
                        </div>
                      </td>
                      <td className="px-3 py-3">
                        <SocialRowActions item={item} ru={ru} onToggleActive={setSourceActive} onEditCompany={editCompanySources} />
                      </td>
                    </tr>
                  )
                })
              )}
            </tbody>
          </table>
        </div>

        <div className="flex items-center justify-between px-4 py-3 border-t border-gray-100 bg-gray-50/50">
          <span className="text-xs text-gray-400">
            {ru ? `Показано ${filtered.length} из ${items.length} источников` : `Showing ${filtered.length} of ${items.length} sources`}
          </span>
          <span className="text-xs text-gray-400 flex items-center gap-1.5">
            <RefreshCw className="w-3 h-3" />
            {ru ? 'Автообновление каждые 10 сек' : 'Auto-refresh every 10s'}
          </span>
        </div>
      </div>

      <CompanySourcesModal
        open={addModalOpen || !!editingCompany}
        onClose={closeCompanySourcesModal}
        ru={ru}
        onSubmit={handleSaveCompanySources}
        initialValues={editingCompany}
      />
    </>
  )
}
