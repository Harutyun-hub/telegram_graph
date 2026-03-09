import { useEffect, useMemo, useState } from 'react';
import { Search, Users, User, ChevronRight, ChevronLeft, X, Clock, MessageCircle, ThumbsUp, Radio, Hash, ArrowUpDown, MapPin, Calendar, Activity, Star } from 'lucide-react';
import { ResponsiveContainer, BarChart, Bar, XAxis, YAxis, Tooltip, CartesianGrid, AreaChart, Area, Cell } from 'recharts';
import { useLanguage } from '../contexts/LanguageContext';
import { useAudienceDetailData } from '../services/detailData';
import type { AudienceMember } from '../types/data';

const typeColors: Record<string, string> = {
  General: '#3b82f6', Work: '#f59e0b', Family: '#ec4899',
  Housing: '#ef4444', Business: '#10b981', Lifestyle: '#8b5cf6', Legal: '#6b7280',
};

const roleColors: Record<string, { bg: string; text: string }> = {
  Admin: { bg: 'bg-red-50', text: 'text-red-700' },
  Moderator: { bg: 'bg-amber-50', text: 'text-amber-700' },
  Active: { bg: 'bg-emerald-50', text: 'text-emerald-700' },
  Member: { bg: 'bg-gray-50', text: 'text-gray-500' },
};

const roleMapRU: Record<string, string> = {
  Admin: 'Администратор', Moderator: 'Модератор', Active: 'Активный', Member: 'Участник',
};

const personaMapRU: Record<string, string> = {
  'IT Relocant': 'IT-релокант', 'Young Family': 'Молодая семья',
  'Entrepreneur': 'Предприниматель', 'Digital Nomad': 'Цифровой кочевник',
  'Established Expat': 'Укоренившийся экспат',
};

const integrationMapRU: Record<string, string> = {
  'Russian Only': 'Только по-русски', 'Bilingual Bubble': 'Двуязычный пузырь',
  'Learning & Mixing': 'Учится и смешивается', 'Fully Integrated': 'Полностью интегрирован',
};

const genderFilters = ['All', 'Male', 'Female'] as const;

// ── COMPONENT ──

export function AudiencePage() {
  const { lang } = useLanguage();
  const ru = lang === 'ru';
  const {
    data: allAudience,
    loading: audienceLoading,
    error: audienceError,
    refresh: refreshAudience,
  } = useAudienceDetailData();

  const [searchQuery, setSearchQuery] = useState('');
  const [selectedMember, setSelectedMember] = useState<AudienceMember | null>(null);
  const [genderFilter, setGenderFilter] = useState<string>('All');
  const [sortBy, setSortBy] = useState<'messages' | 'helpScore' | 'reactions' | 'recent'>('messages');
  const [activeTab, setActiveTab] = useState<'profile' | 'channels' | 'activity'>('profile');

  const tabLabels = ru
    ? [['profile', 'Профиль'], ['channels', 'Каналы'], ['activity', 'Активность']] as const
    : [['profile', 'Profile'], ['channels', 'Channels'], ['activity', 'Activity']] as const;

  const sortLabels = ru
    ? [['messages', 'Сообщения'], ['helpScore', 'Рейтинг'], ['reactions', 'Реакции']] as const
    : [['messages', 'Messages'], ['helpScore', 'Help Score'], ['reactions', 'Reactions']] as const;

  const filtered = useMemo(() => {
    return allAudience
      .filter((m) => {
        if (genderFilter !== 'All' && m.gender !== genderFilter) return false;
        if (searchQuery) {
          const q = searchQuery.toLowerCase();
          return m.displayName.toLowerCase().includes(q) ||
            m.username.toLowerCase().includes(q) ||
            m.interests.some(i => i.toLowerCase().includes(q)) ||
            m.channels.some(c => c.name.toLowerCase().includes(q));
        }
        return true;
      })
      .sort((a, b) => {
        if (sortBy === 'messages') return b.totalMessages - a.totalMessages;
        if (sortBy === 'helpScore') return b.helpScore - a.helpScore;
        if (sortBy === 'reactions') return b.totalReactions - a.totalReactions;
        return 0; // recent — keep order
      });
  }, [allAudience, searchQuery, genderFilter, sortBy]);

  // Summary stats
  const genderStats = {
    Male: allAudience.filter(m => m.gender === 'Male').length,
    Female: allAudience.filter(m => m.gender === 'Female').length,
    Unknown: allAudience.filter(m => m.gender === 'Unknown').length,
  };

  useEffect(() => {
    if (!selectedMember) return;
    const fresh = allAudience.find((member) => member.id === selectedMember.id);
    if (!fresh) {
      setSelectedMember(null);
      return;
    }
    if (fresh !== selectedMember) {
      setSelectedMember(fresh);
    }
  }, [allAudience, selectedMember]);

  return (
    <div className="flex flex-col md:flex-row h-full">
      {/* LEFT: Member List — hidden on mobile when a member is selected */}
      <div className={`${selectedMember ? 'hidden md:flex md:w-[400px]' : 'flex flex-1'} flex-col border-r border-gray-200 bg-white transition-all`}>
        {/* Header */}
        <div className="px-6 py-5 border-b border-gray-200">
          <div className="flex items-center justify-between mb-1">
            <div>
              <h1 className="text-gray-900" style={{ fontSize: '1.25rem', fontWeight: 600 }}>
                {ru ? 'Аудитория' : 'Audience'}
              </h1>
              <p className="text-xs text-gray-500 mt-0.5">
                {allAudience.length} {ru ? 'участников отслеживается' : 'members tracked'} &middot;
                {' '}{genderStats.Male} {ru ? 'мужчин' : 'male'} &middot; {genderStats.Female} {ru ? 'женщин' : 'female'}
              </p>
            </div>
          </div>

          {/* Gender Summary Bar */}
          <div className="flex h-2 rounded-full overflow-hidden mb-3 mt-3">
            <div className="bg-blue-400" style={{ width: `${allAudience.length ? (genderStats.Male / allAudience.length) * 100 : 0}%` }} />
            <div className="bg-pink-400" style={{ width: `${allAudience.length ? (genderStats.Female / allAudience.length) * 100 : 0}%` }} />
            {genderStats.Unknown > 0 && <div className="bg-gray-300" style={{ width: `${(genderStats.Unknown / allAudience.length) * 100}%` }} />}
          </div>
          <div className="flex items-center gap-4 mb-3 text-xs">
            <span className="flex items-center gap-1.5"><span className="w-2 h-2 rounded-full bg-blue-400" />{ru ? 'Мужчины' : 'Male'} {genderStats.Male}</span>
            <span className="flex items-center gap-1.5"><span className="w-2 h-2 rounded-full bg-pink-400" />{ru ? 'Женщины' : 'Female'} {genderStats.Female}</span>
            {genderStats.Unknown > 0 && <span className="flex items-center gap-1.5"><span className="w-2 h-2 rounded-full bg-gray-300" />{ru ? 'Неизвестно' : 'Unknown'} {genderStats.Unknown}</span>}
          </div>

          {/* Search */}
          <div className="relative mb-3">
            <Search className="absolute left-3 top-1/2 -translate-y-1/2 w-4 h-4 text-gray-400" />
            <input
              type="text"
              value={searchQuery}
              onChange={(e) => setSearchQuery(e.target.value)}
              placeholder={ru ? 'Поиск по имени, интересам или каналу...' : 'Search by name, interest, or channel...'}
              className="w-full pl-9 pr-4 py-2 border border-gray-200 rounded-lg text-sm focus:outline-none focus:ring-2 focus:ring-blue-500 focus:border-transparent bg-gray-50"
            />
          </div>

          {/* Filters Row */}
          <div className="flex items-center gap-3">
            {/* Gender */}
            <div className="flex gap-1">
              {genderFilters.map((g) => (
                <button
                  key={g}
                  onClick={() => setGenderFilter(g)}
                  className={`px-2.5 py-1 rounded-full text-xs transition-colors ${
                    genderFilter === g ? 'bg-slate-800 text-white' : 'bg-gray-100 text-gray-600 hover:bg-gray-200'
                  }`}
                  style={{ fontWeight: genderFilter === g ? 500 : 400 }}
                >
                  {g === 'All' ? (ru ? 'Все' : 'All') : g === 'Male' ? (ru ? '♂ Мужчины' : '♂ Male') : (ru ? '♀ Женщины' : '♀ Female')}
                </button>
              ))}
            </div>

            <div className="w-px h-5 bg-gray-200" />

            {/* Sort */}
            <div className="flex items-center gap-1">
              <ArrowUpDown className="w-3 h-3 text-gray-400" />
              {sortLabels.map(([key, label]) => (
                <button
                  key={key}
                  onClick={() => setSortBy(key)}
                  className={`text-xs px-2 py-0.5 rounded ${sortBy === key ? 'bg-blue-50 text-blue-700' : 'text-gray-500 hover:text-gray-700'}`}
                  style={{ fontWeight: sortBy === key ? 500 : 400 }}
                >
                  {label}
                </button>
              ))}
            </div>
          </div>
        </div>

        {/* Member List */}
        <div className="flex-1 overflow-y-auto">
          {audienceError && (
            <div className="px-6 py-3 border-b border-red-100 bg-red-50 flex items-center justify-between gap-2">
              <span className="text-xs text-red-700 truncate">
                {ru ? 'Не удалось обновить аудиторию. Показаны последние сохранённые данные.' : 'Unable to refresh audience. Showing last saved data.'}
              </span>
              <button
                onClick={refreshAudience}
                className="text-xs text-red-700 hover:text-red-800 underline"
                style={{ fontWeight: 600 }}
              >
                {ru ? 'Повторить' : 'Retry'}
              </button>
            </div>
          )}
          {audienceLoading && allAudience.length === 0 && (
            <div className="flex flex-col items-center justify-center py-12 text-gray-400">
              <Users className="w-8 h-8 mb-2" />
              <p className="text-sm">{ru ? 'Загружаем аудиторию...' : 'Loading audience...'}</p>
            </div>
          )}
          {filtered.map((member) => (
            <button
              key={member.id}
              onClick={() => { setSelectedMember(member); setActiveTab('profile'); }}
              className={`w-full text-left px-6 py-4 border-b border-gray-100 hover:bg-gray-50 transition-colors ${
                selectedMember?.id === member.id ? 'bg-blue-50 border-l-3 border-l-blue-500' : ''
              }`}
            >
              <div className="flex items-center gap-3">
                {/* Avatar */}
                <div className={`w-10 h-10 rounded-full flex items-center justify-center flex-shrink-0 ${
                  member.gender === 'Female' ? 'bg-pink-100' : 'bg-blue-100'
                }`}>
                  <User className={`w-4.5 h-4.5 ${member.gender === 'Female' ? 'text-pink-500' : 'text-blue-500'}`} />
                </div>

                <div className="flex-1 min-w-0">
                  <div className="flex items-center gap-2">
                    <span className="text-sm text-gray-900 truncate" style={{ fontWeight: 500 }}>{member.displayName}</span>
                    <span className="text-xs text-gray-400">{member.username}</span>
                  </div>
                  {/* Gender + Persona + Integration */}
                  <div className="flex items-center gap-2 mt-0.5">
                    <span className={`text-xs px-1.5 py-0.5 rounded ${member.gender === 'Female' ? 'bg-pink-50 text-pink-600' : 'bg-blue-50 text-blue-600'}`} style={{ fontSize: '10px', fontWeight: 500 }}>
                      {member.gender === 'Female' ? '♀' : '♂'} {member.gender}
                    </span>
                    <span className="text-xs text-gray-400">
                      {ru ? (personaMapRU[member.persona] || member.persona) : member.persona}
                    </span>
                  </div>
                  {/* Channels + Stats */}
                  <div className="flex items-center gap-3 mt-1 text-xs text-gray-400">
                    <span className="flex items-center gap-0.5"><Radio className="w-3 h-3" />{member.channels.length} {ru ? 'кан.' : 'ch'}</span>
                    <span className="flex items-center gap-0.5"><MessageCircle className="w-3 h-3" />{member.totalMessages.toLocaleString()}</span>
                    <span className="flex items-center gap-0.5"><Star className="w-3 h-3" />{member.helpScore}</span>
                  </div>
                </div>
                <ChevronRight className="w-4 h-4 text-gray-300 flex-shrink-0" />
              </div>
            </button>
          ))}
          {!audienceLoading && filtered.length === 0 && (
            <div className="flex flex-col items-center justify-center py-12 text-gray-400">
              <Users className="w-8 h-8 mb-2" />
              <p className="text-sm">{ru ? 'Участники не найдены' : 'No members match your filters'}</p>
            </div>
          )}
        </div>
      </div>

      {/* RIGHT: Member Detail */}
      {selectedMember ? (
        <div className="flex-1 flex flex-col overflow-hidden bg-gray-50">
          {/* Mobile back bar */}
          <div className="md:hidden flex items-center gap-2 px-4 py-3 bg-white border-b border-gray-200 flex-shrink-0">
            <button
              onClick={() => setSelectedMember(null)}
              className="flex items-center gap-1.5 text-sm text-blue-600 active:opacity-70 transition-opacity"
              style={{ fontWeight: 500 }}
            >
              <ChevronLeft className="w-4 h-4" />
              {ru ? 'Аудитори' : 'All Members'}
            </button>
          </div>
          {/* Member Header */}
          <div className="bg-white border-b border-gray-200 px-6 py-5">
            <div className="flex items-center justify-between mb-3">
              <div className="flex items-center gap-4">
                <div className={`w-14 h-14 rounded-xl flex items-center justify-center ${
                  selectedMember.gender === 'Female' ? 'bg-pink-100' : 'bg-blue-100'
                }`}>
                  <User className={`w-7 h-7 ${selectedMember.gender === 'Female' ? 'text-pink-500' : 'text-blue-500'}`} />
                </div>
                <div>
                  <div className="flex items-center gap-2">
                    <h2 className="text-gray-900" style={{ fontSize: '1.1rem', fontWeight: 600 }}>{selectedMember.displayName}</h2>
                    <span className="text-sm text-gray-400">{selectedMember.username}</span>
                  </div>
                  <div className="flex items-center gap-2 mt-1">
                    <span className={`text-xs px-2 py-0.5 rounded-full ${selectedMember.gender === 'Female' ? 'bg-pink-50 text-pink-600' : 'bg-blue-50 text-blue-600'}`} style={{ fontWeight: 500 }}>
                      {selectedMember.gender === 'Female' ? '♀' : '♂'} {selectedMember.gender}
                    </span>
                    <span className="text-xs text-gray-400">{ru ? 'Возраст:' : 'Age:'} {selectedMember.age}</span>
                    <span className="text-xs text-gray-400 flex items-center gap-0.5"><MapPin className="w-3 h-3" />{selectedMember.origin} → {selectedMember.location}</span>
                  </div>
                </div>
              </div>
              <button onClick={() => setSelectedMember(null)} className="p-2 hover:bg-gray-100 rounded-lg transition-colors">
                <X className="w-5 h-5 text-gray-400" />
              </button>
            </div>

            {/* Stats Row */}
            <div className="grid grid-cols-3 md:grid-cols-6 gap-2">
              {[
                { label: ru ? 'Сообщений' : 'Messages', value: selectedMember.totalMessages.toLocaleString() },
                { label: ru ? 'Реакций' : 'Reactions', value: selectedMember.totalReactions.toLocaleString() },
                { label: ru ? 'Рейтинг' : 'Help Score', value: selectedMember.helpScore.toString() },
                { label: ru ? 'Каналов' : 'Channels', value: selectedMember.channels.length.toString() },
                { label: ru ? 'Тип' : 'Persona', value: ru ? (personaMapRU[selectedMember.persona] || selectedMember.persona).split(' ')[0] : selectedMember.persona.split(' ')[0] },
                { label: ru ? 'Интеграция' : 'Integration', value: ru ? (integrationMapRU[selectedMember.integrationLevel] || selectedMember.integrationLevel).split(' ')[0] : selectedMember.integrationLevel.split(' ')[0] },
              ].map((stat) => (
                <div key={stat.label} className="bg-gray-50 rounded-lg px-2.5 py-2">
                  <p className="text-xs text-gray-500" style={{ fontSize: '10px' }}>{stat.label}</p>
                  <p className="text-sm text-gray-900 truncate" style={{ fontWeight: 600 }}>{stat.value}</p>
                </div>
              ))}
            </div>

            {/* Tabs */}
            <div className="flex gap-1 mt-4 border-b border-gray-100 -mx-6 px-6">
              {tabLabels.map(([key, label]) => (
                <button
                  key={key}
                  onClick={() => setActiveTab(key)}
                  className={`px-4 py-2 text-xs border-b-2 transition-colors ${
                    activeTab === key ? 'border-blue-500 text-blue-700' : 'border-transparent text-gray-500 hover:text-gray-700'
                  }`}
                  style={{ fontWeight: activeTab === key ? 600 : 400 }}
                >
                  {label}
                </button>
              ))}
            </div>
          </div>

          {/* Tab Content */}
          <div className="flex-1 overflow-y-auto px-6 py-4">

            {/* ── PROFILE & INTERESTS ── */}
            {activeTab === 'profile' && (
              <div className="space-y-4">
                {/* Bio Card */}
                <div className="bg-white rounded-xl border border-gray-200 p-4">
                  <h4 className="text-sm text-gray-900 mb-3" style={{ fontWeight: 600 }}>
                    {ru ? 'Профиль участника' : 'Profile Summary'}
                  </h4>
                  <div className="grid grid-cols-1 md:grid-cols-2 gap-3 text-xs">
                    <div className="flex items-center gap-2">
                      <User className="w-3.5 h-3.5 text-gray-400" />
                      <span className="text-gray-500">{ru ? 'Пол:' : 'Gender:'}</span>
                      <span className="text-gray-900" style={{ fontWeight: 500 }}>{selectedMember.gender}</span>
                    </div>
                    <div className="flex items-center gap-2">
                      <Calendar className="w-3.5 h-3.5 text-gray-400" />
                      <span className="text-gray-500">{ru ? 'Возраст:' : 'Age range:'}</span>
                      <span className="text-gray-900" style={{ fontWeight: 500 }}>{selectedMember.age}</span>
                    </div>
                    <div className="flex items-center gap-2">
                      <MapPin className="w-3.5 h-3.5 text-gray-400" />
                      <span className="text-gray-500">{ru ? 'Откуда:' : 'From:'}</span>
                      <span className="text-gray-900" style={{ fontWeight: 500 }}>{selectedMember.origin}</span>
                    </div>
                    <div className="flex items-center gap-2">
                      <MapPin className="w-3.5 h-3.5 text-gray-400" />
                      <span className="text-gray-500">{ru ? 'Живёт:' : 'Lives in:'}</span>
                      <span className="text-gray-900" style={{ fontWeight: 500 }}>{selectedMember.location}</span>
                    </div>
                    <div className="flex items-center gap-2">
                      <Clock className="w-3.5 h-3.5 text-gray-400" />
                      <span className="text-gray-500">{ru ? 'Вступил:' : 'Joined:'}</span>
                      <span className="text-gray-900" style={{ fontWeight: 500 }}>{selectedMember.joinedDate}</span>
                    </div>
                    <div className="flex items-center gap-2">
                      <Activity className="w-3.5 h-3.5 text-gray-400" />
                      <span className="text-gray-500">{ru ? 'Активен:' : 'Last active:'}</span>
                      <span className="text-emerald-600" style={{ fontWeight: 500 }}>{selectedMember.lastActive}</span>
                    </div>
                  </div>
                </div>

                {/* Interests */}
                <div className="bg-white rounded-xl border border-gray-200 p-4">
                  <h4 className="text-sm text-gray-900 mb-3" style={{ fontWeight: 600 }}>
                    {ru ? 'Интересы' : 'Interests'}
                  </h4>
                  <div className="flex flex-wrap gap-2">
                    {selectedMember.interests.map((interest) => (
                      <span key={interest} className="text-xs px-3 py-1.5 rounded-full bg-blue-50 text-blue-700 border border-blue-100" style={{ fontWeight: 500 }}>
                        {interest}
                      </span>
                    ))}
                  </div>
                </div>

                {/* Top Topics */}
                <div className="bg-white rounded-xl border border-gray-200 p-4">
                  <h4 className="text-sm text-gray-900 mb-3" style={{ fontWeight: 600 }}>{ru ? 'Главные темы' : 'Most Discussed Topics'}</h4>
                  <div className="space-y-2">
                    {selectedMember.topTopics.map((topic, i) => (
                      <div key={topic.name} className="flex items-center gap-3">
                        <span className="text-xs text-gray-400 w-4 text-right">{i + 1}</span>
                        <Hash className="w-3 h-3 text-gray-400" />
                        <span className="text-xs text-gray-700 flex-1" style={{ fontWeight: 500 }}>{topic.name}</span>
                        <div className="w-24 bg-gray-100 rounded-full h-2">
                          <div className="h-2 rounded-full bg-blue-400" style={{ width: `${(topic.count / Math.max(...selectedMember.topTopics.map(t => t.count), 1)) * 100}%` }} />
                        </div>
                        <span className="text-xs text-gray-500 w-10 text-right">{topic.count}</span>
                      </div>
                    ))}
                  </div>
                </div>

                {/* Sentiment */}
                <div className="bg-white rounded-xl border border-gray-200 p-4">
                  <h4 className="text-sm text-gray-900 mb-3" style={{ fontWeight: 600 }}>{ru ? 'Тональность публикаций' : 'Sentiment Profile'}</h4>
                  <div className="flex h-4 rounded-full overflow-hidden mb-2">
                    <div className="bg-emerald-400" style={{ width: `${selectedMember.sentiment.positive}%` }} />
                    <div className="bg-gray-300" style={{ width: `${selectedMember.sentiment.neutral}%` }} />
                    <div className="bg-red-400" style={{ width: `${selectedMember.sentiment.negative}%` }} />
                  </div>
                  <div className="flex items-center justify-between text-xs">
                    <span className="text-emerald-600">{selectedMember.sentiment.positive}% {ru ? 'позит.' : 'positive'}</span>
                    <span className="text-gray-400">{selectedMember.sentiment.neutral}% {ru ? 'нейтр.' : 'neutral'}</span>
                    <span className="text-red-500">{selectedMember.sentiment.negative}% {ru ? 'негат.' : 'negative'}</span>
                  </div>
                </div>
              </div>
            )}

            {/* ── CHANNELS ── */}
            {activeTab === 'channels' && (
              <div className="space-y-4">
                <p className="text-xs text-gray-500">{ru ? 'Каналы, в которых активен участник, и уровень активности в каждом' : 'Channels this person is subscribed to and their activity level in each'}</p>

                {selectedMember.channels.map((ch) => {
                  const color = typeColors[ch.type] || '#6b7280';
                  const rc = roleColors[ch.role] || roleColors['Member'];
                  return (
                    <div key={ch.name} className="bg-white rounded-xl border border-gray-200 p-4 hover:shadow-sm transition-shadow">
                      <div className="flex items-center gap-3 mb-3">
                        <div className="w-10 h-10 rounded-full flex items-center justify-center flex-shrink-0"
                          style={{ backgroundColor: color + '15' }}>
                          <Radio className="w-4 h-4" style={{ color }} />
                        </div>
                        <div className="flex-1 min-w-0">
                          <div className="flex items-center gap-2">
                            <span className="text-sm text-gray-900" style={{ fontWeight: 500 }}>{ch.name}</span>
                            <span className="text-xs px-1.5 py-0.5 rounded" style={{ backgroundColor: color + '15', color, fontWeight: 500 }}>
                              {ch.type}
                            </span>
                          </div>
                        </div>
                        <span className={`text-xs px-2 py-0.5 rounded-full ${rc.bg} ${rc.text}`} style={{ fontWeight: 600 }}>
                          {ru ? (roleMapRU[ch.role] || ch.role) : ch.role}
                        </span>
                      </div>

                      {/* Message bar */}
                      <div className="flex items-center gap-3">
                        <span className="text-xs text-gray-500">{ru ? 'Сообщений в канале:' : 'Messages in channel:'}</span>
                        <div className="flex-1 bg-gray-100 rounded-full h-2.5">
                          <div className="h-2.5 rounded-full" style={{
                            width: `${(ch.messageCount / Math.max(...selectedMember.channels.map(c => c.messageCount), 1)) * 100}%`,
                            backgroundColor: color,
                          }} />
                        </div>
                        <span className="text-xs text-gray-900" style={{ fontWeight: 600 }}>{ch.messageCount}</span>
                      </div>

                      {/* Percentage of total */}
                      <div className="mt-2 text-xs text-gray-400">
                        {selectedMember.totalMessages > 0 ? Math.round((ch.messageCount / selectedMember.totalMessages) * 100) : 0}% {ru ? 'от общей активности участника' : "of this person's total activity"}
                      </div>
                    </div>
                  );
                })}

                {/* Channel distribution chart */}
                <div className="bg-white rounded-xl border border-gray-200 p-4">
                  <h4 className="text-sm text-gray-900 mb-3" style={{ fontWeight: 600 }}>{ru ? 'Распределение активности по каналам' : 'Activity Distribution Across Channels'}</h4>
                  <ResponsiveContainer width="100%" height={180}>
                    <BarChart data={selectedMember.channels.map(c => ({ name: c.name.split(' ')[0], msgs: c.messageCount, type: c.type }))}>
                      <CartesianGrid strokeDasharray="3 3" stroke="#f3f4f6" />
                      <XAxis dataKey="name" tick={{ fontSize: 10 }} stroke="#9ca3af" />
                      <YAxis tick={{ fontSize: 10 }} stroke="#9ca3af" />
                      <Tooltip />
                      <Bar dataKey="msgs" radius={[4, 4, 0, 0]}>
                        {selectedMember.channels.map((c, i) => (
                          <Cell key={i} fill={typeColors[c.type] || '#6b7280'} />
                        ))}
                      </Bar>
                    </BarChart>
                  </ResponsiveContainer>
                </div>
              </div>
            )}

            {/* ── ACTIVITY & MESSAGES ── */}
            {activeTab === 'activity' && (
              <div className="space-y-4">
                {/* Activity Chart */}
                <div className="bg-white rounded-xl border border-gray-200 p-4">
                  <h4 className="text-sm text-gray-900 mb-2" style={{ fontWeight: 600 }}>{ru ? 'Активность за 7 недель' : 'Weekly Activity'}</h4>
                  <ResponsiveContainer width="100%" height={140}>
                    <AreaChart data={selectedMember.activityData}>
                      <CartesianGrid strokeDasharray="3 3" stroke="#f3f4f6" />
                      <XAxis dataKey="week" tick={{ fontSize: 10 }} stroke="#9ca3af" />
                      <YAxis tick={{ fontSize: 10 }} stroke="#9ca3af" />
                      <Tooltip />
                      <Area type="monotone" dataKey="msgs" stroke="#0d9488" fill="#0d948820" strokeWidth={2} />
                    </AreaChart>
                  </ResponsiveContainer>
                </div>

                {/* Recent Messages */}
                <div>
                  <h4 className="text-sm text-gray-900 mb-3" style={{ fontWeight: 600 }}>
                    {ru ? `Последние сообщения (${selectedMember.recentMessages.length})` : `Recent Messages (${selectedMember.recentMessages.length})`}
                  </h4>
                  {selectedMember.recentMessages.length === 0 ? (
                    <div className="bg-white rounded-xl border border-gray-200 p-8 text-center text-gray-400">
                      <MessageCircle className="w-8 h-8 mx-auto mb-2" />
                      <p className="text-sm">{ru ? 'Сообщения не загружены' : 'No recent messages loaded'}</p>
                    </div>
                  ) : (
                    <div className="space-y-3">
                      {selectedMember.recentMessages.map((msg, i) => (
                        <div key={i} className="bg-white rounded-xl border border-gray-200 p-4 hover:shadow-sm transition-shadow">
                          <div className="flex items-center justify-between mb-2">
                            <span className="text-xs text-blue-600" style={{ fontWeight: 500 }}>{msg.channel}</span>
                            <div className="flex items-center gap-1.5 text-xs text-gray-400">
                              <Clock className="w-3 h-3" />{msg.timestamp}
                            </div>
                          </div>
                          <p className="text-sm text-gray-700 leading-relaxed mb-3">{msg.text}</p>
                          <div className="flex items-center gap-4">
                            <div className="flex items-center gap-1 text-xs text-gray-400">
                              <ThumbsUp className="w-3 h-3" />{msg.reactions}
                            </div>
                            <div className="flex items-center gap-1 text-xs text-gray-400">
                              <MessageCircle className="w-3 h-3" />{msg.replies} {ru ? 'ответов' : 'replies'}
                            </div>
                          </div>
                        </div>
                      ))}
                    </div>
                  )}
                </div>
              </div>
            )}
          </div>
        </div>
      ) : (
        <div className="hidden md:flex flex-1 flex-col items-center justify-center bg-gray-50 text-gray-400 px-4 md:px-8">
          <div className="max-w-md text-center">
            <Users className="w-12 h-12 mx-auto mb-4 text-gray-300" />
            <h3 className="text-gray-600 mb-1" style={{ fontSize: '1rem', fontWeight: 500 }}>
              {ru ? 'Выберите участника для изучения' : 'Select a member to explore'}
            </h3>
            <p className="text-sm text-gray-400">
              {ru
                ? 'Нажмите на любого участника из списка, чтобы увидеть его профиль, интересы, каналы и активность.'
                : 'Click any person from the list to see their gender, interests, channel subscriptions, activity patterns, and recent messages.'}
            </p>
          </div>
        </div>
      )}
    </div>
  );
}
