import { Link } from 'react-router';
import { ResponsiveContainer, LineChart, Line, XAxis, YAxis, CartesianGrid, Tooltip } from 'recharts';
import { useLanguage } from '../../contexts/LanguageContext';
import { useData } from '../../contexts/DataContext';
import { EmptyWidget } from '../ui/EmptyWidget';

// ============================================================
// W4: TOPIC LANDSCAPE
// ============================================================

// Dynamic category color fallback
const FALLBACK_COLORS = ['#ef4444', '#3b82f6', '#8b5cf6', '#f59e0b', '#ec4899', '#10b981', '#06b6d4', '#6b7280', '#f97316', '#14b8a6'];

function getCategoryColor(_category: string, index: number): string {
  return FALLBACK_COLORS[index % FALLBACK_COLORS.length];
}

export function TopicLandscape() {
  const { lang } = useLanguage();
  const { data } = useData();
  const ru = lang === 'ru';
  const topicBubbles = data.topicBubbles[lang] ?? [];
  const totalMentions = topicBubbles.reduce((sum, t) => sum + (t.value || 0), 0);
  const lowEvidenceGrowth = topicBubbles.filter((t) => !t.growthReliable).length;

  if (!topicBubbles.length) return <EmptyWidget title={ru ? 'Карта тем' : 'Topic Landscape'} />;

  // Derive unique categories dynamically from the data
  const categoryMap = new Map<string, string>();
  topicBubbles.forEach(t => { if (!categoryMap.has(t.category)) categoryMap.set(t.category, t.color); });
  const catLabels = Array.from(categoryMap.keys());
  const catColors = Object.fromEntries(categoryMap);

  return (
    <div className="bg-white rounded-xl border border-gray-200 p-6">
      <div className="flex items-center justify-between mb-1">
        <h3 className="text-gray-900" style={{ fontSize: '1.05rem' }}>
          {ru ? 'Карта тем' : 'Topic Landscape'}
        </h3>
        <span className="text-xs text-gray-500">
          {ru ? 'Что обсуждают чаще всего' : 'Most discussed topics'}
        </span>
      </div>
      <p className="text-xs text-gray-500 mb-4">
        {ru
          ? 'Размер = число упоминаний. Рост показывается только если данных за 2 недели достаточно.'
          : 'Size = mention volume. Growth is shown only when 2-week evidence is sufficient.'}
      </p>

      <div className="flex flex-wrap gap-2">
        {topicBubbles.map((topic) => {
          const size = Math.max(60, Math.sqrt(topic.value) * 2);
          return (
            <Link
              key={topic.name}
              to={`/topics?topic=${encodeURIComponent(topic.name)}`}
              className="rounded-xl flex flex-col items-center justify-center cursor-pointer hover:scale-105 transition-transform"
              style={{ width: size, height: size * 0.7, backgroundColor: topic.color + '15', border: `1px solid ${topic.color}30` }}
            >
              <span className="text-xs text-gray-900 text-center px-1 leading-tight" style={{ fontWeight: 500, fontSize: size > 80 ? '11px' : '9px' }}>
                {topic.name}
              </span>
                <span className={`text-xs ${topic.growthReliable ? (topic.growth >= 0 ? 'text-emerald-600' : 'text-red-500') : 'text-gray-400'}`} style={{ fontSize: '9px', fontWeight: 600 }}>
                  {topic.growthReliable ? `${topic.growth > 0 ? '+' : ''}${topic.growth}%` : (ru ? 'н/д' : 'n/a')}
                </span>
              </Link>
            );
          })}
        </div>

      <div className="flex items-center gap-4 mt-4 pt-3 border-t border-gray-100">
        {catLabels.map((cat, i) => (
          <div key={cat} className="flex items-center gap-1">
            <div className="w-2 h-2 rounded-full" style={{ backgroundColor: catColors[cat] ?? getCategoryColor(cat, i) }} />
            <span className="text-xs text-gray-500">{cat}</span>
          </div>
        ))}
        <Link to="/topics" className="ml-auto text-xs text-blue-600 hover:text-blue-800 hover:underline transition-colors" style={{ fontWeight: 500 }}>
          {ru ? 'Все темы →' : 'See all topics →'}
        </Link>
      </div>
      <p className="text-xs text-gray-400 mt-2">
        {ru
          ? `Основа: ${totalMentions.toLocaleString()} упоминаний по ${topicBubbles.length} темам. ${lowEvidenceGrowth > 0 ? `${lowEvidenceGrowth} тем с недостатком данных для роста.` : ''}`
          : `Evidence: ${totalMentions.toLocaleString()} mentions across ${topicBubbles.length} topics. ${lowEvidenceGrowth > 0 ? `${lowEvidenceGrowth} topics have insufficient growth evidence.` : ''}`}
      </p>
    </div>
  );
}


// ============================================================
// W5: CONVERSATION TRENDS
// ============================================================

export function ConversationTrends() {
  const { lang } = useLanguage();
  const { data } = useData();
  const ru = lang === 'ru';
  const trendLines = data.trendLines[lang] ?? [];
  const trendData = data.trendData;

  if (!trendLines.length || !trendData.length) return <EmptyWidget title={ru ? 'Динамика разговоров' : 'Conversation Trends'} />;

  // Compute fastest-growing topics dynamically
  const sorted = [...trendLines].sort((a, b) => b.change - a.change);
  const top2 = sorted.slice(0, 2);

  return (
    <div className="bg-white rounded-xl border border-gray-200 p-6">
      <div className="flex items-center justify-between mb-1">
        <h3 className="text-gray-900" style={{ fontSize: '1.05rem' }}>
          {ru ? 'Динамика разговоров' : 'Conversation Trends'}
        </h3>
        <span className="text-xs text-gray-500">{ru ? `${trendData.length}-недельная траектория` : `${trendData.length}-week trajectory`}</span>
      </div>
      <p className="text-xs text-gray-500 mb-4">
        {ru ? 'Какие темы растут? Что угасает? Следите за трендами.' : 'What topics are rising? What\'s fading? Follow the momentum.'}
      </p>

      <ResponsiveContainer width="100%" height={220}>
        <LineChart data={trendData}>
          <CartesianGrid strokeDasharray="3 3" stroke="#f3f4f6" />
          <XAxis dataKey="week" tick={{ fontSize: 11 }} stroke="#9ca3af" />
          <YAxis tick={{ fontSize: 11 }} stroke="#9ca3af" />
          <Tooltip />
          {trendLines.map((line) => (
            <Line key={line.key} type="monotone" dataKey={line.key} stroke={line.color} strokeWidth={2} dot={false} />
          ))}
        </LineChart>
      </ResponsiveContainer>

      <div className="grid grid-cols-3 gap-2 mt-3">
        {trendLines.map((line) => (
          <div key={line.key} className="flex items-center gap-2">
            <div className="w-2.5 h-2.5 rounded-full flex-shrink-0" style={{ backgroundColor: line.color }} />
            <span className="text-xs text-gray-600 truncate">{line.label}</span>
            <span className={`text-xs ml-auto ${line.change > 0 ? 'text-emerald-500' : 'text-red-500'}`} style={{ fontWeight: 600 }}>
              {line.change > 0 ? '+' : ''}{line.change}%
            </span>
          </div>
        ))}
      </div>

      {top2.length >= 2 && (
        <div className="mt-3 pt-3 border-t border-gray-100">
          <div className="bg-emerald-50 border border-emerald-100 rounded-lg px-3 py-2">
            <p className="text-xs text-emerald-800">
              {ru
                ? <><span style={{ fontWeight: 600 }}>Быстрее всего растут:</span> {top2[0].label} ({top2[0].change > 0 ? '+' : ''}{top2[0].change}%) и {top2[1].label} ({top2[1].change > 0 ? '+' : ''}{top2[1].change}%)</>
                : <><span style={{ fontWeight: 600 }}>Fastest growing:</span> {top2[0].label} ({top2[0].change > 0 ? '+' : ''}{top2[0].change}%) and {top2[1].label} ({top2[1].change > 0 ? '+' : ''}{top2[1].change}%)</>
              }
            </p>
          </div>
        </div>
      )}
    </div>
  );
}


// ============================================================
// W6: CONTENT ENGAGEMENT HEATMAP
// ============================================================

function getEngColor(v: number) {
  if (v >= 80) return '#059669'; if (v >= 60) return '#10b981';
  if (v >= 40) return '#6ee7b7'; if (v >= 20) return '#d1fae5'; return '#f0fdf4';
}
function getEngText(v: number) { return v >= 60 ? '#ffffff' : '#374151'; }

export function ContentEngagementHeatmap() {
  const { lang } = useLanguage();
  const { data } = useData();
  const ru = lang === 'ru';
  const heatmapData = data.heatmap[lang];
  const contentTypes = heatmapData?.contentTypes ?? [];
  const topicCols = heatmapData?.topicCols ?? [];
  const engagement = heatmapData?.engagement ?? {};

  if (!contentTypes.length || !topicCols.length) return <EmptyWidget title={ru ? 'Карта вовлечённости по контенту' : 'Content Engagement Map'} />;

  // Compute top performer dynamically
  let maxVal = 0, maxType = '', maxTopic = '';
  contentTypes.forEach(type => {
    topicCols.forEach(topic => {
      const v = engagement[type]?.[topic] ?? 0;
      if (v > maxVal) { maxVal = v; maxType = type; maxTopic = topic; }
    });
  });

  return (
    <div className="bg-white rounded-xl border border-gray-200 p-6">
      <div className="flex items-center justify-between mb-1">
        <h3 className="text-gray-900" style={{ fontSize: '1.05rem' }}>
          {ru ? 'Карта вовлечённости по контенту' : 'Content Engagement Map'}
        </h3>
        <span className="text-xs text-gray-500">{ru ? 'Формат x Тема' : 'Format x Topic performance'}</span>
      </div>
      <p className="text-xs text-gray-500 mb-4">
        {ru
          ? 'Какой формат работает лучше для каждой темы? Используйте для планирования контент-стратегии.'
          : 'Which content format works best for each topic? Plan your content strategy.'}
      </p>

      <div className="overflow-x-auto">
        <table className="w-full">
          <thead>
            <tr>
              <th className="text-xs text-gray-500 text-left pb-2 pr-3" style={{ fontWeight: 500 }}></th>
              {topicCols.map((t) => (
                <th key={t} className="text-xs text-gray-500 pb-2 px-1 text-center" style={{ fontWeight: 500 }}>{t}</th>
              ))}
            </tr>
          </thead>
          <tbody>
            {contentTypes.map((type) => (
              <tr key={type}>
                <td className="text-xs text-gray-700 pr-3 py-1 whitespace-nowrap" style={{ fontWeight: 500 }}>{type}</td>
                {topicCols.map((topic) => {
                  const value = engagement[type]?.[topic] ?? 0;
                  return (
                    <td key={topic} className="px-1 py-1">
                      <div
                        className="rounded-md flex items-center justify-center h-9 cursor-pointer hover:ring-2 hover:ring-gray-900 transition-all"
                        style={{ backgroundColor: getEngColor(value), color: getEngText(value) }}
                      >
                        <span className="text-xs" style={{ fontWeight: 600 }}>{value}</span>
                      </div>
                    </td>
                  );
                })}
              </tr>
            ))}
          </tbody>
        </table>
      </div>

      <div className="flex items-center gap-2 mt-4 pt-3 border-t border-gray-100">
        <span className="text-xs text-gray-500">{ru ? 'Низкое' : 'Low'}</span>
        <div className="flex gap-0.5">
          {[10, 30, 50, 70, 85].map((v) => (
            <div key={v} className="w-6 h-3 rounded-sm" style={{ backgroundColor: getEngColor(v) }} />
          ))}
        </div>
        <span className="text-xs text-gray-500">{ru ? 'Высокое вовлечение' : 'High engagement'}</span>
        {maxVal > 0 && (
          <span className="text-xs text-gray-400 ml-auto">
            {maxType} + {maxTopic} = {maxVal} ({ru ? 'лидер' : 'top performer'})
          </span>
        )}
      </div>
    </div>
  );
}


// ============================================================
// W7: QUESTION CLOUD
// ============================================================

export function QuestionCloud() {
  const { lang } = useLanguage();
  const { data } = useData();
  const ru = lang === 'ru';
  const questionCategories = data.questionCategories[lang] ?? [];

  if (!questionCategories.length) return <EmptyWidget title={ru ? 'Самые частые вопросы' : 'Most Asked Questions'} />;

  // Compute unanswered count dynamically
  const unansweredCount = questionCategories.reduce(
    (sum, cat) => sum + cat.questions.filter(q => !q.answered && !q.lowEvidence).length, 0
  );
  const lowEvidenceCount = questionCategories.reduce(
    (sum, cat) => sum + cat.questions.filter(q => q.lowEvidence).length, 0
  );
  const totalAsked = questionCategories.reduce(
    (sum, cat) => sum + cat.questions.reduce((inner, q) => inner + q.count, 0), 0
  );

  return (
    <div className="bg-white rounded-xl border border-gray-200 p-6">
      <div className="flex items-center justify-between mb-1">
        <h3 className="text-gray-900" style={{ fontSize: '1.05rem' }}>
          {ru ? 'Самые частые вопросы' : 'Most Asked Questions'}
        </h3>
        <span className="text-xs text-gray-500">{ru ? 'По категориям' : 'Grouped by category'}</span>
      </div>
      <p className="text-xs text-gray-500 mb-4">
        {ru
          ? 'Показываем реальные формулировки, если они есть. При малом объёме данных отмечаем это явно.'
          : 'Shows real question wording when available. Low-sample items are flagged explicitly.'}
      </p>

      <div className="space-y-4">
        {questionCategories.map((cat) => (
          <div key={cat.category}>
            <div className="flex items-center gap-2 mb-2">
              <div className="w-2 h-2 rounded-full" style={{ backgroundColor: cat.color }} />
              <span className="text-xs text-gray-900" style={{ fontWeight: 600 }}>{cat.category}</span>
            </div>
            <div className="grid grid-cols-2 gap-1.5">
              {cat.questions.map((q) => (
                <Link
                  key={`${cat.category}-${q.topic || q.q}`}
                  to={`/topics?topic=${encodeURIComponent(q.topic || q.q)}&view=questions`}
                  className={`px-2.5 py-1.5 rounded-lg border text-xs cursor-pointer transition-colors ${
                    q.lowEvidence
                      ? 'bg-slate-50 border-slate-200 text-slate-500 hover:bg-slate-100'
                      : q.answered
                      ? 'bg-gray-50 border-gray-100 text-gray-600 hover:bg-gray-100'
                      : 'bg-amber-50 border-amber-200 text-amber-800 hover:bg-amber-100'
                  }`}
                >
                  <div className="flex items-center gap-1.5 mb-0.5">
                    <span className="truncate" style={{ fontWeight: 600 }}>{q.topic || q.q}</span>
                  </div>
                  <span className="text-xs text-gray-500 mt-0.5 block truncate">
                    {q.preview || (ru ? 'Нет зафиксированного вопроса в текущем срезе' : 'No captured question in current slice')}
                  </span>
                  <span className="text-xs text-gray-400 mt-0.5 block">
                    {q.count} {ru ? 'раз задан' : 'times asked'}
                  </span>
                  {q.lowEvidence && (
                    <span className="text-xs text-slate-400 mt-0.5 block">
                      {ru ? 'Недостаточно данных для оценки ответов' : 'Insufficient data to score answers'}
                    </span>
                  )}
                </Link>
              ))}
            </div>
          </div>
        ))}
      </div>

      <div className="mt-3 pt-3 border-t border-gray-100 flex items-center gap-3">
        <div className="flex items-center gap-1.5">
          <div className="w-3 h-3 rounded bg-amber-50 border border-amber-200" />
          <span className="text-xs text-gray-500">{ru ? 'Нужен гайд' : 'Needs a guide'}</span>
        </div>
        <div className="flex items-center gap-1.5">
          <div className="w-3 h-3 rounded bg-gray-50 border border-gray-100" />
          <span className="text-xs text-gray-500">{ru ? 'Ответ / гайд есть' : 'Answered/guide exists'}</span>
        </div>
        <div className="flex items-center gap-1.5">
          <div className="w-3 h-3 rounded bg-slate-50 border border-slate-200" />
          <span className="text-xs text-gray-500">{ru ? 'Мало данных' : 'Low evidence'}</span>
        </div>
        <span className="text-xs text-amber-600 ml-auto" style={{ fontWeight: 500 }}>
          {unansweredCount} {ru ? 'вопросов без гайда' : 'questions need guides'}
        </span>
      </div>
      <p className="text-xs text-gray-400 mt-2">
        {ru
          ? `Основа: ${totalAsked.toLocaleString()} запросов в выборке. ${lowEvidenceCount > 0 ? `${lowEvidenceCount} пунктов с низкой статистикой.` : ''}`
          : `Evidence: ${totalAsked.toLocaleString()} asks in sample. ${lowEvidenceCount > 0 ? `${lowEvidenceCount} low-evidence items.` : ''}`}
      </p>
    </div>
  );
}


// ============================================================
// QUESTION-TO-ANSWER GAP BY TOPIC
// ============================================================

function getGapColor(rate: number) {
  if (rate < 35) return '#ef4444';
  if (rate < 56) return '#f59e0b';
  return '#10b981';
}

export function QuestionAnswerGap() {
  const { lang } = useLanguage();
  const { data } = useData();
  const ru = lang === 'ru';
  const qaGapData = data.qaGap[lang] ?? [];

  if (!qaGapData.length) return <EmptyWidget title={ru ? 'Разрыв "Вопрос - Ответ"' : 'Question-Answer Gap'} />;

  // Compute critical gaps and worst topic dynamically
  const criticalGaps = qaGapData.filter(item => !item.lowEvidence && item.rate < 35);
  const lowEvidenceItems = qaGapData.filter(item => item.lowEvidence);
  const sorted = [...qaGapData].sort((a, b) => {
    if (!!a.lowEvidence !== !!b.lowEvidence) return a.lowEvidence ? 1 : -1;
    return a.rate - b.rate;
  });
  const worst = sorted.find((item) => !item.lowEvidence);

  return (
    <div className="bg-white rounded-xl border border-gray-200 p-6">
      <div className="flex items-center justify-between mb-1">
        <h3 className="text-gray-900" style={{ fontSize: '1.05rem' }}>
          {ru ? 'Разрыв "Вопрос - Ответ"' : 'Question-Answer Gap'}
        </h3>
        <span className="text-xs text-red-600" style={{ fontWeight: 500 }}>
          {criticalGaps.length} {ru ? 'критических разрывов' : 'critical gaps'}
        </span>
      </div>
      <p className="text-xs text-gray-500 mb-4">
        {ru
          ? 'Покрытие ответами считаем только там, где хватает наблюдений. Иначе показываем как «мало данных».'
          : 'Answer coverage is scored only when evidence is sufficient; otherwise marked as low evidence.'}
      </p>

      <div className="space-y-2.5">
        {sorted.map((item) => (
          <div key={item.topic} className="flex items-center gap-3">
            <span className="text-xs text-gray-700 w-36 flex-shrink-0" style={{ fontWeight: 500 }}>{item.topic}</span>
            <div className="flex-1 relative bg-gray-100 rounded-full h-5">
              <div
                className="h-5 rounded-full flex items-center px-2"
                style={{ width: `${Math.max(10, item.lowEvidence ? 10 : item.rate)}%`, backgroundColor: item.lowEvidence ? '#cbd5e1' : getGapColor(item.rate) }}
              >
                {!item.lowEvidence && item.rate > 18 && (
                  <span className="text-xs text-white" style={{ fontWeight: 600, fontSize: '10px' }}>{item.rate}%</span>
                )}
                {item.lowEvidence && (
                  <span className="text-xs text-slate-700" style={{ fontWeight: 600, fontSize: '10px' }}>{ru ? 'н/д' : 'n/a'}</span>
                )}
              </div>
            </div>
            <span className="text-gray-500 w-24 text-right flex-shrink-0" style={{ fontSize: '10px' }}>
              {item.asked.toLocaleString()} {ru ? 'вопросов' : 'asked'}
            </span>
          </div>
        ))}
      </div>

      <div className="flex items-center gap-4 mt-3 pt-3 border-t border-gray-100">
        {[
          { color: '#ef4444', label: ru ? '<35% отвечено' : '<35% answered' },
          { color: '#f59e0b', label: ru ? '35-55%' : '35-55%' },
          { color: '#10b981', label: ru ? '>55% покрыто' : '>55% well served' },
        ].map((item) => (
          <div key={item.label} className="flex items-center gap-1.5">
            <div className="w-3 h-3 rounded" style={{ backgroundColor: item.color }} />
            <span className="text-xs text-gray-500">{item.label}</span>
          </div>
        ))}
      </div>

      {worst && (
        <div className="mt-3 bg-amber-50 border border-amber-100 rounded-lg px-3 py-2">
          <p className="text-xs text-amber-800">
            {ru
              ? <><span style={{ fontWeight: 600 }}>Приоритет:</span> {worst.topic} {'\u2014'} ответ только в {worst.rate}% случаев при {worst.asked.toLocaleString()}+ еженедельных запросах.</>
              : <><span style={{ fontWeight: 600 }}>Priority:</span> {worst.topic} questions are answered only {worst.rate}% of the time despite {worst.asked.toLocaleString()}+ weekly asks.</>
            }
          </p>
        </div>
      )}
      {lowEvidenceItems.length > 0 && (
        <p className="text-xs text-gray-400 mt-2">
          {ru
            ? `${lowEvidenceItems.length} тем отмечены как «мало данных» и не участвуют в приоритизации.`
            : `${lowEvidenceItems.length} topics are marked low evidence and excluded from prioritization.`}
        </p>
      )}
    </div>
  );
}


// ============================================================
// TOPIC LIFECYCLE
// ============================================================

export function TopicLifecycle() {
  const { lang } = useLanguage();
  const { data } = useData();
  const ru = lang === 'ru';
  const lifecycleStages = data.lifecycleStages[lang] ?? [];

  if (!lifecycleStages.length) return <EmptyWidget title={ru ? 'Жизненный цикл тем' : 'Topic Lifecycle'} />;

  return (
    <div className="bg-white rounded-xl border border-gray-200 p-6">
      <div className="flex items-center justify-between mb-1">
        <h3 className="text-gray-900" style={{ fontSize: '1.05rem' }}>
          {ru ? 'Жизненный цикл тем' : 'Topic Lifecycle'}
        </h3>
        <span className="text-xs text-gray-500">
          {ru ? 'Зарождение -> Пик -> Угасание' : 'Emerging -> Peak -> Fading'}
        </span>
      </div>
      <p className="text-xs text-gray-500 mb-4">
        {ru
          ? 'На каком этапе жизненного цикла находится каждая тема? Действуйте на стадии Зарождения, усиливайте Подъём, готовьте выход на Спаде.'
          : 'Where is each conversation in its lifecycle? Act early on Emerging, capitalize on Rising, plan exits for Declining.'}
      </p>

      <div className="grid grid-cols-4 gap-1 mb-4">
        {lifecycleStages.map((s) => (
          <div key={s.stage} className={`${s.bgColor} border ${s.borderColor} rounded-lg px-2 py-1.5 text-center`}>
            <div className="w-2 h-2 rounded-full mx-auto mb-1" style={{ backgroundColor: s.color }} />
            <span className={`text-xs block ${s.textColor}`} style={{ fontWeight: 600 }}>{s.stage}</span>
            <span className="text-gray-400" style={{ fontSize: '9px' }}>{s.desc}</span>
          </div>
        ))}
      </div>

      <div className="space-y-3">
        {lifecycleStages.map((stageGroup) => (
          <div key={stageGroup.stage}>
            <div className="flex items-center gap-2 mb-1.5">
              <div className="w-2 h-2 rounded-full flex-shrink-0" style={{ backgroundColor: stageGroup.color }} />
              <span className="text-xs text-gray-900" style={{ fontWeight: 600 }}>{stageGroup.stage}</span>
              <span className={`text-xs ${stageGroup.textColor}`} style={{ fontSize: '10px' }}>({stageGroup.topics.length})</span>
            </div>
            <div className="space-y-1.5 pl-4">
              {stageGroup.topics.map((topic) => (
                <div key={topic.name} className={`flex items-center justify-between px-3 py-2 rounded-lg ${stageGroup.bgColor} border ${stageGroup.borderColor}`}>
                  <span className="text-xs text-gray-900" style={{ fontWeight: 500 }}>{topic.name}</span>
                  <div className="flex items-center gap-3 text-xs">
                    <span className="text-gray-400">{topic.volume.toLocaleString()} {ru ? '/нед.' : '/wk'}</span>
                    <span className={topic.momentum > 0 ? 'text-emerald-600' : 'text-red-500'} style={{ fontWeight: 600 }}>
                      {topic.momentum > 0 ? '+' : ''}{topic.momentum}%
                    </span>
                    <span className="text-gray-400 w-8 text-right">{topic.daysActive}{ru ? 'д.' : 'd'}</span>
                  </div>
                </div>
              ))}
            </div>
          </div>
        ))}
      </div>
    </div>
  );
}
