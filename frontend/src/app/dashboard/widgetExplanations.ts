import { ADMIN_WIDGET_DEFINITIONS, type AdminWidgetId } from '../admin/catalog';
import type { Lang } from '../contexts/LanguageContext';

export interface WidgetExplanationContent {
  short: string;
  overview: string;
  metrics: string[];
  note?: string;
}

export type WidgetExplanation = Record<Lang, WidgetExplanationContent>;

const WIDGET_LABELS = Object.fromEntries(
  ADMIN_WIDGET_DEFINITIONS.map((widget) => [
    widget.id,
    { en: widget.labelEn, ru: widget.labelRu },
  ]),
) as Record<AdminWidgetId, Record<Lang, string>>;

export const WIDGET_EXPLANATIONS = {
  community_brief: {
    en: {
      short: 'Summarizes the selected window into a short executive readout.',
      overview: 'This widget turns the latest community activity into a plain-language summary so someone can understand the big picture quickly.',
      metrics: [
        'AI summary grounded in analyzed posts and grouped comment scopes, not a single message.',
        'Message volume, positive intent share, and negative intent share from the selected window.',
        'Freshness signals such as update time and total analyzed units.',
      ],
      note: 'Use it as an executive summary first, then open the supporting widgets for the exact drivers behind the headline.',
    },
    ru: {
      short: 'Коротко переводит выбранное окно в понятную руководителю сводку.',
      overview: 'Этот виджет превращает свежую активность сообщества в простое текстовое резюме, чтобы быстро понять общую картину.',
      metrics: [
        'AI-сводка опирается на проанализированные посты и сгруппированные контексты комментариев, а не на одно сообщение.',
        'Объём сообщений, доля позитивного интента и доля негативного интента в выбранном окне.',
        'Сигналы свежести: время обновления и общее число проанализированных единиц.',
      ],
      note: 'Используйте карточку как быстрое резюме для руководителя, а детали причин смотрите в виджетах ниже.',
    },
  },
  community_health_score: {
    en: {
      short: 'Builds a 0-100 health score from intent, tone, and discussion quality.',
      overview: 'This widget estimates how constructive and stable the conversation feels right now by combining several explainable quality signals into one score.',
      metrics: [
        'Constructive intent share across analyzed messages.',
        'Emotional stability, discussion diversity, and conversation depth components.',
        'Current score, score history, and delta versus the previous matching window.',
      ],
      note: 'It is a composite index, so the component bars explain what is pulling the score up or down.',
    },
    ru: {
      short: 'Собирает индекс 0-100 из интента, тона и качества обсуждений.',
      overview: 'Этот виджет оценивает, насколько разговор сейчас конструктивный и устойчивый, объединяя несколько объяснимых сигналов качества в один индекс.',
      metrics: [
        'Доля конструктивного интента в проанализированных сообщениях.',
        'Компоненты эмоциональной стабильности, разнообразия тем и глубины обсуждений.',
        'Текущий индекс, история индекса и изменение к предыдущему сопоставимому окну.',
      ],
      note: 'Это составной индекс, поэтому полосы компонентов показывают, что именно тянет оценку вверх или вниз.',
    },
  },
  trending_topics_feed: {
    en: {
      short: 'Ranks the topics rising fastest in the current conversation.',
      overview: 'This widget highlights the subjects that are drawing the most attention right now and shows the evidence behind the trend.',
      metrics: [
        'Direct mention volume in the current window, plus change versus the previous comparable window.',
        'Topic category, sentiment pattern, and evidence quality.',
        'Distinct people, channels, and message examples behind the trend when available.',
      ],
    },
    ru: {
      short: 'Показывает темы, которые сейчас растут быстрее всего.',
      overview: 'Этот виджет выделяет сюжеты, которые прямо сейчас сильнее всего притягивают внимание, и показывает доказательства этого роста.',
      metrics: [
        'Объём прямых упоминаний в текущем окне и изменение к предыдущему сопоставимому окну.',
        'Категория темы, тональность и качество доказательной базы.',
        'Разные люди, каналы и примеры сообщений, которые поддерживают тренд, если они доступны.',
      ],
    },
  },
  topic_landscape: {
    en: {
      short: 'Shows which topics dominate the selected window and how large each one is.',
      overview: 'This widget maps the conversation space so you can see which themes occupy the most attention and which categories they belong to.',
      metrics: [
        'Tile size is driven by direct mention volume.',
        'Category colors group related topics into broader themes.',
        'Growth compares the last 7 days with the previous 7 days when evidence is strong enough.',
      ],
      note: 'Small tiles can still matter if they are growing quickly or belong to a strategic category.',
    },
    ru: {
      short: 'Показывает, какие темы занимают больше всего внимания в выбранном окне.',
      overview: 'Этот виджет раскладывает разговор на карту, чтобы было видно, какие сюжеты занимают основной объём внимания и к каким категориям они относятся.',
      metrics: [
        'Размер плитки зависит от объёма прямых упоминаний.',
        'Цвет категории объединяет родственные темы в более широкий блок.',
        'Рост сравнивает последние 7 дней с предыдущими 7 днями, если данных достаточно.',
      ],
      note: 'Даже небольшая плитка может быть важной, если тема быстро растёт или относится к стратегической категории.',
    },
  },
  conversation_trends: {
    en: {
      short: 'Tracks whether key topics are rising, flattening, or fading over time.',
      overview: 'This widget shows the trajectory of major topics across the selected window so you can spot momentum shifts, not just raw volume.',
      metrics: [
        'Topic-level mention counts across the chart buckets in the selected range.',
        'Percent change for each tracked topic.',
        'Relative ranking of the fastest-growing topics versus the ones cooling down.',
      ],
    },
    ru: {
      short: 'Отслеживает, какие темы растут, выравниваются или затухают со временем.',
      overview: 'Этот виджет показывает траекторию ключевых тем внутри выбранного окна, чтобы видеть не только объём, но и смену импульса.',
      metrics: [
        'Число упоминаний по каждой теме в последовательных корзинах выбранного диапазона.',
        'Процент изменения для каждой отслеживаемой темы.',
        'Сравнение самых быстрорастущих тем с темами, которые теряют импульс.',
      ],
    },
  },
  question_cloud: {
    en: {
      short: 'Groups repeated community questions into clear asks and coverage statuses.',
      overview: 'This widget uses AI to combine similar questions into one clean card, then shows whether the community already answers that need well.',
      metrics: [
        'Clusters of similar questions grounded in real messages and evidence excerpts.',
        'Demand signals: total messages, unique people, channels, and 7-day trend.',
        'Confidence score plus coverage status such as needs guide, partially answered, or well covered.',
      ],
      note: 'These are synthesized question cards, but they stay tied to real evidence instead of free-form AI writing.',
    },
    ru: {
      short: 'Объединяет повторяющиеся вопросы сообщества в понятные карточки и статусы покрытия.',
      overview: 'Этот виджет с помощью AI собирает похожие вопросы в одну чистую карточку и показывает, насколько сообщество уже закрывает этот запрос.',
      metrics: [
        'Кластеры похожих вопросов, привязанные к реальным сообщениям и фрагментам доказательств.',
        'Сигналы спроса: число сообщений, уникальных людей, каналов и 7-дневный тренд.',
        'Оценка уверенности и статус покрытия: нужен гайд, частично покрыто или в целом закрыто.',
      ],
      note: 'Карточки синтезируются AI, но остаются жёстко привязанными к реальным доказательствам, а не к свободной генерации.',
    },
  },
  topic_lifecycle: {
    en: {
      short: 'Shows which topics are emerging, peaking, stabilizing, or declining.',
      overview: 'This widget places each topic into a lifecycle stage so you can see where attention is building and where it is starting to cool.',
      metrics: [
        'Recent discussion volume and momentum for each topic.',
        'Days active inside the current window.',
        'Supporting channel and evidence context for expanded rows.',
      ],
    },
    ru: {
      short: 'Показывает, какие темы зарождаются, достигают пика, стабилизируются или снижаются.',
      overview: 'Этот виджет относит каждую тему к стадии жизненного цикла, чтобы видеть, где внимание набирается, а где начинает остывать.',
      metrics: [
        'Недавний объём обсуждений и импульс каждой темы.',
        'Сколько дней тема была активна в текущем окне.',
        'Дополнительный контекст каналов и доказательств в раскрытых строках.',
      ],
    },
  },
  problem_tracker: {
    en: {
      short: 'Turns repeated pain points into grounded, prioritized problem cards.',
      overview: 'This widget uses AI to summarize the most repeated community problems in clear language while keeping each card anchored to real evidence.',
      metrics: [
        'Clusters of similar complaints or stress signals from messages.',
        'Demand signals: messages, unique people, channels, and 7-day trend.',
        'Severity and confidence scores plus linked evidence excerpts.',
      ],
      note: 'The card text is AI-written, but the ranking comes from repeated evidence and not from one dramatic quote.',
    },
    ru: {
      short: 'Превращает повторяющиеся болевые точки в подтверждённые и приоритизированные карточки проблем.',
      overview: 'Этот виджет с помощью AI формулирует главные повторяющиеся проблемы сообщества простым языком, но каждая карточка остаётся привязанной к реальным доказательствам.',
      metrics: [
        'Кластеры похожих жалоб или стресс-сигналов из сообщений.',
        'Сигналы спроса: число сообщений, уникальных людей, каналов и 7-дневный тренд.',
        'Оценки серьёзности и уверенности, а также связанные доказательства.',
      ],
      note: 'Текст карточки пишет AI, но ранжирование строится на повторяемых доказательствах, а не на одной яркой цитате.',
    },
  },
  service_gap_detector: {
    en: {
      short: 'Finds community needs that have strong demand but weak visible supply.',
      overview: 'This widget surfaces service needs people repeatedly ask for when the conversation shows too little evidence that the need is already being met.',
      metrics: [
        'AI-grouped unmet need clusters grounded in real messages.',
        'Demand volume, unique seekers, channels, and 7-day trend.',
        'Gap score and unmet percentage, which estimate how much demand is still unresolved.',
      ],
      note: 'A higher gap means people keep asking, but the conversation shows little recommendation or support supply in return.',
    },
    ru: {
      short: 'Находит потребности сообщества с высоким спросом и слабым видимым предложением.',
      overview: 'Этот виджет поднимает сервисные запросы, которые люди повторяют снова и снова, но в разговоре почти нет признаков, что потребность уже закрыта.',
      metrics: [
        'AI-сгруппированные кластеры неудовлетворённых потребностей, привязанные к реальным сообщениям.',
        'Объём спроса, уникальные ищущие, каналы и 7-дневный тренд.',
        'Оценка пробела и процент unmet, которые показывают, какая часть спроса остаётся без ответа.',
      ],
      note: 'Чем выше пробел, тем чаще люди просят помощь и тем меньше в разговоре видно рекомендаций или предложения в ответ.',
    },
  },
  satisfaction_by_area: {
    en: {
      short: 'Compares how satisfied people feel across major life areas.',
      overview: 'This widget ranks life areas by how positively or negatively people talk about them, helping you see what supports wellbeing and what drags it down.',
      metrics: [
        'Satisfaction score for each life area.',
        'Direction of change for each area.',
        'Top strengths and lowest-scoring pain points across the set.',
      ],
    },
    ru: {
      short: 'Сравнивает, насколько люди довольны разными сферами жизни.',
      overview: 'Этот виджет ранжирует сферы жизни по тому, насколько позитивно или негативно о них говорят, чтобы было видно, что поддерживает благополучие, а что его тянет вниз.',
      metrics: [
        'Индекс удовлетворённости по каждой жизненной сфере.',
        'Направление изменения по каждой сфере.',
        'Главные сильные стороны и самые низкооценённые болевые точки.',
      ],
    },
  },
  mood_over_time: {
    en: {
      short: 'Shows how the emotional mix of the community changes over time.',
      overview: 'This widget tracks the balance of positive and negative emotional states so you can see whether the overall mood is settling, improving, or getting riskier.',
      metrics: [
        'Stacked emotional-state volumes across the time series.',
        'Positive-share percentage in the latest bucket.',
        'Direction of positive versus negative mood components from the start to the latest point.',
      ],
    },
    ru: {
      short: 'Показывает, как со временем меняется эмоциональный состав сообщества.',
      overview: 'Этот виджет отслеживает баланс позитивных и негативных состояний, чтобы видеть, успокаивается ли общий фон, улучшается или становится рискованнее.',
      metrics: [
        'Объёмы эмоциональных состояний в динамике на составном графике.',
        'Доля позитивных состояний в последней точке.',
        'Направление позитивных и негативных компонентов от начала ряда к последней точке.',
      ],
    },
  },
  emotional_urgency_index: {
    en: {
      short: 'Highlights signals that look urgent enough to need fast support.',
      overview: 'This widget focuses on the most time-sensitive distress patterns so the team can distinguish ordinary frustration from situations that may need immediate attention.',
      metrics: [
        'Urgency clusters grouped into critical and high-priority levels.',
        'Number of similar posts or messages affected by each urgent pattern.',
        'Suggested action label tied to the detected topic and signal strength.',
      ],
    },
    ru: {
      short: 'Выделяет сигналы, которые выглядят достаточно срочными для быстрой поддержки.',
      overview: 'Этот виджет концентрируется на самых чувствительных по времени паттернах, чтобы команда могла отличать обычное раздражение от случаев, где уже нужна оперативная реакция.',
      metrics: [
        'Кластеры срочности, разделённые на критический и высокий приоритет.',
        'Число похожих публикаций или сообщений по каждому срочному паттерну.',
        'Рекомендованное действие, связанное с темой и силой сигнала.',
      ],
      note: 'Срочность показывает вероятность немедленной потребности в помощи, а не просто общий негатив.',
    },
  },
  top_channels: {
    en: {
      short: 'Ranks the channels where the community is most active and engaged.',
      overview: 'This widget shows where the community conversation actually lives, combining scale and activity so you know which channels matter most.',
      metrics: [
        'Channel engagement rate, not just audience size.',
        'Member count, daily message volume, and growth.',
        'Channel type to separate work, housing, family, and other conversation spaces.',
      ],
    },
    ru: {
      short: 'Ранжирует каналы, где сообщество активнее всего и сильнее вовлечено.',
      overview: 'Этот виджет показывает, где разговор сообщества действительно живёт, совмещая масштаб и активность, чтобы было понятно, какие каналы важнее всего.',
      metrics: [
        'Уровень вовлечённости канала, а не только размер аудитории.',
        'Число участников, ежедневный объём сообщений и рост.',
        'Тип канала, чтобы отделять рабочие, жилищные, семейные и другие пространства общения.',
      ],
    },
  },
  key_voices: {
    en: {
      short: 'Surfaces the participants who appear most often in active discussions.',
      overview: 'This widget highlights the voices that shape the conversation most in the selected window so you can see who repeatedly shows up and where they contribute.',
      metrics: [
        'Frequency of appearance in discussion threads during the selected window.',
        'Reply participation rate and typical activity volume.',
        'Top topics and top channels associated with each voice.',
      ],
    },
    ru: {
      short: 'Показывает участников, которые чаще всего появляются в активных обсуждениях.',
      overview: 'Этот виджет выделяет голоса, которые сильнее других формируют разговор в выбранном окне, чтобы было видно, кто регулярно участвует и где именно.',
      metrics: [
        'Частота появления в обсуждениях внутри выбранного окна.',
        'Доля участия в ответах и типичный объём активности.',
        'Главные темы и каналы, связанные с каждым голосом.',
      ],
    },
  },
  recommendation_tracker: {
    en: {
      short: 'Shows what the community recommends most often to other people.',
      overview: 'This widget captures the places, services, and solutions the community repeatedly recommends, which is a strong trust signal.',
      metrics: [
        'Recommendation mention count for each item.',
        'Rating and sentiment around the recommendation.',
        'Category totals that show which recommendation types dominate.',
      ],
    },
    ru: {
      short: 'Показывает, что сообщество чаще всего советует другим людям.',
      overview: 'Этот виджет собирает места, сервисы и решения, которые участники регулярно рекомендуют друг другу, а это сильный сигнал доверия.',
      metrics: [
        'Число упоминаний рекомендации по каждому объекту.',
        'Рейтинг и тональность вокруг этой рекомендации.',
        'Суммы по категориям, показывающие, какие типы рекомендаций доминируют.',
      ],
    },
  },
  information_velocity: {
    en: {
      short: 'Measures how quickly topics spread and which channels amplify them.',
      overview: 'This widget explains how narratives travel through the network, from the channel where they start to the channels that multiply their reach.',
      metrics: [
        'Origin channel, spread time, and channels reached for each topic.',
        'Total estimated reach of the narrative.',
        'Amplifier channels that repeatedly help explosive topics travel.',
      ],
    },
    ru: {
      short: 'Измеряет, как быстро темы распространяются и какие каналы их усиливают.',
      overview: 'Этот виджет показывает, как нарративы проходят по сети: от канала-источника до каналов, которые многократно увеличивают охват.',
      metrics: [
        'Канал-источник, время распространения и число достигнутых каналов по каждой теме.',
        'Оценочный суммарный охват нарратива.',
        'Каналы-усилители, которые регулярно разгоняют взрывные темы.',
      ],
    },
  },
  persona_gallery: {
    en: {
      short: 'Clusters the audience into personas with different needs and behaviors.',
      overview: 'This widget groups similar members into practical personas so the team can understand who is in the community and what each group needs most.',
      metrics: [
        'Persona size and share of the community.',
        'Recurring profile traits, needs, interests, and pain points.',
        'Relative prominence of each cluster inside the audience mix.',
      ],
    },
    ru: {
      short: 'Группирует аудиторию в персоны с разными потребностями и поведением.',
      overview: 'Этот виджет объединяет похожих участников в практичные персоны, чтобы команда понимала, кто именно находится в сообществе и что важнее всего для каждой группы.',
      metrics: [
        'Размер персоны и её доля в сообществе.',
        'Повторяющиеся черты профиля, потребности, интересы и болевые точки.',
        'Относительная заметность каждого кластера внутри общей аудитории.',
      ],
    },
  },
  interest_radar: {
    en: {
      short: 'Shows which interest areas have the widest active-member reach.',
      overview: 'This widget measures how deeply major interest areas penetrate the active audience so you can prioritize topics with broad resonance.',
      metrics: [
        'Share of active members discussing each interest area.',
        'Relative score of each interest on the radar.',
        'Top interest leaders in the current selected window.',
      ],
    },
    ru: {
      short: 'Показывает, какие интересы имеют самый широкий охват среди активных участников.',
      overview: 'Этот виджет измеряет, насколько глубоко ключевые интересы проникают в активную аудиторию, чтобы можно было приоритизировать темы с широким откликом.',
      metrics: [
        'Доля активных участников, обсуждавших каждое направление интереса.',
        'Относительный балл каждого интереса на радаре.',
        'Лидирующие интересы внутри текущего выбранного окна.',
      ],
    },
  },
  emerging_interests: {
    en: {
      short: 'Flags newer topics that are still small but gathering momentum.',
      overview: 'This widget looks for recently appeared topics that are growing fast enough to matter early, before they become mainstream.',
      metrics: [
        'Topic age, first-seen timing, and current mention volume.',
        'Growth rate and early-signal strength for the topic.',
        'Origin channel and opportunity level for each emerging topic.',
      ],
    },
    ru: {
      short: 'Отмечает новые темы, которые пока небольшие, но уже набирают импульс.',
      overview: 'Этот виджет ищет недавно появившиеся темы, которые растут достаточно быстро, чтобы стать важными ещё до того, как станут мейнстримом.',
      metrics: [
        'Возраст темы, момент первого появления и текущий объём упоминаний.',
        'Темп роста и сила раннего сигнала по теме.',
        'Канал-источник и уровень возможности по каждой новой теме.',
      ],
    },
  },
  retention_risk_gauge: {
    en: {
      short: 'Estimates how likely active members are to stay engaged and what threatens that.',
      overview: 'This widget combines repeat-activity factors with churn signals to show whether the community is likely to keep people involved or lose them.',
      metrics: [
        'Continuity score built from weighted retention factors.',
        'Topic-level risk signals that run above the baseline.',
        'Signal counts and risk trend percentage for the most fragile areas.',
      ],
      note: 'A high continuity score is good, while rising risk signals mean specific topics need intervention.',
    },
    ru: {
      short: 'Оценивает, насколько вероятно, что активные участники останутся вовлечёнными, и что этому угрожает.',
      overview: 'Этот виджет объединяет факторы повторной активности и сигналы оттока, чтобы показать, удерживает ли сообщество людей или начинает их терять.',
      metrics: [
        'Индекс непрерывности, собранный из взвешенных факторов удержания.',
        'Сигналы риска по темам, которые превышают базовый уровень.',
        'Число сигналов и процент тренда риска в самых хрупких зонах.',
      ],
      note: 'Высокий индекс непрерывности полезен, а растущие сигналы риска означают, что конкретные темы требуют вмешательства.',
    },
  },
  community_growth_funnel: {
    en: {
      short: 'Shows how people move from passive reading into deeper participation.',
      overview: 'This widget measures how far members progress through the engagement journey, from reading to asking, helping, and leading.',
      metrics: [
        'Stage counts and percentages across the funnel.',
        'Drop-off between early stages and conversion into deeper participation.',
        'Estimated number of passive readers who have not yet crossed into visible contribution.',
      ],
    },
    ru: {
      short: 'Показывает, как люди переходят от пассивного чтения к более глубокой вовлечённости.',
      overview: 'Этот виджет измеряет, насколько далеко участники продвигаются по пути вовлечения: от чтения к вопросам, помощи и лидерству.',
      metrics: [
        'Число людей и доля на каждой стадии воронки.',
        'Отсев на ранних этапах и конверсия в более глубокое участие.',
        'Оценка числа пассивных читателей, которые ещё не перешли к заметному вкладу.',
      ],
    },
  },
  decision_stage_tracker: {
    en: {
      short: 'Maps members across different decision and relocation stages.',
      overview: 'This widget shows where people are in their Armenia journey so the team can align messaging, guidance, and services to each stage.',
      metrics: [
        'Count and share of people in each stage.',
        'Growth trend for every stage.',
        'Stage-specific needs that explain what each group is looking for next.',
      ],
    },
    ru: {
      short: 'Раскладывает участников по стадиям решения и релокационного пути.',
      overview: 'Этот виджет показывает, на каком этапе своего пути с Арменией находятся люди, чтобы команда могла подстраивать сообщения, гайды и сервисы под каждую стадию.',
      metrics: [
        'Число людей и доля на каждой стадии.',
        'Тренд роста по каждой стадии.',
        'Потребности этапа, объясняющие, что этой группе нужно дальше.',
      ],
    },
  },
  new_vs_returning_voice: {
    en: {
      short: 'Compares fresh contributors with returning regular voices.',
      overview: 'This widget helps you see whether the conversation is attracting new participants or being carried mostly by the same returning people.',
      metrics: [
        'Counts of new voices versus returning voices in each bucket.',
        'Share of new voices in the latest period and change versus the prior period.',
        'Topics where newcomers speak first or appear most strongly.',
      ],
    },
    ru: {
      short: 'Сравнивает новых участников с возвращающимися постоянными голосами.',
      overview: 'Этот виджет помогает понять, притягивает ли разговор новых участников или его в основном несут одни и те же возвращающиеся люди.',
      metrics: [
        'Число новых и возвращающихся голосов в каждой корзине.',
        'Доля новых голосов в последнем периоде и изменение к предыдущему периоду.',
        'Темы, где новички говорят первыми или проявляются сильнее всего.',
      ],
    },
  },
  business_opportunity_tracker: {
    en: {
      short: 'Converts repeated unmet needs into specific business ideas.',
      overview: 'This widget uses AI to turn recurring community demand into concrete opportunity cards, while keeping each idea tied to measurable evidence.',
      metrics: [
        'Repeated unmet-need clusters grounded in real messages.',
        'Demand signals: messages, unique people, channels, and 7-day trend.',
        'Confidence, launch readiness, and delivery model for each opportunity card.',
      ],
      note: 'These are synthesized ideas, but they are ranked by grounded demand signals rather than by creative speculation.',
    },
    ru: {
      short: 'Преобразует повторяющиеся неудовлетворённые потребности в конкретные бизнес-идеи.',
      overview: 'Этот виджет с помощью AI переводит повторяющийся спрос сообщества в понятные карточки возможностей, сохраняя каждую идею привязанной к измеримым доказательствам.',
      metrics: [
        'Кластеры повторяющихся неудовлетворённых потребностей, подтверждённые реальными сообщениями.',
        'Сигналы спроса: число сообщений, уникальных людей, каналов и 7-дневный тренд.',
        'Уверенность, готовность к запуску и модель реализации для каждой карточки возможности.',
      ],
      note: 'Это синтезированные идеи, но ранжируются они по подтверждённому спросу, а не по свободной креативной генерации.',
    },
  },
  job_market_pulse: {
    en: {
      short: 'Reads how work, hiring, and job-seeking show up in community discussion.',
      overview: 'This widget shows the shape of labor and employment demand inside the community so you can spot where work-related needs are strongest.',
      metrics: [
        'Share of work-intent signals by role or need category.',
        'Employment trend statements extracted from the conversation.',
        'Real evidence excerpts that support the strongest job signal.',
      ],
    },
    ru: {
      short: 'Показывает, как темы работы, найма и поиска занятости проявляются в обсуждениях сообщества.',
      overview: 'Этот виджет раскрывает структуру трудового спроса внутри сообщества, чтобы было видно, где рабочие потребности выражены сильнее всего.',
      metrics: [
        'Доля рабочих сигналов по роли или типу потребности.',
        'Трендовые выводы о занятости, извлечённые из разговора.',
        'Реальные фрагменты доказательств, подтверждающие самый сильный рабочий сигнал.',
      ],
    },
  },
  week_over_week_shifts: {
    en: {
      short: 'Compares core dashboard metrics with the previous matching window.',
      overview: 'This widget is a quick change detector. It tells you which important metrics improved, held flat, or worsened versus the prior equivalent period.',
      metrics: [
        'Current versus previous values for the tracked KPI set.',
        'Absolute and percent change for each metric.',
        'Metric semantics such as inverse indicators, where lower can be better.',
      ],
      note: 'Read the direction in context: an upward move is good for some metrics and bad for inverse metrics like churn signals.',
    },
    ru: {
      short: 'Сравнивает ключевые метрики дашборда с предыдущим сопоставимым окном.',
      overview: 'Этот виджет работает как быстрый детектор изменений. Он показывает, какие важные метрики улучшились, не изменились или ухудшились относительно предыдущего эквивалентного периода.',
      metrics: [
        'Текущие и предыдущие значения для набора ключевых KPI.',
        'Абсолютное и процентное изменение по каждой метрике.',
        'Семантика метрики, включая обратные индикаторы, где меньше может быть лучше.',
      ],
      note: 'Смотрите направление в контексте: рост хорош для одних метрик и плох для обратных показателей, например сигналов оттока.',
    },
  },
  sentiment_by_topic: {
    en: {
      short: 'Breaks each topic into positive, neutral, and negative conversation share.',
      overview: 'This widget shows how people feel about each major topic so you can see which conversations create goodwill and which ones generate friction.',
      metrics: [
        'Positive, neutral, and negative share inside each topic.',
        'Total topic volume to show sample size.',
        'Top positive and top negative topics for action planning.',
      ],
    },
    ru: {
      short: 'Раскладывает каждую тему на позитивную, нейтральную и негативную долю разговора.',
      overview: 'Этот виджет показывает, как люди относятся к каждой ключевой теме, чтобы было видно, какие разговоры создают доверие, а какие рождают трение.',
      metrics: [
        'Доля позитивных, нейтральных и негативных сообщений внутри каждой темы.',
        'Общий объём темы, чтобы понимать размер выборки.',
        'Самые позитивные и самые негативные темы для приоритизации действий.',
      ],
    },
  },
  content_performance: {
    en: {
      short: 'Shows which formats and posts create the strongest engagement.',
      overview: 'This widget helps the team understand what content format performs best and which individual posts are setting the bar.',
      metrics: [
        'Average engagement by content format.',
        'Volume count for each format so averages can be read with context.',
        'Top-performing posts ranked by engagement and sharing signals.',
      ],
      note: 'Use both the average score and the post count together. A format can look strong but still be based on a small sample.',
    },
    ru: {
      short: 'Показывает, какие форматы и публикации дают самую сильную вовлечённость.',
      overview: 'Этот виджет помогает понять, какой формат контента работает лучше всего и какие отдельные публикации задают планку.',
      metrics: [
        'Средняя вовлечённость по каждому формату контента.',
        'Число публикаций по формату, чтобы читать среднее значение в контексте.',
        'Лучшие публикации, ранжированные по вовлечённости и сигналам распространения.',
      ],
      note: 'Смотрите одновременно на средний балл и число публикаций. Формат может выглядеть сильным, но опираться на маленькую выборку.',
    },
  },
  social_situation_strip: {
    en: {
      short: 'Summarizes the current competitor social picture in five fast KPI tiles.',
      overview: 'This widget gives a 3-second view of how much social activity is being collected, how many ads are active, what tone dominates, and which topic leads the conversation.',
      metrics: [
        'Active tracked competitors from the Social entity registry.',
        'Collected social activities, detected ads, and the dominant topic within the selected range.',
        'Average sentiment score derived from stored Social analysis results.',
      ],
      note: 'Use these tiles as the top-line readout, then open the deeper widgets below for the explanation and evidence behind each number.',
    },
    ru: {
      short: 'Сводит картину по конкурентам в соцсетях к пяти быстрым KPI.',
      overview: 'Этот виджет даёт 3-секундный обзор: сколько social-активности собрано, сколько рекламных материалов обнаружено, какой тон доминирует и какая тема лидирует в разговоре.',
      metrics: [
        'Активные отслеживаемые конкуренты из Social-реестра сущностей.',
        'Собранные social-активности, обнаруженные объявления и доминирующая тема в выбранном диапазоне.',
        'Средний sentiment score на основе сохранённых результатов Social-анализа.',
      ],
      note: 'Используйте плитки как быстрый верхнеуровневый срез, а затем переходите к нижним виджетам за объяснением и доказательствами.',
    },
  },
  social_topic_timeline: {
    en: {
      short: 'Shows when volume changed and whether the tone became more positive or negative.',
      overview: 'This widget provides the time context for the Social dashboard by showing how activity volume and sentiment mix moved across the selected period.',
      metrics: [
        'Daily activity counts within the selected Social date range.',
        'Positive, neutral, and negative sentiment distribution for each time bucket.',
        'The strongest recent shift in tone or activity level for the current filter set.',
      ],
      note: 'Read this before the detailed topic cards so you know whether a spike is recent, sustained, or already cooling off.',
    },
    ru: {
      short: 'Показывает, когда объём менялся и становился ли тон более позитивным или негативным.',
      overview: 'Этот виджет даёт временной контекст для Social-дашборда, показывая, как менялись объём активности и состав тональности в выбранном периоде.',
      metrics: [
        'Ежедневное количество social-активностей в выбранном диапазоне.',
        'Распределение позитивной, нейтральной и негативной тональности по каждой временной корзине.',
        'Самый заметный недавний сдвиг в тоне или объёме для текущего набора фильтров.',
      ],
      note: 'Смотрите этот график до детальных карточек тем, чтобы понимать: всплеск только что начался, держится уже давно или уже затухает.',
    },
  },
  social_topic_intelligence: {
    en: {
      short: 'Ranks the topics competitors discuss most and shows how people feel about them.',
      overview: 'This widget turns extracted topic labels into an analyst-friendly ranking so you can see what competitors are talking about, how large each topic is, and what tone surrounds it.',
      metrics: [
        'Topic frequency from stored Social analysis payloads.',
        'Average sentiment and sentiment distribution for each topic.',
        'Top entities, platforms, and evidence-backed topic summaries.',
      ],
      note: 'Each topic card is one click away from the raw evidence so analysts can verify the narrative instead of trusting the label alone.',
    },
    ru: {
      short: 'Ранжирует темы, о которых конкуренты говорят чаще всего, и показывает тон вокруг них.',
      overview: 'Этот виджет превращает извлечённые topic labels в аналитический рейтинг, чтобы было видно, о чём говорят конкуренты, насколько велика каждая тема и какая тональность её сопровождает.',
      metrics: [
        'Частота тем из сохранённых payload-ов Social-анализа.',
        'Средняя тональность и распределение sentiment по каждой теме.',
        'Топовые сущности, платформы и краткие summary, подтверждённые доказательствами.',
      ],
      note: 'Каждая карточка темы открывает сырой evidence-слой, чтобы аналитик мог проверить вывод, а не просто доверять ярлыку.',
    },
  },
  social_ad_intelligence: {
    en: {
      short: 'Shows the competitor ads currently detected in the Social evidence stream.',
      overview: 'This widget isolates ad-like activity so analysts can see what competitors are promoting, how they frame the offer, and which intent or urgency signals appear most often.',
      metrics: [
        'Activities tagged as ads or Google-origin campaign items.',
        'CTA type, content format, engagement totals, and publish timing.',
        'AI-extracted intent, products, value propositions, and urgency indicators.',
      ],
      note: 'Unlike a normal feed, this view is filtered for ad intelligence, so the card text itself is the evidence behind the insight.',
    },
    ru: {
      short: 'Показывает конкурентные объявления, обнаруженные в Social evidence stream.',
      overview: 'Этот виджет выделяет рекламоподобную активность, чтобы аналитик видел, что именно продвигают конкуренты, как формулируют оффер и какие intent или urgency-сигналы встречаются чаще всего.',
      metrics: [
        'Активности, помеченные как ads, или кампанийные элементы Google.',
        'Тип CTA, формат контента, суммарная вовлечённость и время публикации.',
        'AI-извлечённые intent, продукты, value propositions и urgency indicators.',
      ],
      note: 'В отличие от обычной ленты, здесь сама карточка уже является доказательством рекламного инсайта.',
    },
  },
  social_audience_response: {
    en: {
      short: 'Compares how audiences react to each competitor and what pain points show up most.',
      overview: 'This widget separates entity-level sentiment from the underlying pain points and customer-intent signals so analysts can see not just whether reaction is good or bad, but what is driving it.',
      metrics: [
        'Entity-by-entity sentiment splits across the current filter set.',
        'Ranked pain points and customer-intent labels extracted from analyzed activities.',
        'Dominant sentiment and associated entities for each repeated response signal.',
      ],
      note: 'This is where the Social system becomes differentiated: it surfaces the specific problems audiences react to, not only generic sentiment.',
    },
    ru: {
      short: 'Сравнивает реакцию аудитории по конкурентам и показывает самые частые pain points.',
      overview: 'Этот виджет разделяет тональность по сущностям и underlying pain points / customer-intent сигналы, чтобы аналитик видел не только хороший или плохой отклик, но и то, что именно его вызывает.',
      metrics: [
        'Разделение sentiment по сущностям в текущем наборе фильтров.',
        'Ранжированные pain points и customer-intent labels, извлечённые из проанализированных активностей.',
        'Доминирующая тональность и связанные сущности для каждого повторяющегося response-сигнала.',
      ],
      note: 'Именно здесь Social-система начинает отличаться: она показывает конкретные проблемы, на которые реагирует аудитория, а не только общий sentiment.',
    },
  },
  social_competitor_scorecard: {
    en: {
      short: 'Compares competitors side by side across core Social intelligence metrics.',
      overview: 'This widget is the executive comparison table for the Social dashboard. It lets someone quickly see who is most active, who is advertising hardest, what intent dominates, and which topics or propositions define each competitor.',
      metrics: [
        'Posts and ads count for each tracked competitor.',
        'Average sentiment plus top marketing intent, topics, products, and value propositions.',
        'Expandable evidence rows with recent activities for each competitor.',
      ],
      note: 'Use this table for competitor briefing and prioritization. It is built for comparison first, not deep reading.',
    },
    ru: {
      short: 'Сравнивает конкурентов бок о бок по ключевым метрикам Social intelligence.',
      overview: 'Этот виджет — executive comparison table для Social-дашборда. Он позволяет быстро увидеть, кто активнее всех, кто сильнее давит рекламой, какой intent доминирует и какие темы или ценностные предложения определяют каждого конкурента.',
      metrics: [
        'Количество публикаций и объявлений по каждому отслеживаемому конкуренту.',
        'Средняя тональность плюс топовый marketing intent, темы, продукты и value propositions.',
        'Раскрывающиеся evidence-строки с недавними активностями по каждому конкуренту.',
      ],
      note: 'Используйте эту таблицу для конкурентных брифингов и приоритизации. Она создана прежде всего для сравнения, а не для детального чтения.',
    },
  },
} satisfies Record<AdminWidgetId, WidgetExplanation>;

const registryIds = new Set<AdminWidgetId>(Object.keys(WIDGET_EXPLANATIONS) as AdminWidgetId[]);
const catalogIds = new Set<AdminWidgetId>(ADMIN_WIDGET_DEFINITIONS.map((widget) => widget.id));
const missingWidgetExplanations = ADMIN_WIDGET_DEFINITIONS
  .map((widget) => widget.id)
  .filter((id) => !registryIds.has(id));
const extraWidgetExplanations = (Object.keys(WIDGET_EXPLANATIONS) as AdminWidgetId[])
  .filter((id) => !catalogIds.has(id));

if (missingWidgetExplanations.length > 0 || extraWidgetExplanations.length > 0) {
  throw new Error(
    `Widget explanation registry is out of sync. Missing: ${missingWidgetExplanations.join(', ') || 'none'}. Extra: ${extraWidgetExplanations.join(', ') || 'none'}.`,
  );
}

export function getWidgetExplanation(widgetId: AdminWidgetId, lang: Lang): WidgetExplanationContent {
  return WIDGET_EXPLANATIONS[widgetId][lang];
}

export function getWidgetLabel(widgetId: AdminWidgetId, lang: Lang): string {
  return WIDGET_LABELS[widgetId][lang];
}
