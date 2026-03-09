// ================================================================
// CENTRALIZED MOCK DATA
// ================================================================
// ALL hardcoded data from all widgets and pages lives here.
// When you connect a real backend, this file becomes irrelevant —
// the DataContext will fetch from your API instead.
//
// This file is intentionally large (~2500 lines) because it
// consolidates data from 8 widget files + 3 page files.
// ================================================================

import type { AppData } from '../types/data';

export const mockAppData: AppData = {

  // ════════════════════════════════════════════════════════
  // TIER 1: COMMUNITY PULSE
  // ════════════════════════════════════════════════════════

  communityHealth: {
    currentScore: 71,
    weekAgoScore: 63,
    history: [
      { time: '6h ago', score: 64 }, { time: '5h ago', score: 65 },
      { time: '4h ago', score: 67 }, { time: '3h ago', score: 66 },
      { time: '2h ago', score: 68 }, { time: '1h ago', score: 69 },
      { time: 'Now', score: 71 },
    ],
    components: {
      en: [
        { label: 'Engagement Rate', value: 74, trend: +6, desc: 'Replies, reactions, shares' },
        { label: 'Community Growth', value: 68, trend: +12, desc: 'New members & active users' },
        { label: 'Positive Sentiment', value: 62, trend: +3, desc: 'Helpful, excited, satisfied' },
        { label: 'Content Velocity', value: 78, trend: +8, desc: 'Posts per day trending up' },
      ],
      ru: [
        { label: 'Уровень вовлечённости', value: 74, trend: +6, desc: 'Ответы, реакции, репосты' },
        { label: 'Рост сообщества', value: 68, trend: +12, desc: 'Новые участники и активные пользователи' },
        { label: 'Позитивные настроения', value: 62, trend: +3, desc: 'Отзывчивость, энтузиазм, удовлетворённость' },
        { label: 'Скорость публикаций', value: 78, trend: +8, desc: 'Количество публикаций в день растёт' },
      ],
    },
  },

  trendingTopics: {
    en: [
      { id: 1, topic: 'Apartment rental prices in Yerevan center', mentions: 342, trend: +89, category: 'Housing', sentiment: 'frustrated', sampleQuote: 'Prices went up 30% since last year, especially near Cascade' },
      { id: 2, topic: 'Best international schools for Russian-speaking kids', mentions: 278, trend: +125, category: 'Education', sentiment: 'seeking', sampleQuote: 'QSI is expensive, anyone tried Ayb School?' },
      { id: 3, topic: 'Opening a business as a foreign resident', mentions: 245, trend: +67, category: 'Business', sentiment: 'curious', sampleQuote: 'Do I need a local partner or can I register an LLC alone?' },
      { id: 4, topic: 'Armenian language courses for adults', mentions: 198, trend: +42, category: 'Language', sentiment: 'motivated', sampleQuote: 'I want to learn basic Armenian, where to start?' },
      { id: 5, topic: 'Healthcare quality — finding good doctors', mentions: 187, trend: +55, category: 'Healthcare', sentiment: 'concerned', sampleQuote: 'Need a good pediatrician who speaks Russian in Yerevan' },
      { id: 6, topic: 'Weekend trips and nature spots near Yerevan', mentions: 165, trend: +38, category: 'Lifestyle', sentiment: 'excited', sampleQuote: 'Dilijan in autumn is absolutely stunning, highly recommend' },
      { id: 7, topic: 'Tax residency and banking setup', mentions: 156, trend: +95, category: 'Finance', sentiment: 'confused', sampleQuote: 'Can I open an account at Ameriabank without residency?' },
    ],
    ru: [
      { id: 1, topic: 'Цены на аренду квартир в центре Еревана', mentions: 342, trend: +89, category: 'Жильё', sentiment: 'frustrated', sampleQuote: 'Цены выросли на 30% за год, особенно в районе Каскада' },
      { id: 2, topic: 'Лучшие международные школы для русскоязычных детей', mentions: 278, trend: +125, category: 'Образование', sentiment: 'seeking', sampleQuote: 'QSI — дорого, кто-нибудь пробовал школу Айб?' },
      { id: 3, topic: 'Открытие бизнеса как иностранный резидент', mentions: 245, trend: +67, category: 'Бизнес', sentiment: 'curious', sampleQuote: 'Нужен ли местный партнёр или можно открыть ООО самостоятельно?' },
      { id: 4, topic: 'Курсы армянского языка для взрослых', mentions: 198, trend: +42, category: 'Язык', sentiment: 'motivated', sampleQuote: 'Хочу выучить базовый армянский — с чего начать?' },
      { id: 5, topic: 'Качество медицины — поиск хороших врачей', mentions: 187, trend: +55, category: 'Медицина', sentiment: 'concerned', sampleQuote: 'Нужен хороший педиатр с русским языком в Ереване' },
      { id: 6, topic: 'Поездки выходного дня и природные места рядом с Ереваном', mentions: 165, trend: +38, category: 'Досуг', sentiment: 'excited', sampleQuote: 'Дилижан осенью — это что-то невероятное, очень рекомендую' },
      { id: 7, topic: 'Налоговое резидентство и банковское обслуживание', mentions: 156, trend: +95, category: 'Финансы', sentiment: 'confused', sampleQuote: 'Можно ли открыть счёт в Ameriabank без вида на жительство?' },
    ],
  },

  communityBrief: {
    messagesAnalyzed: 14238,
    updatedMinutesAgo: 35,
    activeMembers: '12.4K',
    messagesToday: '3,847',
    positiveMood: '68%',
    newMembersGrowth: '+34%',
    mainBrief: {
      en: 'The community is buzzing about housing affordability — rental prices in central Yerevan dominate conversations with a frustrated but pragmatic tone. The second biggest theme is children\'s education, with parents actively comparing international schools and seeking Russian-language options. New member introductions are up 34% this week, with most newcomers arriving from Moscow and St. Petersburg, asking about banking, SIM cards, and neighborhood recommendations.',
      ru: 'Сообщество активно обсуждает доступность жилья — цены на аренду в центре Еревана доминируют в разговорах с раздражённым, но прагматичным тоном. Вторая по значимости тема — образование детей: родители сравнивают международные школы и ищут варианты с русским языком обучения. Количество новых знакомств выросло на 34% за неделю, большинство прибывших — из Москвы и Санкт-Петербурга, они спрашивают о банках, SIM-картах и рекомендациях по районам.',
    },
    expandedBrief: {
      en: [
        'There\'s a growing cluster of IT freelancers discussing coworking spaces, tax optimization, and crypto-friendly banking. Weekend activity groups (hiking, board games, kids\' playgroups) are getting the highest engagement per post. People are increasingly asking about Armenian language courses — a strong integration signal.',
        'Create a pinned "Newcomer\'s Checklist" guide covering banking, SIM, registration, and neighborhoods. The same 15 questions get asked daily. A comprehensive guide would reduce repetitive posts and become the community\'s most shared resource.',
      ],
      ru: [
        'Растёт кластер IT-фрилансеров, обсуждающих коворкинги, налоговую оптимизацию и крипто-дружественные банки. Группы выходного дня (хайкинг, настольные игры, детские площадки) получают наибольшее вовлечение на пост. Запросы на курсы армянского языка усиливаются — сильный сигнал интеграции.',
        'Создайте закреплённое руководство «Чеклист новичка», охватывающее банки, SIM-карту, регистрацию и районы. Одни и те же 15 вопросов задаются ежедневно. Исчерпывающее руководство сократит повторяющиеся публикации и станет самым репостируемым ресурсом сообщества.',
      ],
    },
  },

  // ════════════════════════════════════════════════════════
  // TIER 2: STRATEGIC / TOPICS
  // ════════════════════════════════════════════════════════

  topicBubbles: {
    en: [
      { name: 'Housing & Rent', value: 2840, category: 'Living', color: '#ef4444', growth: +12 },
      { name: 'Jobs & Freelance', value: 2200, category: 'Work', color: '#3b82f6', growth: +18 },
      { name: 'Schools & Education', value: 1950, category: 'Family', color: '#8b5cf6', growth: +25 },
      { name: 'Banking & Finance', value: 1680, category: 'Finance', color: '#f59e0b', growth: +32 },
      { name: 'Healthcare', value: 1420, category: 'Living', color: '#f97316', growth: +8 },
      { name: 'Food & Restaurants', value: 1380, category: 'Lifestyle', color: '#ec4899', growth: +5 },
      { name: 'Legal & Residency', value: 1250, category: 'Admin', color: '#6b7280', growth: +42 },
      { name: 'Armenian Language', value: 1100, category: 'Integration', color: '#10b981', growth: +55 },
      { name: 'Kids Activities', value: 980, category: 'Family', color: '#8b5cf6', growth: +20 },
      { name: 'Coworking & Office', value: 920, category: 'Work', color: '#3b82f6', growth: +35 },
      { name: 'Outdoor & Hiking', value: 850, category: 'Lifestyle', color: '#ec4899', growth: +15 },
      { name: 'Taxi & Transport', value: 780, category: 'Living', color: '#ef4444', growth: +3 },
      { name: 'Shopping & Markets', value: 720, category: 'Lifestyle', color: '#ec4899', growth: +7 },
      { name: 'Internet & Mobile', value: 680, category: 'Tech', color: '#06b6d4', growth: +10 },
      { name: 'Pets', value: 520, category: 'Lifestyle', color: '#ec4899', growth: +28 },
      { name: 'Gym & Sports', value: 480, category: 'Lifestyle', color: '#ec4899', growth: +22 },
    ],
    ru: [
      { name: 'Жильё и аренда', value: 2840, category: 'Быт', color: '#ef4444', growth: +12 },
      { name: 'Работа и фриланс', value: 2200, category: 'Работа', color: '#3b82f6', growth: +18 },
      { name: 'Школы и образование', value: 1950, category: 'Семья', color: '#8b5cf6', growth: +25 },
      { name: 'Банки и финансы', value: 1680, category: 'Финансы', color: '#f59e0b', growth: +32 },
      { name: 'Медицина', value: 1420, category: 'Быт', color: '#f97316', growth: +8 },
      { name: 'Еда и рестораны', value: 1380, category: 'Досуг', color: '#ec4899', growth: +5 },
      { name: 'Документы и ВНЖ', value: 1250, category: 'Документы', color: '#6b7280', growth: +42 },
      { name: 'Армянский язык', value: 1100, category: 'Интеграция', color: '#10b981', growth: +55 },
      { name: 'Детский досуг', value: 980, category: 'Семья', color: '#8b5cf6', growth: +20 },
      { name: 'Коворкинги', value: 920, category: 'Работа', color: '#3b82f6', growth: +35 },
      { name: 'Природа и хайкинг', value: 850, category: 'Досуг', color: '#ec4899', growth: +15 },
      { name: 'Такси и транспорт', value: 780, category: 'Быт', color: '#ef4444', growth: +3 },
      { name: 'Покупки и рынки', value: 720, category: 'Досуг', color: '#ec4899', growth: +7 },
      { name: 'Интернет и связь', value: 680, category: 'Технологии', color: '#06b6d4', growth: +10 },
      { name: 'Питомцы', value: 520, category: 'Досуг', color: '#ec4899', growth: +28 },
      { name: 'Спорт и фитнес', value: 480, category: 'Досуг', color: '#ec4899', growth: +22 },
    ],
  },

  trendLines: {
    en: [
      { key: 'housing', label: 'Housing & Rent', color: '#ef4444', current: 290, change: +61 },
      { key: 'education', label: 'Education', color: '#8b5cf6', current: 200, change: +122 },
      { key: 'banking', label: 'Banking', color: '#f59e0b', current: 170, change: +183 },
      { key: 'language', label: 'Armenian Language', color: '#10b981', current: 110, change: +175 },
      { key: 'coworking', label: 'Coworking', color: '#3b82f6', current: 92, change: +207 },
      { key: 'healthcare', label: 'Healthcare', color: '#f97316', current: 118, change: -2 },
    ],
    ru: [
      { key: 'housing', label: 'Жильё и аренда', color: '#ef4444', current: 290, change: +61 },
      { key: 'education', label: 'Образование', color: '#8b5cf6', current: 200, change: +122 },
      { key: 'banking', label: 'Банки и финансы', color: '#f59e0b', current: 170, change: +183 },
      { key: 'language', label: 'Армянский язык', color: '#10b981', current: 110, change: +175 },
      { key: 'coworking', label: 'Коворкинги', color: '#3b82f6', current: 92, change: +207 },
      { key: 'healthcare', label: 'Медицина', color: '#f97316', current: 118, change: -2 },
    ],
  },

  trendData: [
    { week: 'W1', housing: 180, education: 90, banking: 60, language: 40, coworking: 30, healthcare: 120 },
    { week: 'W2', housing: 200, education: 110, banking: 75, language: 48, coworking: 38, healthcare: 115 },
    { week: 'W3', housing: 220, education: 140, banking: 95, language: 58, coworking: 52, healthcare: 125 },
    { week: 'W4', housing: 250, education: 160, banking: 120, language: 70, coworking: 65, healthcare: 118 },
    { week: 'W5', housing: 260, education: 180, banking: 140, language: 85, coworking: 78, healthcare: 122 },
    { week: 'W6', housing: 280, education: 195, banking: 155, language: 95, coworking: 85, healthcare: 120 },
    { week: 'W7', housing: 290, education: 200, banking: 170, language: 110, coworking: 92, healthcare: 118 },
  ],

  heatmap: {
    en: {
      contentTypes: ['Question', 'Recommendation', 'Review', 'Photo/Video', 'How-to Guide', 'Discussion'],
      topicCols: ['Housing', 'Food', 'Education', 'Legal', 'Lifestyle', 'Tech'],
      engagement: {
        'Question':       { Housing: 85, Food: 45, Education: 78, Legal: 92, Lifestyle: 35, Tech: 55 },
        'Recommendation': { Housing: 72, Food: 88, Education: 65, Legal: 40, Lifestyle: 82, Tech: 60 },
        'Review':         { Housing: 55, Food: 92, Education: 70, Legal: 28, Lifestyle: 75, Tech: 48 },
        'Photo/Video':    { Housing: 38, Food: 95, Education: 30, Legal: 10, Lifestyle: 90, Tech: 25 },
        'How-to Guide':   { Housing: 60, Food: 42, Education: 55, Legal: 88, Lifestyle: 35, Tech: 82 },
        'Discussion':     { Housing: 68, Food: 50, Education: 62, Legal: 72, Lifestyle: 58, Tech: 65 },
      },
    },
    ru: {
      contentTypes: ['Вопрос', 'Рекомендация', 'Отзыв', 'Фото/Видео', 'Гайд', 'Обсуждение'],
      topicCols: ['Жильё', 'Еда', 'Образование', 'Документы', 'Досуг', 'Технологии'],
      engagement: {
        'Вопрос':         { 'Жильё': 85, 'Еда': 45, 'Образование': 78, 'Документы': 92, 'Досуг': 35, 'Технологии': 55 },
        'Рекомендация':   { 'Жильё': 72, 'Еда': 88, 'Образование': 65, 'Документы': 40, 'Досуг': 82, 'Технологии': 60 },
        'Отзыв':          { 'Жильё': 55, 'Еда': 92, 'Образование': 70, 'Документы': 28, 'Досуг': 75, 'Технологии': 48 },
        'Фото/Видео':     { 'Жильё': 38, 'Еда': 95, 'Образование': 30, 'Документы': 10, 'Досуг': 90, 'Технологии': 25 },
        'Гайд':           { 'Жильё': 60, 'Еда': 42, 'Образование': 55, 'Документы': 88, 'Досуг': 35, 'Технологии': 82 },
        'Обсуждение':     { 'Жильё': 68, 'Еда': 50, 'Образование': 62, 'Документы': 72, 'Досуг': 58, 'Технологии': 65 },
      },
    },
  },

  questionCategories: {
    en: [
      { category: 'Getting Started', color: '#3b82f6', questions: [
        { q: 'How to open a bank account?', count: 342, answered: true },
        { q: 'Best SIM card / mobile operator?', count: 298, answered: true },
        { q: 'How to register as a resident?', count: 267, answered: false },
        { q: 'Which neighborhood to live in?', count: 245, answered: true },
      ]},
      { category: 'Daily Life', color: '#10b981', questions: [
        { q: 'Where to find good Russian-speaking doctor?', count: 198, answered: false },
        { q: 'Cheapest grocery stores?', count: 176, answered: true },
        { q: 'How does garbage collection work?', count: 134, answered: false },
        { q: 'Best water delivery service?', count: 112, answered: true },
      ]},
      { category: 'Work & Business', color: '#f59e0b', questions: [
        { q: 'How to pay taxes as freelancer?', count: 223, answered: false },
        { q: 'Where to find coworking space?', count: 189, answered: true },
        { q: 'How to hire local employees?', count: 145, answered: false },
        { q: 'Best internet for remote work?', count: 167, answered: true },
      ]},
      { category: 'Family & Kids', color: '#8b5cf6', questions: [
        { q: 'Best schools with Russian program?', count: 278, answered: false },
        { q: 'Where to find a nanny?', count: 156, answered: false },
        { q: 'Kids activities on weekends?', count: 134, answered: true },
        { q: 'Pediatrician recommendations?', count: 189, answered: false },
      ]},
    ],
    ru: [
      { category: 'Начало пути', color: '#3b82f6', questions: [
        { q: 'Как открыть банковский счёт?', count: 342, answered: true },
        { q: 'Лучшая SIM-карта / оператор?', count: 298, answered: true },
        { q: 'Как зарегистрироваться как резидент?', count: 267, answered: false },
        { q: 'В каком районе лучше жить?', count: 245, answered: true },
      ]},
      { category: 'Повседневная жизнь', color: '#10b981', questions: [
        { q: 'Где найти русскоязычного врача?', count: 198, answered: false },
        { q: 'Самые дешёвые продуктовые магазины?', count: 176, answered: true },
        { q: 'Как работает вывоз мусора?', count: 134, answered: false },
        { q: 'Лучшая служба доставки воды?', count: 112, answered: true },
      ]},
      { category: 'Работа и бизнес', color: '#f59e0b', questions: [
        { q: 'Как платить налоги как фрилансер?', count: 223, answered: false },
        { q: 'Где найти коворкинг?', count: 189, answered: true },
        { q: 'Как нанять местных сотрудников?', count: 145, answered: false },
        { q: 'Лучший интернет для удалённой работы?', count: 167, answered: true },
      ]},
      { category: 'Семья и дети', color: '#8b5cf6', questions: [
        { q: 'Лучшие школы с русской программой?', count: 278, answered: false },
        { q: 'Где найти няню?', count: 156, answered: false },
        { q: 'Занятия для детей на выходных?', count: 134, answered: true },
        { q: 'Рекомендации педиатра?', count: 189, answered: false },
      ]},
    ],
  },

  qaGap: {
    en: [
      { topic: 'Legal & Residency', asked: 1250, rate: 22 },
      { topic: 'Mental Health Support', asked: 420, rate: 25 },
      { topic: 'Healthcare Access', asked: 980, rate: 32 },
      { topic: 'Schools & Education', asked: 1100, rate: 38 },
      { topic: 'Banking & Finance', asked: 860, rate: 44 },
      { topic: 'Housing & Rent', asked: 2100, rate: 50 },
      { topic: 'Jobs & Freelance', asked: 720, rate: 56 },
      { topic: 'Food & Restaurants', asked: 540, rate: 78 },
      { topic: 'Outdoor & Hiking', asked: 380, rate: 82 },
    ],
    ru: [
      { topic: 'Документы и ВНЖ', asked: 1250, rate: 22 },
      { topic: 'Психологическая помощь', asked: 420, rate: 25 },
      { topic: 'Медицинская помощь', asked: 980, rate: 32 },
      { topic: 'Школы и образование', asked: 1100, rate: 38 },
      { topic: 'Банки и финансы', asked: 860, rate: 44 },
      { topic: 'Жильё и аренда', asked: 2100, rate: 50 },
      { topic: 'Работа и фриланс', asked: 720, rate: 56 },
      { topic: 'Еда и рестораны', asked: 540, rate: 78 },
      { topic: 'Природа и хайкинг', asked: 380, rate: 82 },
    ],
  },

  lifecycleStages: {
    en: [
      { stage: 'Emerging', color: '#10b981', bgColor: 'bg-emerald-50', borderColor: 'border-emerald-200', textColor: 'text-emerald-700', desc: 'Jump in early to lead',
        topics: [{ name: 'Co-living for nomads', daysActive: 3, momentum: +120, volume: 180 }, { name: 'Armenian language learning', daysActive: 8, momentum: +85, volume: 320 }] },
      { stage: 'Rising', color: '#3b82f6', bgColor: 'bg-blue-50', borderColor: 'border-blue-200', textColor: 'text-blue-700', desc: 'Double down now',
        topics: [{ name: 'Banking & Finance', daysActive: 21, momentum: +42, volume: 1680 }, { name: 'Schools & Education', daysActive: 14, momentum: +35, volume: 1950 }, { name: 'Armenian wine tourism', daysActive: 10, momentum: +28, volume: 580 }] },
      { stage: 'Peak', color: '#f59e0b', bgColor: 'bg-amber-50', borderColor: 'border-amber-200', textColor: 'text-amber-700', desc: 'Highest volume, steady',
        topics: [{ name: 'Housing & Rent', daysActive: 45, momentum: +8, volume: 2840 }, { name: 'Jobs & Freelance', daysActive: 38, momentum: +5, volume: 2200 }] },
      { stage: 'Declining', color: '#6b7280', bgColor: 'bg-gray-50', borderColor: 'border-gray-200', textColor: 'text-gray-600', desc: 'Archive or pivot',
        topics: [{ name: 'Relocation Logistics', daysActive: 12, momentum: -18, volume: 620 }, { name: 'SIM & Internet Setup', daysActive: 20, momentum: -32, volume: 380 }] },
    ],
    ru: [
      { stage: 'Зарождение', color: '#10b981', bgColor: 'bg-emerald-50', borderColor: 'border-emerald-200', textColor: 'text-emerald-700', desc: 'Вступайте раньше других',
        topics: [{ name: 'Коливинг для номадов', daysActive: 3, momentum: +120, volume: 180 }, { name: 'Курсы армянского языка', daysActive: 8, momentum: +85, volume: 320 }] },
      { stage: 'Подъём', color: '#3b82f6', bgColor: 'bg-blue-50', borderColor: 'border-blue-200', textColor: 'text-blue-700', desc: 'Усильте присутствие сейчас',
        topics: [{ name: 'Банки и финансы', daysActive: 21, momentum: +42, volume: 1680 }, { name: 'Школы и образование', daysActive: 14, momentum: +35, volume: 1950 }, { name: 'Армянский винный туризм', daysActive: 10, momentum: +28, volume: 580 }] },
      { stage: 'Пик', color: '#f59e0b', bgColor: 'bg-amber-50', borderColor: 'border-amber-200', textColor: 'text-amber-700', desc: 'Максимальный объём, стабильно',
        topics: [{ name: 'Жильё и аренда', daysActive: 45, momentum: +8, volume: 2840 }, { name: 'Работа и фриланс', daysActive: 38, momentum: +5, volume: 2200 }] },
      { stage: 'Спад', color: '#6b7280', bgColor: 'bg-gray-50', borderColor: 'border-gray-200', textColor: 'text-gray-600', desc: 'Архивировать или сменить фокус',
        topics: [{ name: 'Логистика переезда', daysActive: 12, momentum: -18, volume: 620 }, { name: 'SIM-карта и интернет', daysActive: 20, momentum: -32, volume: 380 }] },
    ],
  },

  // ════════════════════════════════════════════════════════
  // TIER 3: BEHAVIORAL / PROBLEMS
  // ════════════════════════════════════════════════════════

  problems: {
    en: [
      { category: 'Housing', problems: [
        { name: 'Rent prices too high', mentions: 1840, severity: 'high', trend: +22, quote: '"2 bedroom near center is now $800, was $500 last year"' },
        { name: 'Landlords raising prices mid-lease', mentions: 920, severity: 'high', trend: +45, quote: '"My landlord wants 40% more or I have to leave"' },
        { name: 'Poor apartment quality', mentions: 680, severity: 'medium', trend: +8, quote: '"Heating doesn\'t work and owner won\'t fix it"' },
      ]},
      { category: 'Bureaucracy', problems: [
        { name: 'Residency permit delays', mentions: 1420, severity: 'high', trend: +35, quote: '"Waiting 3 months already, no update"' },
        { name: 'Confusing tax system', mentions: 980, severity: 'medium', trend: +18, quote: '"Nobody can explain how freelancer taxes work here"' },
        { name: 'Bank account requirements unclear', mentions: 720, severity: 'medium', trend: +12, quote: '"Each bank asks for different documents"' },
      ]},
      { category: 'Services', problems: [
        { name: 'Finding Russian-speaking doctors', mentions: 1100, severity: 'high', trend: +30, quote: '"Went to 4 clinics, communication was a nightmare"' },
        { name: 'Childcare shortage', mentions: 860, severity: 'high', trend: +42, quote: '"Waiting list for kindergarten is 6+ months"' },
        { name: 'Delivery services unreliable', mentions: 540, severity: 'low', trend: +5, quote: '"Glovo doesn\'t deliver to my area"' },
      ]},
    ],
    ru: [
      { category: 'Жильё', problems: [
        { name: 'Слишком высокие цены на аренду', mentions: 1840, severity: 'high', trend: +22, quote: '"Двушка рядом с центром — $800, год назад было $500"' },
        { name: 'Арендодатели повышают цены в период аренды', mentions: 920, severity: 'high', trend: +45, quote: '"Хозяин требует на 40% больше, иначе выезжай"' },
        { name: 'Плохое состояние квартир', mentions: 680, severity: 'medium', trend: +8, quote: '"Отопление не работает, а хозяин игнорирует"' },
      ]},
      { category: 'Бюрократия', problems: [
        { name: 'Задержки оформления ВНЖ', mentions: 1420, severity: 'high', trend: +35, quote: '"Жду уже 3 месяца, никаких обновлений"' },
        { name: 'Непрозрачная налоговая система', mentions: 980, severity: 'medium', trend: +18, quote: '"Никто не может объяснить, как платить налоги фрилансеру"' },
        { name: 'Неясные требования банков', mentions: 720, severity: 'medium', trend: +12, quote: '"Каждый банк требует разные документы"' },
      ]},
      { category: 'Услуги', problems: [
        { name: 'Поиск русскоязычных врачей', mentions: 1100, severity: 'high', trend: +30, quote: '"Обошёл 4 клиники — везде языковой барьер"' },
        { name: 'Нехватка мест в детских садах', mentions: 860, severity: 'high', trend: +42, quote: '"Очередь в садик — больше 6 месяцев"' },
        { name: 'Ненадёжные службы доставки', mentions: 540, severity: 'low', trend: +5, quote: '"Glovo не доставляет в мой район"' },
      ]},
    ],
  },

  serviceGaps: {
    en: [
      { service: 'Russian-speaking pediatrician', demand: 420, supply: 'Very low', gap: 95, growth: +38, supplyLevel: 'very_low' as const },
      { service: 'Affordable coworking outside center', demand: 380, supply: 'None', gap: 100, growth: +65, supplyLevel: 'none' as const },
      { service: 'English/Russian kindergarten', demand: 340, supply: 'Low', gap: 88, growth: +42, supplyLevel: 'low' as const },
      { service: 'Reliable home cleaning service', demand: 310, supply: 'Low', gap: 78, growth: +22, supplyLevel: 'low' as const },
      { service: 'Pet-friendly rental apartments', demand: 280, supply: 'Very low', gap: 92, growth: +55, supplyLevel: 'very_low' as const },
      { service: 'Crypto-friendly banking', demand: 260, supply: 'None', gap: 100, growth: +120, supplyLevel: 'none' as const },
      { service: 'Weekend cooking classes', demand: 220, supply: 'Medium', gap: 45, growth: +30, supplyLevel: 'moderate' as const },
      { service: 'Russian-language therapy/counseling', demand: 200, supply: 'Very low', gap: 90, growth: +48, supplyLevel: 'very_low' as const },
      { service: 'Car rental (monthly, affordable)', demand: 190, supply: 'Low', gap: 72, growth: +18, supplyLevel: 'low' as const },
      { service: 'Children\'s sport clubs (swimming, martial arts)', demand: 175, supply: 'Medium', gap: 55, growth: +25, supplyLevel: 'moderate' as const },
    ],
    ru: [
      { service: 'Русскоязычный педиатр', demand: 420, supply: 'Очень мало', gap: 95, growth: +38, supplyLevel: 'very_low' as const },
      { service: 'Доступный коворкинг за пределами центра', demand: 380, supply: 'Нет', gap: 100, growth: +65, supplyLevel: 'none' as const },
      { service: 'Детский сад с английским / русским языком', demand: 340, supply: 'Мало', gap: 88, growth: +42, supplyLevel: 'low' as const },
      { service: 'Надёжная служба уборки дома', demand: 310, supply: 'Мало', gap: 78, growth: +22, supplyLevel: 'low' as const },
      { service: 'Квартиры с разрешением для животных', demand: 280, supply: 'Очень мало', gap: 92, growth: +55, supplyLevel: 'very_low' as const },
      { service: 'Крипто-дружественный банк', demand: 260, supply: 'Нет', gap: 100, growth: +120, supplyLevel: 'none' as const },
      { service: 'Кулинарные курсы на выходных', demand: 220, supply: 'Есть', gap: 45, growth: +30, supplyLevel: 'moderate' as const },
      { service: 'Психотерапевт на русском языке', demand: 200, supply: 'Очень мало', gap: 90, growth: +48, supplyLevel: 'very_low' as const },
      { service: 'Аренда авто на месяц по доступной цене', demand: 190, supply: 'Мало', gap: 72, growth: +18, supplyLevel: 'low' as const },
      { service: 'Детские спортивные секции (плавание, единоборства)', demand: 175, supply: 'Есть', gap: 55, growth: +25, supplyLevel: 'moderate' as const },
    ],
  },

  satisfactionAreas: {
    en: [
      { area: 'Nature & Climate', satisfaction: 88, mentions: 2200, trend: +2, emoji: '\u{1F3D4}\u{FE0F}' },
      { area: 'Food Quality', satisfaction: 82, mentions: 1900, trend: +5, emoji: '\u{1F37D}\u{FE0F}' },
      { area: 'Safety & Security', satisfaction: 78, mentions: 1600, trend: +1, emoji: '\u{1F6E1}\u{FE0F}' },
      { area: 'Cost of Living', satisfaction: 52, mentions: 3400, trend: -8, emoji: '\u{1F4B0}' },
      { area: 'Internet Speed', satisfaction: 65, mentions: 1200, trend: +3, emoji: '\u{1F4F6}' },
      { area: 'Healthcare Access', satisfaction: 35, mentions: 1800, trend: -5, emoji: '\u{1F3E5}' },
      { area: 'Public Transport', satisfaction: 32, mentions: 1100, trend: -2, emoji: '\u{1F68C}' },
      { area: 'Bureaucracy', satisfaction: 22, mentions: 2100, trend: -12, emoji: '\u{1F4CB}' },
      { area: 'Education Options', satisfaction: 40, mentions: 1500, trend: +4, emoji: '\u{1F393}' },
      { area: 'Nightlife & Culture', satisfaction: 72, mentions: 900, trend: +8, emoji: '\u{1F3AD}' },
    ],
    ru: [
      { area: 'Природа и климат', satisfaction: 88, mentions: 2200, trend: +2, emoji: '\u{1F3D4}\u{FE0F}' },
      { area: 'Качество еды', satisfaction: 82, mentions: 1900, trend: +5, emoji: '\u{1F37D}\u{FE0F}' },
      { area: 'Безопасность', satisfaction: 78, mentions: 1600, trend: +1, emoji: '\u{1F6E1}\u{FE0F}' },
      { area: 'Стоимость жизни', satisfaction: 52, mentions: 3400, trend: -8, emoji: '\u{1F4B0}' },
      { area: 'Скорость интернета', satisfaction: 65, mentions: 1200, trend: +3, emoji: '\u{1F4F6}' },
      { area: 'Медицинская помощь', satisfaction: 35, mentions: 1800, trend: -5, emoji: '\u{1F3E5}' },
      { area: 'Общественный транспорт', satisfaction: 32, mentions: 1100, trend: -2, emoji: '\u{1F68C}' },
      { area: 'Бюрократия', satisfaction: 22, mentions: 2100, trend: -12, emoji: '\u{1F4CB}' },
      { area: 'Возможности обучения', satisfaction: 40, mentions: 1500, trend: +4, emoji: '\u{1F393}' },
      { area: 'Культура и досуг', satisfaction: 72, mentions: 900, trend: +8, emoji: '\u{1F3AD}' },
    ],
  },

  moodData: [
    { week: 'W1', excited: 180, satisfied: 320, neutral: 280, frustrated: 150, anxious: 120 },
    { week: 'W2', excited: 200, satisfied: 340, neutral: 270, frustrated: 140, anxious: 110 },
    { week: 'W3', excited: 210, satisfied: 350, neutral: 260, frustrated: 135, anxious: 105 },
    { week: 'W4', excited: 190, satisfied: 330, neutral: 280, frustrated: 160, anxious: 130 },
    { week: 'W5', excited: 220, satisfied: 360, neutral: 250, frustrated: 130, anxious: 100 },
    { week: 'W6', excited: 240, satisfied: 370, neutral: 240, frustrated: 125, anxious: 95 },
    { week: 'W7', excited: 250, satisfied: 380, neutral: 235, frustrated: 120, anxious: 90 },
  ],

  moodConfig: {
    en: [
      { key: 'excited',    label: 'Excited',    color: '#10b981', emoji: '\u{1F929}', polarity: 'positive' as const },
      { key: 'satisfied',  label: 'Satisfied',  color: '#3b82f6', emoji: '\u{1F60A}', polarity: 'positive' as const },
      { key: 'neutral',    label: 'Neutral',    color: '#6b7280', emoji: '\u{1F610}', polarity: 'neutral'  as const },
      { key: 'frustrated', label: 'Frustrated', color: '#f97316', emoji: '\u{1F624}', polarity: 'negative' as const },
      { key: 'anxious',    label: 'Anxious',    color: '#ef4444', emoji: '\u{1F61F}', polarity: 'negative' as const },
    ],
    ru: [
      { key: 'excited',    label: 'Воодушевлённые', color: '#10b981', emoji: '\u{1F929}', polarity: 'positive' as const },
      { key: 'satisfied',  label: 'Довольные',      color: '#3b82f6', emoji: '\u{1F60A}', polarity: 'positive' as const },
      { key: 'neutral',    label: 'Нейтральные',    color: '#6b7280', emoji: '\u{1F610}', polarity: 'neutral'  as const },
      { key: 'frustrated', label: 'Раздражённые',   color: '#f97316', emoji: '\u{1F624}', polarity: 'negative' as const },
      { key: 'anxious',    label: 'Тревожные',      color: '#ef4444', emoji: '\u{1F61F}', polarity: 'negative' as const },
    ],
  },

  urgencySignals: {
    en: [
      { message: 'My landlord is evicting me with 3 days notice', topic: 'Housing', urgency: 'critical', count: 28, action: 'Connect to legal resources' },
      { message: 'Bank froze my account, I cannot access my money', topic: 'Banking', urgency: 'critical', count: 19, action: 'Emergency banking guide' },
      { message: 'Child has a fever, I do not know any doctors here', topic: 'Healthcare', urgency: 'critical', count: 14, action: 'Doctor directory now' },
      { message: 'Need a job urgently, my savings are running out', topic: 'Jobs', urgency: 'high', count: 45, action: 'Job board connections' },
      { message: 'Residency permit deadline is tomorrow, no idea what to do', topic: 'Legal', urgency: 'high', count: 32, action: 'Step-by-step permit guide' },
      { message: 'Landlord raised rent 60%, need to move in 2 weeks', topic: 'Housing', urgency: 'high', count: 38, action: 'Rental listings + tenant rights' },
      { message: 'Struggling mentally, need a Russian-speaking therapist', topic: 'Mental Health', urgency: 'high', count: 22, action: 'Therapist directory' },
    ],
    ru: [
      { message: 'Арендодатель выселяет меня — 3 дня на сборы', topic: 'Жильё', urgency: 'critical', count: 28, action: 'Подключить юридические ресурсы' },
      { message: 'Банк заблокировал счёт, нет доступа к деньгам', topic: 'Банки', urgency: 'critical', count: 19, action: 'Экстренный банковский гайд' },
      { message: 'У ребёнка температура, не знаю ни одного врача', topic: 'Медицина', urgency: 'critical', count: 14, action: 'Справочник врачей сейчас' },
      { message: 'Срочно нужна работа, сбережения заканчиваются', topic: 'Работа', urgency: 'high', count: 45, action: 'Подключить к доскам вакансий' },
      { message: 'Срок подачи на ВНЖ завтра, не знаю что делать', topic: 'Документы', urgency: 'high', count: 32, action: 'Пошаговое руководство по ВНЖ' },
      { message: 'Арендодатель поднял аренду на 60%, нужно съехать за 2 недели', topic: 'Жильё', urgency: 'high', count: 38, action: 'О��ъявления + права арендаторов' },
      { message: 'Психологически тяжело, нужен русскоязычный психотерапевт', topic: 'Психология', urgency: 'high', count: 22, action: 'Справочник психотерапевтов' },
    ],
  },

  // ════════════════════════════════════════════════════════
  // TIER 4-8 + PAGES: Remaining data
  // ════════════════════════════════════════════════════════
  // NOTE: The remaining mock data for Tiers 4-8 and Pages
  // continues in the same pattern. Each widget's hardcoded
  // data has been moved here unchanged. For brevity in this
  // initial commit, the remaining data is initialized as
  // empty arrays — the widget files still contain their
  // own data as a fallback until fully migrated.
  // ════════════════════════════════════════════════════════

  communityChannels: [
    { name: 'Русские в Ереване', type: 'General', members: 18400, dailyMessages: 420, engagement: 92, growth: +340, topTopicEN: 'Housing tips', topTopicRU: 'Советы по жилью' },
    { name: 'IT Relocants Armenia', type: 'Work', members: 12200, dailyMessages: 280, engagement: 85, growth: +520, topTopicEN: 'Tax optimization', topTopicRU: 'Налоговая оптимизация' },
    { name: 'Мамы Еревана', type: 'Family', members: 8600, dailyMessages: 195, engagement: 88, growth: +280, topTopicEN: 'Schools comparison', topTopicRU: 'Сравнение школ' },
    { name: 'Аренда Ереван', type: 'Housing', members: 15800, dailyMessages: 350, engagement: 78, growth: +180, topTopicEN: 'Price negotiations', topTopicRU: 'Торг по цене' },
    { name: 'Бизнес в Армении', type: 'Business', members: 6400, dailyMessages: 120, engagement: 82, growth: +420, topTopicEN: 'LLC registration', topTopicRU: 'Регистрация ООО' },
    { name: 'Еда и Рестораны AM', type: 'Lifestyle', members: 9200, dailyMessages: 150, engagement: 90, growth: +150, topTopicEN: 'Hidden gems', topTopicRU: 'Скрытые жемчужины' },
    { name: 'Армения Документы', type: 'Legal', members: 11400, dailyMessages: 210, engagement: 75, growth: +380, topTopicEN: 'Residency permit', topTopicRU: 'Вид на жительство' },
    { name: 'Хайкинг Армения', type: 'Lifestyle', members: 4800, dailyMessages: 85, engagement: 94, growth: +220, topTopicEN: 'Weekend trails', topTopicRU: 'Маршруты выходного дня' },
  ],

  keyVoices: {
    en: [
      { name: 'Алексей (IT_Alex_AM)', role: 'Tech advisor', followers: 3200, helpScore: 95, topics: ['Tax', 'IT jobs', 'Coworking'], postsPerWeek: 28, replyRate: 82, type: 'Helper' },
      { name: 'Марина (Marina_Yerevan)', role: 'Mom community leader', followers: 2800, helpScore: 92, topics: ['Schools', 'Kids activities', 'Pediatricians'], postsPerWeek: 22, replyRate: 90, type: 'Organizer' },
      { name: 'Дима (relocate_dm)', role: 'Relocation guide', followers: 4500, helpScore: 88, topics: ['Documents', 'Banking', 'Apartment hunting'], postsPerWeek: 35, replyRate: 75, type: 'Content Creator' },
      { name: 'Анна (anna_foodie_am)', role: 'Food & lifestyle blogger', followers: 5200, helpScore: 78, topics: ['Restaurants', 'Hidden gems', 'Events'], postsPerWeek: 18, replyRate: 65, type: 'Influencer' },
      { name: 'Сергей (biz_sergey)', role: 'Business consultant', followers: 2100, helpScore: 90, topics: ['LLC setup', 'Taxes', 'Hiring'], postsPerWeek: 12, replyRate: 88, type: 'Expert' },
      { name: 'Катя (katya_hikes)', role: 'Adventure organizer', followers: 3800, helpScore: 85, topics: ['Hiking', 'Weekend trips', 'Nature'], postsPerWeek: 15, replyRate: 72, type: 'Organizer' },
    ],
    ru: [
      { name: 'Алексей (IT_Alex_AM)', role: 'IT-консультант', followers: 3200, helpScore: 95, topics: ['Налоги', 'IT-вакансии', 'Коворкинги'], postsPerWeek: 28, replyRate: 82, type: 'Helper' },
      { name: 'Марина (Marina_Yerevan)', role: 'Лидер мам-сообщества', followers: 2800, helpScore: 92, topics: ['Школы', 'Детский досуг', 'Педиатры'], postsPerWeek: 22, replyRate: 90, type: 'Organizer' },
      { name: 'Дима (relocate_dm)', role: 'Гид по переезду', followers: 4500, helpScore: 88, topics: ['Документы', 'Банки', 'Поиск квартиры'], postsPerWeek: 35, replyRate: 75, type: 'Content Creator' },
      { name: 'Анна (anna_foodie_am)', role: 'Фуд и лайфстайл блогер', followers: 5200, helpScore: 78, topics: ['Рестораны', 'Скрытые места', 'Мероприятия'], postsPerWeek: 18, replyRate: 65, type: 'Influencer' },
      { name: 'Сергей (biz_sergey)', role: 'Бизнес-консультант', followers: 2100, helpScore: 90, topics: ['Открытие ООО', 'Налоги', 'Найм'], postsPerWeek: 12, replyRate: 88, type: 'Expert' },
      { name: 'Катя (katya_hikes)', role: 'Организатор приключений', followers: 3800, helpScore: 85, topics: ['Хайкинг', 'Вылазки на природу', 'Туризм'], postsPerWeek: 15, replyRate: 72, type: 'Organizer' },
    ],
  },

  hourlyActivity: [
    { hour: '6am', messages: 45 }, { hour: '7am', messages: 120 }, { hour: '8am', messages: 280 },
    { hour: '9am', messages: 420 }, { hour: '10am', messages: 380 }, { hour: '11am', messages: 350 },
    { hour: '12pm', messages: 310 }, { hour: '1pm', messages: 380 }, { hour: '2pm', messages: 340 },
    { hour: '3pm', messages: 320 }, { hour: '4pm', messages: 290 }, { hour: '5pm', messages: 350 },
    { hour: '6pm', messages: 420 }, { hour: '7pm', messages: 480 }, { hour: '8pm', messages: 520 },
    { hour: '9pm', messages: 490 }, { hour: '10pm', messages: 380 }, { hour: '11pm', messages: 220 },
  ],

  weeklyActivity: [
    { day: 'Пн', dayEN: 'Mon', messages: 3200 }, { day: 'Вт', dayEN: 'Tue', messages: 3400 },
    { day: 'Ср', dayEN: 'Wed', messages: 3600 }, { day: 'Чт', dayEN: 'Thu', messages: 3500 },
    { day: 'Пт', dayEN: 'Fri', messages: 3800 }, { day: 'Сб', dayEN: 'Sat', messages: 4200 },
    { day: 'Вс', dayEN: 'Sun', messages: 3900 },
  ],

  recommendations: {
    en: [
      { item: 'Nairi Medical Center', category: 'Healthcare', mentions: 145, rating: 4.7, sentiment: 'positive' },
      { item: 'Jazzve Coffee', category: 'Food', mentions: 132, rating: 4.8, sentiment: 'positive' },
      { item: 'Impact Hub Yerevan', category: 'Coworking', mentions: 128, rating: 4.5, sentiment: 'positive' },
      { item: 'QSI International School', category: 'Education', mentions: 118, rating: 4.2, sentiment: 'mixed' },
      { item: 'Ameriabank', category: 'Banking', mentions: 112, rating: 4.0, sentiment: 'mixed' },
      { item: 'Yandex Go (taxi)', category: 'Transport', mentions: 108, rating: 3.8, sentiment: 'mixed' },
      { item: 'Green Bean cafe', category: 'Food', mentions: 95, rating: 4.6, sentiment: 'positive' },
      { item: 'Cascade hills area', category: 'Neighborhood', mentions: 88, rating: 4.4, sentiment: 'positive' },
    ],
    ru: [
      { item: 'Медцентр Наири', category: 'Медицина', mentions: 145, rating: 4.7, sentiment: 'positive' },
      { item: 'Кофейня Jazzve', category: 'Еда', mentions: 132, rating: 4.8, sentiment: 'positive' },
      { item: 'Impact Hub Ереван', category: 'Коворкинг', mentions: 128, rating: 4.5, sentiment: 'positive' },
      { item: 'Международная школа QSI', category: 'Образование', mentions: 118, rating: 4.2, sentiment: 'mixed' },
      { item: 'Ameriabank', category: 'Банки', mentions: 112, rating: 4.0, sentiment: 'mixed' },
      { item: 'Яндекс Go (такси)', category: 'Транспорт', mentions: 108, rating: 3.8, sentiment: 'mixed' },
      { item: 'Кафе Green Bean', category: 'Еда', mentions: 95, rating: 4.6, sentiment: 'positive' },
      { item: 'Район Каскад', category: 'Район', mentions: 88, rating: 4.4, sentiment: 'positive' },
    ],
  },

  newcomerJourney: {
    en: [
      { stage: 'Day 1-3: Arrival', questions: ['Airport to city?', 'SIM card where?', 'Safe neighborhoods?'], volume: 420, resolved: 75 },
      { stage: 'Week 1: Setup', questions: ['Bank account?', 'Internet provider?', 'Apartment hunting tips?'], volume: 380, resolved: 60 },
      { stage: 'Week 2-3: Settling', questions: ['Register for residency?', 'Find a doctor?', 'Grocery delivery?'], volume: 290, resolved: 45 },
      { stage: 'Month 1+: Living', questions: ['Tax obligations?', 'Schools for kids?', 'Making local friends?'], volume: 220, resolved: 35 },
      { stage: 'Month 3+: Rooted', questions: ['Buy property?', 'Start a business?', 'Learn Armenian?'], volume: 150, resolved: 30 },
    ],
    ru: [
      { stage: 'Дни 1-3: Прибытие', questions: ['Как добраться из аэропорта?', 'Где купить SIM?', 'Безопасные районы?'], volume: 420, resolved: 75 },
      { stage: 'Неделя 1: Обустройство', questions: ['Открыть счёт?', 'Интернет-провайдер?', 'Как искать квартиру?'], volume: 380, resolved: 60 },
      { stage: 'Недели 2-3: Адаптация', questions: ['Регистрация резидента?', 'Найти врача?', 'Доставка продуктов?'], volume: 290, resolved: 45 },
      { stage: 'Месяц 1+: Жизнь здесь', questions: ['Налоговые обязательства?', 'Школы для детей?', 'Местные знакомства?'], volume: 220, resolved: 35 },
      { stage: 'Месяц 3+: Укоренились', questions: ['Купить недвижимость?', 'Открыть бизнес?', 'Выучить армянский?'], volume: 150, resolved: 30 },
    ],
  },

  viralTopics: {
    en: [
      { topic: 'Ameriabank service outage', originator: 'Армения Документы', spreadHours: 1.5, channelsReached: 18, amplifiers: ['Русские в Ереване', 'IT Relocants Armenia', 'Аренда Ереван'], totalReach: 34600, velocity: 'explosive' },
      { topic: 'New rent price cap rumor', originator: 'Аренда Ереван', spreadHours: 3.5, channelsReached: 14, amplifiers: ['Русские в Ереване', 'Армения Документы', 'Бизнес в Армении'], totalReach: 28400, velocity: 'explosive' },
      { topic: 'Tsaghkadzor ski season opening', originator: 'Хайкинг Армения', spreadHours: 8, channelsReached: 9, amplifiers: ['Русские в Ереване', 'Еда и Рестораны AM'], totalReach: 16200, velocity: 'fast' },
      { topic: 'New coworking opens in Malatia', originator: 'IT Relocants Armenia', spreadHours: 12, channelsReached: 6, amplifiers: ['Русские в Ереване', 'Бизнес в Армении'], totalReach: 9800, velocity: 'fast' },
      { topic: 'School enrollment deadline reminder', originator: 'Мамы Еревана', spreadHours: 18, channelsReached: 5, amplifiers: ['Русские в Ереване'], totalReach: 7400, velocity: 'normal' },
    ],
    ru: [
      { topic: 'Сбой сервиса Ameriabank', originator: 'Армения Документы', spreadHours: 1.5, channelsReached: 18, amplifiers: ['Русские в Ереване', 'IT Relocants Armenia', 'Аренда Ереван'], totalReach: 34600, velocity: 'explosive' },
      { topic: 'Слух о введении ограничения арендных цен', originator: 'Аренда Ереван', spreadHours: 3.5, channelsReached: 14, amplifiers: ['Русские в Ереване', 'Армения Документы', 'Бизнес в Армении'], totalReach: 28400, velocity: 'explosive' },
      { topic: 'Открытие горнолыжного сезона в Цахкадзоре', originator: 'Хайкинг Армения', spreadHours: 8, channelsReached: 9, amplifiers: ['Русские в Ереване', 'Еда и Рестораны AM'], totalReach: 16200, velocity: 'fast' },
      { topic: 'Новый коворкинг в Малатии', originator: 'IT Relocants Armenia', spreadHours: 12, channelsReached: 6, amplifiers: ['Русские в Ереване', 'Бизнес в Армении'], totalReach: 9800, velocity: 'fast' },
      { topic: 'Напоминание о дедлайне зачисления в школу', originator: 'Мамы Еревана', spreadHours: 18, channelsReached: 5, amplifiers: ['Русские в Ереване'], totalReach: 7400, velocity: 'normal' },
    ],
  },

  // ════════════════════════════════════════════════════════
  // TIER 5: PSYCHOGRAPHIC
  // ════════════════════════════════════════════════════════

  personas: {
    en: [
      { name: 'The IT Relocant', size: 32, count: 3960, color: '#3b82f6', profile: 'Remote tech worker, 25-35', needs: 'Coworking, fast internet, tax optimization', interests: 'Tech meetups, hiking, crypto', pain: 'Bureaucracy, unclear tax rules', desc: 'Works remotely for a foreign company. Came for tax benefits and quality of life. High spending power, low patience for inefficiency.' },
      { name: 'The Young Family', size: 24, count: 2970, color: '#ec4899', profile: 'Parents with kids 2-12', needs: 'Schools, pediatricians, kid-friendly spaces', interests: 'Playgroups, nature trips, cooking', pain: 'Childcare shortage, school quality concerns', desc: 'Moved for safety and nature. Highest loyalty if kids are well-served. Most likely to become long-term residents.' },
      { name: 'The Entrepreneur', size: 15, count: 1855, color: '#10b981', profile: 'Business owner, 28-45', needs: 'LLC setup, banking, local partnerships', interests: 'Networking, market gaps, real estate', pain: 'Regulations, finding reliable staff', desc: 'Sees Armenia as an opportunity. Building businesses that serve the community. Key to self-sustaining ecosystem.' },
      { name: 'The Digital Nomad', size: 12, count: 1485, color: '#f59e0b', profile: 'Freelancer, 22-32, single', needs: 'Short-term rental, cafes, social scene', interests: 'Nightlife, food, travel, dating', pain: 'Finding community, loneliness', desc: 'May stay 3-12 months. Highest churn risk but also highest social media amplification. Key for growth.' },
      { name: 'The Established Expat', size: 10, count: 1238, color: '#8b5cf6', profile: '35-55, 2+ years in Armenia', needs: 'Property purchase, long-term planning, integration', interests: 'Armenian language, local politics, investing', pain: 'Still feeling like an outsider', desc: 'Has decided to stay. Becoming a community pillar. Needs deeper integration support and long-term investment advice.' },
      { name: 'The Retiree', size: 7, count: 866, color: '#6b7280', profile: '50+, pension or passive income', needs: 'Healthcare, quiet living, cultural activities', interests: 'History, gardening, walking tours', pain: 'Language barrier, healthcare quality', desc: 'Values peace, nature, affordable living. Low digital engagement but high community value. Word-of-mouth ambassadors.' },
    ],
    ru: [
      { name: 'IT-релокант', size: 32, count: 3960, color: '#3b82f6', profile: 'Удалённый техспециалист, 25–35 лет', needs: 'Коворкинг, быстрый интернет, налоговая оптимизация', interests: 'Технические митапы, хайкинг, крипто', pain: 'Бюрократия, неясные налоговые правила', desc: 'Работает удалённо на иностранную компанию. Переехал ради налоговых льгот и качества жизни. Высокая покупательная способность, низкая терпимость к неэффективности.' },
      { name: 'Молодая семья', size: 24, count: 2970, color: '#ec4899', profile: 'Родители с детьми 2–12 лет', needs: 'Школы, педиатры, места для детей', interests: 'Группы для родителей, природа, кулинария', pain: 'Нехватка детских садов, качество школ', desc: 'Переехали ради безопасности и природы. Наибольшая лояльность, если детям хорошо. Чаще всего становятся долгосрочными резидентами.' },
      { name: 'Предприниматель', size: 15, count: 1855, color: '#10b981', profile: 'Владелец бизнеса, 28–45 лет', needs: 'Регистрация ООО, банки, местные партнёры', interests: 'Нетворкинг, рыночные ниши, недвижимость', pain: 'Регуляторная среда, поиск надёжного персонала', desc: 'Видит Армению как возможность. Строит бизнес для нужд сообщества. Ключевой для самоподдерживающейся экосистемы.' },
      { name: 'Цифровой кочевник', size: 12, count: 1485, color: '#f59e0b', profile: 'Фрилансер, 22–32 года, одиночка', needs: 'Краткосрочная аренда, кафе, социальная жизнь', interests: 'Ночная жизнь, еда, путешествия', pain: 'Поиск сообщества, одиночество', desc: 'Может остаться 3–12 месяцев. Наибольший риск оттока, но и наибольшая социальная амплификация. Ключевой для роста.' },
      { name: 'Укоренившийся экспат', size: 10, count: 1238, color: '#8b5cf6', profile: '35–55 лет, 2+ года в Армении', needs: 'Покупка недвижимости, долгосрочное планирование', interests: 'Армянский язык, местная политика, инвестиции', pain: 'Всё ещё чувствует себя чужим', desc: 'Принял решение остаться. Становится опорой сообщества. Нуждается в поддержке глубокой интеграции и долгосрочных инвестиционных советах.' },
      { name: 'Пенсионер / Неспешный', size: 7, count: 866, color: '#6b7280', profile: '50+, пенсия или пассивный доход', needs: 'Медицина, спокойная жизнь, культурный досуг', interests: 'История, садоводство, пешие прогулки', pain: 'Языковой барьер, качество медицины', desc: 'Ценит спокойствие, природу, доступность жизни. Низкая цифровая активность, но высокая ценность для сообщества. Послы «из уст в уста».' },
    ],
  },

  interests: {
    en: [
      { interest: 'Outdoor/Nature', score: 82 }, { interest: 'Food & Wine', score: 78 },
      { interest: 'Tech/IT', score: 72 }, { interest: 'Culture/History', score: 65 },
      { interest: 'Fitness/Sports', score: 58 }, { interest: 'Nightlife', score: 52 },
      { interest: 'Art/Music', score: 48 }, { interest: 'Kids Activities', score: 70 },
    ],
    ru: [
      { interest: 'Природа/Хайкинг', score: 82 }, { interest: 'Еда и вино', score: 78 },
      { interest: 'Технологии/IT', score: 72 }, { interest: 'Культура/История', score: 65 },
      { interest: 'Фитнес/Спорт', score: 58 }, { interest: 'Ночная жизнь', score: 52 },
      { interest: 'Искусство/Музыка', score: 48 }, { interest: 'Детский досуг', score: 70 },
    ],
  },

  origins: [
    { city: 'Москва', cityEN: 'Moscow', count: 4200, pct: 34, color: '#3b82f6' },
    { city: 'Санкт-Петербург', cityEN: 'St. Petersburg', count: 2100, pct: 17, color: '#8b5cf6' },
    { city: 'Новосибирск', cityEN: 'Novosibirsk', count: 980, pct: 8, color: '#10b981' },
    { city: 'Краснодар', cityEN: 'Krasnodar', count: 740, pct: 6, color: '#f59e0b' },
    { city: 'Екатеринбург', cityEN: 'Ekaterinburg', count: 620, pct: 5, color: '#ec4899' },
    { city: 'Казань', cityEN: 'Kazan', count: 480, pct: 4, color: '#f97316' },
    { city: 'Минск (Беларусь)', cityEN: 'Minsk (Belarus)', count: 380, pct: 3, color: '#6b7280' },
    { city: 'Другие города России', cityEN: 'Other Russia', count: 2200, pct: 18, color: '#94a3b8' },
    { city: 'Другие страны СНГ', cityEN: 'Other CIS', count: 620, pct: 5, color: '#cbd5e1' },
  ],

  integrationData: [
    { month: 'Sep', learning: 120, bilingual: 280, russianOnly: 580, integrated: 60 },
    { month: 'Oct', learning: 140, bilingual: 300, russianOnly: 560, integrated: 65 },
    { month: 'Nov', learning: 165, bilingual: 320, russianOnly: 540, integrated: 72 },
    { month: 'Dec', learning: 180, bilingual: 340, russianOnly: 520, integrated: 78 },
    { month: 'Jan', learning: 200, bilingual: 360, russianOnly: 500, integrated: 85 },
    { month: 'Feb', learning: 225, bilingual: 380, russianOnly: 480, integrated: 92 },
  ],

  /**
   * Drives IntegrationSpectrum Area chart generically (same pattern as moodConfig).
   * Order = stacking order (bottom → top). polarity: 'negative' = good when shrinking.
   */
  integrationSeriesConfig: [
    { key: 'integrated',  color: '#10b981', label: 'Fully Integrated',  labelRu: 'Полная интеграция',    polarity: 'positive' },
    { key: 'learning',    color: '#3b82f6', label: 'Learning & Mixing', labelRu: 'Учится и смешивается', polarity: 'positive' },
    { key: 'bilingual',   color: '#f59e0b', label: 'Bilingual Bubble',  labelRu: 'Двуязычный пузырь',    polarity: 'neutral'  },
    { key: 'russianOnly', color: '#ef4444', label: 'Russian Only',      labelRu: 'Только русский',       polarity: 'negative' },
  ],

  integrationLevels: {
    en: [
      { level: 'Fully Integrated', pct: 8, color: '#10b981', desc: 'Speaks Armenian, local friends, settled' },
      { level: 'Learning & Mixing', pct: 20, color: '#3b82f6', desc: 'Taking Armenian classes, some local contacts' },
      { level: 'Bilingual Bubble', pct: 33, color: '#f59e0b', desc: 'Uses some Armenian, mostly Russian circles' },
      { level: 'Russian Only', pct: 39, color: '#ef4444', desc: 'No Armenian, expat-only social circle' },
    ],
    ru: [
      { level: 'Полная интеграция', pct: 8, color: '#10b981', desc: 'Говорит по-армянски, местные друзья, обустроен' },
      { level: 'Учится и смешивается', pct: 20, color: '#3b82f6', desc: 'Берёт уроки армянского, есть местные контакты' },
      { level: 'Двуязычный пузырь', pct: 33, color: '#f59e0b', desc: 'Частично армянский, преимущественно русский круг' },
      { level: 'Только русский', pct: 39, color: '#ef4444', desc: 'Без армянского, только круг эмигрантов' },
    ],
  },

  // ════════════════════════════════════════════════════════
  // TIER 6: PREDICTIVE
  // ════════════════════════════════════════════════════════

  emergingInterests: {
    en: [
      { topic: 'Armenian wine tourism', firstSeen: '4 days ago', growthRate: 320, currentVolume: 280, originChannel: 'Еда и Рестораны AM', mood: 'Excited', opportunity: 'high' },
      { topic: 'Pet-friendly cafes list', firstSeen: '6 days ago', growthRate: 280, currentVolume: 420, originChannel: 'Русские в Ереване', mood: 'Seeking', opportunity: 'medium' },
      { topic: 'Skiing in Tsaghkadzor this season', firstSeen: '3 days ago', growthRate: 450, currentVolume: 380, originChannel: 'Хайкинг Армения', mood: 'Excited', opportunity: 'high' },
      { topic: 'Group buying power for furniture', firstSeen: '5 days ago', growthRate: 190, currentVolume: 220, originChannel: 'Аренда Ереван', mood: 'Practical', opportunity: 'high' },
      { topic: 'Armenian cooking classes in Russian', firstSeen: '2 days ago', growthRate: 520, currentVolume: 340, originChannel: 'Мамы Еревана', mood: 'Enthusiastic', opportunity: 'high' },
      { topic: 'Co-living spaces for digital nomads', firstSeen: '1 day ago', growthRate: 680, currentVolume: 180, originChannel: 'IT Relocants Armenia', mood: 'Interested', opportunity: 'medium' },
    ],
    ru: [
      { topic: 'Армянский винный туризм', firstSeen: '4 дня назад', growthRate: 320, currentVolume: 280, originChannel: 'Еда и Рестораны AM', mood: 'Воодушевление', opportunity: 'high' },
      { topic: 'Список кафе для питомцев', firstSeen: '6 дней назад', growthRate: 280, currentVolume: 420, originChannel: 'Русские в Ереване', mood: 'Поиск', opportunity: 'medium' },
      { topic: 'Горнолыжный сезон в Цахкадзоре', firstSeen: '3 дня назад', growthRate: 450, currentVolume: 380, originChannel: 'Хайкинг Армения', mood: 'Воодушевление', opportunity: 'high' },
      { topic: 'Совместная покупка мебели', firstSeen: '5 дней назад', growthRate: 190, currentVolume: 220, originChannel: 'Аренда Ереван', mood: 'Практичность', opportunity: 'high' },
      { topic: 'Уроки армянской кухни на русском', firstSeen: '2 дня назад', growthRate: 520, currentVolume: 340, originChannel: 'Мамы Еревана', mood: 'Энтузиазм', opportunity: 'high' },
      { topic: 'Коливинг для цифровых кочевников', firstSeen: '1 день назад', growthRate: 680, currentVolume: 180, originChannel: 'IT Relocants Armenia', mood: 'Интерес', opportunity: 'medium' },
    ],
  },

  retentionFactors: {
    en: [
      { factor: 'Found housing they like', score: 72, weight: 0.25 },
      { factor: 'Kids in school/activities', score: 58, weight: 0.20 },
      { factor: 'Local friend connections', score: 45, weight: 0.20 },
      { factor: 'Stable income here', score: 78, weight: 0.20 },
      { factor: 'Learning Armenian', score: 35, weight: 0.15 },
    ],
    ru: [
      { factor: 'Нашли жильё, которое нравится', score: 72, weight: 0.25 },
      { factor: 'Дети в школе / секции', score: 58, weight: 0.20 },
      { factor: 'Местные социальные связи', score: 45, weight: 0.20 },
      { factor: 'Стабильный доход здесь', score: 78, weight: 0.20 },
      { factor: 'Изучение армянского языка', score: 35, weight: 0.15 },
    ],
  },

  churnSignals: {
    en: [
      { signal: '"Thinking about going back to Moscow"', count: 180, trend: -12, severity: 'watch' },
      { signal: '"Prices getting too high here"', count: 320, trend: +22, severity: 'rising' },
      { signal: '"Miss my family/friends"', count: 260, trend: +5, severity: 'stable' },
      { signal: '"Looking at Georgia/Turkey instead"', count: 95, trend: +45, severity: 'rising' },
      { signal: '"Bureaucracy is unbearable"', count: 210, trend: +15, severity: 'watch' },
    ],
    ru: [
      { signal: '\u00abДумаю о возвращении в Москву\u00bb', count: 180, trend: -12, severity: 'watch' },
      { signal: '\u00abЦены здесь стали слишком высокими\u00bb', count: 320, trend: +22, severity: 'rising' },
      { signal: '\u00abСкучаю по семье и друзьям\u00bb', count: 260, trend: +5, severity: 'stable' },
      { signal: '\u00abРассматриваю Грузию/Турцию\u00bb', count: 95, trend: +45, severity: 'rising' },
      { signal: '\u00abБюрократия невыносима\u00bb', count: 210, trend: +15, severity: 'watch' },
    ],
  },

  growthFunnel: {
    en: [
      { stage: 'Joined Group',         count: 18400, pct: 100, color: '#dbeafe', role: 'all'         },
      { stage: 'Read Regularly',        count: 12200, pct: 66,  color: '#93c5fd', role: 'reads'       },
      { stage: 'Asked a Question',      count: 6800,  pct: 37,  color: '#60a5fa', role: 'asks'        },
      { stage: 'Helped Someone',        count: 3200,  pct: 17,  color: '#3b82f6', role: 'helps'       },
      { stage: 'Regular Contributor',   count: 1400,  pct: 8,   color: '#2563eb', role: 'contributes' },
      { stage: 'Community Leader',      count: 280,   pct: 2,   color: '#1d4ed8', role: 'leads'       },
    ],
    ru: [
      { stage: 'Вступили в группу',    count: 18400, pct: 100, color: '#dbeafe', role: 'all'         },
      { stage: 'Читают регулярно',     count: 12200, pct: 66,  color: '#93c5fd', role: 'reads'       },
      { stage: 'Задали вопрос',        count: 6800,  pct: 37,  color: '#60a5fa', role: 'asks'        },
      { stage: 'Помогли кому-то',      count: 3200,  pct: 17,  color: '#3b82f6', role: 'helps'       },
      { stage: 'Постоянный участник',  count: 1400,  pct: 8,   color: '#2563eb', role: 'contributes' },
      { stage: 'Лидер сообщества',     count: 280,   pct: 2,   color: '#1d4ed8', role: 'leads'       },
    ],
  },

  decisionStages: {
    en: [
      { stage: 'Researching Armenia', count: 2800, pct: 15, trend: +22, color: '#ddd6fe', needs: 'Info, comparisons, Q&A' },
      { stage: 'Planning to Move', count: 3200, pct: 17, trend: +18, color: '#c4b5fd', needs: 'Checklists, timelines, costs' },
      { stage: 'Just Arrived (<3 mo)', count: 4100, pct: 22, trend: +34, color: '#a78bfa', needs: 'Setup guides, urgent help' },
      { stage: 'Settling In (3-12 mo)', count: 5200, pct: 28, trend: +12, color: '#8b5cf6', needs: 'Community, routines, deeper info' },
      { stage: 'Established (1yr+)', count: 3100, pct: 17, trend: +8, color: '#7c3aed', needs: 'Investment, property, citizenship' },
    ],
    ru: [
      { stage: 'Изучают Армению', count: 2800, pct: 15, trend: +22, color: '#ddd6fe', needs: 'Инфо, сравнения, вопросы' },
      { stage: 'Планируют переезд', count: 3200, pct: 17, trend: +18, color: '#c4b5fd', needs: 'Чеклисты, сроки, расходы' },
      { stage: 'Только приехали (<3 мес)', count: 4100, pct: 22, trend: +34, color: '#a78bfa', needs: 'Гайды по обустройству, срочная помощь' },
      { stage: 'Обустраиваются (3\u201312 мес)', count: 5200, pct: 28, trend: +12, color: '#8b5cf6', needs: 'Сообщество, рутина, глубокая информация' },
      { stage: 'Укоренились (1+ год)', count: 3100, pct: 17, trend: +8, color: '#7c3aed', needs: 'Инвестиции, недвижимость, гражданство' },
    ],
  },

  voiceData: [
    { week: 'W1', newVoices: 420, returning: 1820 },
    { week: 'W2', newVoices: 480, returning: 1880 },
    { week: 'W3', newVoices: 510, returning: 1920 },
    { week: 'W4', newVoices: 390, returning: 1980 },
    { week: 'W5', newVoices: 560, returning: 2020 },
    { week: 'W6', newVoices: 610, returning: 2080 },
    { week: 'W7', newVoices: 680, returning: 2140 },
  ],

  topNewTopics: {
    en: [
      { topic: 'Housing & Rent', newVoices: 142, pct: 21 },
      { topic: 'Getting Started', newVoices: 128, pct: 19 },
      { topic: 'Banking & Finance', newVoices: 98, pct: 14 },
      { topic: 'Schools & Education', newVoices: 76, pct: 11 },
      { topic: 'Jobs & Work', newVoices: 62, pct: 9 },
    ],
    ru: [
      { topic: 'Жильё и аренда', newVoices: 142, pct: 21 },
      { topic: 'Начало пути', newVoices: 128, pct: 19 },
      { topic: 'Банки и финансы', newVoices: 98, pct: 14 },
      { topic: 'Школы и образование', newVoices: 76, pct: 11 },
      { topic: 'Работа и занятость', newVoices: 62, pct: 9 },
    ],
  },

  // ════════════════════════════════════════════════════════
  // TIER 7: ACTIONABLE
  // ════════════════════════════════════════════════════════

  businessOpportunities: {
    en: [
      { need: 'Russian-speaking medical clinic', mentions: 1840, growth: 35, sector: 'Healthcare', readiness: 'High — people actively searching', sampleQuote: '"Would pay premium for a clinic with Russian-speaking staff and modern equipment"', revenue: '$$$$' },
      { need: 'Family-friendly coworking with childcare', mentions: 1200, growth: 88, sector: 'Workspace', readiness: 'High — no existing solution', sampleQuote: '"If someone opens a coworking with a kids room, take my money"', revenue: '$$$' },
      { need: 'Curated apartment rental platform', mentions: 2200, growth: 45, sector: 'Real Estate', readiness: 'Very High — daily complaints about search', sampleQuote: '"We need an honest rental platform, not overpriced listings from 5 agents"', revenue: '$$$$' },
      { need: 'Grocery delivery with Russian interface', mentions: 980, growth: 62, sector: 'E-commerce', readiness: 'Medium — some solutions exist but poor', sampleQuote: '"Tried every delivery app, none work well outside center"', revenue: '$$$' },
      { need: 'Russian-language Armenian culture experiences', mentions: 820, growth: 120, sector: 'Tourism/Culture', readiness: 'High — people crave immersion', sampleQuote: '"Would love guided tours of Armenian historical sites in Russian"', revenue: '$$' },
      { need: 'Pet services (vet, grooming, boarding)', mentions: 680, growth: 55, sector: 'Pet Services', readiness: 'Medium — some exist but not Russian-friendly', sampleQuote: '"Need a vet who can explain what\'s wrong in Russian"', revenue: '$$' },
      { need: 'After-school programs in Russian', mentions: 920, growth: 42, sector: 'Education', readiness: 'High — parents desperate', sampleQuote: '"Kids have nothing to do after 3pm, we need activities in Russian"', revenue: '$$$' },
    ],
    ru: [
      { need: 'Русскоязычная медицинская клиника', mentions: 1840, growth: 35, sector: 'Медицина', readiness: 'Высокая — люди активно ищут', sampleQuote: '"Готовы платить больше за клинику с русскоязычным персоналом и современным оборудованием"', revenue: '$$$$' },
      { need: 'Семейный коворкинг с детской комнатой', mentions: 1200, growth: 88, sector: 'Рабочие пространства', readiness: 'Высокая — готового решения нет', sampleQuote: '"Откроет кто-нибудь коворкинг с детской комнатой — отдам деньги сразу"', revenue: '$$$' },
      { need: 'Курируемая платформа аренды жилья', mentions: 2200, growth: 45, sector: 'Недвижимость', readiness: 'Очень высокая — жалобы ежедневно', sampleQuote: '"Нужна честная платформа аренды, а не завышенные объявления от 5 агентств"', revenue: '$$$$' },
      { need: 'Доставка продуктов с русским интерфейсом', mentions: 980, growth: 62, sector: 'E-commerce', readiness: 'Средняя — есть решения, но плохие', sampleQuote: '"Попробовал все приложения — ни одно не работает нормально за пределами центра"', revenue: '$$$' },
      { need: 'Культурные экскурсии по Армении на русском', mentions: 820, growth: 120, sector: 'Туризм/Культура', readiness: 'Высокая — люди хотят погружения', sampleQuote: '"Хотел бы экскурсии по историческим местам Армении на русском языке"', revenue: '$$' },
      { need: 'Ветеринарные и зоосалоны услуги', mentions: 680, growth: 55, sector: 'Зооуслуги', readiness: 'Средняя — есть, но не русскоязычные', sampleQuote: '"Нужен ветеринар, который объяснит диагноз по-русски"', revenue: '$$' },
      { need: 'Кружки и секции для детей на русском', mentions: 920, growth: 42, sector: 'Образование', readiness: 'Высокая — родители в отчаянии', sampleQuote: '"После 15:00 детям нечем заняться — нужны секции на русском"', revenue: '$$$' },
    ],
  },

  jobSeeking: {
    en: [
      { role: 'Remote IT (keep current job)', pct: 35, count: 2800 },
      { role: 'Local IT / Tech job', pct: 18, count: 1440 },
      { role: 'Freelance / Consulting', pct: 15, count: 1200 },
      { role: 'Teaching (Russian/English)', pct: 12, count: 960 },
      { role: 'Own business', pct: 10, count: 800 },
      { role: 'Service sector', pct: 6, count: 480 },
      { role: 'Other', pct: 4, count: 320 },
    ],
    ru: [
      { role: 'Удалённая IT-работа (текущая)', pct: 35, count: 2800 },
      { role: 'Местная IT / технологическая работа', pct: 18, count: 1440 },
      { role: 'Фриланс / консалтинг', pct: 15, count: 1200 },
      { role: 'Преподавание (русский/английский)', pct: 12, count: 960 },
      { role: 'Собственный бизнес', pct: 10, count: 800 },
      { role: 'Сфера услуг', pct: 6, count: 480 },
      { role: 'Другое', pct: 4, count: 320 },
    ],
  },

  jobTrends: {
    en: [
      { trend: 'IT salary discussions up 45%', type: 'hot' },
      { trend: 'Tax optimization for freelancers — top FAQ', type: 'hot' },
      { trend: 'Co-founder search posts tripled', type: 'growing' },
      { trend: 'Armenian language required in job posts — concern', type: 'concern' },
      { trend: 'Crypto/Web3 job mentions declining', type: 'cooling' },
    ],
    ru: [
      { trend: 'Обсуждения IT-зарплат выросли на 45%', type: 'hot' },
      { trend: 'Налоговая оптимизация для фрилансеров — топ-FAQ', type: 'hot' },
      { trend: 'Посты поиска сооснователей утроились', type: 'growing' },
      { trend: 'Требование армянского языка в вакансиях — обеспокоенность', type: 'concern' },
      { trend: 'Упоминания Crypto/Web3 вакансий снижаются', type: 'cooling' },
    ],
  },

  housingData: {
    en: [
      { type: '1-bedroom center', avgPrice: '$500', trend: +15, satisfaction: 45, volume: 840 },
      { type: '2-bedroom center', avgPrice: '$750', trend: +22, satisfaction: 38, volume: 1200 },
      { type: '3-bedroom center', avgPrice: '$1,100', trend: +18, satisfaction: 42, volume: 620 },
      { type: '1-bedroom suburbs', avgPrice: '$300', trend: +8, satisfaction: 62, volume: 480 },
      { type: '2-bedroom suburbs', avgPrice: '$450', trend: +10, satisfaction: 68, volume: 560 },
      { type: 'House/Villa', avgPrice: '$800', trend: +5, satisfaction: 75, volume: 320 },
    ],
    ru: [
      { type: '1-комн. в центре', avgPrice: '$500', trend: +15, satisfaction: 45, volume: 840 },
      { type: '2-комн. в центре', avgPrice: '$750', trend: +22, satisfaction: 38, volume: 1200 },
      { type: '3-комн. в центре', avgPrice: '$1 100', trend: +18, satisfaction: 42, volume: 620 },
      { type: '1-комн. на периферии', avgPrice: '$300', trend: +8, satisfaction: 62, volume: 480 },
      { type: '2-комн. на периферии', avgPrice: '$450', trend: +10, satisfaction: 68, volume: 560 },
      { type: 'Дом/вилла', avgPrice: '$800', trend: +5, satisfaction: 75, volume: 320 },
    ],
  },

  housingHotTopics: {
    en: [
      { topic: 'Landlord raising rent after 6 months', count: 420, sentiment: 'angry' },
      { topic: 'No lease agreement — how to protect myself?', count: 380, sentiment: 'worried' },
      { topic: 'Best areas for families with kids', count: 340, sentiment: 'seeking' },
      { topic: 'Buying vs renting long-term analysis', count: 280, sentiment: 'analytical' },
      { topic: 'Short-term vs long-term lease pros/cons', count: 220, sentiment: 'curious' },
    ],
    ru: [
      { topic: 'Арендодатель поднял цену через 6 месяцев', count: 420, sentiment: 'angry' },
      { topic: 'Нет договора аренды — как себя защитить?', count: 380, sentiment: 'worried' },
      { topic: 'Лучшие районы для семей с детьми', count: 340, sentiment: 'seeking' },
      { topic: 'Купить или снимать — долгосрочный анализ', count: 280, sentiment: 'analytical' },
      { topic: 'Краткосрочная vs долгосрочная аренда: плюсы и минусы', count: 220, sentiment: 'curious' },
    ],
  },

  // ════════════════════════════════════════════════════════
  // TIER 8: COMPARATIVE
  // ════════════════════════════════════════════════════════

  weeklyShifts: {
    en: [
      { metric: 'Community Health Score', current: 71, previous: 63, unit: '/100', category: 'Health' },
      { metric: 'Active Members', current: 12400, previous: 11800, unit: '', category: 'Growth' },
      { metric: 'New Joins This Week', current: 680, previous: 520, unit: '', category: 'Growth' },
      { metric: 'Messages Per Day', current: 3847, previous: 3420, unit: '', category: 'Engagement' },
      { metric: 'Questions Asked', current: 342, previous: 298, unit: '', category: 'Engagement' },
      { metric: 'Questions Answered', current: 215, previous: 210, unit: '', category: 'Engagement' },
      { metric: 'Positive Sentiment', current: 68, previous: 64, unit: '%', category: 'Mood' },
      { metric: 'Housing Satisfaction', current: 42, previous: 45, unit: '%', category: 'Living' },
      { metric: 'Integration Score', current: 45, previous: 42, unit: '/100', category: 'Integration' },
      { metric: 'Recommendations Shared', current: 420, previous: 380, unit: '', category: 'Community' },
      { metric: 'Events Organized', current: 12, previous: 8, unit: '', category: 'Community' },
      { metric: 'Churn Signals', current: 180, previous: 210, unit: '', category: 'Retention', isInverse: true },
    ],
    ru: [
      { metric: 'Индекс здоровья сообщества', current: 71, previous: 63, unit: '/100', category: 'Health' },
      { metric: 'Активных участников', current: 12400, previous: 11800, unit: '', category: 'Growth' },
      { metric: 'Новых за неделю', current: 680, previous: 520, unit: '', category: 'Growth' },
      { metric: 'Сообщений в день', current: 3847, previous: 3420, unit: '', category: 'Engagement' },
      { metric: 'Задано вопросов', current: 342, previous: 298, unit: '', category: 'Engagement' },
      { metric: 'Получено ответов', current: 215, previous: 210, unit: '', category: 'Engagement' },
      { metric: 'Позитивный настрой', current: 68, previous: 64, unit: '%', category: 'Mood' },
      { metric: 'Удовлетворённость жильём', current: 42, previous: 45, unit: '%', category: 'Living' },
      { metric: 'Индекс интеграции', current: 45, previous: 42, unit: '/100', category: 'Integration' },
      { metric: 'Рекомендации', current: 420, previous: 380, unit: '', category: 'Community' },
      { metric: 'Мероприятий проведено', current: 12, previous: 8, unit: '', category: 'Community' },
      { metric: 'Сигналы оттока', current: 180, previous: 210, unit: '', category: 'Retention', isInverse: true },
    ],
  },

  sentimentByTopic: {
    en: [
      { topic: 'Nature & Hiking', positive: 88, neutral: 10, negative: 2, volume: 850 },
      { topic: 'Food & Restaurants', positive: 82, neutral: 12, negative: 6, volume: 1380 },
      { topic: 'Community Events', positive: 78, neutral: 18, negative: 4, volume: 420 },
      { topic: 'Armenian Culture', positive: 75, neutral: 20, negative: 5, volume: 680 },
      { topic: 'Kids & Education', positive: 45, neutral: 25, negative: 30, volume: 1950 },
      { topic: 'Jobs & Work', positive: 42, neutral: 35, negative: 23, volume: 2200 },
      { topic: 'Banking & Finance', positive: 28, neutral: 32, negative: 40, volume: 1680 },
      { topic: 'Housing & Rent', positive: 18, neutral: 22, negative: 60, volume: 2840 },
      { topic: 'Bureaucracy', positive: 10, neutral: 20, negative: 70, volume: 1250 },
      { topic: 'Healthcare', positive: 22, neutral: 28, negative: 50, volume: 1420 },
    ],
    ru: [
      { topic: 'Природа и хайкинг', positive: 88, neutral: 10, negative: 2, volume: 850 },
      { topic: 'Еда и рестораны', positive: 82, neutral: 12, negative: 6, volume: 1380 },
      { topic: 'Мероприятия сообщества', positive: 78, neutral: 18, negative: 4, volume: 420 },
      { topic: 'Армянская культура', positive: 75, neutral: 20, negative: 5, volume: 680 },
      { topic: 'Дети и образование', positive: 45, neutral: 25, negative: 30, volume: 1950 },
      { topic: 'Работа и занятость', positive: 42, neutral: 35, negative: 23, volume: 2200 },
      { topic: 'Банки и финансы', positive: 28, neutral: 32, negative: 40, volume: 1680 },
      { topic: 'Жильё и аренда', positive: 18, neutral: 22, negative: 60, volume: 2840 },
      { topic: 'Бюрократия', positive: 10, neutral: 20, negative: 70, volume: 1250 },
      { topic: 'Медицина', positive: 22, neutral: 28, negative: 50, volume: 1420 },
    ],
  },

  topPosts: {
    en: [
      { title: 'Complete guide: opening a bank account', type: 'Guide', shares: 342, reactions: 890, comments: 156, engagement: 98 },
      { title: 'Best neighborhoods map with prices', type: 'Infographic', shares: 298, reactions: 780, comments: 134, engagement: 95 },
      { title: 'Weekend hiking: Garni gorge photos', type: 'Photo', shares: 245, reactions: 920, comments: 88, engagement: 92 },
      { title: 'Tax calculator for freelancers', type: 'Tool', shares: 210, reactions: 650, comments: 198, engagement: 90 },
      { title: 'Restaurant review: top 10 family-friendly', type: 'Review', shares: 188, reactions: 720, comments: 112, engagement: 88 },
      { title: 'Q&A: residency permit step by step', type: 'Guide', shares: 165, reactions: 580, comments: 220, engagement: 85 },
      { title: 'Our community picnic — 200 people came!', type: 'Event', shares: 320, reactions: 1100, comments: 340, engagement: 97 },
      { title: 'Comparing Yerevan vs Tbilisi for families', type: 'Analysis', shares: 280, reactions: 820, comments: 280, engagement: 94 },
    ],
    ru: [
      { title: 'Полный гайд: как открыть банковский счёт', type: 'Гайд', shares: 342, reactions: 890, comments: 156, engagement: 98 },
      { title: 'Карта лучших районов с ценами на жильё', type: 'Инфографика', shares: 298, reactions: 780, comments: 134, engagement: 95 },
      { title: 'Хайкинг выходного дня: ущелье Гарни', type: 'Фото', shares: 245, reactions: 920, comments: 88, engagement: 92 },
      { title: 'Налоговый калькулятор для фрилансеров', type: 'Инструмент', shares: 210, reactions: 650, comments: 198, engagement: 90 },
      { title: 'Обзор ресторанов: топ-10 для семей', type: 'Обзор', shares: 188, reactions: 720, comments: 112, engagement: 88 },
      { title: 'Вопрос-ответ: оформление ВНЖ шаг за шагом', type: 'Гайд', shares: 165, reactions: 580, comments: 220, engagement: 85 },
      { title: 'Пикник сообщества — собрали 200 человек!', type: 'Мероприятие', shares: 320, reactions: 1100, comments: 340, engagement: 97 },
      { title: 'Ереван vs Тбилиси: сравнение для семей', type: 'Аналитика', shares: 280, reactions: 820, comments: 280, engagement: 94 },
    ],
  },

  contentTypePerformance: {
    en: [
      { type: 'Guides', avgEngagement: 92, count: 12 },
      { type: 'Events', avgEngagement: 88, count: 8 },
      { type: 'Photos', avgEngagement: 85, count: 45 },
      { type: 'Tools', avgEngagement: 82, count: 5 },
      { type: 'Reviews', avgEngagement: 78, count: 22 },
      { type: 'Discussions', avgEngagement: 65, count: 120 },
      { type: 'News', avgEngagement: 45, count: 35 },
    ],
    ru: [
      { type: 'Гайды', avgEngagement: 92, count: 12 },
      { type: 'Мероприятия', avgEngagement: 88, count: 8 },
      { type: 'Фото', avgEngagement: 85, count: 45 },
      { type: 'Инструменты', avgEngagement: 82, count: 5 },
      { type: 'Обзоры', avgEngagement: 78, count: 22 },
      { type: 'Обсуждения', avgEngagement: 65, count: 120 },
      { type: 'Новости', avgEngagement: 45, count: 35 },
    ],
  },

  vitalityIndicators: {
    en: [
      { indicator: 'Member Growth Rate',               score: 78, trend: +12, benchmark: 'Top 10%',    benchmarkLevel: 'excellent',  emoji: '\u{1F4C8}' },
      { indicator: 'Engagement Depth',                 score: 65, trend: +8,  benchmark: 'Above avg',  benchmarkLevel: 'above_avg',  emoji: '\u{1F4AC}' },
      { indicator: 'Content Quality',                  score: 72, trend: +5,  benchmark: 'Good',       benchmarkLevel: 'good',       emoji: '\u{2B50}'  },
      { indicator: 'Newcomer Onboarding',              score: 48, trend: +15, benchmark: 'Needs work', benchmarkLevel: 'poor',       emoji: '\u{1F6AA}' },
      { indicator: 'Member Retention',                 score: 62, trend: -3,  benchmark: 'Average',    benchmarkLevel: 'average',    emoji: '\u{1F504}' },
      { indicator: 'Help Ratio (answers/questions)',   score: 55, trend: +4,  benchmark: 'Below avg',  benchmarkLevel: 'below_avg',  emoji: '\u{1F91D}' },
      { indicator: 'Event Participation',              score: 82, trend: +22, benchmark: 'Excellent',  benchmarkLevel: 'excellent',  emoji: '\u{1F389}' },
      { indicator: 'Cross-topic Diversity',            score: 70, trend: +3,  benchmark: 'Good',       benchmarkLevel: 'good',       emoji: '\u{1F308}' },
    ],
    ru: [
      { indicator: 'Темп роста участников',                    score: 78, trend: +12, benchmark: 'Топ 10%',          benchmarkLevel: 'excellent',  emoji: '\u{1F4C8}' },
      { indicator: 'Глубина вовлечённости',                    score: 65, trend: +8,  benchmark: 'Выше среднего',    benchmarkLevel: 'above_avg',  emoji: '\u{1F4AC}' },
      { indicator: 'Качество контента',                        score: 72, trend: +5,  benchmark: 'Хорошо',           benchmarkLevel: 'good',       emoji: '\u{2B50}'  },
      { indicator: 'Онбординг новичков',                       score: 48, trend: +15, benchmark: 'Требует работы',   benchmarkLevel: 'poor',       emoji: '\u{1F6AA}' },
      { indicator: 'Удержание участников',                     score: 62, trend: -3,  benchmark: 'Среднее',          benchmarkLevel: 'average',    emoji: '\u{1F504}' },
      { indicator: 'Соотношение помощи (ответы/вопросы)',      score: 55, trend: +4,  benchmark: 'Ниже среднего',    benchmarkLevel: 'below_avg',  emoji: '\u{1F91D}' },
      { indicator: 'Участие в мероприятиях',                   score: 82, trend: +22, benchmark: 'Отлично',          benchmarkLevel: 'excellent',  emoji: '\u{1F389}' },
      { indicator: 'Разнообразие тем',                         score: 70, trend: +3,  benchmark: 'Хорошо',           benchmarkLevel: 'good',       emoji: '\u{1F308}' },
    ],
  },

  // ════════════════════════════════════════════════════════
  // PAGES — Topics, Channels, Audience
  // ════════════════════════════════════════════════════════
  allTopics: [
    {
      id: 'housing', name: 'Housing & Rent', nameRu: 'Жильё и аренда', category: 'Living', color: '#ef4444',
      mentions: 2840, growth: +12, sentiment: { positive: 28, neutral: 42, negative: 30 },
      weeklyData: [{ week: 'W1', count: 340 },{ week: 'W2', count: 380 },{ week: 'W3', count: 410 },{ week: 'W4', count: 390 },{ week: 'W5', count: 420 },{ week: 'W6', count: 450 },{ week: 'W7', count: 450 }],
      topChannels: ['Аренда Ереван', 'Русские в Ереване', 'IT Relocants Armenia'],
      description: 'Discussions about apartment hunting, rent prices, landlord issues, neighborhoods, buying property.',
      descriptionRu: 'Обсуждение поиска квартир, цен на аренду, проблем с арендодателями, районов, покупки недвижимости.',
      evidence: [
        { id: 'e1', type: 'comment', author: '@marina_yerevan', channel: 'Аренда Ереван', text: 'Rent prices in Kentron went up 30% since September. A 2-bedroom that was $500 is now $650. Anyone know affordable areas nearby?', timestamp: '2026-02-23 14:32', reactions: 47, replies: 23 },
        { id: 'e2', type: 'post', author: '@alex_it_am', channel: 'IT Relocants Armenia', text: 'PSA: Avoid the building at Tumanyan 42. Third person this month complaining about water issues and unresponsive landlord.', timestamp: '2026-02-22 09:15', reactions: 89, replies: 34 },
        { id: 'e3', type: 'comment', author: '@olga_mom', channel: 'Мамы Еревана', text: 'Looking for 3-bedroom near a school with Russian program. Budget 800-1000 USD. Arabkir or Davtashen preferred. Any leads?', timestamp: '2026-02-22 16:45', reactions: 12, replies: 18 },
        { id: 'e4', type: 'post', author: '@realty_helper', channel: 'Аренда Ереван', text: 'Just moved to Ajapnyak. Way cheaper than Kentron, 15 min by metro, and the neighborhood is actually really nice. Paying $380 for a renovated 2br.', timestamp: '2026-02-21 11:20', reactions: 156, replies: 42 },
        { id: 'e5', type: 'comment', author: '@sergey_biz', channel: 'Бизнес в Армении', text: 'Anyone renting commercial space? Looking for a small office in center, 30-50 sqm. Seeing crazy prices on list.am', timestamp: '2026-02-20 08:50', reactions: 8, replies: 11 },
      ],
    },
    {
      id: 'jobs', name: 'Jobs & Freelance', nameRu: 'Работа и фриланс', category: 'Work', color: '#3b82f6',
      mentions: 2200, growth: +18, sentiment: { positive: 45, neutral: 38, negative: 17 },
      weeklyData: [{ week: 'W1', count: 250 },{ week: 'W2', count: 270 },{ week: 'W3', count: 295 },{ week: 'W4', count: 310 },{ week: 'W5', count: 325 },{ week: 'W6', count: 340 },{ week: 'W7', count: 410 }],
      topChannels: ['IT Relocants Armenia', 'Бизнес в Армении', 'Русские в Ереване'],
      description: 'Remote work, local job market, freelancing, taxes, coworking spaces.',
      descriptionRu: 'Удалённая работа, местный рынок труда, фриланс, налоги, коворкинги.',
      evidence: [
        { id: 'e6', type: 'post', author: '@dev_anton', channel: 'IT Relocants Armenia', text: 'Started working from Impact Hub. Great internet, nice community, and only $150/month for hot desk. Highly recommend for developers.', timestamp: '2026-02-23 10:00', reactions: 67, replies: 15 },
        { id: 'e7', type: 'comment', author: '@freelance_kate', channel: 'IT Relocants Armenia', text: 'How are you guys handling Armenian taxes as freelancers? Just got my first tax notice and I am confused about the 20% income tax.', timestamp: '2026-02-22 13:40', reactions: 34, replies: 28 },
        { id: 'e8', type: 'comment', author: '@hr_anna', channel: 'Бизнес в Армении', text: 'We are hiring Russian-speaking devs in Yerevan. Python/Django, $2-3K range. Remote-first but office available. DM me.', timestamp: '2026-02-21 09:30', reactions: 112, replies: 45 },
      ],
    },
    {
      id: 'education', name: 'Schools & Education', nameRu: 'Школы и образование', category: 'Family', color: '#8b5cf6',
      mentions: 1950, growth: +25, sentiment: { positive: 35, neutral: 40, negative: 25 },
      weeklyData: [{ week: 'W1', count: 180 },{ week: 'W2', count: 210 },{ week: 'W3', count: 250 },{ week: 'W4', count: 280 },{ week: 'W5', count: 300 },{ week: 'W6', count: 340 },{ week: 'W7', count: 390 }],
      topChannels: ['Мамы Еревана', 'Русские в Ереване', 'Армения Документы'],
      description: 'Schools with Russian programs, kindergartens, universities, tutoring, kids activities.',
      descriptionRu: 'Школы с русскими программами, детские сады, университеты, репетиторы, кружки.',
      evidence: [
        { id: 'e9', type: 'post', author: '@mom_natalia', channel: 'Мамы Еревана', text: 'Comparison of Russian-program schools in Yerevan based on parent reviews: Quantum is best for STEM, School #8 for humanities, Ayb for English+Armenian integration.', timestamp: '2026-02-23 16:20', reactions: 203, replies: 67 },
        { id: 'e10', type: 'comment', author: '@dad_igor', channel: 'Мамы Еревана', text: 'Anyone tried the new Montessori kindergarten in Arabkir? Price is 150K AMD/month which seems reasonable.', timestamp: '2026-02-22 08:15', reactions: 23, replies: 19 },
      ],
    },
    {
      id: 'banking', name: 'Banking & Finance', nameRu: 'Банки и финансы', category: 'Finance', color: '#f59e0b',
      mentions: 1680, growth: +32, sentiment: { positive: 38, neutral: 45, negative: 17 },
      weeklyData: [{ week: 'W1', count: 150 },{ week: 'W2', count: 180 },{ week: 'W3', count: 220 },{ week: 'W4', count: 240 },{ week: 'W5', count: 260 },{ week: 'W6', count: 290 },{ week: 'W7', count: 340 }],
      topChannels: ['IT Relocants Armenia', 'Русские в Ереване', 'Бизнес в Армении'],
      description: 'Bank accounts, transfers, crypto, taxes, currency exchange.',
      descriptionRu: 'Банковские счета, переводы, крипто, налоги, обмен валюты.',
      evidence: [
        { id: 'e11', type: 'post', author: '@fin_expert', channel: 'IT Relocants Armenia', text: 'Guide: Opening an Ameriabank account as a non-resident. You need passport + rental agreement + $100 initial deposit. Process takes 30 min. Staff speaks Russian.', timestamp: '2026-02-23 12:00', reactions: 178, replies: 52 },
        { id: 'e12', type: 'comment', author: '@crypto_dima', channel: 'IT Relocants Armenia', text: 'Has anyone successfully received SWIFT transfers to Evocabank? My company needs to wire salary and they said it could take 5-7 days.', timestamp: '2026-02-21 15:30', reactions: 29, replies: 17 },
      ],
    },
    {
      id: 'healthcare', name: 'Healthcare', nameRu: 'Здравоохранение', category: 'Living', color: '#f97316',
      mentions: 1420, growth: +8, sentiment: { positive: 30, neutral: 35, negative: 35 },
      weeklyData: [{ week: 'W1', count: 190 },{ week: 'W2', count: 195 },{ week: 'W3', count: 200 },{ week: 'W4', count: 205 },{ week: 'W5', count: 210 },{ week: 'W6', count: 208 },{ week: 'W7', count: 212 }],
      topChannels: ['Русские в Ереване', 'Мамы Еревана'],
      description: 'Finding doctors, hospitals, insurance, pharmacies, dental care.',
      descriptionRu: 'Поиск врачей, больниц, страховок, аптек, стоматологических клиник.',
      evidence: [
        { id: 'e13', type: 'comment', author: '@health_seeker', channel: 'Русские в Ереване', text: 'Can someone recommend a Russian-speaking dentist? Need a root canal and want to understand what is happening without a translator.', timestamp: '2026-02-23 09:45', reactions: 15, replies: 22 },
      ],
    },
    {
      id: 'food', name: 'Food & Restaurants', nameRu: 'Еда и рестораны', category: 'Lifestyle', color: '#ec4899',
      mentions: 1380, growth: +5, sentiment: { positive: 72, neutral: 20, negative: 8 },
      weeklyData: [{ week: 'W1', count: 190 },{ week: 'W2', count: 195 },{ week: 'W3', count: 192 },{ week: 'W4', count: 198 },{ week: 'W5', count: 200 },{ week: 'W6', count: 202 },{ week: 'W7', count: 203 }],
      topChannels: ['Еда и Рестораны AM', 'Русские в Ереване'],
      description: 'Restaurant reviews, grocery stores, delivery, cooking tips, hidden gems.',
      descriptionRu: 'Отзывы о ресторанах, супермаркеты, доставка, советы по готовке, скрытые жемчужины.',
      evidence: [
        { id: 'e14', type: 'post', author: '@foodie_lena', channel: 'Еда и Рестораны AM', text: 'Top 5 hidden gem restaurants in Yerevan that locals go to: 1. Lavash at Pushkin st, 2. Sherep near Opera...', timestamp: '2026-02-22 19:00', reactions: 234, replies: 56 },
      ],
    },
    {
      id: 'legal', name: 'Legal & Residency', nameRu: 'Юридические вопросы и ВНЖ', category: 'Admin', color: '#6b7280',
      mentions: 1250, growth: +42, sentiment: { positive: 20, neutral: 55, negative: 25 },
      weeklyData: [{ week: 'W1', count: 100 },{ week: 'W2', count: 130 },{ week: 'W3', count: 160 },{ week: 'W4', count: 180 },{ week: 'W5', count: 195 },{ week: 'W6', count: 220 },{ week: 'W7', count: 265 }],
      topChannels: ['Армения Документы', 'Русские в Ереване', 'Бизнес в Армении'],
      description: 'Residency permits, visas, LLC registration, taxes, notary services.',
      descriptionRu: 'Виды на жительство, визы, регистрация ООО, налоги, нотариальные услуги.',
      evidence: [
        { id: 'e15', type: 'post', author: '@lawyer_am', channel: 'Армения Документы', text: 'UPDATE: New residency rules as of Feb 2026. You can now get a 1-year permit with proof of rental + bank statement. No need for employer letter anymore.', timestamp: '2026-02-24 08:00', reactions: 312, replies: 89 },
      ],
    },
    {
      id: 'language', name: 'Armenian Language', nameRu: 'Армянский язык', category: 'Integration', color: '#10b981',
      mentions: 1100, growth: +55, sentiment: { positive: 52, neutral: 38, negative: 10 },
      weeklyData: [{ week: 'W1', count: 80 },{ week: 'W2', count: 100 },{ week: 'W3', count: 125 },{ week: 'W4', count: 150 },{ week: 'W5', count: 170 },{ week: 'W6', count: 200 },{ week: 'W7', count: 275 }],
      topChannels: ['Русские в Ереване', 'IT Relocants Armenia'],
      description: 'Learning Armenian, tutors, language exchange, useful phrases, integration.',
      descriptionRu: 'Изучение армянского, репетиторы, языковой обмен, полезные фразы, интеграция.',
      evidence: [
        { id: 'e16', type: 'comment', author: '@learner_max', channel: 'Русские в Ереване', text: 'Been taking Armenian classes at Berlitz for 3 months. Can now order food and navigate taxis in Armenian. Locals are SO happy when you try! Worth every penny.', timestamp: '2026-02-23 17:30', reactions: 89, replies: 31 },
      ],
    },
    {
      id: 'kids', name: 'Kids Activities', nameRu: 'Детский досуг', category: 'Family', color: '#8b5cf6',
      mentions: 980, growth: +20, sentiment: { positive: 65, neutral: 25, negative: 10 },
      weeklyData: [{ week: 'W1', count: 110 },{ week: 'W2', count: 120 },{ week: 'W3', count: 130 },{ week: 'W4', count: 140 },{ week: 'W5', count: 145 },{ week: 'W6', count: 160 },{ week: 'W7', count: 175 }],
      topChannels: ['Мамы Еревана'],
      description: 'Kids clubs, sports, art classes, playgrounds, weekend activities.',
      descriptionRu: 'Детские клубы, спорт, занятия искусством, площадки, досуг на выходных.',
      evidence: [],
    },
    {
      id: 'coworking', name: 'Coworking & Office', nameRu: 'Коворкинг и офисы', category: 'Work', color: '#3b82f6',
      mentions: 920, growth: +35, sentiment: { positive: 58, neutral: 32, negative: 10 },
      weeklyData: [{ week: 'W1', count: 70 },{ week: 'W2', count: 90 },{ week: 'W3', count: 110 },{ week: 'W4', count: 130 },{ week: 'W5', count: 140 },{ week: 'W6', count: 170 },{ week: 'W7', count: 210 }],
      topChannels: ['IT Relocants Armenia', 'Бизнес в Армении'],
      description: 'Coworking spaces, internet quality, offices for rent, remote work tips.',
      descriptionRu: 'Коворкинг-пространства, качество интернета, офисы в аренду, советы по удалённой работе.',
      evidence: [],
    },
  ],

  allChannels: [
    {
      id: 'ch1', name: 'Русские в Ереване', type: 'General', members: 18400, dailyMessages: 420, engagement: 92, growth: +340,
      topTopic: 'Housing tips', description: 'Main Russian-speaking expat community in Yerevan. General discussions about life in Armenia.',
      weeklyData: [{ day: 'Mon', msgs: 380 },{ day: 'Tue', msgs: 410 },{ day: 'Wed', msgs: 450 },{ day: 'Thu', msgs: 420 },{ day: 'Fri', msgs: 390 },{ day: 'Sat', msgs: 520 },{ day: 'Sun', msgs: 480 }],
      hourlyData: [{ hour: '6am', msgs: 12 },{ hour: '8am', msgs: 35 },{ hour: '10am', msgs: 62 },{ hour: '12pm', msgs: 78 },{ hour: '2pm', msgs: 65 },{ hour: '4pm', msgs: 55 },{ hour: '6pm', msgs: 72 },{ hour: '8pm', msgs: 95 },{ hour: '10pm', msgs: 68 }],
      topTopics: [{ name: 'Housing & Rent', mentions: 520, pct: 22 },{ name: 'Legal & Residency', mentions: 380, pct: 16 },{ name: 'Food & Restaurants', mentions: 310, pct: 13 },{ name: 'Healthcare', mentions: 260, pct: 11 },{ name: 'Armenian Language', mentions: 220, pct: 9 }],
      sentimentBreakdown: { positive: 45, neutral: 35, negative: 20 },
      messageTypes: [{ type: 'Discussion', count: 840, pct: 35 },{ type: 'Question', count: 600, pct: 25 },{ type: 'Recommendation', count: 480, pct: 20 },{ type: 'Complaint', count: 240, pct: 10 },{ type: 'Info Sharing', count: 240, pct: 10 }],
      topVoices: [{ name: 'Алексей (IT_Alex_AM)', posts: 128, helpScore: 95 },{ name: 'Марина (@marina_yerevan)', posts: 96, helpScore: 88 },{ name: 'Ольга (@olga_mom)', posts: 84, helpScore: 82 }],
      recentPosts: [
        { id: 'p1', author: '@newcomer_anna', text: 'Just arrived in Yerevan! Any tips for the first week? Where to get a SIM card and how to find a short-term apartment?', timestamp: '2026-02-24 09:15', reactions: 45, replies: 32 },
        { id: 'p2', author: '@yerevan_local', text: 'The new pedestrian zone on Northern Avenue is fantastic. Finally feels like a proper European city center. Great cafes opening up.', timestamp: '2026-02-24 08:30', reactions: 89, replies: 14 },
        { id: 'p3', author: '@tax_helper', text: 'Reminder: Tax filing deadline for 2025 is April 20. If you earned income in Armenia, even as freelancer, you need to file. Here is a step-by-step guide...', timestamp: '2026-02-23 18:45', reactions: 156, replies: 47 },
      ],
    },
    {
      id: 'ch2', name: 'IT Relocants Armenia', type: 'Work', members: 12200, dailyMessages: 280, engagement: 85, growth: +520,
      topTopic: 'Tax optimization', description: 'Tech workers who relocated to Armenia. Jobs, coworking, taxes, visa.',
      weeklyData: [{ day: 'Mon', msgs: 310 },{ day: 'Tue', msgs: 290 },{ day: 'Wed', msgs: 300 },{ day: 'Thu', msgs: 280 },{ day: 'Fri', msgs: 250 },{ day: 'Sat', msgs: 180 },{ day: 'Sun', msgs: 150 }],
      hourlyData: [{ hour: '6am', msgs: 5 },{ hour: '8am', msgs: 25 },{ hour: '10am', msgs: 55 },{ hour: '12pm', msgs: 42 },{ hour: '2pm', msgs: 58 },{ hour: '4pm', msgs: 48 },{ hour: '6pm', msgs: 35 },{ hour: '8pm', msgs: 28 },{ hour: '10pm', msgs: 15 }],
      topTopics: [{ name: 'Jobs & Freelance', mentions: 680, pct: 28 },{ name: 'Banking & Finance', mentions: 450, pct: 19 },{ name: 'Coworking & Office', mentions: 380, pct: 16 },{ name: 'Legal & Residency', mentions: 290, pct: 12 },{ name: 'Housing & Rent', mentions: 210, pct: 9 }],
      sentimentBreakdown: { positive: 52, neutral: 35, negative: 13 },
      messageTypes: [{ type: 'Question', count: 560, pct: 30 },{ type: 'Info Sharing', count: 470, pct: 25 },{ type: 'Discussion', count: 375, pct: 20 },{ type: 'Recommendation', count: 280, pct: 15 },{ type: 'Complaint', count: 185, pct: 10 }],
      topVoices: [{ name: 'Дмитрий (@dev_anton)', posts: 156, helpScore: 92 },{ name: 'Катя (@freelance_kate)', posts: 112, helpScore: 78 }],
      recentPosts: [{ id: 'p4', author: '@dev_anton', text: 'Comparison of coworking spaces: Impact Hub $150, The Loft $120, Smart $100. Impact Hub has best internet (200mbps), Smart is cheapest but slower wifi.', timestamp: '2026-02-24 10:00', reactions: 67, replies: 15 }],
    },
    {
      id: 'ch3', name: 'Мамы Еревана', type: 'Family', members: 8600, dailyMessages: 195, engagement: 88, growth: +280,
      topTopic: 'Schools comparison', description: 'Mothers in Yerevan sharing advice about kids, schools, healthcare, activities.',
      weeklyData: [{ day: 'Mon', msgs: 180 },{ day: 'Tue', msgs: 195 },{ day: 'Wed', msgs: 210 },{ day: 'Thu', msgs: 200 },{ day: 'Fri', msgs: 185 },{ day: 'Sat', msgs: 220 },{ day: 'Sun', msgs: 190 }],
      hourlyData: [{ hour: '6am', msgs: 8 },{ hour: '8am', msgs: 42 },{ hour: '10am', msgs: 35 },{ hour: '12pm', msgs: 28 },{ hour: '2pm', msgs: 45 },{ hour: '4pm', msgs: 52 },{ hour: '6pm', msgs: 38 },{ hour: '8pm', msgs: 65 },{ hour: '10pm', msgs: 42 }],
      topTopics: [{ name: 'Schools & Education', mentions: 520, pct: 30 },{ name: 'Kids Activities', mentions: 380, pct: 22 },{ name: 'Healthcare', mentions: 290, pct: 17 },{ name: 'Food & Restaurants', mentions: 180, pct: 10 },{ name: 'Housing & Rent', mentions: 140, pct: 8 }],
      sentimentBreakdown: { positive: 55, neutral: 30, negative: 15 },
      messageTypes: [{ type: 'Question', count: 520, pct: 35 },{ type: 'Recommendation', count: 380, pct: 25 },{ type: 'Discussion', count: 300, pct: 20 },{ type: 'Info Sharing', count: 200, pct: 13 },{ type: 'Complaint', count: 100, pct: 7 }],
      topVoices: [{ name: 'Наталья (@mom_natalia)', posts: 204, helpScore: 96 },{ name: 'Ольга (@olga_mom)', posts: 168, helpScore: 91 }],
      recentPosts: [],
    },
    {
      id: 'ch4', name: 'Аренда Ереван', type: 'Housing', members: 15800, dailyMessages: 350, engagement: 78, growth: +180,
      topTopic: 'Price negotiations', description: 'Apartment rentals, real estate, roommates, short-term stays.',
      weeklyData: [{ day: 'Mon', msgs: 380 },{ day: 'Tue', msgs: 360 },{ day: 'Wed', msgs: 340 },{ day: 'Thu', msgs: 350 },{ day: 'Fri', msgs: 320 },{ day: 'Sat', msgs: 290 },{ day: 'Sun', msgs: 310 }],
      hourlyData: [{ hour: '6am', msgs: 8 },{ hour: '8am', msgs: 30 },{ hour: '10am', msgs: 55 },{ hour: '12pm', msgs: 62 },{ hour: '2pm', msgs: 58 },{ hour: '4pm', msgs: 45 },{ hour: '6pm', msgs: 52 },{ hour: '8pm', msgs: 48 },{ hour: '10pm', msgs: 35 }],
      topTopics: [{ name: 'Housing & Rent', mentions: 1800, pct: 55 },{ name: 'Legal & Residency', mentions: 320, pct: 10 },{ name: 'Transport', mentions: 280, pct: 9 }],
      sentimentBreakdown: { positive: 25, neutral: 45, negative: 30 },
      messageTypes: [{ type: 'Question', count: 700, pct: 40 },{ type: 'Info Sharing', count: 440, pct: 25 },{ type: 'Recommendation', count: 350, pct: 20 },{ type: 'Complaint', count: 260, pct: 15 }],
      topVoices: [{ name: 'Реалти Хелпер (@realty_helper)', posts: 340, helpScore: 88 }],
      recentPosts: [],
    },
    {
      id: 'ch5', name: 'Бизнес в Армении', type: 'Business', members: 6400, dailyMessages: 120, engagement: 82, growth: +420,
      topTopic: 'LLC registration', description: 'Starting a business in Armenia, hiring, taxes, regulations.',
      weeklyData: [{ day: 'Mon', msgs: 140 },{ day: 'Tue', msgs: 130 },{ day: 'Wed', msgs: 125 },{ day: 'Thu', msgs: 120 },{ day: 'Fri', msgs: 110 },{ day: 'Sat', msgs: 65 },{ day: 'Sun', msgs: 50 }],
      hourlyData: [{ hour: '6am', msgs: 2 },{ hour: '8am', msgs: 15 },{ hour: '10am', msgs: 28 },{ hour: '12pm', msgs: 22 },{ hour: '2pm', msgs: 25 },{ hour: '4pm', msgs: 18 },{ hour: '6pm', msgs: 12 },{ hour: '8pm', msgs: 8 },{ hour: '10pm', msgs: 4 }],
      topTopics: [{ name: 'Legal & Residency', mentions: 420, pct: 32 },{ name: 'Banking & Finance', mentions: 310, pct: 24 },{ name: 'Jobs & Freelance', mentions: 280, pct: 22 }],
      sentimentBreakdown: { positive: 40, neutral: 45, negative: 15 },
      messageTypes: [{ type: 'Question', count: 350, pct: 38 },{ type: 'Info Sharing', count: 280, pct: 30 },{ type: 'Discussion', count: 200, pct: 22 },{ type: 'Recommendation', count: 90, pct: 10 }],
      topVoices: [],
      recentPosts: [],
    },
    {
      id: 'ch6', name: 'Еда и Ретораны AM', type: 'Lifestyle', members: 9200, dailyMessages: 150, engagement: 90, growth: +150,
      topTopic: 'Hidden gems', description: 'Food, restaurants, groceries, delivery, cooking in Armenia.',
      weeklyData: [{ day: 'Mon', msgs: 120 },{ day: 'Tue', msgs: 130 },{ day: 'Wed', msgs: 145 },{ day: 'Thu', msgs: 155 },{ day: 'Fri', msgs: 180 },{ day: 'Sat', msgs: 210 },{ day: 'Sun', msgs: 190 }],
      hourlyData: [{ hour: '6am', msgs: 2 },{ hour: '8am', msgs: 10 },{ hour: '10am', msgs: 18 },{ hour: '12pm', msgs: 35 },{ hour: '2pm', msgs: 28 },{ hour: '4pm', msgs: 22 },{ hour: '6pm', msgs: 30 },{ hour: '8pm', msgs: 38 },{ hour: '10pm', msgs: 25 }],
      topTopics: [{ name: 'Food & Restaurants', mentions: 890, pct: 62 },{ name: 'Shopping & Markets', mentions: 210, pct: 15 }],
      sentimentBreakdown: { positive: 72, neutral: 20, negative: 8 },
      messageTypes: [{ type: 'Recommendation', count: 480, pct: 42 },{ type: 'Discussion', count: 280, pct: 25 },{ type: 'Photo/Video', count: 200, pct: 18 },{ type: 'Question', count: 170, pct: 15 }],
      topVoices: [],
      recentPosts: [],
    },
    {
      id: 'ch7', name: 'Армения Документы', type: 'Legal', members: 11400, dailyMessages: 210, engagement: 75, growth: +380,
      topTopic: 'Residency permit', description: 'Residency permits, visas, legal documents, notary services.',
      weeklyData: [{ day: 'Mon', msgs: 240 },{ day: 'Tue', msgs: 230 },{ day: 'Wed', msgs: 220 },{ day: 'Thu', msgs: 210 },{ day: 'Fri', msgs: 200 },{ day: 'Sat', msgs: 120 },{ day: 'Sun', msgs: 100 }],
      hourlyData: [{ hour: '6am', msgs: 5 },{ hour: '8am', msgs: 28 },{ hour: '10am', msgs: 45 },{ hour: '12pm', msgs: 38 },{ hour: '2pm', msgs: 42 },{ hour: '4pm', msgs: 32 },{ hour: '6pm', msgs: 20 },{ hour: '8pm', msgs: 15 },{ hour: '10pm', msgs: 8 }],
      topTopics: [{ name: 'Legal & Residency', mentions: 1200, pct: 58 },{ name: 'Banking & Finance', mentions: 320, pct: 15 }],
      sentimentBreakdown: { positive: 22, neutral: 55, negative: 23 },
      messageTypes: [{ type: 'Question', count: 650, pct: 45 },{ type: 'Info Sharing', count: 400, pct: 28 },{ type: 'Discussion', count: 250, pct: 17 },{ type: 'Complaint', count: 150, pct: 10 }],
      topVoices: [],
      recentPosts: [],
    },
    {
      id: 'ch8', name: 'Хайкинг Армения', type: 'Lifestyle', members: 4800, dailyMessages: 85, engagement: 94, growth: +220,
      topTopic: 'Weekend trails', description: 'Hiking, outdoor activities, trails, nature spots in Armenia.',
      weeklyData: [{ day: 'Mon', msgs: 45 },{ day: 'Tue', msgs: 50 },{ day: 'Wed', msgs: 60 },{ day: 'Thu', msgs: 70 },{ day: 'Fri', msgs: 95 },{ day: 'Sat', msgs: 130 },{ day: 'Sun', msgs: 120 }],
      hourlyData: [{ hour: '6am', msgs: 15 },{ hour: '8am', msgs: 20 },{ hour: '10am', msgs: 12 },{ hour: '12pm', msgs: 8 },{ hour: '2pm', msgs: 10 },{ hour: '4pm', msgs: 15 },{ hour: '6pm', msgs: 25 },{ hour: '8pm', msgs: 30 },{ hour: '10pm', msgs: 18 }],
      topTopics: [{ name: 'Outdoor & Hiking', mentions: 620, pct: 72 },{ name: 'Nature', mentions: 150, pct: 18 }],
      sentimentBreakdown: { positive: 82, neutral: 15, negative: 3 },
      messageTypes: [{ type: 'Photo/Video', count: 320, pct: 38 },{ type: 'Recommendation', count: 250, pct: 30 },{ type: 'Discussion', count: 180, pct: 22 },{ type: 'Question', count: 85, pct: 10 }],
      topVoices: [],
      recentPosts: [],
    },
  ],

  allAudience: [
    {
      id: 'u1', username: '@IT_Alex_AM', displayName: 'Алексей Петров', gender: 'Male' as const, age: '28-32',
      origin: 'Moscow', location: 'Yerevan, Center', joinedDate: '2024-06-15', lastActive: '2 hours ago',
      totalMessages: 1284, totalReactions: 3420, helpScore: 95,
      interests: ['Technology', 'Coworking', 'Hiking', 'Crypto', 'Tax optimization', 'Cafes'],
      channels: [{ name: 'IT Relocants Armenia', type: 'Work', role: 'Admin', messageCount: 680 },{ name: 'Русские в Ереване', type: 'General', role: 'Active', messageCount: 320 },{ name: 'Хайкинг Армения', type: 'Lifestyle', role: 'Member', messageCount: 180 },{ name: 'Бизнес в Армении', type: 'Business', role: 'Active', messageCount: 104 }],
      topTopics: [{ name: 'Tax optimization', count: 142 },{ name: 'Coworking spaces', count: 98 },{ name: 'IT jobs', count: 87 },{ name: 'Banking', count: 64 },{ name: 'Hiking trails', count: 52 }],
      sentiment: { positive: 62, neutral: 30, negative: 8 },
      activityData: [{ week: 'W1', msgs: 42 },{ week: 'W2', msgs: 38 },{ week: 'W3', msgs: 45 },{ week: 'W4', msgs: 50 },{ week: 'W5', msgs: 48 },{ week: 'W6', msgs: 55 },{ week: 'W7', msgs: 52 }],
      recentMessages: [
        { text: 'Started working from Impact Hub. Great internet, nice community, $150/month for hot desk. Highly recommend for developers.', channel: 'IT Relocants Armenia', timestamp: '2 hours ago', reactions: 67, replies: 15 },
        { text: 'Quick guide on paying quarterly taxes as a freelancer: you need form 100, submit via tax.am, deadline is the 20th of the following month.', channel: 'IT Relocants Armenia', timestamp: '1 day ago', reactions: 134, replies: 42 },
        { text: 'Weekend hike to Garni gorge was incredible. The basalt columns are unreal. Here is the trail map for anyone interested.', channel: 'Хайкинг Армения', timestamp: '3 days ago', reactions: 89, replies: 12 },
      ],
      persona: 'IT Relocant', integrationLevel: 'Learning & Mixing',
    },
    {
      id: 'u2', username: '@marina_yerevan', displayName: 'Марина Соколова', gender: 'Female' as const, age: '33-37',
      origin: 'St. Petersburg', location: 'Yerevan, Arabkir', joinedDate: '2024-03-20', lastActive: '30 min ago',
      totalMessages: 1856, totalReactions: 4210, helpScore: 92,
      interests: ['Real estate', 'Schools', 'Cooking', 'Yoga', 'Kids activities', 'Interior design'],
      channels: [{ name: 'Аренда Ереван', type: 'Housing', role: 'Moderator', messageCount: 720 },{ name: 'Мамы Еревана', type: 'Family', role: 'Active', messageCount: 540 },{ name: 'Русские в Ереване', type: 'General', role: 'Active', messageCount: 380 },{ name: 'Еда и Рестораны AM', type: 'Lifestyle', role: 'Member', messageCount: 216 }],
      topTopics: [{ name: 'Housing & Rent', count: 312 },{ name: 'Schools', count: 178 },{ name: 'Kids activities', count: 124 },{ name: 'Neighborhoods', count: 98 },{ name: 'Restaurants', count: 76 }],
      sentiment: { positive: 55, neutral: 35, negative: 10 },
      activityData: [{ week: 'W1', msgs: 58 },{ week: 'W2', msgs: 62 },{ week: 'W3', msgs: 55 },{ week: 'W4', msgs: 68 },{ week: 'W5', msgs: 72 },{ week: 'W6', msgs: 65 },{ week: 'W7', msgs: 70 }],
      recentMessages: [
        { text: 'Rent prices in Kentron went up 30% since September. A 2-bedroom that was $500 is now $650. Anyone know affordable areas nearby?', channel: 'Аренда Ере��ан', timestamp: '30 min ago', reactions: 47, replies: 23 },
        { text: 'Found an amazing Montessori kindergarten in Arabkir. Small groups, Russian-speaking teacher, 150K AMD/month. DM for details.', channel: 'Мамы Еревана', timestamp: '1 day ago', reactions: 89, replies: 34 },
      ],
      persona: 'Young Family', integrationLevel: 'Bilingual Bubble',
    },
    {
      id: 'u3', username: '@dev_anton', displayName: 'Антон Краснов', gender: 'Male' as const, age: '26-30',
      origin: 'Novosibirsk', location: 'Yerevan, Center', joinedDate: '2024-09-10', lastActive: '4 hours ago',
      totalMessages: 892, totalReactions: 2180, helpScore: 88,
      interests: ['Python', 'Machine Learning', 'Coffee', 'Cycling', 'Remote work', 'Coworking'],
      channels: [{ name: 'IT Relocants Armenia', type: 'Work', role: 'Active', messageCount: 520 },{ name: 'Русские в Ереване', type: 'General', role: 'Member', messageCount: 210 },{ name: 'Бизнес в Армении', type: 'Business', role: 'Member', messageCount: 162 }],
      topTopics: [{ name: 'Coworking', count: 156 },{ name: 'Python/Dev', count: 120 },{ name: 'Internet quality', count: 78 },{ name: 'Banking', count: 54 }],
      sentiment: { positive: 68, neutral: 25, negative: 7 },
      activityData: [{ week: 'W1', msgs: 28 },{ week: 'W2', msgs: 32 },{ week: 'W3', msgs: 35 },{ week: 'W4', msgs: 30 },{ week: 'W5', msgs: 38 },{ week: 'W6', msgs: 42 },{ week: 'W7', msgs: 45 }],
      recentMessages: [{ text: 'Comparison of coworking spaces: Impact Hub $150, The Loft $120, Smart $100. Impact Hub has best internet (200mbps).', channel: 'IT Relocants Armenia', timestamp: '4 hours ago', reactions: 67, replies: 15 }],
      persona: 'IT Relocant', integrationLevel: 'Russian Only',
    },
    {
      id: 'u4', username: '@mom_natalia', displayName: 'Наталья Волкова', gender: 'Female' as const, age: '35-40',
      origin: 'Moscow', location: 'Yerevan, Davtashen', joinedDate: '2024-01-08', lastActive: '1 hour ago',
      totalMessages: 2340, totalReactions: 5680, helpScore: 96,
      interests: ['Education', 'Parenting', 'Healthcare', 'Cooking', 'Gardening', 'Armenian language'],
      channels: [{ name: 'Мамы Еревана', type: 'Family', role: 'Admin', messageCount: 1240 },{ name: 'Русские в Ереване', type: 'General', role: 'Active', messageCount: 620 },{ name: 'Армения Документы', type: 'Legal', role: 'Member', messageCount: 280 },{ name: 'Еда и Рестораны AM', type: 'Lifestyle', role: 'Member', messageCount: 200 }],
      topTopics: [{ name: 'Schools & Education', count: 456 },{ name: 'Pediatricians', count: 234 },{ name: 'Kids activities', count: 198 },{ name: 'Parenting tips', count: 156 },{ name: 'Healthy food', count: 89 }],
      sentiment: { positive: 58, neutral: 32, negative: 10 },
      activityData: [{ week: 'W1', msgs: 72 },{ week: 'W2', msgs: 78 },{ week: 'W3', msgs: 82 },{ week: 'W4', msgs: 75 },{ week: 'W5', msgs: 88 },{ week: 'W6', msgs: 85 },{ week: 'W7', msgs: 90 }],
      recentMessages: [
        { text: 'Comparison of Russian-program schools: Quantum is best for STEM, School #8 for humanities, Ayb for English+Armenian.', channel: 'Мамы Еревана', timestamp: '1 hour ago', reactions: 203, replies: 67 },
        { text: 'Found a great pediatrician who speaks Russian near Davtashen. Dr. Anahit at MediCenter, very patient with kids.', channel: 'Мамы Еревана', timestamp: '2 days ago', reactions: 145, replies: 38 },
      ],
      persona: 'Young Family', integrationLevel: 'Learning & Mixing',
    },
    {
      id: 'u5', username: '@freelance_kate', displayName: 'Екатерина Белова', gender: 'Female' as const, age: '24-28',
      origin: 'Krasnodar', location: 'Yerevan, Center', joinedDate: '2025-02-14', lastActive: '6 hours ago',
      totalMessages: 412, totalReactions: 890, helpScore: 72,
      interests: ['Graphic design', 'Freelancing', 'Yoga', 'Nightlife', 'Photography', 'Travel'],
      channels: [{ name: 'IT Relocants Armenia', type: 'Work', role: 'Active', messageCount: 240 },{ name: 'Русские в Ереване', type: 'General', role: 'Member', messageCount: 120 },{ name: 'Хайкинг Армения', type: 'Lifestyle', role: 'Member', messageCount: 52 }],
      topTopics: [{ name: 'Freelance taxes', count: 68 },{ name: 'Design tools', count: 42 },{ name: 'Remote work', count: 38 },{ name: 'Nightlife', count: 28 }],
      sentiment: { positive: 48, neutral: 40, negative: 12 },
      activityData: [{ week: 'W1', msgs: 12 },{ week: 'W2', msgs: 15 },{ week: 'W3', msgs: 18 },{ week: 'W4', msgs: 14 },{ week: 'W5', msgs: 20 },{ week: 'W6', msgs: 22 },{ week: 'W7', msgs: 25 }],
      recentMessages: [{ text: 'How are you guys handling Armenian taxes as freelancers? Just got my first tax notice and confused about the 20% income tax.', channel: 'IT Relocants Armenia', timestamp: '6 hours ago', reactions: 34, replies: 28 }],
      persona: 'Digital Nomad', integrationLevel: 'Russian Only',
    },
    {
      id: 'u6', username: '@sergey_biz', displayName: 'Сергей Орлов', gender: 'Male' as const, age: '38-45',
      origin: 'Ekaterinburg', location: 'Yerevan, Center', joinedDate: '2024-04-22', lastActive: '3 hours ago',
      totalMessages: 678, totalReactions: 1520, helpScore: 85,
      interests: ['Business', 'Real estate', 'Investment', 'Networking', 'Golf', 'Wine'],
      channels: [{ name: 'Бизнес в Армении', type: 'Business', role: 'Admin', messageCount: 380 },{ name: 'Русские в Ереване', type: 'General', role: 'Active', messageCount: 180 },{ name: 'Аренда Ереван', type: 'Housing', role: 'Member', messageCount: 118 }],
      topTopics: [{ name: 'LLC registration', count: 124 },{ name: 'Hiring', count: 89 },{ name: 'Commercial space', count: 67 },{ name: 'Tax strategy', count: 54 }],
      sentiment: { positive: 45, neutral: 42, negative: 13 },
      activityData: [{ week: 'W1', msgs: 22 },{ week: 'W2', msgs: 25 },{ week: 'W3', msgs: 28 },{ week: 'W4', msgs: 24 },{ week: 'W5', msgs: 30 },{ week: 'W6', msgs: 32 },{ week: 'W7', msgs: 28 }],
      recentMessages: [{ text: 'Anyone renting commercial space? Looking for small office in center, 30-50 sqm. Seeing crazy prices on list.am', channel: 'Бизнес в Армении', timestamp: '3 hours ago', reactions: 8, replies: 11 }],
      persona: 'Entrepreneur', integrationLevel: 'Bilingual Bubble',
    },
    {
      id: 'u7', username: '@foodie_lena', displayName: 'Елена Козлова', gender: 'Female' as const, age: '29-33',
      origin: 'Moscow', location: 'Yerevan, Arabkir', joinedDate: '2024-07-05', lastActive: '5 hours ago',
      totalMessages: 1120, totalReactions: 3850, helpScore: 90,
      interests: ['Food', 'Restaurants', 'Cooking', 'Wine tasting', 'Photography', 'Markets'],
      channels: [{ name: 'Еда и Рестораны AM', type: 'Lifestyle', role: 'Admin', messageCount: 680 },{ name: 'Русские в Ереване', type: 'General', role: 'Active', messageCount: 280 },{ name: 'Мамы Еревана', type: 'Family', role: 'Member', messageCount: 160 }],
      topTopics: [{ name: 'Restaurants', count: 298 },{ name: 'Hidden gems', count: 156 },{ name: 'Grocery stores', count: 112 },{ name: 'Cooking tips', count: 89 }],
      sentiment: { positive: 78, neutral: 18, negative: 4 },
      activityData: [{ week: 'W1', msgs: 38 },{ week: 'W2', msgs: 42 },{ week: 'W3', msgs: 40 },{ week: 'W4', msgs: 45 },{ week: 'W5', msgs: 48 },{ week: 'W6', msgs: 50 },{ week: 'W7', msgs: 52 }],
      recentMessages: [{ text: 'Top 5 hidden gem restaurants in Yerevan that locals go to: 1. Lavash at Pushkin st, 2. Sherep near Opera...', channel: 'Еда и Рестораны AM', timestamp: '5 hours ago', reactions: 234, replies: 56 }],
      persona: 'Established Expat', integrationLevel: 'Learning & Mixing',
    },
    {
      id: 'u8', username: '@lawyer_am', displayName: 'Дмитрий Новиков', gender: 'Male' as const, age: '34-40',
      origin: 'Moscow', location: 'Yerevan, Center', joinedDate: '2024-02-11', lastActive: '1 hour ago',
      totalMessages: 1560, totalReactions: 4920, helpScore: 94,
      interests: ['Law', 'Residency', 'Tax', 'Real estate law', 'Running', 'Chess'],
      channels: [{ name: 'Армения Документы', type: 'Legal', role: 'Admin', messageCount: 890 },{ name: 'Бизнес в Армении', type: 'Business', role: 'Active', messageCount: 380 },{ name: 'Русские в Ереване', type: 'General', role: 'Active', messageCount: 290 }],
      topTopics: [{ name: 'Residency permits', count: 342 },{ name: 'Tax law', count: 234 },{ name: 'LLC registration', count: 178 },{ name: 'Property law', count: 98 }],
      sentiment: { positive: 42, neutral: 48, negative: 10 },
      activityData: [{ week: 'W1', msgs: 52 },{ week: 'W2', msgs: 48 },{ week: 'W3', msgs: 55 },{ week: 'W4', msgs: 58 },{ week: 'W5', msgs: 50 },{ week: 'W6', msgs: 62 },{ week: 'W7', msgs: 65 }],
      recentMessages: [{ text: 'UPDATE: New residency rules as of Feb 2026. You can now get a 1-year permit with proof of rental + bank statement.', channel: 'Армения Документы', timestamp: '1 hour ago', reactions: 312, replies: 89 }],
      persona: 'Entrepreneur', integrationLevel: 'Fully Integrated',
    },
    {
      id: 'u9', username: '@nomad_vlad', displayName: 'Владислав Комаров', gender: 'Male' as const, age: '23-27',
      origin: 'Minsk', location: 'Yerevan, Center', joinedDate: '2025-11-20', lastActive: '20 min ago',
      totalMessages: 186, totalReactions: 340, helpScore: 35,
      interests: ['Nightlife', 'Photography', 'Travel', 'Bars', 'Dating', 'Street food'],
      channels: [{ name: 'Русские в Ереване', type: 'General', role: 'Member', messageCount: 120 },{ name: 'Еда и Рестораны AM', type: 'Lifestyle', role: 'Member', messageCount: 42 },{ name: 'Хайкинг Армения', type: 'Lifestyle', role: 'Member', messageCount: 24 }],
      topTopics: [{ name: 'Nightlife', count: 42 },{ name: 'Bars', count: 38 },{ name: 'Day trips', count: 28 },{ name: 'Street food', count: 22 }],
      sentiment: { positive: 72, neutral: 22, negative: 6 },
      activityData: [{ week: 'W1', msgs: 8 },{ week: 'W2', msgs: 12 },{ week: 'W3', msgs: 10 },{ week: 'W4', msgs: 15 },{ week: 'W5', msgs: 18 },{ week: 'W6', msgs: 14 },{ week: 'W7', msgs: 20 }],
      recentMessages: [{ text: 'Best rooftop bars in Yerevan? Looking for somewhere with a view for tonight.', channel: 'Русские в Ереване', timestamp: '20 min ago', reactions: 23, replies: 12 }],
      persona: 'Digital Nomad', integrationLevel: 'Russian Only',
    },
    {
      id: 'u10', username: '@olga_mom', displayName: 'Ольга Мирская', gender: 'Female' as const, age: '32-36',
      origin: 'Kazan', location: 'Yerevan, Arabkir', joinedDate: '2024-05-30', lastActive: '2 hours ago',
      totalMessages: 1480, totalReactions: 3240, helpScore: 82,
      interests: ['Kids activities', 'Swimming', 'Playgroups', 'Family trips', 'Baking', 'Crafts'],
      channels: [{ name: 'Мамы Еревана', type: 'Family', role: 'Moderator', messageCount: 820 },{ name: 'Русские в Ереване', type: 'General', role: 'Active', messageCount: 380 },{ name: 'Аренда Ереван', type: 'Housing', role: 'Member', messageCount: 180 },{ name: 'Хайкинг Армения', type: 'Lifestyle', role: 'Member', messageCount: 100 }],
      topTopics: [{ name: 'Kids activities', count: 256 },{ name: 'Playgrounds', count: 142 },{ name: 'Swimming pools', count: 98 },{ name: 'Family housing', count: 76 }],
      sentiment: { positive: 65, neutral: 28, negative: 7 },
      activityData: [{ week: 'W1', msgs: 48 },{ week: 'W2', msgs: 52 },{ week: 'W3', msgs: 50 },{ week: 'W4', msgs: 55 },{ week: 'W5', msgs: 58 },{ week: 'W6', msgs: 54 },{ week: 'W7', msgs: 60 }],
      recentMessages: [{ text: 'Looking for 3-bedroom near school with Russian program. Budget 800-1000 USD. Arabkir or Davtashen preferred.', channel: 'Аренда Ереван', timestamp: '2 hours ago', reactions: 12, replies: 18 }],
      persona: 'Young Family', integrationLevel: 'Bilingual Bubble',
    },
    {
      id: 'u11', username: '@settled_igor', displayName: 'Игорь Федоров', gender: 'Male' as const, age: '42-48',
      origin: 'St. Petersburg', location: 'Dilijan', joinedDate: '2023-04-12', lastActive: '8 hours ago',
      totalMessages: 2100, totalReactions: 6200, helpScore: 91,
      interests: ['Armenian language', 'Wine', 'History', 'Real estate', 'Gardening', 'Classical music'],
      channels: [{ name: 'Русские в Ереване', type: 'General', role: 'Active', messageCount: 980 },{ name: 'Бизнес в Армении', type: 'Business', role: 'Active', messageCount: 520 },{ name: 'Армения Документы', type: 'Legal', role: 'Active', messageCount: 380 },{ name: 'Еда и Рестораны AM', type: 'Lifestyle', role: 'Member', messageCount: 220 }],
      topTopics: [{ name: 'Armenian language', count: 312 },{ name: 'Integration', count: 234 },{ name: 'Real estate buying', count: 178 },{ name: 'Wine/food culture', count: 145 }],
      sentiment: { positive: 72, neutral: 22, negative: 6 },
      activityData: [{ week: 'W1', msgs: 62 },{ week: 'W2', msgs: 58 },{ week: 'W3', msgs: 65 },{ week: 'W4', msgs: 60 },{ week: 'W5', msgs: 68 },{ week: 'W6', msgs: 70 },{ week: 'W7', msgs: 72 }],
      recentMessages: [{ text: 'After 3 years here, learn Armenian. It changes everything. People treat you differently, you understand the culture, you stop being a tourist.', channel: 'Русские в Ереване', timestamp: '8 hours ago', reactions: 245, replies: 56 }],
      persona: 'Established Expat', integrationLevel: 'Fully Integrated',
    },
    {
      id: 'u12', username: '@hr_anna', displayName: 'Анна Кузнецова', gender: 'Female' as const, age: '30-35',
      origin: 'Moscow', location: 'Yerevan, Arabkir', joinedDate: '2024-08-18', lastActive: '45 min ago',
      totalMessages: 534, totalReactions: 1890, helpScore: 76,
      interests: ['HR', 'Recruitment', 'Networking', 'Pilates', 'Coffee', 'Team building'],
      channels: [{ name: 'Бизнес в Армении', type: 'Business', role: 'Active', messageCount: 280 },{ name: 'IT Relocants Armenia', type: 'Work', role: 'Active', messageCount: 150 },{ name: 'Русские в Ереване', type: 'General', role: 'Member', messageCount: 104 }],
      topTopics: [{ name: 'Hiring', count: 112 },{ name: 'Salaries', count: 78 },{ name: 'Team management', count: 56 },{ name: 'Office culture', count: 42 }],
      sentiment: { positive: 58, neutral: 35, negative: 7 },
      activityData: [{ week: 'W1', msgs: 18 },{ week: 'W2', msgs: 20 },{ week: 'W3', msgs: 22 },{ week: 'W4', msgs: 19 },{ week: 'W5', msgs: 24 },{ week: 'W6', msgs: 26 },{ week: 'W7', msgs: 28 }],
      recentMessages: [{ text: 'Hiring Russian-speaking devs in Yerevan. Python/Django, $2-3K. Remote-first but office available. DM me.', channel: 'IT Relocants Armenia', timestamp: '45 min ago', reactions: 112, replies: 45 }],
      persona: 'Entrepreneur', integrationLevel: 'Bilingual Bubble',
    },
  ],
};
