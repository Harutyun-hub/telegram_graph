import React, { useEffect, useMemo, useState } from 'react';
import { useNavigate } from 'react-router';
import {
  Users, MessageCircle, Megaphone, Heart, Hash,
  Target, BarChart3, ChevronDown, ChevronUp,
  TrendingUp, TrendingDown,
  Eye, HelpCircle, ArrowUpRight, ArrowDownRight,
  ThumbsUp, ThumbsDown, MessageSquare, Lightbulb, ShieldAlert,
  Compass, Flame, Globe, Star, Layers, Loader2
} from 'lucide-react';
import { useLanguage } from '../contexts/LanguageContext';
import { useSocialDateRange } from '../contexts/SocialDateRangeContext';
import { apiFetch } from '../services/api';
import { translateTopicRu } from '../services/topicPresentation';
import {
  ResponsiveContainer, LineChart, Line,
  XAxis, YAxis, Tooltip, CartesianGrid,
  RadarChart, Radar, PolarGrid, PolarAngleAxis, PolarRadiusAxis,
  PieChart, Pie, Cell
} from 'recharts';

// ═══════════════════════════════════════════════════════════════
// DESIGN SYSTEM (matches DashboardPage)
// ═══════════════════════════════════════════════════════════════

const C = {
  blue:    '#3b82f6',
  violet:  '#8b5cf6',
  pink:    '#ec4899',
  emerald: '#10b981',
  amber:   '#f59e0b',
  rose:    '#ef4444',
  cyan:    '#06b6d4',
  indigo:  '#6366f1',
  grid:    '#f1f5f9',
  border:  '#e2e8f0',
  muted:   '#94a3b8',
};

const BRAND_COLORS: Record<string, string> = {
  'Brand X':      C.blue,
  'Brand Y':      C.violet,
  'Brand Z':      C.pink,
  'Competitor A': C.emerald,
};

const TOOLTIP_STYLE = {
  contentStyle: {
    borderRadius: '10px',
    border: `1px solid ${C.border}`,
    boxShadow: '0 10px 15px -3px rgb(0 0 0 / 0.06)',
    fontSize: '12px',
    padding: '10px 14px',
  },
};

const AXIS_COMMON = {
  axisLine: false as const,
  tickLine: false as const,
  tick: { fontSize: 11, fill: C.muted },
};

const GRID_COMMON = { strokeDasharray: '3 3', stroke: C.grid, vertical: false as const };

const SOCIAL_LABEL_RU: Record<string, string> = {
  'All Sources': 'Все источники',
  Positive: 'Позитив',
  Neutral: 'Нейтрал',
  Negative: 'Негатив',
  Mixed: 'Смешанная',
  Urgent: 'Срочная',
  Sarcastic: 'Сарказм',
  'Total Mentions': 'Всего упоминаний',
  'Positive Sentiment': 'Позитивная тональность',
  'Questions Asked': 'Заданные вопросы',
  'Ads Running': 'Активная реклама',
  Support: 'Поддержка',
  Concern: 'Опасения',
  Questions: 'Вопросы',
  Complaints: 'Жалобы',
  'Trust / Distrust': 'Доверие / недоверие',
  Praise: 'Похвала',
  'Purchase Intent': 'Намерение покупки',
  Comparison: 'Сравнение',
  'Feature Request': 'Запрос функции',
  'Churn Signal': 'Риск ухода',
  Acquisition: 'Привлечение',
  Awareness: 'Осведомленность',
  Retention: 'Удержание',
  'Lead Gen': 'Лидогенерация',
  'Political Support': 'Политическая поддержка',
  'National Pride': 'Национальная гордость',
  'Campaign Messaging': 'Кампания и сообщения',
  'Support For Pashinyan': 'Поддержка Пашиняна',
  'Announcement Teaser': 'Анонс',
  'Army Readiness': 'Готовность армии',
  'Artificial Intelligence Development': 'Развитие ИИ',
  'Artsakh Position': 'Позиция по Арцаху',
  'Audience Appreciation': 'Благодарность аудитории',
  'Border Troops': 'Пограничные войска',
  'Border Troops Ceremony': 'Церемония пограничных войск',
  'Charitable Foundation': 'Благотворительный фонд',
  'Church Evidence': 'Церковные свидетельства',
  'Church Evidence Debate': 'Дискуссия о церковных свидетельствах',
  'Citizen S Day': 'День гражданина',
  'City Leadership Criticism': 'Критика городского руководства',
  'Community Education Event': 'Образовательное событие сообщества',
  'Concert Programming': 'Концертная программа',
  'Corruption Allegation': 'Обвинения в коррупции',
  'Crowd Size Debate': 'Спор о численности участников',
  'Customer Service': 'Обслуживание клиентов',
  'Product Quality': 'Качество продукта',
  Pricing: 'Цены',
  'Delivery Speed': 'Скорость доставки',
  'App Interface': 'Интерфейс приложения',
  Sustainability: 'Устойчивость',
  Refunds: 'Возвраты',
  'Community Interests': 'Интересы сообщества',
  'Discussion Focus Areas': 'Фокусы обсуждения',
  'Government & Leadership': 'Государство и лидерство',
  'Security & Border Issues': 'Безопасность и границы',
  'Infrastructure Projects': 'Инфраструктурные проекты',
  'Economy & Jobs': 'Экономика и занятость',
  'Campaign & Public Messaging': 'Кампании и публичные сообщения',
  'Evidence, Trust & Accountability': 'Доказательства, доверие и ответственность',
  'Culture & Public Events': 'Культура и публичные события',
  'Church & Identity Debate': 'Церковь и идентичность',
  'Education & Community Programs': 'Образование и программы сообщества',
  'Charity & Social Support': 'Благотворительность и социальная поддержка',
  'Technology & Modernization': 'Технологии и модернизация',
  'Public Services': 'Государственные услуги',
  'Other Civic Discussion': 'Прочие общественные обсуждения',
};

// ═══════════════════════════════════════════════════════════════
// TIER CONFIG  — Tailwind palette matching DashboardPage
// ═══════════════════════════════════════════════════════════════

interface TierDef {
  id: string;
  icon: React.ElementType;
  color: string;
  bgColor: string;
  borderColor: string;
  title: string;
  subtitle: string;
}

// ═══════════════════════════════════════════════════════════════
// MOCK DATA
// ═══════════════════════════════════════════════════════════════

interface Organization { id: string; name: string; color: string; }

const ALL_ORG: Organization = { id: 'all', name: 'All Sources', color: C.blue };

const TOPIC_BUBBLES = [
  { topic: 'Customer Service', count: 145, sentiment: 'negative' as const, x: 115, y: 122, r: 52 },
  { topic: 'Product Quality',  count: 120, sentiment: 'positive' as const, x: 258, y: 72,  r: 47 },
  { topic: 'Pricing',          count: 95,  sentiment: 'neutral'  as const, x: 200, y: 195, r: 42 },
  { topic: 'Delivery Speed',   count: 80,  sentiment: 'positive' as const, x: 352, y: 145, r: 37 },
  { topic: 'App Interface',    count: 65,  sentiment: 'negative' as const, x: 40,  y: 52,  r: 32 },
  { topic: 'Sustainability',   count: 45,  sentiment: 'positive' as const, x: 370, y: 222, r: 27 },
  { topic: 'Refunds',          count: 30,  sentiment: 'negative' as const, x: 88,  y: 218, r: 22 },
];

// Topic Momentum (velocity over 5 weeks)
const TOPIC_MOMENTUM = [
  { topic: 'Sustainability',   w1: 22,  w2: 27,  w3: 32,  w4: 38,  w5: 45,  velocity: +18.4, sentiment: 'positive' as const },
  { topic: 'Product Quality',  w1: 78,  w2: 88,  w3: 99,  w4: 110, w5: 120, velocity: +9.1,  sentiment: 'positive' as const },
  { topic: 'Pricing',          w1: 72,  w2: 78,  w3: 83,  w4: 88,  w5: 95,  velocity: +7.9,  sentiment: 'neutral'  as const },
  { topic: 'Delivery Speed',   w1: 48,  w2: 56,  w3: 64,  w4: 72,  w5: 80,  velocity: +11.1, sentiment: 'positive' as const },
  { topic: 'Customer Service', w1: 100, w2: 112, w3: 125, w4: 135, w5: 145, velocity: +8.5,  sentiment: 'negative' as const },
  { topic: 'App Interface',    w1: 78,  w2: 74,  w3: 70,  w4: 67,  w5: 65,  velocity: -4.4,  sentiment: 'negative' as const },
  { topic: 'Refunds',          w1: 38,  w2: 36,  w3: 33,  w4: 31,  w5: 30,  velocity: -3.2,  sentiment: 'negative' as const },
];

const SENTIMENT_TREND = [
  { week: 'W1', positive: 40, neutral: 24, negative: 20 },
  { week: 'W2', positive: 45, neutral: 28, negative: 18 },
  { week: 'W3', positive: 35, neutral: 30, negative: 25 },
  { week: 'W4', positive: 50, neutral: 20, negative: 15 },
  { week: 'W5', positive: 53, neutral: 22, negative: 17 },
];

const INTENT_SIGNALS = [
  { intent: 'Questions',       icon: HelpCircle,   count: 342, pct: 27.4, delta: +3.2,  color: C.blue,    examples: ['How do I cancel subscription?', 'What plan includes API access?', 'Is there a family discount?'] },
  { intent: 'Complaints',      icon: ThumbsDown,   count: 218, pct: 17.5, delta: -1.8,  color: C.rose,    examples: ['Support takes forever', 'App keeps crashing', 'Overcharged on my bill'] },
  { intent: 'Praise',          icon: ThumbsUp,     count: 195, pct: 15.6, delta: +5.1,  color: C.emerald, examples: ['Love the new update!', 'Best customer support ever', 'Finally works perfectly'] },
  { intent: 'Purchase Intent', icon: Target,       count: 156, pct: 12.5, delta: +8.3,  color: C.violet,  examples: ['Thinking of switching to Pro', 'Where to buy?', 'Any Black Friday deals?'] },
  { intent: 'Comparison',      icon: Layers,       count: 124, pct: 9.9,  delta: +2.1,  color: C.amber,   examples: ['Brand X vs Brand Y', 'Which is better for teams?', 'Pricing comparison needed'] },
  { intent: 'Feature Request', icon: Lightbulb,    count: 112, pct: 9.0,  delta: +0.5,  color: C.cyan,    examples: ['Need dark mode', 'Add calendar integration', 'Offline mode please'] },
  { intent: 'Churn Signal',    icon: ShieldAlert,  count: 101, pct: 8.1,  delta: +4.7,  color: C.rose,    examples: ['Looking for alternatives', 'Canceling next month', 'Not worth the price'] },
];

const SIGNAL_TREND = [
  { week: 'W1', questions: 85, complaints: 54, praise: 48, purchase: 32, churn: 21 },
  { week: 'W2', questions: 92, complaints: 48, praise: 55, purchase: 38, churn: 18 },
  { week: 'W3', questions: 78, complaints: 62, praise: 41, purchase: 29, churn: 28 },
  { week: 'W4', questions: 87, complaints: 54, praise: 51, purchase: 57, churn: 34 },
  { week: 'W5', questions: 96, complaints: 45, praise: 60, purchase: 64, churn: 30 },
];

const TOP_QUESTIONS = [
  { question: 'How do I cancel my subscription?',       count: 67, trend: 'up'     as const, entity: 'Brand X',      category: 'Billing',     answered: false },
  { question: 'Is there a free trial available?',       count: 54, trend: 'up'     as const, entity: 'Brand Y',      category: 'Pricing',     answered: true  },
  { question: 'Why is the app so slow lately?',         count: 48, trend: 'up'     as const, entity: 'Brand Z',      category: 'Performance', answered: false },
  { question: 'Can I use it on multiple devices?',      count: 41, trend: 'stable' as const, entity: 'Brand X',      category: 'Features',    answered: true  },
  { question: 'When is the next update coming?',        count: 38, trend: 'down'   as const, entity: 'Brand Y',      category: 'Roadmap',     answered: false },
  { question: 'Do you have an API for developers?',     count: 35, trend: 'up'     as const, entity: 'Competitor A', category: 'Features',    answered: true  },
  { question: 'How does pricing compare to alternatives?', count: 32, trend: 'up'  as const, entity: 'Brand X',      category: 'Pricing',     answered: false },
  { question: 'Is my data safe with your service?',    count: 28, trend: 'stable' as const, entity: 'Brand Z',      category: 'Security',    answered: true  },
];

const AD_FEED = [
  { id: '1', entity: 'Brand X',      platform: 'Google Ads', copy: 'Experience the next generation of our product. 20% off for new users.', cta: 'Shop Now',   format: 'Search', intent: 'Acquisition', products: ['Pro Suite'],    valueProps: ['Discount', 'Innovation'], urgency: true,  date: '2 days ago',  engagement: 1200 },
  { id: '2', entity: 'Brand Z',      platform: 'Meta Ads',   copy: 'Why settle for less? Upgrade your workflow today with our new AI tools.', cta: 'Learn More',format: 'Video',  intent: 'Awareness',   products: ['AI Tools'],     valueProps: ['Efficiency'],             urgency: false, date: '5 days ago',  engagement: 3400 },
  { id: '3', entity: 'Competitor A', platform: 'LinkedIn',   copy: 'Join our upcoming webinar on the future of remote work. Limited seats.', cta: 'Register',  format: 'Image',  intent: 'Lead Gen',    products: ['Consulting'],   valueProps: ['Expert Insights'],        urgency: true,  date: '1 week ago',  engagement: 850  },
];

type AdSource = 'all' | 'meta' | 'google' | 'facebook' | 'instagram';

interface ScrapedAd {
  id: string;
  entity: string;
  source: AdSource;
  platform: string;
  copy: string;
  cta: string;
  format: string;
  intent: string;
  valueProps: string[];
  urgency: boolean;
  date: string;
  impressions: number;
  engagement: number;
  clicks: number;
}

interface OrganicPostItem {
  id: string;
  activity_uid?: string;
  entity_id?: string;
  entity: string;
  platform?: string;
  source_kind?: string;
  text?: string;
  media?: { kind?: string; url?: string } | null;
  source_url?: string;
  published_at?: string;
  reach?: number | null;
  likes?: number;
  comments?: number;
  shares?: number;
}

const AD_SCRAPE: ScrapedAd[] = [
  { id: 's1',  entity: 'Brand X',      source: 'google',    platform: 'Google Search',  copy: 'Experience the next generation of our product. 20% off for new users. Get started today with Pro Suite.',    cta: 'Shop Now',      format: 'Search',   intent: 'Acquisition', valueProps: ['20% Discount', 'Innovation'],   urgency: true,  date: 'Apr 22, 2026', impressions: 45200, engagement: 1820, clicks: 1200 },
  { id: 's2',  entity: 'Brand X',      source: 'google',    platform: 'Google Display', copy: 'Pro Suite 3.0 is here. The smartest platform for modern teams. Try it free for 14 days.',                   cta: 'Start Free Trial',format: 'Display',  intent: 'Acquisition', valueProps: ['Free Trial', 'Modern UX'],      urgency: false, date: 'Apr 20, 2026', impressions: 88000, engagement: 3100, clicks: 2400 },
  { id: 's3',  entity: 'Brand Z',      source: 'google',    platform: 'Google Shopping',copy: 'Free next-day shipping on all orders over $50. Real-time tracking included.',                                cta: 'Order Now',     format: 'Shopping', intent: 'Acquisition', valueProps: ['Free Shipping', 'Tracking'],    urgency: false, date: 'Apr 21, 2026', impressions: 56000, engagement: 2100, clicks: 3400 },
  { id: 's4',  entity: 'Competitor A', source: 'google',    platform: 'Google Search',  copy: 'Professional consulting for remote teams. Book a free 30-min strategy session today.',                       cta: 'Book Free Call',format: 'Search',   intent: 'Lead Gen',    valueProps: ['Free Session', 'Expert'],       urgency: false, date: 'Apr 10, 2026', impressions: 14200, engagement: 580,  clicks: 620  },
  { id: 's5',  entity: 'Brand Z',      source: 'meta',      platform: 'Meta Feed',      copy: 'Why settle for less? Upgrade your workflow with AI tools trusted by 10,000+ teams.',                         cta: 'Learn More',    format: 'Video',    intent: 'Awareness',   valueProps: ['AI Tools', 'Social Proof'],     urgency: false, date: 'Apr 19, 2026', impressions: 82100, engagement: 3940, clicks: 3400 },
  { id: 's6',  entity: 'Brand X',      source: 'meta',      platform: 'Meta Feed',      copy: 'Our annual sale starts early! Up to 40% off all Pro Suite plans. Don\'t miss the best deal of the year.',   cta: 'Claim Offer',   format: 'Image',    intent: 'Retention',   valueProps: ['40% Off', 'Urgency'],           urgency: true,  date: 'Apr 17, 2026', impressions: 88000, engagement: 5100, clicks: 4300 },
  { id: 's7',  entity: 'Competitor A', source: 'meta',      platform: 'Meta Feed',      copy: 'Future of work is here. Join 500+ companies using our platform to manage remote teams seamlessly.',          cta: 'See How',       format: 'Carousel', intent: 'Awareness',   valueProps: ['Case Studies', 'Enterprise'],   urgency: false, date: 'Apr 15, 2026', impressions: 31000, engagement: 1420, clicks: 890  },
  { id: 's8',  entity: 'Brand Y',      source: 'facebook',  platform: 'Facebook Feed',  copy: 'See why 50,000+ teams switched to Brand Y. Real results from real companies — no fluff.',                  cta: 'See Stories',   format: 'Video',    intent: 'Awareness',   valueProps: ['Social Proof', 'Trust'],        urgency: false, date: 'Apr 19, 2026', impressions: 42000, engagement: 1620, clicks: 1850 },
  { id: 's9',  entity: 'Brand X',      source: 'facebook',  platform: 'Facebook Feed',  copy: 'Introducing Pro Suite 3.0 — smarter, faster, better. See what\'s new in our biggest update ever.',         cta: 'Explore',       format: 'Carousel', intent: 'Awareness',   valueProps: ['Innovation', 'Product Update'], urgency: false, date: 'Apr 21, 2026', impressions: 63400, engagement: 2150, clicks: 2700 },
  { id: 's10', entity: 'Brand Z',      source: 'facebook',  platform: 'Facebook Story', copy: 'Weekend flash sale: 25% off sitewide. Use code FLASH25 at checkout. Ends Sunday!',                          cta: 'Shop Sale',     format: 'Story',    intent: 'Acquisition', valueProps: ['Flash Sale', 'Promo Code'],      urgency: true,  date: 'Apr 20, 2026', impressions: 38200, engagement: 2800, clicks: 3100 },
  { id: 's11', entity: 'Brand Y',      source: 'instagram', platform: 'Instagram Feed', copy: 'Your productivity, reimagined. Try Brand Y free for 30 days — no credit card needed. Cancel anytime.',      cta: 'Try Free',      format: 'Image',    intent: 'Acquisition', valueProps: ['Free Trial', 'No CC Required'], urgency: true,  date: 'Apr 20, 2026', impressions: 94200, engagement: 4800, clicks: 5100 },
  { id: 's12', entity: 'Brand Z',      source: 'instagram', platform: 'Instagram Reel', copy: 'Same-day delivery, guaranteed. Shop now and get 15% off your first order with code FAST15.',                cta: 'Order Now',     format: 'Reel',     intent: 'Acquisition', valueProps: ['Speed', '15% Off'],             urgency: true,  date: 'Apr 18, 2026', impressions: 145000,engagement: 8300, clicks: 7200 },
  { id: 's13', entity: 'Brand X',      source: 'instagram', platform: 'Instagram Story',copy: 'Power your team with Pro Suite. Used by top companies in 40+ countries. Start your free trial.',            cta: 'Start Now',     format: 'Story',    intent: 'Acquisition', valueProps: ['Global Reach', 'Free Trial'],    urgency: false, date: 'Apr 16, 2026', impressions: 71000, engagement: 3600, clicks: 2900 },
  { id: 's14', entity: 'Competitor A', source: 'instagram', platform: 'Instagram Feed', copy: 'Consulting that moves at the speed of your business. Book a discovery call with our senior advisors.',       cta: 'Book Now',      format: 'Image',    intent: 'Lead Gen',    valueProps: ['Senior Experts', 'Speed'],      urgency: false, date: 'Apr 14, 2026', impressions: 22400, engagement: 980,  clicks: 760  },
];

const SENTIMENT_BY_ENTITY = [
  { entity: 'Brand X',      pos: 45, neu: 30, neg: 25, total: 1250 },
  { entity: 'Brand Y',      pos: 60, neu: 25, neg: 15, total: 840  },
  { entity: 'Brand Z',      pos: 30, neu: 40, neg: 30, total: 920  },
  { entity: 'Competitor A', pos: 50, neu: 40, neg: 10, total: 610  },
];

const PAIN_POINTS = [
  { text: 'Long response times on weekends',  count: 85, entities: ['Brand X', 'Brand Z'],             severity: 'high'   },
  { text: 'App crashing during checkout',     count: 62, entities: ['Brand Y'],                        severity: 'high'   },
  { text: 'Confusing pricing tiers',          count: 45, entities: ['Brand X', 'Brand Y', 'Brand Z'], severity: 'medium' },
  { text: 'Unhelpful automated chatbot',      count: 38, entities: ['Brand X', 'Competitor A'],        severity: 'medium' },
];

const ENGAGEMENT_RADAR = [
  { subject: 'Likes',    brandX: 78, brandY: 65, brandZ: 45, fullMark: 100 },
  { subject: 'Comments', brandX: 62, brandY: 72, brandZ: 55, fullMark: 100 },
  { subject: 'Shares',   brandX: 45, brandY: 38, brandZ: 68, fullMark: 100 },
  { subject: 'Saves',    brandX: 55, brandY: 48, brandZ: 32, fullMark: 100 },
  { subject: 'Clicks',   brandX: 82, brandY: 55, brandZ: 60, fullMark: 100 },
  { subject: 'Replies',  brandX: 35, brandY: 68, brandZ: 42, fullMark: 100 },
];



const VISIBILITY_DATA = [
  { entity: 'Brand X',      visibility: 73.04, delta: +8.17, reach: 45200, deltaReach: +12.3, engagement: 3.2, deltaEngage: -0.4, sov: 35.5, deltaSov: +2.1 },
  { entity: 'Brand Y',      visibility: 58.21, delta: +2.34, reach: 28400, deltaReach: +5.8,  engagement: 4.1, deltaEngage: +0.8, sov: 22.1, deltaSov: -0.5 },
  { entity: 'Brand Z',      visibility: 45.67, delta: -3.12, reach: 52100, deltaReach: -2.1,  engagement: 2.8, deltaEngage: -1.2, sov: 28.3, deltaSov: -1.8 },
  { entity: 'Competitor A', visibility: 31.89, delta: +1.05, reach: 15600, deltaReach: +8.9,  engagement: 5.2, deltaEngage: +1.5, sov: 14.1, deltaSov: +0.2 },
];

const VISIBILITY_TREND = [
  { day: 'Mar 1',  brandX: 65, brandY: 52, brandZ: 48, compA: 28 },
  { day: 'Mar 8',  brandX: 67, brandY: 54, brandZ: 47, compA: 29 },
  { day: 'Mar 15', brandX: 69, brandY: 55, brandZ: 46, compA: 30 },
  { day: 'Mar 22', brandX: 71, brandY: 57, brandZ: 44, compA: 31 },
  { day: 'Mar 29', brandX: 73, brandY: 58, brandZ: 46, compA: 32 },
];

const POSITIVE_IMPACT = [
  { topic: 'Product Quality',  gain: '+11.30%', mentions: 120 },
  { topic: 'Delivery Speed',   gain: '+10.41%', mentions: 80  },
  { topic: 'Sustainability',   gain: '+9.79%',  mentions: 45  },
  { topic: 'New Features',     gain: '+8.21%',  mentions: 38  },
  { topic: 'Customer Stories', gain: '+3.61%',  mentions: 22  },
];

const NEGATIVE_IMPACT = [
  { topic: 'Customer Service', loss: '-18.48%', mentions: 145 },
  { topic: 'App Interface',    loss: '-2.01%',  mentions: 65  },
  { topic: 'Refunds',          loss: '-1.91%',  mentions: 30  },
  { topic: 'Pricing Confusion',loss: '-0.85%',  mentions: 28  },
  { topic: 'Data Privacy',     loss: '-0.21%',  mentions: 12  },
];

const WEEKLY_SHIFTS = [
  { metric: 'Total Mentions',     current: 1245, previous: 1102, unit: '',  goodIfUp: true  },
  { metric: 'Positive Sentiment', current: 65,   previous: 58,   unit: '%', goodIfUp: true  },
  { metric: 'Questions Asked',    current: 342,  previous: 298,  unit: '',  goodIfUp: false },
  { metric: 'Complaints',         current: 218,  previous: 245,  unit: '',  goodIfUp: false },
  { metric: 'Purchase Intent',    current: 156,  previous: 112,  unit: '',  goodIfUp: true  },
  { metric: 'Share of Voice',     current: 35.5, previous: 33.4, unit: '%', goodIfUp: true  },
];



const SCORECARD = [
  { id: 'brand-x',      name: 'Brand X',      posts: 145, ads: 12, sentiment: 65, intent: 'Acquisition', topics: ['Service', 'Quality'],       products: ['Pro Suite']  },
  { id: 'brand-y',      name: 'Brand Y',      posts: 89,  ads: 5,  sentiment: 78, intent: 'Awareness',   topics: ['App Interface', 'Pricing'], products: ['App']        },
  { id: 'brand-z',      name: 'Brand Z',      posts: 210, ads: 34, sentiment: 45, intent: 'Retention',   topics: ['Delivery', 'Refunds'],      products: ['Logistics']  },
  { id: 'competitor-a', name: 'Competitor A', posts: 64,  ads: 8,  sentiment: 72, intent: 'Lead Gen',    topics: ['Consulting'],               products: ['Services']   },
];

type TopicBubbleItem = typeof TOPIC_BUBBLES[number];
type TopicRankingItem = TopicBubbleItem & {
  dominantSentiment?: string;
  growthPct?: number | null;
  growthReliable?: boolean;
  sampleSummary?: string;
  evidence?: Array<{
    activity_uid?: string;
    entity?: string;
    platform?: string;
    published_at?: string;
    summary?: string;
    source_url?: string;
  }>;
  topEntities?: string[];
  topPlatforms?: string[];
  strictMetrics?: {
    engagementTotal?: number;
    likes?: number;
    comments?: number;
    shares?: number;
    views?: number;
    reactions?: number;
    evidenceCount?: number;
  };
};
type TopicMomentumItem = typeof TOPIC_MOMENTUM[number];
type SentimentTrendItem = typeof SENTIMENT_TREND[number] & {
  bucket?: string;
  total?: number;
  mixed?: number;
  urgent?: number;
  sarcastic?: number;
};
interface SignalTrendItem {
  bucket?: string;
  generatedAt?: string;
  total?: number;
  [key: string]: string | number | undefined;
}
interface IntentSignalItem {
  intent: string;
  family?: string;
  title_en?: string;
  title_ru?: string;
  summary_en?: string;
  summary_ru?: string;
  main_topic?: string;
  sentiment?: string;
  signal_count?: number;
  count: number;
  pct?: number;
  trend_pct?: number;
  delta?: number;
  confidence?: number;
  evidence_ids?: string[];
  evidence_quotes?: string[];
  examples?: string[];
  icon?: React.ElementType;
  color?: string;
}
interface TopQuestionItem {
  question: string;
  question_en?: string;
  question_ru?: string;
  topic?: string;
  count: number;
  confidence?: number;
  sources?: string[];
  evidence_quotes?: string[];
  evidence_count?: number;
  evidence_ids?: string[];
}
type AdFeedItem = typeof AD_FEED[number];
type SentimentByEntityItem = typeof SENTIMENT_BY_ENTITY[number] & {
  entity_id?: string;
};
interface PainPointItem {
  problem?: string;
  problem_en?: string;
  problem_ru?: string;
  title_en?: string;
  title_ru?: string;
  text?: string;
  summary_en?: string;
  summary_ru?: string;
  topic?: string;
  count: number;
  confidence?: number;
  sources?: string[];
  entities?: string[];
  evidence_quotes?: string[];
  evidence_count?: number;
  evidence_ids?: string[];
  severity?: string;
}
interface CommunityInterestItem {
  interest: string;
  score: number;
  mentions?: number;
  topTopics?: string[];
  description?: string;
  interestLabel?: string;
}
type EngagementRadarItem = typeof ENGAGEMENT_RADAR[number];
type VisibilityItem = typeof VISIBILITY_DATA[number] & {
  entity_id?: string;
  reachShare?: number;
  interactions?: number;
  engagementRate?: number | null;
  posts?: number;
  comments?: number;
  hasReach?: boolean;
  delta?: number | null;
  deltaReach?: number | null;
  deltaEngage?: number | null;
  deltaSov?: number | null;
};
type VisibilityTrendItem = typeof VISIBILITY_TREND[number];
type WeeklyShiftItem = typeof WEEKLY_SHIFTS[number];
type PositiveImpactItem = typeof POSITIVE_IMPACT[number];
type NegativeImpactItem = typeof NEGATIVE_IMPACT[number];
type ScorecardItem = typeof SCORECARD[number];

function topicEvidencePreview(topic: TopicRankingItem, ru: boolean): string {
  const evidenceSummary = Array.isArray(topic.evidence)
    ? topic.evidence.find((item) => String(item?.summary || '').trim())?.summary
    : '';
  const preview = String(topic.sampleSummary || evidenceSummary || '').trim();
  if (preview) return preview;
  return ru
    ? 'Откройте тему, чтобы увидеть посты, комментарии и доказательства.'
    : 'Open the topic to view posts, comments, and evidence.';
}

interface SocialDashboardSnapshot {
  meta?: {
    requestId?: string;
    cacheStatus?: string;
    generatedAt?: string;
    degradedSections?: string[];
    semanticStale?: boolean;
    semanticPreservedFromCache?: boolean;
    semanticPreservedSections?: string[];
    emptyReasons?: Record<string, string>;
    timingsMs?: Record<string, number>;
  };
  filters?: {
    entities?: { id: string; name: string }[];
    platforms?: string[];
    sourceKinds?: string[];
  };
  deepAnalysis?: {
    topicBubbles?: TopicBubbleItem[];
    topicRanking?: TopicRankingItem[];
    topicMomentum?: TopicMomentumItem[];
    sentimentTrend?: SentimentTrendItem[];
    intentSignals?: IntentSignalItem[];
    topSignals?: IntentSignalItem[];
    signalTrend?: SignalTrendItem[];
    topQuestions?: TopQuestionItem[];
    painPoints?: PainPointItem[];
    communityInterests?: CommunityInterestItem[];
    evidence?: any[];
  };
  adIntelligence?: {
    items?: (ScrapedAd & Partial<AdFeedItem>)[];
    summary?: Record<string, unknown>;
  };
  strictMetrics?: {
    summary?: {
      trackedSources?: number;
      posts?: number;
      comments?: number;
      ads?: number;
      avgPositive?: number;
      organicMentions?: number;
      rowCapReached?: boolean;
    };
    sentimentByEntity?: SentimentByEntityItem[];
    engagementRadar?: EngagementRadarItem[];
    visibilityData?: VisibilityItem[];
    visibilityTrend?: VisibilityTrendItem[];
    positiveImpact?: PositiveImpactItem[];
    negativeImpact?: NegativeImpactItem[];
    weeklyShifts?: WeeklyShiftItem[];
    scorecard?: ScorecardItem[];
    shareOfVoice?: { name: string; value: number; color?: string }[];
    organicPosts?: {
      items?: OrganicPostItem[];
      summary?: {
        total?: number;
        returned?: number;
        withMedia?: number;
        withReach?: number;
      };
    };
  };
}

function colorForEntity(entity: string, index = 0) {
  if (BRAND_COLORS[entity]) return BRAND_COLORS[entity];
  const palette = [C.blue, C.violet, C.pink, C.emerald, C.amber, C.cyan, C.indigo];
  return palette[index % palette.length];
}

function seriesKey(label: string) {
  return label.toLowerCase().replace(/[^\p{L}\p{N}]+/gu, '_').replace(/^_+|_+$/g, '') || 'entity';
}

function translateSocialLabel(value: unknown, ru: boolean): string {
  const label = typeof value === 'string' ? value.replace(/\s+/g, ' ').trim() : String(value ?? '').trim();
  if (!label) return '';
  if (!ru) return label;
  return SOCIAL_LABEL_RU[label] || translateTopicRu(label) || label;
}

function sentimentKey(value: unknown): 'positive' | 'neutral' | 'negative' {
  const label = String(value || '').trim().toLowerCase();
  if (label === 'positive') return 'positive';
  if (label === 'negative') return 'negative';
  return 'neutral';
}

function sentimentLabel(value: unknown, ru: boolean): string {
  const key = sentimentKey(value);
  if (!ru) return key === 'positive' ? 'Positive' : key === 'negative' ? 'Negative' : 'Neutral';
  return key === 'positive' ? 'Позитив' : key === 'negative' ? 'Негатив' : 'Нейтрал';
}

function formatTrendDay(value: unknown, ru: boolean): string {
  const text = String(value || '').trim();
  if (!/^\d{4}-\d{2}-\d{2}$/.test(text)) return text;
  const parsed = new Date(`${text}T00:00:00Z`);
  if (Number.isNaN(parsed.getTime())) return text;
  return parsed.toLocaleDateString(ru ? 'ru-RU' : 'en-US', { month: 'short', day: 'numeric', timeZone: 'UTC' });
}

function wrapBubbleLabel(label: string, radius: number): string[] {
  const maxLines = radius < 32 ? 2 : 3;
  const maxChars = Math.max(6, Math.floor(radius / 3.2));
  const words = label.split(/\s+/).filter(Boolean);
  const lines: string[] = [];
  for (const word of words) {
    const last = lines[lines.length - 1] || '';
    if (!last) {
      lines.push(word);
    } else if (`${last} ${word}`.length <= maxChars) {
      lines[lines.length - 1] = `${last} ${word}`;
    } else if (lines.length < maxLines) {
      lines.push(word);
    } else {
      lines[lines.length - 1] = `${last}…`;
      break;
    }
  }
  return lines.slice(0, maxLines);
}

function wrapRadarAxisLabel(label: string): string[] {
  const words = label.split(/\s+/).filter(Boolean);
  const lines: string[] = [];
  for (const word of words) {
    const last = lines[lines.length - 1] || '';
    if (!last) {
      lines.push(word);
    } else if (`${last} ${word}`.length <= 18) {
      lines[lines.length - 1] = `${last} ${word}`;
    } else {
      lines.push(word);
    }
  }
  return lines.slice(0, 3);
}

function CommunityInterestAxisTick({
  x,
  y,
  textAnchor,
  payload,
}: {
  x?: number;
  y?: number;
  textAnchor?: 'start' | 'middle' | 'end';
  payload?: { value?: string };
}) {
  const lines = wrapRadarAxisLabel(String(payload?.value || ''));
  return (
    <text x={x} y={y} textAnchor={textAnchor || 'middle'} fill="#64748b" fontSize={11} fontWeight={600}>
      {lines.map((line, index) => (
        <tspan key={`${line}-${index}`} x={x} dy={index === 0 ? 0 : 13}>
          {line}
        </tspan>
      ))}
    </text>
  );
}

function iconForIntent(intent: string): React.ElementType {
  const normalized = intent.toLowerCase();
  if (normalized.includes('complaint')) return ThumbsDown;
  if (normalized.includes('praise')) return ThumbsUp;
  if (normalized.includes('purchase')) return Target;
  if (normalized.includes('comparison')) return Layers;
  if (normalized.includes('feature')) return Lightbulb;
  if (normalized.includes('churn')) return ShieldAlert;
  return HelpCircle;
}

function colorForIntent(intent: string, fallback?: string): string {
  if (fallback) return fallback;
  const normalized = intent.toLowerCase();
  if (normalized.includes('complaint') || normalized.includes('churn')) return C.rose;
  if (normalized.includes('concern') || normalized.includes('distrust')) return C.amber;
  if (normalized.includes('praise')) return C.emerald;
  if (normalized.includes('support') || normalized.includes('trust')) return C.emerald;
  if (normalized.includes('purchase')) return C.violet;
  if (normalized.includes('comparison')) return C.amber;
  if (normalized.includes('feature')) return C.cyan;
  return C.blue;
}

function signalTrendLabel(key: string, ru: boolean): string {
  const normalized = key.replace(/_/g, ' ');
  const title = normalized
    .split(' ')
    .filter(Boolean)
    .map((part) => part.charAt(0).toUpperCase() + part.slice(1))
    .join(' ');
  if (key === 'trust_distrust') return ru ? 'Доверие / недоверие' : 'Trust / Distrust';
  return translateSocialLabel(title, ru);
}

function signalTrendKey(value: unknown): string {
  const family = String(value || '').trim().toLowerCase();
  if (family.includes('trust') || family.includes('distrust')) return 'trust_distrust';
  if (family.includes('support')) return 'support';
  if (family.includes('concern')) return 'concern';
  if (family.includes('question')) return 'questions';
  if (family.includes('complaint')) return 'complaints';
  return family.replace(/[^\p{L}\p{N}]+/gu, '_').replace(/^_+|_+$/g, '') || 'other';
}

function signalFamilyForTrend(signal: IntentSignalItem): string {
  const explicit = signal.family || signal.intent;
  if (explicit) return signalTrendKey(explicit);
  const text = `${signal.title_en || ''} ${signal.summary_en || ''}`.toLowerCase();
  if (/\b(ask|question|wonder|how|why|whether)\b/.test(text)) return 'questions';
  if (/\b(complain|complaint|critic|accountability|problem|issue|road|service)\b/.test(text)) return 'complaints';
  if (/\b(support|supportive|positive|praise|appreciat)\b/.test(text)) return 'support';
  if (/\b(trust|distrust|confidence)\b/.test(text)) return 'trust_distrust';
  return 'concern';
}

function readableEvidence(value: unknown): string {
  const text = String(value ?? '').replace(/\s+/g, ' ').trim();
  if (!text) return '';
  if (text === '[object Object]') return '';
  if (/^[a-z]+:[a-z]+:[\w.-]+$/i.test(text)) return '';
  if (/^[0-9a-f]{8}-[0-9a-f-]{27,}$/i.test(text)) return '';
  if ((text.startsWith('{') && text.endsWith('}')) || (text.startsWith('[') && text.endsWith(']'))) return '';
  return text;
}

function signalTrendFromCurrentSignals(signals: IntentSignalItem[], generatedAt?: string): SignalTrendItem[] {
  if (!signals.length) return [];
  const point: SignalTrendItem = {
    bucket: (generatedAt || new Date().toISOString()).slice(0, 10),
    generatedAt,
    total: 0,
  };
  for (const signal of signals) {
    const key = signalFamilyForTrend(signal);
    const count = Math.max(1, Number(signal.signal_count ?? signal.count ?? 1) || 1);
    point[key] = Number(point[key] || 0) + count;
    point.total = Number(point.total || 0) + count;
  }
  return [point];
}

function aiBriefTitle(card: IntentSignalItem, ru: boolean): string {
  return (ru ? card.title_ru : card.title_en) || card.intent || card.family || '';
}

function aiBriefSummary(card: IntentSignalItem, ru: boolean): string {
  return (ru ? card.summary_ru : card.summary_en) || card.examples?.[0] || '';
}

function confidenceLabel(value: number | undefined, ru: boolean): string {
  const confidence = Math.round(Math.max(0, Math.min(1, Number(value) || 0)) * 100);
  return ru ? `${confidence}% увер.` : `${confidence}% conf.`;
}

function buildSocialDashboardPath(params: {
  from?: string;
  to?: string;
  entityId?: string;
  compareEntityId?: string;
  platform?: string;
}) {
  const query = new URLSearchParams();
  if (params.from) query.set('from', params.from);
  if (params.to) query.set('to', params.to);
  if (params.entityId && params.entityId !== 'all') query.set('entity_id', params.entityId);
  if (
    params.entityId &&
    params.entityId !== 'all' &&
    params.compareEntityId &&
    params.compareEntityId !== 'all' &&
    params.compareEntityId !== params.entityId
  ) {
    query.set('compare_entity_id', params.compareEntityId);
  }
  if (params.platform) query.set('platform', params.platform);
  const suffix = query.toString();
  return `/social/dashboard${suffix ? `?${suffix}` : ''}`;
}

const SOCIAL_DASHBOARD_CACHE_PREFIX = 'radar.social.dashboard.snapshot.v2:';
const SOCIAL_DASHBOARD_CACHE_MANIFEST = 'radar.social.dashboard.snapshot.keys.v2';
const SOCIAL_DASHBOARD_CACHE_LIMIT = 10;
const SOCIAL_DASHBOARD_WARMING_RETRY_DELAYS_MS = [1500, 2500, 3500, 5000];

function socialDashboardCacheKey(path: string): string {
  return `${SOCIAL_DASHBOARD_CACHE_PREFIX}${path}`;
}

function readCachedSocialDashboard(path: string): SocialDashboardSnapshot | null {
  if (typeof window === 'undefined') return null;
  try {
    const raw = window.localStorage.getItem(socialDashboardCacheKey(path));
    return raw ? JSON.parse(raw) as SocialDashboardSnapshot : null;
  } catch {
    return null;
  }
}

function readLatestCachedSocialDashboard(excludePath?: string): SocialDashboardSnapshot | null {
  if (typeof window === 'undefined') return null;
  const excludedKey = excludePath ? socialDashboardCacheKey(excludePath) : null;
  try {
    const rawManifest = window.localStorage.getItem(SOCIAL_DASHBOARD_CACHE_MANIFEST);
    const keys = rawManifest ? JSON.parse(rawManifest) : [];
    if (!Array.isArray(keys)) return null;
    for (const key of keys) {
      if (typeof key !== 'string' || !key.startsWith(SOCIAL_DASHBOARD_CACHE_PREFIX)) continue;
      if (excludedKey && key === excludedKey) continue;
      const raw = window.localStorage.getItem(key);
      if (!raw) continue;
      return JSON.parse(raw) as SocialDashboardSnapshot;
    }
  } catch {
    return null;
  }
  return null;
}

function writeCachedSocialDashboard(path: string, snapshot: SocialDashboardSnapshot): void {
  if (typeof window === 'undefined') return;
  try {
    const key = socialDashboardCacheKey(path);
    window.localStorage.setItem(key, JSON.stringify(snapshot));
    const rawManifest = window.localStorage.getItem(SOCIAL_DASHBOARD_CACHE_MANIFEST);
    const previousKeys = rawManifest ? JSON.parse(rawManifest) : [];
    const keys = [key, ...(Array.isArray(previousKeys) ? previousKeys : []).filter((item) => item !== key)];
    keys.slice(SOCIAL_DASHBOARD_CACHE_LIMIT).forEach((oldKey) => {
      if (typeof oldKey === 'string' && oldKey.startsWith(SOCIAL_DASHBOARD_CACHE_PREFIX)) {
        window.localStorage.removeItem(oldKey);
      }
    });
    window.localStorage.setItem(
      SOCIAL_DASHBOARD_CACHE_MANIFEST,
      JSON.stringify(keys.slice(0, SOCIAL_DASHBOARD_CACHE_LIMIT)),
    );
  } catch {
    // Best-effort cache only; never block dashboard rendering on storage quota.
  }
}

function isSocialDashboardWarmingError(error: unknown): boolean {
  const message = error instanceof Error ? error.message : String(error ?? '');
  return /warming|503/i.test(message);
}

// ═══════════════════════════════════════════════════════════════
// REUSABLE COMPONENTS (Dashboard-style)
// ═══════════════════════════════════════════════════════════════

function WidgetCard({ title, subtitle, children, headerRight, className = '' }: {
  title: string; subtitle?: string; children: React.ReactNode;
  headerRight?: React.ReactNode; className?: string;
}) {
  return (
    <div className={`bg-white rounded-2xl border border-slate-200 shadow-sm overflow-hidden ${className}`}>
      <div className="px-5 pt-4 pb-3 border-b border-slate-100 flex items-start justify-between gap-3">
        <div>
          <h3 className="text-sm text-slate-900" style={{ fontWeight: 600 }}>{title}</h3>
          {subtitle && <p className="text-xs text-slate-500 mt-0.5">{subtitle}</p>}
        </div>
        {headerRight}
      </div>
      <div className="p-5">{children}</div>
    </div>
  );
}

/** Dashboard-style TierHeader — uses Tailwind colour classes */
function TierHeader({ tier, isOpen, onToggle, ru }: { tier: TierDef; isOpen: boolean; onToggle: () => void; ru: boolean }) {
  const Icon = tier.icon;
  return (
    <button
      onClick={onToggle}
      className={`w-full flex items-center justify-between px-4 py-3 rounded-xl border transition-colors ${tier.bgColor} ${tier.borderColor} hover:shadow-sm`}
    >
      <div className="flex items-center gap-3">
        <div className={`w-8 h-8 rounded-lg flex items-center justify-center ${tier.bgColor}`}>
          <Icon className={`w-4 h-4 ${tier.color}`} />
        </div>
        <div className="text-left">
          <h2 className={`text-sm ${tier.color}`} style={{ fontWeight: 600 }}>{tier.title}</h2>
          <p className="text-xs text-slate-500">{tier.subtitle}</p>
        </div>
      </div>
      <div className="flex items-center gap-2 flex-shrink-0">
        <span className="text-[10px] text-slate-400 bg-white/70 px-2.5 py-1 rounded-full uppercase tracking-wide hidden sm:block" style={{ fontWeight: 500 }}>
          {isOpen ? (ru ? 'Свернуть' : 'Collapse') : (ru ? 'Развернуть' : 'Expand')}
        </span>
        {isOpen ? <ChevronUp className="w-4 h-4 text-slate-400" /> : <ChevronDown className="w-4 h-4 text-slate-400" />}
      </div>
    </button>
  );
}

function DeltaBadge({ value, suffix = '' }: { value?: number | null; suffix?: string }) {
  if (value === null || value === undefined || Number.isNaN(Number(value))) {
    return <span className="inline-flex text-xs text-slate-400" style={{ fontWeight: 600 }}>—</span>;
  }
  const pos = value > 0;
  return (
    <span className={`inline-flex items-center gap-0.5 text-xs ${pos ? 'text-emerald-600' : 'text-rose-500'}`} style={{ fontWeight: 600 }}>
      {pos ? <ArrowUpRight className="w-3 h-3" /> : <ArrowDownRight className="w-3 h-3" />}
      {pos ? '+' : ''}{value}{suffix}
    </span>
  );
}

function formatCompactMetric(value: unknown): string {
  const numeric = Number(value || 0);
  if (!Number.isFinite(numeric) || numeric <= 0) return '—';
  if (numeric >= 1_000_000) return `${(numeric / 1_000_000).toFixed(1)}M`;
  if (numeric >= 1_000) return `${(numeric / 1_000).toFixed(1)}K`;
  return String(Math.round(numeric));
}

function formatPostDate(value: unknown, ru: boolean): string {
  const text = String(value || '').trim();
  if (!text) return '—';
  const parsed = new Date(text);
  if (Number.isNaN(parsed.getTime())) return text;
  return parsed.toLocaleDateString(ru ? 'ru-RU' : 'en-US', {
    month: 'short',
    day: 'numeric',
    year: 'numeric',
  });
}

function ChartLegend({ items }: { items: { label: string; color: string }[] }) {
  return (
    <div className="flex flex-wrap gap-4 mt-4">
      {items.map(item => (
        <div key={item.label} className="flex items-center gap-1.5">
          <div className="w-2.5 h-2.5 rounded-full flex-shrink-0" style={{ backgroundColor: item.color }} />
          <span className="text-[11px] text-slate-500">{item.label}</span>
        </div>
      ))}
    </div>
  );
}

function PlatformToggle({ selected, onSelect }: { selected: string[]; onSelect: (p: string[]) => void }) {
  const platforms = ['All', 'Facebook', 'Instagram', 'LinkedIn', 'Twitter', 'Google'];
  const toggle = (p: string) => {
    if (p === 'All') { onSelect(['All']); return; }
    const n = selected.includes(p) ? selected.filter(x => x !== p) : [...selected.filter(x => x !== 'All'), p];
    onSelect(n.length === 0 ? ['All'] : n);
  };
  return (
    <div className="flex items-center gap-1.5 flex-wrap">
      {platforms.map(p => (
        <button key={p} onClick={() => toggle(p)}
          className={`px-3 py-1.5 rounded-full text-xs transition-all ${
            selected.includes(p) || (p === 'All' && selected.includes('All'))
              ? 'bg-blue-600 text-white shadow-sm'
              : 'bg-slate-100 text-slate-600 hover:bg-slate-200'
          }`} style={{ fontWeight: selected.includes(p) || (p === 'All' && selected.includes('All')) ? 600 : 400 }}>
          {p}
        </button>
      ))}
    </div>
  );
}

// ═══════════════════════════════════════════════════════════════
// VISUALIZATION COMPONENTS
// ═══════════════════════════════════════════════════════════════

function TopicBubbleViz({ ru, topics, onTopicSelect }: { ru: boolean; topics: TopicBubbleItem[]; onTopicSelect: (topic: string) => void }) {
  const [hovered, setHovered] = useState<string | null>(null);
  const getSentimentColor = (s: string) => sentimentKey(s) === 'positive' ? C.emerald : sentimentKey(s) === 'negative' ? C.rose : '#64748b';
  const getSentimentBg = (s: string) => sentimentKey(s) === 'positive' ? `${C.emerald}cc` : sentimentKey(s) === 'negative' ? `${C.rose}dd` : '#64748bcc';
  const bubbles = useMemo(() => {
    const positions = [
      [154, 118], [294, 78], [455, 132], [356, 208],
      [88, 62], [540, 82], [136, 238], [522, 230],
      [250, 190], [430, 256], [82, 178], [604, 170],
    ];
    const counts = topics.map((topic) => Number(topic.count) || 0);
    const min = Math.min(...counts, 0);
    const max = Math.max(...counts, 1);
    return topics.slice(0, 12).map((topic, index) => {
      const normalized = max === min ? 0.55 : ((Number(topic.count) || 0) - min) / Math.max(1, max - min);
      const r = Math.round(26 + normalized * 42);
      const [x, y] = positions[index % positions.length];
      return { ...topic, x, y, r, displayTopic: translateSocialLabel(topic.topic, ru) };
    });
  }, [ru, topics]);

  if (!bubbles.length) {
    return (
      <div className="flex h-full min-h-[290px] items-center justify-center rounded-xl bg-slate-50/70 text-sm text-slate-500">
        {ru ? 'Нет тем за выбранный период.' : 'No topics in the selected period.'}
      </div>
    );
  }

  return (
    <div className="relative w-full h-full min-h-[290px] rounded-xl overflow-hidden bg-slate-50/60">
      <svg viewBox="0 0 660 320" width="100%" height="100%" className="block">
        <defs>
          {bubbles.map((b, index) => (
            <radialGradient key={b.topic} id={`social-topic-grad-${index}`} cx="35%" cy="30%" r="65%">
              <stop offset="0%"   stopColor="white" stopOpacity={0.3} />
              <stop offset="100%" stopColor={getSentimentColor(b.sentiment)} stopOpacity={0} />
            </radialGradient>
          ))}
        </defs>
        {bubbles.map((b, index) => {
          const isHov = hovered === b.topic;
          const bgColor = getSentimentBg(b.sentiment);
          const lines = wrapBubbleLabel(b.displayTopic, b.r);
          const fs = Math.max(10, Math.min(18, b.r / 3.35));
          return (
            <g key={b.topic}
              role="button"
              tabIndex={0}
              aria-label={`${ru ? 'Открыть тему' : 'Open topic'} ${b.displayTopic}`}
              onMouseEnter={() => setHovered(b.topic)}
              onMouseLeave={() => setHovered(null)}
              onClick={() => onTopicSelect(b.topic)}
              onKeyDown={(event) => {
                if (event.key === 'Enter' || event.key === ' ') {
                  event.preventDefault();
                  onTopicSelect(b.topic);
                }
              }}
              style={{ cursor: 'pointer', transition: 'transform 0.15s ease', transform: isHov ? 'translate(0,-3px)' : 'translate(0,0)', transformOrigin: `${b.x}px ${b.y}px` }}
            >
              <circle cx={b.x} cy={b.y} r={b.r+(isHov?3:0)} fill={bgColor} stroke="white" strokeWidth={2.5}
                style={{ filter: isHov ? 'drop-shadow(0 4px 8px rgba(0,0,0,0.2))' : 'drop-shadow(0 2px 4px rgba(0,0,0,0.1))' }} />
              <circle cx={b.x} cy={b.y} r={b.r+(isHov?3:0)} fill={`url(#social-topic-grad-${index})`} />
              <text x={b.x} y={b.y - ((lines.length - 1) * fs * 0.55)} textAnchor="middle" dominantBaseline="middle" fontSize={fs} fill="white" fontWeight="700" style={{ pointerEvents:'none' }}>
                {lines.map((line, lineIndex) => (
                  <tspan key={lineIndex} x={b.x} dy={lineIndex === 0 ? 0 : fs + 1}>{line}</tspan>
                ))}
              </text>
              <text x={b.x} y={b.y+b.r-12} textAnchor="middle" fontSize={Math.max(9,fs-3)} fill="rgba(255,255,255,0.92)" fontWeight="700" style={{ pointerEvents:'none' }}>{b.count}</text>
            </g>
          );
        })}
        {hovered && (() => {
          const b = bubbles.find(x => x.topic === hovered)!;
          const showLeft = b.x+b.r+150 > 650;
          const tx = showLeft ? b.x-b.r-120 : b.x+b.r+8;
          return (
            <g style={{ pointerEvents:'none' }}>
              <rect x={tx} y={b.y-26} width={112} height={46} rx={6} fill="white" stroke={C.border} strokeWidth={1} style={{ filter:'drop-shadow(0 2px 6px rgba(0,0,0,0.1))' }} />
              <text x={tx+56} y={b.y-10} textAnchor="middle" fontSize={10} fill="#0f172a" fontWeight="700">{b.displayTopic}</text>
              <text x={tx+56} y={b.y+6}  textAnchor="middle" fontSize={9}  fill={C.muted}>{b.count} {ru?'упом.':'mentions'} · {sentimentLabel(b.sentiment, ru)}</text>
            </g>
          );
        })()}
      </svg>
      <div className="absolute bottom-2 left-3 flex items-center gap-3">
        {(['positive','neutral','negative'] as const).map(s => (
          <div key={s} className="flex items-center gap-1">
            <div className="w-2 h-2 rounded-full" style={{ backgroundColor: getSentimentColor(s) }} />
            <span className="text-[10px] text-slate-500" style={{ fontWeight:500 }}>
              {s==='positive'?(ru?'Позит.':'Positive'):s==='negative'?(ru?'Негат.':'Negative'):(ru?'Нейтр.':'Neutral')}
            </span>
          </div>
        ))}
      </div>
    </div>
  );
}

function SentimentAreaChart({ ru, data }: { ru: boolean; data: SentimentTrendItem[] }) {
  const chartData = useMemo(
    () => data.map((item) => ({
      ...item,
      label: formatTrendDay(item.bucket || item.week, ru),
      positive: Number(item.positive) || 0,
      neutral: Number(item.neutral) || 0,
      negative: Number(item.negative) || 0,
    })),
    [data, ru],
  );
  const maxValue = Math.max(1, ...chartData.flatMap((item) => [item.positive, item.neutral, item.negative]));
  const yMax = Math.max(5, Math.ceil(maxValue / 5) * 5);
  if (!chartData.length) {
    return (
      <div className="flex h-[260px] items-center justify-center rounded-xl bg-slate-50/70 text-sm text-slate-500">
        {ru ? 'Нет тональности за выбранный период.' : 'No sentiment data in the selected period.'}
      </div>
    );
  }
  return (
    <>
      <ResponsiveContainer width="100%" height={260}>
        <LineChart data={chartData} margin={{ top:10, right:10, left:-12, bottom:0 }}>
          <CartesianGrid {...GRID_COMMON} />
          <XAxis dataKey="label" {...AXIS_COMMON} dy={8} interval={0} tick={{ fontSize: 10, fill: C.muted }} />
          <YAxis {...AXIS_COMMON} domain={[0, yMax]} allowDecimals={false} />
          <Tooltip {...TOOLTIP_STYLE} />
          <Line type="monotone" dataKey="positive" stroke={C.emerald} strokeWidth={2.5} dot={{ r:4, fill:C.emerald, strokeWidth:0 }} activeDot={{ r:5 }} name={ru?'Позитив':'Positive'} />
          <Line type="monotone" dataKey="neutral"  stroke="#64748b"   strokeWidth={2.5} dot={{ r:4, fill:'#64748b', strokeWidth:0 }} activeDot={{ r:5 }} name={ru?'Нейтрал':'Neutral'} />
          <Line type="monotone" dataKey="negative" stroke={C.rose}    strokeWidth={2.5} dot={{ r:4, fill:C.rose, strokeWidth:0 }}   activeDot={{ r:5 }} name={ru?'Негатив':'Negative'} />
        </LineChart>
      </ResponsiveContainer>
      <ChartLegend items={[
        { label: ru?'Позитив':'Positive', color:C.emerald },
        { label: ru?'Нейтрал':'Neutral',  color:'#64748b' },
        { label: ru?'Негатив':'Negative', color:C.rose    },
      ]} />
    </>
  );
}

function SignalTrendChart({ ru, data }: { ru: boolean; data: SignalTrendItem[] }) {
  const chartData = data.map((item) => ({
    ...item,
    label: formatTrendDay(item.bucket || item.generatedAt, ru),
  }));
  const keys = Array.from(new Set(chartData.flatMap((item) => Object.keys(item))))
    .filter((key) => !['bucket', 'generatedAt', 'label', 'total'].includes(key))
    .filter((key) => chartData.some((item) => Number(item[key]) > 0));
  const colorByKey: Record<string, string> = {
    support: C.emerald,
    concern: C.amber,
    questions: C.blue,
    complaints: C.rose,
    trust_distrust: C.violet,
  };
  const series = keys.map((key, index) => ({
    key,
    label: signalTrendLabel(key, ru),
    color: colorByKey[key] || [C.blue, C.emerald, C.amber, C.rose, C.violet, C.cyan][index % 6],
  }));
  if (!chartData.length || !series.length) {
    return (
      <div className="flex h-[260px] items-center justify-center rounded-xl bg-slate-50/70 px-6 text-center text-sm text-slate-500">
        {ru
          ? 'Динамика сигналов появится после нескольких сохранённых AI-обзоров.'
          : 'Signal trend will appear after saved AI brief snapshots are generated.'}
      </div>
    );
  }
  return (
    <>
      <ResponsiveContainer width="100%" height={260}>
        <LineChart data={chartData} margin={{ top:10, right:10, left:-20, bottom:0 }}>
          <CartesianGrid {...GRID_COMMON} />
          <XAxis dataKey="label" {...AXIS_COMMON} dy={8} />
          <YAxis {...AXIS_COMMON} allowDecimals={false} />
          <Tooltip {...TOOLTIP_STYLE} />
          {series.map(s => (
            <Line key={s.key} type="monotone" dataKey={s.key} stroke={s.color}
              strokeWidth={2.5} dot={{ r: 4, fill: s.color, strokeWidth: 0 }} activeDot={{ r: 5 }} name={s.label}
            />
          ))}
        </LineChart>
      </ResponsiveContainer>
      <ChartLegend items={series.map(s => ({ label:s.label, color:s.color }))} />
    </>
  );
}

// ═══════════════════════════════════════════════════════════════
// AD SCRAPE TABLE COMPONENT
// ═══════════════════════════════════════════════════════════════

const SOURCE_TABS: { key: AdSource; label: string; icon: string; color: string }[] = [
  { key: 'all',       label: 'All Sources',  icon: '🌐', color: '#64748b' },
  { key: 'meta',      label: 'Meta',         icon: '🔵', color: '#1877f2' },
  { key: 'google',    label: 'Google',       icon: '🟢', color: '#34a853' },
  { key: 'facebook',  label: 'Facebook',     icon: '📘', color: '#1877f2' },
  { key: 'instagram', label: 'Instagram',    icon: '📸', color: '#e1306c' },
];

const SOURCE_COLORS: Record<AdSource, string> = {
  all:       '#64748b',
  meta:      '#1877f2',
  google:    '#34a853',
  facebook:  '#1877f2',
  instagram: '#e1306c',
};

const FORMAT_COLORS: Record<string, string> = {
  Search:   C.blue,
  Display:  C.cyan,
  Shopping: C.amber,
  Video:    C.violet,
  Image:    C.indigo,
  Carousel: C.pink,
  Story:    '#f97316',
  Reel:     '#e1306c',
};

const INTENT_COLORS: Record<string, string> = {
  Acquisition: C.emerald,
  Awareness:   C.blue,
  'Lead Gen':  C.violet,
  Retention:   C.amber,
};

function AdScrapeTable({ ru, items }: { ru: boolean; items: ScrapedAd[] }) {
  const [activeSource, setActiveSource] = useState<AdSource>('all');
  const [activeBrand,  setActiveBrand]  = useState<string>('All');
  const [expandedId,   setExpandedId]   = useState<string | null>(null);
  const [sortBy,       setSortBy]       = useState<'date'|'engagement'|'impressions'>('date');

  const brands = ['All', ...Array.from(new Set(items.map(a => a.entity)))];

  const filtered = items
    .filter(a => activeSource === 'all' || a.source === activeSource)
    .filter(a => activeBrand === 'All' || a.entity === activeBrand)
    .sort((a, b) => {
      if (sortBy === 'engagement')  return b.engagement  - a.engagement;
      if (sortBy === 'impressions') return b.impressions - a.impressions;
      return 0; // date order = insertion order
    });

  const sourceCounts: Record<AdSource, number> = {
    all:       items.length,
    meta:      items.filter(a => a.source === 'meta').length,
    google:    items.filter(a => a.source === 'google').length,
    facebook:  items.filter(a => a.source === 'facebook').length,
    instagram: items.filter(a => a.source === 'instagram').length,
  };

  return (
    <div className="bg-white rounded-2xl border border-slate-200 shadow-sm overflow-hidden">
      {/* ── Source tabs bar ── */}
      <div className="border-b border-slate-100 px-5 pt-4">
        <div className="flex items-center justify-between mb-3">
          <div>
            <h3 className="text-sm text-slate-900" style={{ fontWeight:600 }}>
              {ru ? 'Банк рекламы конкурентов' : 'Competitor Ad Intelligence'}
            </h3>
            <p className="text-xs text-slate-500 mt-0.5">
              {ru ? 'Собранная реклама из всех источников' : 'Scraped ads across all platforms'}
            </p>
          </div>
          <div className="flex items-center gap-2">
            <span className="text-[11px] text-slate-500">{ru?'Сорт.':'Sort:'}</span>
            {(['date','engagement','impressions'] as const).map(s => (
              <button key={s} onClick={() => setSortBy(s)}
                className={`text-[11px] px-2.5 py-1 rounded-lg transition-colors ${sortBy===s?'bg-blue-100 text-blue-700':'bg-slate-100 text-slate-500 hover:bg-slate-200'}`}
                style={{ fontWeight: sortBy===s ? 600 : 400 }}>
                {s === 'date' ? (ru?'Дата':'Date') : s === 'engagement' ? (ru?'Вовлеч.':'Engage') : (ru?'Показы':'Impress.')}
              </button>
            ))}
          </div>
        </div>
        {/* Source tabs */}
        <div className="flex items-center gap-0 overflow-x-auto">
          {SOURCE_TABS.map(tab => (
            <button key={tab.key} onClick={() => setActiveSource(tab.key)}
              className={`flex items-center gap-2 px-4 py-2.5 text-xs whitespace-nowrap transition-colors relative border-b-2 ${
                activeSource === tab.key
                  ? 'text-slate-900 border-blue-600'
                  : 'text-slate-500 border-transparent hover:text-slate-700'
              }`}
              style={{ fontWeight: activeSource === tab.key ? 600 : 400 }}>
              <span>{tab.icon}</span>
              {tab.label}
              <span className={`text-[10px] px-1.5 py-0.5 rounded-full ${
                activeSource === tab.key ? 'bg-blue-100 text-blue-700' : 'bg-slate-100 text-slate-500'
              }`} style={{ fontWeight: 600 }}>
                {sourceCounts[tab.key]}
              </span>
            </button>
          ))}
        </div>
      </div>

      {/* ── Brand filter pills ── */}
      <div className="flex items-center gap-2 px-5 py-3 border-b border-slate-50 overflow-x-auto">
        <span className="text-[11px] text-slate-400 flex-shrink-0">{ru?'Бренд:':'Brand:'}</span>
        {brands.map(b => {
          const color = b === 'All' ? '#64748b' : (BRAND_COLORS[b] || '#64748b');
          return (
            <button key={b} onClick={() => setActiveBrand(b)}
              className={`flex items-center gap-1.5 px-3 py-1 rounded-full text-[11px] transition-all flex-shrink-0 ${
                activeBrand === b ? 'text-white shadow-sm' : 'bg-slate-100 text-slate-600 hover:bg-slate-200'
              }`}
              style={{ fontWeight: 500, backgroundColor: activeBrand===b ? color : undefined }}>
              {b !== 'All' && (
                <div className="w-1.5 h-1.5 rounded-full bg-white/80 flex-shrink-0" />
              )}
              {b}
            </button>
          );
        })}
        <span className="ml-auto text-[11px] text-slate-400 flex-shrink-0">{filtered.length} {ru?'объявл.':'ads'}</span>
      </div>

      {/* ── Table ── */}
      <div className="overflow-x-auto">
        <table className="w-full text-left min-w-[860px]">
          <thead>
            <tr className="bg-slate-50/80 border-b border-slate-100">
              {[
                ru?'Превью':'Preview',
                ru?'Бренд / Площадка':'Brand / Platform',
                ru?'Текст объявления':'Ad Copy',
                ru?'Формат':'Format',
                ru?'Цель':'Intent',
                ru?'Ценность':'Value Props',
                ru?'Показы':'Impress.',
                ru?'Вовлеч.':'Engage',
                ru?'Действие':'Actions',
              ].map((h, i) => (
                <th key={i} className="px-4 py-3 text-[11px] text-slate-500 whitespace-nowrap" style={{ fontWeight:600 }}>{h}</th>
              ))}
            </tr>
          </thead>
          <tbody className="divide-y divide-slate-50">
            {filtered.map(ad => {
              const brandColor  = BRAND_COLORS[ad.entity] || '#64748b';
              const formatColor = FORMAT_COLORS[ad.format] || C.blue;
              const intentColor = INTENT_COLORS[ad.intent] || C.blue;
              const srcColor    = SOURCE_COLORS[ad.source];
              const isExpanded  = expandedId === ad.id;

              return (
                <tr key={ad.id} className={`hover:bg-slate-50/60 transition-colors cursor-pointer ${isExpanded ? 'bg-blue-50/20' : ''}`}
                  onClick={() => setExpandedId(isExpanded ? null : ad.id)}>
                  {/* Preview thumbnail */}
                  <td className="px-4 py-3.5">
                    <div className="w-12 h-12 rounded-xl flex items-center justify-center flex-shrink-0 border border-slate-100"
                      style={{ backgroundColor: `${brandColor}12` }}>
                      <div className="w-7 h-7 rounded-lg flex items-center justify-center text-sm text-white" style={{ backgroundColor: brandColor, fontWeight: 700 }}>
                        {ad.entity[0]}
                      </div>
                    </div>
                  </td>

                  {/* Brand + Platform */}
                  <td className="px-4 py-3.5">
                    <div className="flex flex-col gap-1">
                      <div className="flex items-center gap-1.5">
                        <div className="w-2 h-2 rounded-full flex-shrink-0" style={{ backgroundColor: brandColor }} />
                        <span className="text-xs text-slate-800" style={{ fontWeight: 600 }}>{ad.entity}</span>
                      </div>
                      <span className="text-[11px] px-2 py-0.5 rounded-full self-start" style={{ fontWeight: 500, backgroundColor: `${srcColor}15`, color: srcColor }}>
                        {ad.platform}
                      </span>
                      <span className="text-[10px] text-slate-400">{ad.date}</span>
                    </div>
                  </td>

                  {/* Ad Copy */}
                  <td className="px-4 py-3.5 max-w-[260px]">
                    <p className="text-xs text-slate-700 leading-relaxed line-clamp-2" title={ad.copy}>
                      {ad.copy}
                    </p>
                    {isExpanded && (
                      <div className="mt-2 pt-2 border-t border-slate-100">
                        <p className="text-xs text-slate-600 leading-relaxed">{ad.copy}</p>
                        <div className="flex items-center gap-1.5 mt-2">
                          <span className="text-[10px] text-slate-500">CTA:</span>
                          <span className="text-[11px] px-2 py-0.5 rounded-lg bg-blue-50 text-blue-700 border border-blue-100" style={{ fontWeight:600 }}>{ad.cta}</span>
                          {ad.urgency && (
                            <span className="text-[10px] px-2 py-0.5 rounded-lg bg-amber-50 text-amber-700 border border-amber-100 flex items-center gap-0.5" style={{ fontWeight:500 }}>
                              <Flame className="w-2.5 h-2.5" /> {ru?'Срочно':'Urgent'}
                            </span>
                          )}
                        </div>
                      </div>
                    )}
                  </td>

                  {/* Format */}
                  <td className="px-4 py-3.5">
                    <span className="text-[11px] px-2.5 py-1 rounded-lg" style={{ fontWeight:500, backgroundColor:`${formatColor}15`, color:formatColor }}>
                      {ad.format}
                    </span>
                  </td>

                  {/* Intent */}
                  <td className="px-4 py-3.5">
                    <span className="text-[11px] px-2.5 py-1 rounded-lg" style={{ fontWeight:500, backgroundColor:`${intentColor}15`, color:intentColor }}>
                      {ad.intent}
                    </span>
                  </td>

                  {/* Value Props */}
                  <td className="px-4 py-3.5">
                    <div className="flex flex-col gap-1">
                      {ad.valueProps.slice(0,2).map(v => (
                        <span key={v} className="text-[10px] text-slate-600 bg-slate-100 px-2 py-0.5 rounded-full self-start" style={{ fontWeight:500 }}>{v}</span>
                      ))}
                    </div>
                  </td>

                  {/* Impressions */}
                  <td className="px-4 py-3.5 text-right">
                    <span className="text-xs text-slate-700" style={{ fontWeight:600 }}>{(ad.impressions/1000).toFixed(1)}K</span>
                  </td>

                  {/* Engagement */}
                  <td className="px-4 py-3.5 text-right">
                    <span className="text-xs text-slate-700" style={{ fontWeight:600 }}>{ad.engagement.toLocaleString()}</span>
                  </td>

                  {/* Actions */}
                  <td className="px-4 py-3.5">
                    <button className="text-xs text-blue-600 hover:text-blue-800 transition-colors" style={{ fontWeight:500 }}
                      onClick={e => { e.stopPropagation(); setExpandedId(isExpanded ? null : ad.id); }}>
                      {isExpanded ? (ru?'Скрыть':'Close') : (ru?'Просмотр':'View')}
                    </button>
                  </td>
                </tr>
              );
            })}
          </tbody>
        </table>
        {filtered.length === 0 && (
          <div className="py-12 text-center text-slate-400 text-sm">
            {ru ? 'Нет объявлений по выбранным фильтрам' : 'No ads match the current filters'}
          </div>
        )}
      </div>

      {/* ── Footer summary ── */}
      <div className="px-5 py-3 border-t border-slate-100 flex items-center justify-between bg-slate-50/50">
        <div className="flex items-center gap-4">
          {(['meta','google','facebook','instagram'] as AdSource[]).map(src => (
            <div key={src} className="flex items-center gap-1.5">
              <div className="w-1.5 h-1.5 rounded-full" style={{ backgroundColor: SOURCE_COLORS[src] }} />
              <span className="text-[11px] text-slate-500">{src.charAt(0).toUpperCase()+src.slice(1)}</span>
              <span className="text-[11px] text-slate-700" style={{ fontWeight:600 }}>{sourceCounts[src as AdSource]}</span>
            </div>
          ))}
        </div>
        <button className="text-xs text-blue-600 hover:text-blue-800 transition-colors" style={{ fontWeight:500 }}>
          {ru ? 'Экспортировать CSV' : 'Export CSV'} →
        </button>
      </div>
    </div>
  );
}

type OrganicPostSource = 'all' | 'facebook' | 'instagram' | 'google';

const ORGANIC_POST_SOURCE_TABS: { key: OrganicPostSource; label: string; icon: string }[] = [
  { key: 'all',       label: 'All Sources', icon: '🌐' },
  { key: 'facebook',  label: 'Facebook',    icon: '🔵' },
  { key: 'instagram', label: 'Instagram',   icon: '📸' },
  { key: 'google',    label: 'Google',      icon: '🟢' },
];

function OrganicPostsTable({ ru, items }: { ru: boolean; items: OrganicPostItem[] }) {
  const [activeSource, setActiveSource] = useState<OrganicPostSource>('all');
  const [activeBrand, setActiveBrand] = useState<string>('All');
  const [expandedId, setExpandedId] = useState<string | null>(null);
  const [sortBy, setSortBy] = useState<'date' | 'reach' | 'likes' | 'comments'>('date');

  const brands = ['All', ...Array.from(new Set(items.map(item => item.entity).filter(Boolean)))];
  const sourceCounts: Record<OrganicPostSource, number> = {
    all: items.length,
    facebook: items.filter(item => String(item.platform || '').toLowerCase() === 'facebook').length,
    instagram: items.filter(item => String(item.platform || '').toLowerCase() === 'instagram').length,
    google: items.filter(item => String(item.platform || '').toLowerCase() === 'google').length,
  };

  const filtered = items
    .filter(item => activeSource === 'all' || String(item.platform || '').toLowerCase() === activeSource)
    .filter(item => activeBrand === 'All' || item.entity === activeBrand)
    .sort((a, b) => {
      if (sortBy === 'reach') return Number(b.reach || 0) - Number(a.reach || 0);
      if (sortBy === 'likes') return Number(b.likes || 0) - Number(a.likes || 0);
      if (sortBy === 'comments') return Number(b.comments || 0) - Number(a.comments || 0);
      return new Date(b.published_at || 0).getTime() - new Date(a.published_at || 0).getTime();
    });

  return (
    <div className="bg-white rounded-2xl border border-slate-200 shadow-sm overflow-hidden">
      <div className="border-b border-slate-100 px-5 pt-4">
        <div className="flex items-center justify-between mb-3">
          <div>
            <h3 className="text-sm text-slate-900" style={{ fontWeight: 600 }}>
              {ru ? 'Органические посты' : 'Organic Posts'}
            </h3>
            <p className="text-xs text-slate-500 mt-0.5">
              {ru ? 'Собранные посты из отслеживаемых social-источников' : 'Scraped posts from tracked social sources'}
            </p>
          </div>
          <div className="flex items-center gap-2">
            <span className="text-[11px] text-slate-500">{ru ? 'Сорт.' : 'Sort:'}</span>
            {(['date', 'reach', 'likes', 'comments'] as const).map(sort => (
              <button
                key={sort}
                onClick={() => setSortBy(sort)}
                className={`text-[11px] px-2.5 py-1 rounded-lg transition-colors ${
                  sortBy === sort ? 'bg-blue-100 text-blue-700' : 'bg-slate-100 text-slate-500 hover:bg-slate-200'
                }`}
                style={{ fontWeight: sortBy === sort ? 600 : 400 }}
              >
                {sort === 'date'
                  ? (ru ? 'Дата' : 'Date')
                  : sort === 'reach'
                    ? (ru ? 'Охват' : 'Reach')
                    : sort === 'likes'
                      ? (ru ? 'Лайки' : 'Likes')
                      : (ru ? 'Коммент.' : 'Comments')}
              </button>
            ))}
          </div>
        </div>
        <div className="flex items-center gap-0 overflow-x-auto">
          {ORGANIC_POST_SOURCE_TABS.map(tab => (
            <button
              key={tab.key}
              onClick={() => setActiveSource(tab.key)}
              className={`flex items-center gap-2 px-4 py-2.5 text-xs whitespace-nowrap transition-colors relative border-b-2 ${
                activeSource === tab.key
                  ? 'text-slate-900 border-blue-600'
                  : 'text-slate-500 border-transparent hover:text-slate-700'
              }`}
              style={{ fontWeight: activeSource === tab.key ? 600 : 400 }}
            >
              <span>{tab.icon}</span>
              {ru ? translateSocialLabel(tab.label, true) : tab.label}
              <span className={`text-[10px] px-1.5 py-0.5 rounded-full ${
                activeSource === tab.key ? 'bg-blue-100 text-blue-700' : 'bg-slate-100 text-slate-500'
              }`} style={{ fontWeight: 600 }}>
                {sourceCounts[tab.key]}
              </span>
            </button>
          ))}
        </div>
      </div>

      <div className="flex items-center gap-2 px-5 py-3 border-b border-slate-50 overflow-x-auto">
        <span className="text-[11px] text-slate-400 flex-shrink-0">{ru ? 'Источник:' : 'Source:'}</span>
        {brands.map((brand, index) => {
          const color = brand === 'All' ? '#64748b' : colorForEntity(brand, index);
          return (
            <button
              key={brand}
              onClick={() => setActiveBrand(brand)}
              className={`flex items-center gap-1.5 px-3 py-1 rounded-full text-[11px] transition-all flex-shrink-0 ${
                activeBrand === brand ? 'text-white shadow-sm' : 'bg-slate-100 text-slate-600 hover:bg-slate-200'
              }`}
              style={{ fontWeight: 500, backgroundColor: activeBrand === brand ? color : undefined }}
            >
              {brand !== 'All' && <div className="w-1.5 h-1.5 rounded-full bg-white/80 flex-shrink-0" />}
              {brand === 'All' ? (ru ? 'Все' : 'All') : brand}
            </button>
          );
        })}
        <span className="ml-auto text-[11px] text-slate-400 flex-shrink-0">{filtered.length} {ru ? 'пост.' : 'posts'}</span>
      </div>

      <div className="overflow-x-auto">
        <table className="w-full text-left min-w-[860px]">
          <thead>
            <tr className="bg-slate-50/80 border-b border-slate-100">
              {[
                ru ? 'Превью' : 'Preview',
                ru ? 'Источник / Площадка' : 'Source / Platform',
                ru ? 'Пост' : 'Post',
                ru ? 'Охват' : 'Reach',
                ru ? 'Лайки' : 'Likes',
                ru ? 'Комментарии' : 'Comments',
                ru ? 'Дата' : 'Date',
                ru ? 'Действие' : 'Actions',
              ].map((header, index) => (
                <th key={index} className="px-4 py-3 text-[11px] text-slate-500 whitespace-nowrap" style={{ fontWeight: 600 }}>{header}</th>
              ))}
            </tr>
          </thead>
          <tbody className="divide-y divide-slate-50">
            {filtered.map((post, index) => {
              const brandColor = colorForEntity(post.entity, index);
              const isExpanded = expandedId === post.id;
              const postText = String(post.text || '').trim() || (ru ? 'Текст поста недоступен' : 'Post text unavailable');
              const mediaUrl = post.media?.url;
              return (
                <tr
                  key={post.id}
                  className={`hover:bg-slate-50/60 transition-colors cursor-pointer ${isExpanded ? 'bg-blue-50/20' : ''}`}
                  onClick={() => setExpandedId(isExpanded ? null : post.id)}
                >
                  <td className="px-4 py-3.5">
                    <div className="relative w-12 h-12 rounded-xl flex items-center justify-center flex-shrink-0 border border-slate-100 overflow-hidden" style={{ backgroundColor: `${brandColor}12` }}>
                      <div className="w-7 h-7 rounded-lg flex items-center justify-center text-sm text-white" style={{ backgroundColor: brandColor, fontWeight: 700 }}>
                        {(post.entity || '?')[0]}
                      </div>
                      {mediaUrl && (
                        <img
                          src={mediaUrl}
                          alt=""
                          className="absolute inset-0 w-full h-full object-cover"
                          loading="lazy"
                          onError={(event) => { event.currentTarget.style.display = 'none'; }}
                        />
                      )}
                    </div>
                  </td>
                  <td className="px-4 py-3.5">
                    <div className="flex flex-col gap-1">
                      <div className="flex items-center gap-1.5">
                        <div className="w-2 h-2 rounded-full flex-shrink-0" style={{ backgroundColor: brandColor }} />
                        <span className="text-xs text-slate-800" style={{ fontWeight: 600 }}>{post.entity}</span>
                      </div>
                      <span className="text-[11px] px-2 py-0.5 rounded-full self-start bg-blue-50 text-blue-700" style={{ fontWeight: 500 }}>
                        {post.platform || 'social'}
                      </span>
                    </div>
                  </td>
                  <td className="px-4 py-3.5 max-w-[360px]">
                    <p className="text-xs text-slate-700 leading-relaxed line-clamp-2" title={postText}>{postText}</p>
                    {isExpanded && (
                      <div className="mt-2 pt-2 border-t border-slate-100">
                        <p className="text-xs text-slate-600 leading-relaxed whitespace-pre-wrap">{postText}</p>
                      </div>
                    )}
                  </td>
                  <td className="px-4 py-3.5 text-right">
                    <span className="text-xs text-slate-700" style={{ fontWeight: 600 }}>{formatCompactMetric(post.reach)}</span>
                  </td>
                  <td className="px-4 py-3.5 text-right">
                    <span className="text-xs text-slate-700" style={{ fontWeight: 600 }}>{formatCompactMetric(post.likes)}</span>
                  </td>
                  <td className="px-4 py-3.5 text-right">
                    <span className="text-xs text-slate-700" style={{ fontWeight: 600 }}>{formatCompactMetric(post.comments)}</span>
                  </td>
                  <td className="px-4 py-3.5">
                    <span className="text-xs text-slate-500">{formatPostDate(post.published_at, ru)}</span>
                  </td>
                  <td className="px-4 py-3.5">
                    {post.source_url ? (
                      <a
                        href={post.source_url}
                        target="_blank"
                        rel="noreferrer"
                        className="inline-flex items-center gap-1 text-xs text-blue-600 hover:text-blue-800 transition-colors"
                        style={{ fontWeight: 500 }}
                        onClick={(event) => event.stopPropagation()}
                      >
                        {ru ? 'Открыть' : 'Open'} <ArrowUpRight className="w-3 h-3" />
                      </a>
                    ) : (
                      <span className="text-xs text-slate-400">—</span>
                    )}
                  </td>
                </tr>
              );
            })}
          </tbody>
        </table>
        {filtered.length === 0 && (
          <div className="py-12 text-center text-slate-400 text-sm">
            {ru ? 'Нет органических постов по выбранным фильтрам' : 'No organic posts match the current filters'}
          </div>
        )}
      </div>

      <div className="px-5 py-3 border-t border-slate-100 flex items-center justify-between bg-slate-50/50">
        <div className="flex items-center gap-4">
          {(['facebook', 'instagram', 'google'] as OrganicPostSource[]).map(source => (
            <div key={source} className="flex items-center gap-1.5">
              <div className="w-1.5 h-1.5 rounded-full" style={{ backgroundColor: SOURCE_COLORS[source] }} />
              <span className="text-[11px] text-slate-500">{source.charAt(0).toUpperCase() + source.slice(1)}</span>
              <span className="text-[11px] text-slate-700" style={{ fontWeight: 600 }}>{sourceCounts[source]}</span>
            </div>
          ))}
        </div>
        <span className="text-[11px] text-slate-400">{ru ? 'Макс. 50 последних постов' : 'Latest 50 posts max'}</span>
      </div>
    </div>
  );
}

// ═══════════════════════════════════════════════════════════════
// MAIN PAGE
// ═══════════════════════════════════════════════════════════════

export function SocialPage() {
  const { lang } = useLanguage();
  const { range, ready: dateRangeReady } = useSocialDateRange();
  const navigate = useNavigate();
  const ru = lang === 'ru';

  const [dashboard, setDashboard] = useState<SocialDashboardSnapshot | null>(null);
  const [dashboardLoading, setDashboardLoading] = useState(true);
  const [dashboardError, setDashboardError] = useState<string | null>(null);
  const [knownEntities, setKnownEntities] = useState<Organization[]>([]);
  const [primarySource,    setPrimarySource]    = useState<Organization>(ALL_ORG);
  const [secondarySource,  setSecondarySource]  = useState<Organization | null>(null);
  const [selectedPlatforms,setSelectedPlatforms]= useState<string[]>(['All']);
  const [activeTab,        setActiveTab]        = useState<'deep'|'metrics'>('deep');
  const [openTiers, setOpenTiers] = useState<Record<string, boolean>>({
    topics:true, intent:true, questions:true, audience:true,
    visibility:true, content:true, position:true, scorecard:true,
  });
  const toggleTier = (id: string) => setOpenTiers(p => ({ ...p, [id]: !p[id] }));

  const platformFilter = useMemo(() => {
    if (selectedPlatforms.includes('All') || selectedPlatforms.length !== 1) return undefined;
    return selectedPlatforms[0].toLowerCase();
  }, [selectedPlatforms]);

  useEffect(() => {
    if (!dateRangeReady) return;
    let cancelled = false;
    const retryTimers: Array<ReturnType<typeof window.setTimeout>> = [];
    const path = buildSocialDashboardPath({
      from: range.from,
      to: range.to,
      entityId: primarySource.id,
      compareEntityId: secondarySource?.id,
      platform: platformFilter,
    });
    const cachedSnapshot = readCachedSocialDashboard(path);
    const fallbackSnapshot = cachedSnapshot ?? readLatestCachedSocialDashboard(path);
    if (fallbackSnapshot) {
      setDashboard(fallbackSnapshot);
      setDashboardLoading(!cachedSnapshot);
    } else {
      setDashboardLoading(true);
    }

    setDashboardError(null);
    const fetchSnapshot = (attempt = 0) => {
      apiFetch<SocialDashboardSnapshot>(path, { includeUserAuth: true, timeoutMs: 15_000 })
        .then((snapshot) => {
          if (cancelled) return;
          setDashboard(snapshot);
          writeCachedSocialDashboard(path, snapshot);
          setDashboardError(null);
          setDashboardLoading(false);
        })
        .catch((error: Error) => {
          if (cancelled) return;
          if (isSocialDashboardWarmingError(error)) {
            setDashboardError(null);
            if (attempt < SOCIAL_DASHBOARD_WARMING_RETRY_DELAYS_MS.length) {
              setDashboardLoading(!cachedSnapshot);
              const timer = window.setTimeout(
                () => fetchSnapshot(attempt + 1),
                SOCIAL_DASHBOARD_WARMING_RETRY_DELAYS_MS[attempt],
              );
              retryTimers.push(timer);
              return;
            }
            if (!fallbackSnapshot) {
              setDashboard(null);
            }
            setDashboardLoading(false);
            return;
          }
          setDashboardError(error.message || String(error));
          setDashboardLoading(false);
        });
    };

    fetchSnapshot();

    return () => {
      cancelled = true;
      retryTimers.forEach((timer) => window.clearTimeout(timer));
    };
  }, [dateRangeReady, platformFilter, primarySource.id, range.from, range.to, secondarySource?.id]);

  useEffect(() => {
    const entities = dashboard?.filters?.entities ?? [];
    if (!entities.length) return;
    setKnownEntities((current) => {
      const byId = new Map(current.map((entity) => [entity.id, entity]));
      entities.forEach((entity, index) => {
        byId.set(entity.id, {
          id: entity.id,
          name: entity.name,
          color: byId.get(entity.id)?.color || colorForEntity(entity.name, byId.size + index),
        });
      });
      return Array.from(byId.values()).sort((a, b) => a.name.localeCompare(b.name));
    });
  }, [dashboard?.filters?.entities]);

  const orgOptions = useMemo<Organization[]>(() => {
    return [ALL_ORG, ...knownEntities];
  }, [knownEntities]);

  useEffect(() => {
    if (primarySource.id === 'all' && secondarySource) {
      setSecondarySource(null);
    }
  }, [primarySource.id, secondarySource]);

  const comparisonOptions = useMemo(() => {
    return orgOptions.filter(o => o.id !== primarySource.id && o.id !== 'all');
  }, [orgOptions, primarySource.id]);

  useEffect(() => {
    if (!orgOptions.some((org) => org.id === primarySource.id)) {
      setPrimarySource(ALL_ORG);
    }
    if (secondarySource && !orgOptions.some((org) => org.id === secondarySource.id)) {
      setSecondarySource(null);
    }
  }, [orgOptions, primarySource.id, secondarySource]);

  const entityColors = useMemo(() => {
    const colors: Record<string, string> = {};
    orgOptions.forEach((org, index) => {
      colors[org.name] = org.color || colorForEntity(org.name, index);
    });
    return colors;
  }, [orgOptions]);

  const topicBubbles = dashboard?.deepAnalysis?.topicBubbles ?? [];
  const topicRanking = dashboard?.deepAnalysis?.topicRanking?.length
    ? dashboard.deepAnalysis.topicRanking
    : topicBubbles;
  const maxTopicRankingCount = Math.max(1, topicRanking.reduce((max, item) => Math.max(max, Number(item.count) || 0), 0));
  const topicMomentum = dashboard?.deepAnalysis?.topicMomentum ?? [];
  const momentumByTopic = useMemo(() => {
    const map = new Map<string, TopicMomentumItem>();
    topicMomentum.forEach((item) => map.set(item.topic.toLowerCase(), item));
    return map;
  }, [topicMomentum]);
  const sentimentTrend = dashboard?.deepAnalysis?.sentimentTrend ?? [];
  const rawIntentSignals = dashboard?.deepAnalysis?.intentSignals ?? [];
  const intentSignalTotal = Math.max(
    1,
    rawIntentSignals.reduce((sum: number, signal: any) => sum + Number(signal.signal_count ?? signal.count ?? 0), 0),
  );
  const intentSignals = rawIntentSignals.map((signal: any) => {
    const intent = signal.intent || signal.family || signal.title_en || '';
    const count = Number(signal.signal_count ?? signal.count ?? 0);
    const quotes = Array.isArray(signal.evidence_quotes) ? signal.evidence_quotes : [];
    const examples = Array.isArray(signal.examples) && signal.examples.length ? signal.examples : quotes;
    return {
      ...signal,
      intent,
      count,
      pct: Number(signal.pct ?? ((count / intentSignalTotal) * 100).toFixed(1)),
      delta: Number(signal.delta ?? signal.trend_pct ?? 0),
      icon: iconForIntent(intent),
      color: colorForIntent(intent, signal.color),
      examples,
    };
  }) as IntentSignalItem[];
  const topSignals = (dashboard?.deepAnalysis?.topSignals ?? []).map((signal: any) => {
    const intent = signal.family || signal.intent || signal.title_en || '';
    const count = Number(signal.signal_count ?? signal.count ?? 0);
    const evidenceQuotes = Array.isArray(signal.evidence_quotes) ? signal.evidence_quotes.map(readableEvidence).filter(Boolean) : [];
    const examples = Array.isArray(signal.examples)
      ? signal.examples.map(readableEvidence).filter(Boolean)
      : evidenceQuotes;
    return {
      ...signal,
      intent,
      count,
      delta: Number(signal.delta ?? signal.trend_pct ?? 0),
      icon: iconForIntent(intent),
      color: colorForIntent(intent, signal.color),
      examples: examples.length ? examples : evidenceQuotes,
    };
  }) as IntentSignalItem[];
  const rawSignalTrend = dashboard?.deepAnalysis?.signalTrend ?? [];
  const signalTrend = rawSignalTrend.length
    ? rawSignalTrend
    : signalTrendFromCurrentSignals(topSignals, dashboard?.meta?.generatedAt);
  const signalTrendIsCurrentSnapshot = rawSignalTrend.length === 0 && signalTrend.length > 0;
  const topQuestions = (dashboard?.deepAnalysis?.topQuestions ?? []).map((question: any) => ({
    question: String(question.question || question.question_en || ''),
    question_en: question.question_en,
    question_ru: question.question_ru,
    topic: question.topic,
    count: Number(question.count ?? question.signal_count ?? 0),
    confidence: question.confidence,
    sources: Array.isArray(question.sources)
      ? question.sources.map((source: unknown) => String(source || '').trim()).filter(Boolean)
      : [String(question.entity || question.source || 'Audience').trim()].filter(Boolean),
    evidence_quotes: Array.isArray(question.evidence_quotes)
      ? question.evidence_quotes.map(readableEvidence).filter(Boolean)
      : [],
    evidence_count: Number(question.evidence_count ?? question.evidence_ids?.length ?? 0),
    evidence_ids: Array.isArray(question.evidence_ids) ? question.evidence_ids : [],
  })) as TopQuestionItem[];
  const painPoints = (dashboard?.deepAnalysis?.painPoints ?? []).map((point: any) => ({
    problem: String(point.problem || point.problem_en || point.title_en || point.text || ''),
    problem_en: point.problem_en || point.title_en || point.problem || point.text,
    problem_ru: point.problem_ru || point.title_ru,
    title_en: point.title_en,
    title_ru: point.title_ru,
    text: point.text,
    summary_en: point.summary_en,
    summary_ru: point.summary_ru,
    topic: point.topic || point.main_topic,
    count: Number(point.count ?? point.signal_count ?? 0),
    confidence: point.confidence,
    sources: Array.isArray(point.sources)
      ? point.sources.map((source: unknown) => String(source || '').trim()).filter(Boolean)
      : Array.isArray(point.entities)
        ? point.entities.map((source: unknown) => String(source || '').trim()).filter(Boolean)
        : [String(point.entity || point.source || 'Audience').trim()].filter(Boolean),
    entities: Array.isArray(point.entities) ? point.entities : [],
    evidence_quotes: Array.isArray(point.evidence_quotes)
      ? point.evidence_quotes.map(readableEvidence).filter(Boolean)
      : Array.isArray(point.examples)
        ? point.examples.map(readableEvidence).filter(Boolean)
        : [],
    evidence_count: Number(point.evidence_count ?? point.evidence_ids?.length ?? 0),
    evidence_ids: Array.isArray(point.evidence_ids) ? point.evidence_ids : [],
    severity: point.severity,
  })).filter((point) => {
    const title = String(point.problem_en || point.title_en || point.problem || point.text || '').trim();
    const confidence = Number(point.confidence ?? 0);
    const evidenceCount = Number(point.evidence_count ?? point.evidence_ids?.length ?? 0);
    return Boolean(title) && confidence >= 0.6 && evidenceCount > 0 && (point.evidence_quotes?.length ?? 0) > 0;
  }) as PainPointItem[];
  const adItems = dashboard?.adIntelligence?.items ?? [];
  const sentimentByEntity = dashboard?.strictMetrics?.sentimentByEntity ?? [];
  const communityInterests = (dashboard?.deepAnalysis?.communityInterests ?? [])
    .map((item) => ({
      ...item,
      interest: String(item.interest || '').trim(),
      interestLabel: translateSocialLabel(item.interest, ru),
      score: Number(item.score ?? 0),
      mentions: Number(item.mentions ?? 0),
      topTopics: Array.isArray(item.topTopics)
        ? item.topTopics.map((topic) => String(topic || '').trim()).filter(Boolean).slice(0, 3)
        : [],
      description: String(item.description || '').trim(),
    }))
    .filter((item) => item.interest && item.score > 0)
    .sort((a, b) => b.score - a.score) as CommunityInterestItem[];
  const communityInterestPeak = Math.max(0, ...communityInterests.map((item) => Number(item.score) || 0));
  const communityInterestScale = Math.min(100, Math.max(20, Math.ceil((communityInterestPeak + 10) / 10) * 10));
  const communityInterestTicks = Array.from(new Set([
    0,
    Math.round(communityInterestScale / 4),
    Math.round(communityInterestScale / 2),
    Math.round((communityInterestScale * 3) / 4),
    communityInterestScale,
  ]));
  const topCommunityInterests = communityInterests.slice(0, 4);
  const communityInterestTopics = (item: CommunityInterestItem) =>
    (item.topTopics ?? [])
      .map((topic) => translateSocialLabel(topic, ru))
      .filter(Boolean)
      .join(', ');
  const visibilityData = dashboard?.strictMetrics?.visibilityData ?? [];
  const visibilityTrend = dashboard?.strictMetrics?.visibilityTrend ?? [];
  const scorecard = dashboard?.strictMetrics?.scorecard ?? [];
  const organicPostItems = dashboard?.strictMetrics?.organicPosts?.items ?? [];
  const strictSummary = dashboard?.strictMetrics?.summary ?? {};
  const sovData = dashboard?.strictMetrics?.shareOfVoice ?? visibilityData.map(v => ({
    name: v.entity,
    value: v.sov,
    color: entityColors[v.entity] || colorForEntity(v.entity),
  }));
  const trackedCount = String(Number(strictSummary.trackedSources ?? (orgOptions.length > 1 ? orgOptions.length - 1 : 0)) || 0);
  const postsCount = String(Number(strictSummary.posts ?? scorecard.reduce((sum, row) => sum + (Number(row.posts) || 0), 0)) || 0);
  const commentsCount = Number(strictSummary.comments ?? scorecard.reduce((sum, row) => sum + (Number((row as any).comments) || 0), 0)) || 0;
  const adsCount = String(Number(strictSummary.ads ?? (adItems.length || scorecard.reduce((sum, row) => sum + (Number(row.ads) || 0), 0))) || 0);
  const avgPositive = `${Math.round(Number(strictSummary.avgPositive ?? 0) || 0)}%`;
  const topTopicRaw = [...topicBubbles].sort((a, b) => b.count - a.count)[0]?.topic || '';
  const topTopic = topTopicRaw ? translateSocialLabel(topTopicRaw, ru) : (ru ? 'Нет данных' : 'No data');
  const showDashboardLoading = dashboardLoading;
  const showDashboardError = Boolean(dashboardError);
  const chartSeries = visibilityData.slice(0, 4).map((item, index) => ({
    key: seriesKey(item.entity),
    label: item.entity,
    color: entityColors[item.entity] || colorForEntity(item.entity, index),
  }));
  const openSocialTopic = (topic: string) => {
    const clean = topic.trim();
    if (!clean) return;
    navigate(`/social/topics?topic=${encodeURIComponent(clean)}&view=evidence`);
  };
  const openSocialSourceEvidence = (item: SentimentByEntityItem) => {
    const entityId = String(item.entity_id || '').trim();
    if (entityId) {
      navigate(`/social/topics?entity=${encodeURIComponent(entityId)}&view=evidence`);
      return;
    }
    const entityName = String(item.entity || '').trim();
    if (entityName) {
      navigate(`/social/topics?topic=${encodeURIComponent(entityName)}&view=evidence`);
    }
  };

  // ── Tier configs (Tailwind classes — matches DashboardPage palette) ──
  const TIERS: Record<string, TierDef> = {
    topics: {
      id:'topics', icon:Target, color:'text-blue-700', bgColor:'bg-blue-50', borderColor:'border-blue-200',
      title: ru?'О чём говорят':'Topic Intelligence',
      subtitle: ru?'Ландшафт тем, тренды тональности и моментум':'Conversation landscape, sentiment trends & topic momentum',
    },
    intent: {
      id:'intent', icon:Compass, color:'text-indigo-700', bgColor:'bg-indigo-50', borderColor:'border-indigo-200',
      title: ru?'Намерения и сигналы':'Intent & Signal Classification',
      subtitle: ru?'Что пользователи хотят и ожидают':'What users want, need & expect',
    },
    questions: {
      id:'questions', icon:HelpCircle, color:'text-amber-700', bgColor:'bg-amber-50', borderColor:'border-amber-200',
      title: ru?'Вопросы аудитории':'Question Intelligence',
      subtitle: ru?'Что спрашивают и пробелы в ответах':'What people ask & answer gaps',
    },
    audience: {
      id:'audience', icon:Heart, color:'text-rose-700', bgColor:'bg-rose-50', borderColor:'border-rose-200',
      title: ru?'Реакция аудитории':'Audience Response',
      subtitle: ru?'Тональность, боли, сущности и вовлечённость':'Sentiment, pain points, entities & engagement',
    },
    visibility: {
      id:'visibility', icon:Globe, color:'text-blue-700', bgColor:'bg-blue-50', borderColor:'border-blue-200',
      title: ru?'Видимость и охват':'Visibility & Reach Tracking',
      subtitle: ru?'Позиции, охват и доля голоса':'Position, reach & share of voice',
    },
    content: {
      id:'content', icon:Layers, color:'text-slate-700', bgColor:'bg-slate-50', borderColor:'border-slate-200',
      title: ru?'Органические посты':'Organic Posts',
      subtitle: ru?'Собранные посты из отслеживаемых social-источников':'Scraped posts from tracked social sources',
    },
    position: {
      id:'position', icon:Star, color:'text-violet-700', bgColor:'bg-violet-50', borderColor:'border-violet-200',
      title: ru?'Конкурентная карта':'Competitive Position Map',
      subtitle: ru?'Рыночное позиционирование':'Market positioning & traffic share',
    },
    scorecard: {
      id:'scorecard', icon:Megaphone, color:'text-violet-700', bgColor:'bg-violet-50', borderColor:'border-violet-200',
      title: ru?'Банк рекламы конкурентов':'Competitor Ad Intelligence',
      subtitle: ru?'Собранная реклама по всем площадкам: Meta, Google, Facebook, Instagram':'Scraped ads across Meta, Google, Facebook & Instagram',
    },
  };

  return (
    <div className="flex flex-col h-full overflow-hidden" style={{ backgroundColor:'#f8fafc' }}>

      {/* ── HEADER BAR ── */}
      <div className="bg-white border-b border-slate-200 px-6 py-4 flex-shrink-0 z-10">
        <div className="flex flex-col lg:flex-row lg:items-center gap-4">
          <div className="flex-shrink-0">
            <h1 className="text-slate-900" style={{ fontSize:'1.2rem', fontWeight:700 }}>
              {ru?'Социальная аналитика':'Social Intelligence'}
            </h1>
            <p className="text-xs text-slate-500 mt-0.5">
              {ru?'Мониторинг конкурентов и анализ присутствия':'Competitor monitoring & presence analysis'}
            </p>
          </div>

          <div className="flex flex-wrap items-center gap-3 flex-1 lg:pl-5 lg:border-l border-slate-100">
            <div className="flex items-center gap-2">
              <span className="text-xs text-slate-400">{ru?'Источник:':'Source:'}</span>
              <select value={primarySource.id} onChange={e => setPrimarySource(orgOptions.find(o=>o.id===e.target.value) || ALL_ORG)}
                className="px-3 py-1.5 border border-slate-200 rounded-lg text-xs bg-white focus:outline-none focus:ring-2 focus:ring-blue-500 text-slate-700" style={{ fontWeight:500 }}>
                {orgOptions.map(o=><option key={o.id} value={o.id}>{translateSocialLabel(o.name, ru)}</option>)}
              </select>
            </div>
            <div className="flex items-center gap-2">
              <span className="text-xs text-slate-400 italic">VS</span>
              <select value={secondarySource?.id||''} onChange={e=>setSecondarySource(e.target.value ? (orgOptions.find(o=>o.id===e.target.value) || null) : null)}
                className="px-3 py-1.5 border border-slate-200 rounded-lg text-xs bg-white focus:outline-none focus:ring-2 focus:ring-blue-500 text-slate-700">
                <option value="">{ru?'— Сравнить —':'— Compare with —'}</option>
                {comparisonOptions.map(o=><option key={o.id} value={o.id}>{o.name}</option>)}
              </select>
            </div>
            <div className="h-5 w-px bg-slate-200 hidden md:block" />
            <PlatformToggle selected={selectedPlatforms} onSelect={setSelectedPlatforms} />
          </div>
        </div>

        {showDashboardLoading && (
          <div className="mt-3 rounded-xl border border-slate-200 bg-white px-4 py-3 shadow-sm">
            <div className="flex items-center gap-3">
              <Loader2 className="w-4 h-4 animate-spin text-blue-600 flex-shrink-0" />
              <div>
                <p className="text-sm text-slate-900" style={{ fontWeight: 600 }}>
                  {ru ? 'Обновляем данные панели...' : 'Refreshing dashboard data...'}
                </p>
              </div>
            </div>
          </div>
        )}

        {showDashboardError && (
          <div className="mt-3 rounded-xl border border-rose-200 bg-rose-50 px-3 py-2 text-xs text-rose-700">
            {ru ? `Не удалось загрузить социальные данные: ${dashboardError}` : `Could not load social data: ${dashboardError}`}
          </div>
        )}

        {/* Tabs */}
        <div className="flex items-center gap-1 mt-4 border-b border-slate-100">
          {([
            { key:'deep',    icon:Eye,      label:ru?'Глубокий анализ':'Deep Analysis'   },
            { key:'metrics', icon:BarChart3, label:ru?'Строгие метрики':'Strict Metrics'  },
          ] as const).map(tab => (
            <button key={tab.key} onClick={()=>setActiveTab(tab.key)}
              className={`flex items-center gap-2 px-4 py-2.5 text-sm transition-colors relative ${activeTab===tab.key?'text-blue-700':'text-slate-500 hover:text-slate-700'}`}
              style={{ fontWeight:activeTab===tab.key?600:500 }}>
              <tab.icon className="w-4 h-4" />
              {tab.label}
              {activeTab===tab.key && <div className="absolute bottom-0 left-0 right-0 h-0.5 bg-blue-600 rounded-t-full" />}
            </button>
          ))}
        </div>
      </div>

      {/* ── PAGE CONTENT ── */}
      <div className="flex-1 overflow-y-auto">
        <div className="p-4 md:p-6 space-y-4 max-w-[1600px] mx-auto">

          {activeTab === 'deep' ? (
            <>
              {/* ════════ DEEP ANALYSIS TAB ════════ */}

              {/* ── TIER 1: TOPIC INTELLIGENCE ── */}
              <TierHeader tier={TIERS.topics} isOpen={openTiers.topics} onToggle={()=>toggleTier('topics')} ru={ru} />
              {openTiers.topics && (
                <div className="space-y-4">
                  <div className="grid grid-cols-1 lg:grid-cols-2 gap-4">
                    <WidgetCard
                      title={ru?'Ландшафт тем':'Topic Landscape'}
                      subtitle={ru?'Размер = кол-во упоминаний · Цвет = тональность':'Size = mention count · Color = sentiment'}
                    >
                      <div className="h-[290px]">
                        <TopicBubbleViz ru={ru} topics={topicBubbles} onTopicSelect={openSocialTopic} />
                      </div>
                    </WidgetCard>

                    <WidgetCard
                      title={ru?'Тренды тональности':'Sentiment Trends'}
                      subtitle={ru?'По дням за выбранный период':'Daily breakdown for selected period'}
                    >
                      <SentimentAreaChart ru={ru} data={sentimentTrend} />
                    </WidgetCard>
                  </div>

                  {/* Topic ranking list */}
                  <WidgetCard
                    title={ru?'Рейтинг и моментум тем':'Topic Ranking & Momentum'}
                    subtitle={ru?'Топ темы по упоминаниям, динамике и вовлечённости':'Top topics by mentions, movement, and engagement'}
                    headerRight={<span className="text-xs text-slate-400">{ru?'Последние 30 дней':'Last 30 days'}</span>}
                  >
                    <div className="max-h-[620px] space-y-3 overflow-y-auto pr-1">
                      {[...topicRanking].sort((a,b)=>b.count-a.count).map((t,i) => {
                        const sentiment = t.dominantSentiment || t.sentiment;
                        const engagementTotal = t.strictMetrics?.engagementTotal ?? 0;
                        const evidenceCount = t.strictMetrics?.evidenceCount ?? t.evidence?.length ?? 0;
                        const momentum = momentumByTopic.get(t.topic.toLowerCase());
                        const velocity = momentum?.velocity ?? t.growthPct ?? 0;
                        const hasMomentum = Boolean(momentum || t.growthReliable);
                        const isUp = velocity >= 0;
                        const col = sentiment==='positive'?C.emerald:sentiment==='negative'?C.rose:'#64748b';
                        const badgeCls = sentiment==='positive'?'bg-emerald-50 text-emerald-700':sentiment==='negative'?'bg-rose-50 text-rose-700':'bg-slate-100 text-slate-600';
                        const sourceContext = t.topEntities?.[0] || t.evidence?.[0]?.entity || t.topPlatforms?.[0] || t.evidence?.[0]?.platform;
                        return (
                          <button
                            key={t.topic}
                            type="button"
                            onClick={() => openSocialTopic(t.topic)}
                            className="block w-full rounded-xl border border-slate-100 bg-slate-50/70 p-3 text-left transition-all hover:border-slate-200 hover:bg-slate-50 hover:shadow-sm focus:outline-none focus:ring-2 focus:ring-blue-500/30"
                            aria-label={`${ru ? 'Открыть тему' : 'Open topic'} ${translateSocialLabel(t.topic, ru)}`}
                          >
                            <div className="flex items-start gap-3">
                              <span className="mt-0.5 w-5 flex-shrink-0 text-center text-xs text-slate-400" style={{ fontWeight:700 }}>{i+1}</span>
                              <div className="min-w-0 flex-1">
                                <div className="flex items-start justify-between gap-3">
                                  <div className="min-w-0">
                                    <p className="text-sm text-slate-800" style={{ fontWeight:700 }}>{translateSocialLabel(t.topic, ru)}</p>
                                    <p className="mt-1 flex min-w-0 items-center gap-1.5 text-xs italic text-slate-400">
                                      <MessageSquare className="h-3.5 w-3.5 flex-shrink-0 text-slate-300" />
                                      <span className="truncate">{topicEvidencePreview(t, ru)}</span>
                                    </p>
                                  </div>
                                  <span className={`flex-shrink-0 rounded-full px-2 py-0.5 text-[10px] ${badgeCls}`} style={{ fontWeight:600 }}>
                                    {sentimentLabel(sentiment, ru)}
                                  </span>
                                </div>

                                <div className="mt-2 flex min-w-0 flex-wrap items-center gap-x-3 gap-y-1.5">
                                  <span className="rounded bg-white px-1.5 py-0.5 text-xs text-slate-500">
                                    {t.count.toLocaleString()} {ru ? 'упоминаний' : 'mentions'}
                                  </span>
                                  <span className={`inline-flex items-center gap-0.5 text-xs ${hasMomentum ? (isUp ? 'text-emerald-600' : 'text-rose-500') : 'text-slate-400'}`} style={{ fontWeight:700 }}>
                                    {hasMomentum ? (isUp ? <ArrowUpRight className="h-3.5 w-3.5" /> : <ArrowDownRight className="h-3.5 w-3.5" />) : null}
                                    {hasMomentum ? `${isUp ? '+' : ''}${Number(velocity).toFixed(1)}%` : (ru ? 'мало данных' : 'low evidence')}
                                  </span>
                                  {engagementTotal > 0 && (
                                    <span className="text-xs text-slate-500">
                                      {engagementTotal.toLocaleString()} {ru ? 'реакц.' : 'eng.'}
                                    </span>
                                  )}
                                  {evidenceCount > 0 && (
                                    <span className="text-xs text-slate-500">
                                      {evidenceCount} {ru ? 'доказ.' : 'evidence'}
                                    </span>
                                  )}
                                  {sourceContext && (
                                    <span className="max-w-[220px] truncate rounded bg-white px-1.5 py-0.5 text-xs text-slate-400">
                                      {sourceContext}
                                    </span>
                                  )}
                                </div>

                                <div className="mt-2.5 h-2 rounded-full border border-slate-100 bg-white">
                                  <div
                                    className="h-full rounded-full transition-all duration-700"
                                    style={{ width:`${(t.count / maxTopicRankingCount) * 100}%`, backgroundColor:col, opacity:0.72 }}
                                  />
                                </div>
                              </div>
                            </div>
                          </button>
                        );
                      })}
                    </div>
                  </WidgetCard>
                </div>
              )}

              {/* ── TIER 2: INTENT & SIGNAL CLASSIFICATION ── */}
              <TierHeader tier={TIERS.intent} isOpen={openTiers.intent} onToggle={()=>toggleTier('intent')} ru={ru} />
              {openTiers.intent && (
                <div className="space-y-4">
                  {intentSignals.length === 0 ? (
                    <WidgetCard title={ru ? 'AI-обзоры подготавливаются' : 'AI briefs are warming'} subtitle={ru ? 'Карточки появятся после достаточного объёма новых обработанных постов.' : 'Cards will appear after enough new processed parent posts are available.'}>
                      <div className="rounded-2xl border border-dashed border-slate-200 bg-slate-50/70 p-6 text-sm text-slate-500">
                        {ru
                          ? 'Социальный worker создаёт эти карточки отдельно от загрузки дашборда. Это экономит AI-токены и не замедляет страницу.'
                          : 'The Social worker creates these cards outside dashboard loading. This keeps AI cost low and keeps the page fast.'}
                      </div>
                    </WidgetCard>
                  ) : (
                    <div className="grid grid-cols-1 sm:grid-cols-2 xl:grid-cols-3 gap-3">
                      {intentSignals.map(sig => {
                        const Icon = sig.icon || HelpCircle;
                        const title = aiBriefTitle(sig, ru);
                        const summary = aiBriefSummary(sig, ru);
                        const topic = translateSocialLabel(sig.main_topic || '', ru);
                        const sentiment = String(sig.sentiment || 'neutral').toLowerCase();
                        return (
                          <div key={`${sig.intent}-${title}`} className="bg-white rounded-2xl border border-slate-200 shadow-sm p-4 hover:shadow-md hover:border-slate-300 transition-all">
                            <div className="flex items-start justify-between gap-3 mb-3">
                              <div className="flex items-center gap-2 min-w-0">
                                <div className="w-8 h-8 rounded-xl flex flex-shrink-0 items-center justify-center" style={{ backgroundColor:`${sig.color}15` }}>
                                  <Icon className="w-3.5 h-3.5" style={{ color:sig.color }} />
                                </div>
                                <div className="min-w-0">
                                  <p className="truncate text-[11px] uppercase tracking-wide text-slate-400" style={{ fontWeight:700 }}>{translateSocialLabel(sig.family || sig.intent, ru)}</p>
                                  <p className="truncate text-sm text-slate-900" style={{ fontWeight:700 }}>{title}</p>
                                </div>
                              </div>
                              <DeltaBadge value={sig.delta} suffix="%" />
                            </div>
                            <p className="min-h-[54px] text-xs leading-relaxed text-slate-600">{summary}</p>
                            <div className="mt-3 flex flex-wrap items-center gap-1.5">
                              {topic && (
                                <span className="max-w-full truncate rounded-full bg-slate-100 px-2 py-0.5 text-[10px] text-slate-600" style={{ fontWeight:600 }}>
                                  {topic}
                                </span>
                              )}
                              <span className="rounded-full bg-slate-50 px-2 py-0.5 text-[10px] text-slate-500" style={{ fontWeight:600 }}>
                                {sentimentLabel(sentiment, ru)}
                              </span>
                              <span className="rounded-full bg-blue-50 px-2 py-0.5 text-[10px] text-blue-700" style={{ fontWeight:700 }}>
                                {confidenceLabel(sig.confidence, ru)}
                              </span>
                            </div>
                            <div className="mt-3 flex items-end justify-between gap-3">
                              <div>
                                <p className="text-xl text-slate-900" style={{ fontWeight:700 }}>{sig.count}</p>
                                <p className="text-[10px] text-slate-400">{ru ? 'сигналов' : 'signals'}</p>
                              </div>
                              <div className="h-1.5 min-w-[96px] flex-1 overflow-hidden rounded-full bg-slate-100">
                                <div className="h-full rounded-full" style={{ width:`${Math.min(100, Math.max(4, Number(sig.pct) || 0))}%`, backgroundColor:sig.color }} />
                              </div>
                            </div>
                          </div>
                        );
                      })}
                    </div>
                  )}

                  <div className="grid grid-cols-1 lg:grid-cols-2 gap-4">
                    <WidgetCard title={ru?'Топ сигналы':'Top Signals'} subtitle={ru?'AI-обзоры с доказательствами из постов и комментариев':'AI signal briefs with post and comment evidence'}>
                      <div className="space-y-3 max-h-[310px] overflow-y-auto pr-1">
                        {topSignals.length === 0 ? (
                          <div className="rounded-xl border border-dashed border-slate-200 bg-slate-50/70 p-4 text-xs text-slate-500">
                            {ru ? 'Пока нет сохранённых AI-сигналов.' : 'No saved AI signal evidence yet.'}
                          </div>
                        ) : topSignals.map(sig => {
                          const Icon = sig.icon || HelpCircle;
                          const title = aiBriefTitle(sig, ru);
                          const summary = aiBriefSummary(sig, ru);
                          const family = translateSocialLabel(sig.family || sig.intent, ru);
                          const evidence = (sig.examples || []).map(readableEvidence).filter(Boolean).slice(0, 3);
                          return (
                            <div key={`${sig.intent}-${title}`} className="rounded-xl border border-slate-100 overflow-hidden">
                              <div className="flex items-center gap-2.5 px-3.5 py-2.5" style={{ backgroundColor:`${sig.color}0d` }}>
                                <Icon className="w-3.5 h-3.5 flex-shrink-0" style={{ color:sig.color }} />
                                <div className="min-w-0 flex-1">
                                  <p className="truncate text-xs text-slate-800" style={{ fontWeight:700 }}>{title}</p>
                                  {summary && <p className="mt-0.5 line-clamp-2 text-[11px] leading-snug text-slate-500">{summary}</p>}
                                </div>
                                <span className="ml-auto text-[10px] text-slate-400" style={{ fontWeight:500 }}>{sig.count} {ru?'сигн.':'signals'}</span>
                              </div>
                              <div className="px-3.5 py-2 space-y-1.5 bg-white">
                                <div className="mb-2 flex flex-wrap items-center gap-1.5">
                                  {family && <span className="rounded-full bg-slate-100 px-2 py-0.5 text-[10px] text-slate-600">{family}</span>}
                                  {sig.sentiment && <span className="rounded-full px-2 py-0.5 text-[10px]" style={{ backgroundColor: `${colorForIntent(sig.sentiment)}14`, color: colorForIntent(sig.sentiment) }}>{translateSocialLabel(sig.sentiment, ru)}</span>}
                                  {typeof sig.confidence === 'number' && <span className="rounded-full bg-blue-50 px-2 py-0.5 text-[10px] text-blue-700">{confidenceLabel(sig.confidence, ru)}</span>}
                                </div>
                                {evidence.length === 0 ? (
                                  <p className="text-[11px] text-slate-400">{ru ? 'Доказательства будут добавлены после следующего AI-обзора.' : 'Evidence will appear after the next AI brief refresh.'}</p>
                                ) : evidence.map((ex,i)=>(
                                  <div key={i} className="flex items-start gap-2">
                                    <MessageSquare className="w-3 h-3 text-slate-300 mt-0.5 flex-shrink-0" />
                                    <span className="text-[11px] text-slate-500 italic">"{ex}"</span>
                                  </div>
                                ))}
                              </div>
                            </div>
                          );
                        })}
                      </div>
                    </WidgetCard>

                    <WidgetCard
                      title={ru?'Динамика сигналов':'Signal Trend Over Time'}
                      subtitle={
                        signalTrendIsCurrentSnapshot
                          ? (ru ? 'Текущий AI-срез; история появится после следующих обновлений' : 'Current AI snapshot; history builds after future refreshes')
                          : (ru ? 'История сохранённых AI-сигналов по типам' : 'Saved AI signal history by family')
                      }
                    >
                      <SignalTrendChart ru={ru} data={signalTrend} />
                    </WidgetCard>
                  </div>
                </div>
              )}

              {/* ── TIER 3: QUESTION INTELLIGENCE ── */}
              <TierHeader tier={TIERS.questions} isOpen={openTiers.questions} onToggle={()=>toggleTier('questions')} ru={ru} />
              {openTiers.questions && (
                <div className="space-y-4">
                  <WidgetCard
                    title={ru?'Топ вопросы аудитории':'Top Audience Questions'}
                    headerRight={
                      <div className="flex items-center gap-2 flex-shrink-0">
                        <span className="text-xs text-slate-500">{topQuestions.reduce((sum, question) => sum + question.count, 0)} {ru?'вопросов':'questions'}</span>
                        <span className="text-[11px] bg-amber-50 text-amber-700 border border-amber-200 px-2 py-0.5 rounded-full" style={{ fontWeight:600 }}>{topQuestions.reduce((sum, question) => sum + Number(question.evidence_count || 0), 0)} {ru?'доказ.':'evidence'}</span>
                        <span className="text-xs text-slate-400 bg-slate-100 px-2 py-0.5 rounded-full">{topQuestions.length} {ru?'активных':'active'}</span>
                      </div>
                    }
                  >
                    {topQuestions.length === 0 ? (
                      <div className="rounded-xl border border-dashed border-slate-200 bg-slate-50/70 p-5 text-sm text-slate-500">
                        {ru ? 'Пока нет сохранённых AI-вопросов аудитории.' : 'No saved AI audience questions yet.'}
                      </div>
                    ) : <div className="overflow-x-auto">
                      <table className="w-full text-left">
                        <thead>
                          <tr className="border-b border-slate-100">
                            {['#',ru?'Вопрос':'Question',ru?'Источник':'Source',ru?'Тема':'Topic',ru?'Сигналы':'Signals',ru?'Уверенность':'Confidence',ru?'Доказ.':'Evidence'].map((h,i)=>(
                              <th key={i} className="pb-3 text-[11px] text-slate-500 pr-4 last:pr-0" style={{ fontWeight:600 }}>{h}</th>
                            ))}
                          </tr>
                        </thead>
                        <tbody className="divide-y divide-slate-50">
                          {topQuestions.map((q,i)=>{
                            const questionText = ru ? (q.question_ru || q.question) : (q.question_en || q.question);
                            const source = q.sources?.[0] || (ru ? 'Аудитория' : 'Audience');
                            const evidence = q.evidence_quotes?.[0];
                            return (
                            <tr key={`${q.question}-${i}`} className="hover:bg-slate-50/50 transition-colors">
                              <td className="py-3 text-xs text-slate-400 pr-4">{i+1}</td>
                              <td className="py-3 text-sm text-slate-800 pr-4 max-w-[360px]" style={{ fontWeight:500 }}>
                                <p>{questionText}</p>
                                {evidence && <p className="mt-1 text-[11px] italic leading-snug text-slate-400">"{evidence}"</p>}
                              </td>
                              <td className="py-3 pr-4">
                                <span className="text-[11px] px-2 py-0.5 rounded-full" style={{ fontWeight:500, backgroundColor:`${entityColors[source] || colorForEntity(source)}15`, color:entityColors[source] || colorForEntity(source) }}>{translateSocialLabel(source, ru)}</span>
                              </td>
                              <td className="py-3 pr-4 text-xs text-slate-500">{translateSocialLabel(q.topic || 'Question', ru)}</td>
                              <td className="py-3 pr-4 text-sm text-slate-700 text-right" style={{ fontWeight:700 }}>{q.count}</td>
                              <td className="py-3 pr-4 text-xs text-slate-500">
                                {confidenceLabel(q.confidence, ru)}
                              </td>
                              <td className="py-3">
                                <span className="text-[11px] px-2.5 py-1 rounded-full bg-blue-50 text-blue-700" style={{ fontWeight:600 }}>
                                  {q.evidence_count || q.evidence_ids?.length || 0}
                                </span>
                              </td>
                            </tr>
                            );
                          })}
                        </tbody>
                      </table>
                    </div>}
                  </WidgetCard>
                </div>
              )}

              {/* ── TIER 4: AUDIENCE RESPONSE ── */}
              <TierHeader tier={TIERS.audience} isOpen={openTiers.audience} onToggle={()=>toggleTier('audience')} ru={ru} />
              {openTiers.audience && (
                <div className="space-y-4">
                  <div className="grid grid-cols-1 lg:grid-cols-2 gap-4">
                    {/* Sentiment by source */}
                    <WidgetCard title={ru?'Тональность по источникам':'Sentiment by Source'} subtitle={ru?'Распределение позитива, нейтрала и негатива по источникам':'Positive, neutral & negative breakdown by source'}>
                      {sentimentByEntity.length === 0 ? (
                        <div className="rounded-xl border border-dashed border-slate-200 bg-slate-50/70 p-5 text-sm text-slate-500">
                          {ru ? 'Пока нет данных тональности по источникам.' : 'No source sentiment data yet.'}
                        </div>
                      ) : <div className="space-y-5">
                        {sentimentByEntity.map(item=>(
                          <button
                            key={item.entity}
                            type="button"
                            onClick={() => openSocialSourceEvidence(item)}
                            className="block w-full rounded-xl p-2 text-left transition-colors hover:bg-slate-50 focus:outline-none focus:ring-2 focus:ring-blue-200"
                            aria-label={ru ? `Открыть доказательства источника ${item.entity}` : `Open evidence for ${item.entity}`}
                          >
                            <div className="flex items-center justify-between mb-2">
                              <div className="flex items-center gap-2">
                                <div className="w-2.5 h-2.5 rounded-full" style={{ backgroundColor:entityColors[item.entity] || colorForEntity(item.entity) }} />
                                <span className="text-sm text-slate-700" style={{ fontWeight:600 }}>{item.entity}</span>
                              </div>
                              <span className="text-xs text-slate-400">{item.total.toLocaleString()} {ru?'упом.':'mentions'}</span>
                            </div>
                            <div className="h-3 w-full flex rounded-full overflow-hidden gap-px">
                              <div className="transition-all rounded-l-full" style={{ width:`${item.pos}%`, backgroundColor:C.emerald }} />
                              <div className="transition-all"              style={{ width:`${item.neu}%`, backgroundColor:'#cbd5e1' }} />
                              <div className="transition-all rounded-r-full" style={{ width:`${item.neg}%`, backgroundColor:C.rose  }} />
                            </div>
                            <div className="flex items-center justify-between mt-1.5">
                              <span className="text-[10px] text-emerald-600" style={{ fontWeight:600 }}>{item.pos}%</span>
                              <span className="text-[10px] text-slate-400">{item.neu}%</span>
                              <span className="text-[10px] text-rose-500" style={{ fontWeight:600 }}>{item.neg}%</span>
                            </div>
                          </button>
                        ))}
                      </div>}
                      <ChartLegend items={[
                        { label:ru?'Позитив':'Positive', color:C.emerald },
                        { label:ru?'Нейтрал':'Neutral',  color:'#cbd5e1' },
                        { label:ru?'Негатив':'Negative', color:C.rose    },
                      ]} />
                    </WidgetCard>

                    {/* Pain points */}
                    <WidgetCard title={ru?'Ключевые боли':'Pain Points & Issues'} subtitle={ru?'Наиболее частые негативные темы':'Most frequently raised negative themes'}>
                      {painPoints.length === 0 ? (
                        <div className="rounded-xl border border-dashed border-slate-200 bg-slate-50/70 p-5 text-sm text-slate-500">
                          {ru ? 'AI-карточки проблем появятся после следующего обновления social brief.' : 'AI problem cards will appear after the next Social AI brief refresh.'}
                        </div>
                      ) : <div className="space-y-3">
                        {painPoints.map((pp,i)=>{
                          const severityColor = pp.severity==='high'?C.rose:C.amber;
                          const title = ru ? (pp.problem_ru || pp.title_ru || pp.problem || pp.text || '') : (pp.problem_en || pp.title_en || pp.problem || pp.text || '');
                          const summary = ru ? pp.summary_ru : pp.summary_en;
                          const source = pp.sources?.[0] || pp.entities?.[0] || (ru ? 'Аудитория' : 'Audience');
                          const evidence = pp.evidence_quotes?.[0];
                          return (
                            <div key={i} className="flex items-start gap-3 p-3.5 rounded-xl border border-slate-100 hover:border-slate-200 transition-colors bg-slate-50/50">
                              <div className="flex-shrink-0 w-8 h-8 rounded-xl flex items-center justify-center mt-0.5" style={{ backgroundColor:`${severityColor}15` }}>
                                <span className="text-xs" style={{ fontWeight:800, color:severityColor }}>{i+1}</span>
                              </div>
                              <div className="flex-1 min-w-0">
                                <div className="mb-2 flex flex-wrap items-center justify-between gap-2">
                                  <p className="text-sm text-slate-800" style={{ fontWeight:600 }}>{title}</p>
                                  <span className="text-[11px] rounded-full bg-blue-50 px-2 py-0.5 text-blue-700" style={{ fontWeight:600 }}>{confidenceLabel(pp.confidence, ru)}</span>
                                </div>
                                {summary && <p className="mb-2 text-xs leading-relaxed text-slate-500">{summary}</p>}
                                <div className="flex flex-wrap items-center gap-1.5">
                                  <span className="text-[10px] px-2 py-0.5 rounded-full" style={{ fontWeight:500, backgroundColor:`${entityColors[source] || colorForEntity(source)}15`, color:entityColors[source] || colorForEntity(source) }}>{translateSocialLabel(source, ru)}</span>
                                  <span className="text-[10px] rounded-full bg-slate-100 px-2 py-0.5 text-slate-500">{translateSocialLabel(pp.topic || 'Audience Issue', ru)}</span>
                                  <span className="text-[10px] rounded-full bg-rose-50 px-2 py-0.5 text-rose-600">{pp.count} {ru?'сигн.':'signals'}</span>
                                  <span className="text-[10px] rounded-full bg-amber-50 px-2 py-0.5 text-amber-700">{pp.evidence_count || pp.evidence_ids?.length || 0} {ru?'доказ.':'evidence'}</span>
                                </div>
                                {evidence && <p className="mt-2 text-[11px] italic leading-snug text-slate-400">"{evidence}"</p>}
                              </div>
                            </div>
                          );
                        })}
                      </div>}
                    </WidgetCard>
                  </div>

                  {/* Community Interests */}
                  <WidgetCard
                    title={ru ? 'Фокусы обсуждения' : 'Discussion Focus Areas'}
                    subtitle={ru ? 'Доля social-упоминаний по конкретным направлениям обсуждения' : 'Share of social topic mentions by specific discussion focus'}
                  >
                    {communityInterests.length === 0 ? (
                      <div className="rounded-xl border border-dashed border-slate-200 bg-slate-50/70 p-5 text-sm text-slate-500">
                        {ru ? 'Фокусы обсуждения появятся, когда за выбранный период будут найдены semantic-темы.' : 'Discussion focus areas will appear when semantic topics are available for the selected period.'}
                      </div>
                    ) : (
                      <div className="space-y-4">
                        <div className="rounded-2xl border border-slate-100 bg-slate-50/70 px-3 py-4">
                          <div className="h-[360px]">
                            <ResponsiveContainer width="100%" height="100%">
                              <RadarChart data={communityInterests} outerRadius="74%">
                                <PolarGrid stroke="#dbe3ee" radialLines />
                                <PolarAngleAxis dataKey="interestLabel" tick={<CommunityInterestAxisTick />} />
                                <PolarRadiusAxis
                                  angle={18}
                                  domain={[0, communityInterestScale]}
                                  ticks={communityInterestTicks}
                                  tick={{ fontSize: 10, fill: C.muted }}
                                  tickFormatter={(value) => `${value}%`}
                                />
                                <Radar
                                  name={ru ? 'Фокусы обсуждения' : 'Discussion Focus Areas'}
                                  dataKey="score"
                                  stroke="#0f766e"
                                  fill="#14b8a6"
                                  fillOpacity={0.24}
                                  strokeWidth={3}
                                  dot={{ r: 3, fill: '#0f766e', strokeWidth: 0 }}
                                />
                                <Tooltip
                                  {...TOOLTIP_STYLE}
                                  formatter={(value: unknown, _name: string, item: any) => [
                                    `${Number(value || 0).toFixed(1)}%`,
                                    item?.payload?.mentions
                                      ? `${item.payload.mentions} ${ru ? 'упоминаний' : 'mentions'}`
                                      : (ru ? 'Доля' : 'Share'),
                                  ]}
                                />
                              </RadarChart>
                            </ResponsiveContainer>
                          </div>
                          <div className="mt-2 flex items-center justify-between text-xs text-slate-500">
                            <span>{ru ? 'Шкала отображения' : 'Display scale'}: 0-{communityInterestScale}%</span>
                            <span>{ru ? 'Пик периода' : 'Window peak'}: {communityInterestPeak.toFixed(1)}%</span>
                          </div>
                        </div>
                        <div className="grid grid-cols-1 gap-2 md:grid-cols-2">
                          {topCommunityInterests.map((item) => (
                            <div key={item.interest} className="rounded-xl border border-slate-100 bg-white px-3 py-2 text-sm shadow-sm">
                              <div className="flex items-center justify-between gap-3">
                                <span className="flex min-w-0 items-center gap-2 font-medium text-slate-700">
                                  <span className="h-2 w-2 flex-shrink-0 rounded-full bg-teal-500" />
                                  <span className="truncate">{item.interestLabel}</span>
                                </span>
                                <span className="font-semibold text-slate-900">{Number(item.score || 0).toFixed(1)}%</span>
                              </div>
                              {communityInterestTopics(item) && (
                                <div className="mt-1 truncate pl-4 text-xs text-slate-500">
                                  {communityInterestTopics(item)}
                                </div>
                              )}
                            </div>
                          ))}
                        </div>
                        {topCommunityInterests.length >= 2 && (
                          <div className="rounded-xl border border-teal-100 bg-teal-50 px-4 py-3 text-sm text-teal-900">
                            <span className="font-semibold">{ru ? 'Приоритетный сигнал' : 'Priority signal'}: </span>
                            {topCommunityInterests[0].interestLabel} ({Number(topCommunityInterests[0].score || 0).toFixed(1)}%) {ru ? 'и' : 'and'}{' '}
                            {topCommunityInterests[1].interestLabel} ({Number(topCommunityInterests[1].score || 0).toFixed(1)}%) {ru ? 'ведут обсуждение' : 'lead discussion'}
                            {(communityInterestTopics(topCommunityInterests[0]) || communityInterestTopics(topCommunityInterests[1])) && (
                              <>
                                {' '}
                                {ru ? 'через темы: ' : 'through topics: '}
                                {[communityInterestTopics(topCommunityInterests[0]), communityInterestTopics(topCommunityInterests[1])].filter(Boolean).join(' · ')}
                              </>
                            )}
                            .
                          </div>
                        )}
                      </div>
                    )}
                  </WidgetCard>
                </div>
              )}


            </>
          ) : (
            <>
              {/* ════════ STRICT METRICS TAB ════════ */}

              {/* KPI Summary row */}
              <div className="grid grid-cols-2 md:grid-cols-5 gap-3">
                {[
                  { icon:Users,         color:C.blue,    label:ru?'Отслеживаемых':'Tracked',     val:trackedCount, sub:ru?'источников':'sources'   },
                  { icon:MessageCircle, color:C.violet,  label:ru?'Постов (диапазон)':'Posts (range)', val:postsCount, sub:commentsCount ? (ru?`${commentsCount} коммент.`:`${commentsCount} comments`) : (ru?'без комментариев':'comments separate') },
                  { icon:Megaphone,     color:C.amber,   label:ru?'Рекламы':'Ads Found',         val:adsCount, sub:ru?'объявлений в диапазоне':'ads in range' },
                  { icon:Heart,         color:C.emerald, label:ru?'Настроение':'Avg Sentiment',  val:avgPositive, sub:ru?'позитив':'positive' },
                  { icon:Hash,          color:C.pink,    label:ru?'Топ тема':'Top Topic',        val:topTopic, sub:ru?'по упоминаниям':'by mentions' },
                ].map((kpi,i)=>{
                  const Icon = kpi.icon;
                  return (
                    <div key={i} className="bg-white rounded-2xl border border-slate-200 shadow-sm px-4 py-4">
                      <div className="flex items-center gap-2 mb-3">
                        <div className="w-7 h-7 rounded-lg flex items-center justify-center" style={{ backgroundColor:`${kpi.color}18` }}>
                          <Icon className="w-3.5 h-3.5" style={{ color:kpi.color }} />
                        </div>
                        <span className="text-[11px] text-slate-500" style={{ fontWeight:500 }}>{kpi.label}</span>
                      </div>
                      <p className="text-xl text-slate-900 truncate" style={{ fontWeight:700 }} title={kpi.val}>{kpi.val}</p>
                      <p className="text-[10px] text-slate-400 mt-0.5">{kpi.sub}</p>
                    </div>
                  );
                })}
              </div>

              {/* ── TIER: VISIBILITY & REACH ── */}
              <TierHeader tier={TIERS.visibility} isOpen={openTiers.visibility} onToggle={()=>toggleTier('visibility')} ru={ru} />
              {openTiers.visibility && (
                <div className="space-y-4">
                  <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-4 gap-4">
                    {visibilityData.map(v=>{
                      const color = entityColors[v.entity] || colorForEntity(v.entity);
                      const interactionsValue = formatCompactMetric(v.interactions);
                      return (
                        <div key={v.entity} className="bg-white rounded-2xl border border-slate-200 shadow-sm p-5 overflow-hidden relative">
                          <div className="absolute top-0 left-0 right-0 h-1 rounded-t-2xl" style={{ backgroundColor:color }} />
                          <div className="flex items-center gap-2 mb-4 mt-1">
                            <div className="w-8 h-8 rounded-xl flex items-center justify-center text-sm text-white" style={{ backgroundColor:color, fontWeight:700 }}>{v.entity[0]}</div>
                            <span className="text-sm text-slate-800" style={{ fontWeight:700 }}>{v.entity}</span>
                          </div>
                          <div className="mb-3">
                            <div className="flex items-baseline justify-between mb-1">
                              <span className="text-[11px] text-slate-500">{ru?'Видимость':'Visibility Score'}</span>
                              <DeltaBadge value={v.delta} suffix="%" />
                            </div>
                            <p className="text-3xl" style={{ fontWeight:800, color }}>{v.visibility}%</p>
                            <div className="mt-2 h-1.5 bg-slate-100 rounded-full overflow-hidden">
                              <div className="h-full rounded-full" style={{ width:`${v.visibility}%`, backgroundColor:color }} />
                            </div>
                          </div>
                          <div className="grid grid-cols-3 gap-2 pt-3 border-t border-slate-100">
                            {[
                              { label:ru?'Охват':'Reach',   val:formatCompactMetric(v.reach),      delta:v.deltaReach  },
                              { label:ru?'Реакции':'Interactions', val:interactionsValue,           delta:v.deltaEngage },
                              { label:'SoV',                  val:`${v.sov}%`,                    delta:v.deltaSov    },
                            ].map((m,i)=>(
                              <div key={i}>
                                <p className="text-[10px] text-slate-400 mb-0.5">{m.label}</p>
                                <p className="text-xs text-slate-800" style={{ fontWeight:700 }}>{m.val}</p>
                                <DeltaBadge value={m.delta} suffix="%" />
                              </div>
                            ))}
                          </div>
                        </div>
                      );
                    })}
                  </div>

                  <div className="grid grid-cols-1 lg:grid-cols-3 gap-4 items-stretch">
                    <div className="lg:col-span-2 flex flex-col">
                      <WidgetCard title={ru?'Динамика видимости':'Visibility Trend'} subtitle={ru?'Доля упоминаний по дням':'Daily share of organic mentions'} className="flex-1">
                        <ResponsiveContainer width="100%" height={260}>
                          <LineChart data={visibilityTrend} margin={{ top:16, right:16, left:-10, bottom:0 }}>
                            <CartesianGrid {...GRID_COMMON} />
                            <XAxis dataKey="day" {...AXIS_COMMON} dy={8} />
                            <YAxis {...AXIS_COMMON} domain={[0, 100]} tickCount={6} />
                            <Tooltip {...TOOLTIP_STYLE} formatter={(v: any, name: string) => [`${v}%`, name]} />
                            {chartSeries.map((series, index) => (
                              <Line
                                key={series.key}
                                type="monotone"
                                dataKey={series.key}
                                stroke={series.color}
                                strokeWidth={2.5}
                                dot={{ r:4, fill:series.color, strokeWidth:2, stroke:'white' }}
                                activeDot={{ r:5 }}
                                name={series.label}
                                isAnimationActive={false}
                                strokeDasharray={index === 0 ? undefined : index === 1 ? '6 2' : index === 2 ? '3 3' : '8 3'}
                              />
                            ))}
                          </LineChart>
                        </ResponsiveContainer>
                        <ChartLegend items={chartSeries.map(series => ({ label: series.label, color: series.color }))} />
                      </WidgetCard>
                    </div>

                    <div className="flex flex-col">
                      <WidgetCard title={ru?'Доля голоса':'Share of Voice'} subtitle={ru?'Распределение упоминаний':'Mention share distribution'} className="flex-1">
                        <ResponsiveContainer width="100%" height={200}>
                          <PieChart>
                            <Pie data={sovData} cx="50%" cy="50%" innerRadius={58} outerRadius={88} paddingAngle={3} dataKey="value">
                              {sovData.map((entry,i)=><Cell key={i} fill={entry.color || colorForEntity(entry.name, i)} strokeWidth={0} />)}
                            </Pie>
                            <Tooltip {...TOOLTIP_STYLE} formatter={(v:any)=>[`${v}%`,'']} />
                          </PieChart>
                        </ResponsiveContainer>
                        <div className="space-y-2.5 mt-3">
                          {[...sovData].sort((a,b)=>b.value-a.value).map((d,i)=>(
                            <div key={d.name} className="flex items-center gap-2.5 py-1 border-b border-slate-50 last:border-0">
                              <span className="text-xs text-slate-400 w-4 text-center" style={{ fontWeight:600 }}>#{i+1}</span>
                              <div className="w-2.5 h-2.5 rounded-full flex-shrink-0" style={{ backgroundColor:d.color || colorForEntity(d.name, i) }} />
                              <span className="text-xs text-slate-600 flex-1">{d.name}</span>
                              <div className="w-16 h-1.5 bg-slate-100 rounded-full overflow-hidden">
                                <div className="h-full rounded-full" style={{ width:`${(d.value/Math.max(1, ...sovData.map(item => item.value)))*100}%`, backgroundColor:d.color || colorForEntity(d.name, i) }} />
                              </div>
                              <span className="text-xs text-slate-800 w-10 text-right" style={{ fontWeight:700 }}>{d.value}%</span>
                            </div>
                          ))}
                        </div>
                      </WidgetCard>
                    </div>
                  </div>

                </div>
              )}

              {/* ── TIER: ORGANIC POSTS ── */}
              <TierHeader tier={TIERS.content} isOpen={openTiers.content} onToggle={()=>toggleTier('content')} ru={ru} />
              {openTiers.content && (
                <div className="space-y-4">
                  <OrganicPostsTable ru={ru} items={organicPostItems} />
                </div>
              )}

              {/* ── TIER: COMPETITIVE SCORECARD — AD INTELLIGENCE ── */}
              {adItems.length > 0 && (
                <>
                  <TierHeader tier={TIERS.scorecard} isOpen={openTiers.scorecard} onToggle={()=>toggleTier('scorecard')} ru={ru} />
                  {openTiers.scorecard && (
                    <div className="space-y-4">
                      <AdScrapeTable ru={ru} items={adItems} />
                    </div>
                  )}
                </>
              )}
            </>
          )}

          <div className="h-4" />
        </div>
      </div>
    </div>
  );
}
