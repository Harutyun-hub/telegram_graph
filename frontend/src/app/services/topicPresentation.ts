import presentation from '../config/topicPresentation.json';

type TopicPresentation = {
  groups: {
    order: string[];
    ru: Record<string, string>;
  };
  categoryToGroup: Record<string, string>;
  categoryRu: Record<string, string>;
  topicRu: Record<string, string>;
};

const topicPresentation = presentation as TopicPresentation;

export const TOPICS_PAGE_GROUPS_EN = topicPresentation.groups.order;
export const TOPICS_PAGE_GROUPS_RU = TOPICS_PAGE_GROUPS_EN.map(
  (group) => topicPresentation.groups.ru[group] || group,
);
export const CATEGORY_RU = topicPresentation.categoryRu;
export const TOPIC_RU = topicPresentation.topicRu;

function normalizeLabel(value: any): string {
  if (typeof value === 'string') return value.replace(/\s+/g, ' ').trim();
  if (value === null || value === undefined) return '';
  return String(value).replace(/\s+/g, ' ').trim();
}

export function groupTopicCategoryForTopicsPage(categoryRaw: any): string {
  const category = normalizeLabel(categoryRaw);
  if (!category) return 'Admin';
  return topicPresentation.categoryToGroup[category] || 'Admin';
}

export function translateTopicRu(topicRaw: any): string {
  const topic = normalizeLabel(topicRaw);
  if (!topic) return topic;
  return TOPIC_RU[topic] || topic;
}

export function translateCategory(categoryRaw: any, ru = false): string {
  const category = normalizeLabel(categoryRaw);
  if (!ru) return category || 'General';
  return CATEGORY_RU[category] || category || 'Общий';
}

export function translateTopicsPageGroup(groupRaw: any, ru = false): string {
  const group = normalizeLabel(groupRaw);
  if (!ru) return group;
  return topicPresentation.groups.ru[group] || group;
}
