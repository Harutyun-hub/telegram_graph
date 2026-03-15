"""
Channel Classification Utility

Classifies Telegram channels into categories based on content analysis.
"""
import re
from typing import List, Optional

# Keywords for each category in multiple languages
CATEGORY_KEYWORDS = {
    'Work': {
        'en': ['job', 'work', 'career', 'employment', 'recruit', 'hiring', 'vacancy',
               'salary', 'resume', 'cv', 'interview', 'position', 'opportunity'],
        'ru': ['работа', 'вакансия', 'карьера', 'найм', 'резюме', 'собеседование',
               'зарплата', 'должность', 'трудоустройство']
    },
    'Housing': {
        'en': ['housing', 'rent', 'apartment', 'real estate', 'room', 'flat', 'house',
               'accommodation', 'lease', 'landlord', 'tenant', 'mortgage'],
        'ru': ['жилье', 'квартира', 'аренда', 'недвижимость', 'комната', 'дом',
               'съем', 'снять', 'сдать', 'арендодатель', 'ипотека']
    },
    'Family': {
        'en': ['family', 'children', 'parent', 'school', 'education', 'kindergarten',
               'daycare', 'pediatric', 'maternity', 'baby', 'kid'],
        'ru': ['семья', 'дети', 'родители', 'школа', 'образование', 'детский сад',
               'ясли', 'педиатр', 'материнство', 'ребенок', 'малыш']
    },
    'Business': {
        'en': ['business', 'entrepreneur', 'startup', 'invest', 'tax', 'market',
               'finance', 'company', 'trade', 'commerce', 'venture'],
        'ru': ['бизнес', 'предприниматель', 'стартап', 'инвестиции', 'налог',
               'рынок', 'финансы', 'компания', 'торговля', 'коммерция']
    },
    'Legal': {
        'en': ['legal', 'law', 'visa', 'passport', 'residence', 'permit', 'immigration',
               'document', 'citizenship', 'registration', 'court'],
        'ru': ['легальный', 'право', 'виза', 'паспорт', 'резиденция', 'разрешение',
               'иммиграция', 'документ', 'гражданство', 'регистрация', 'суд']
    },
    'Lifestyle': {
        'en': ['lifestyle', 'food', 'restaurant', 'entertainment', 'art', 'music',
               'culture', 'event', 'sport', 'fitness', 'travel', 'leisure'],
        'ru': ['досуг', 'еда', 'ресторан', 'развлечения', 'искусство', 'музыка',
               'культура', 'мероприятие', 'спорт', 'фитнес', 'путешествие']
    }
}


def classify_channel(
    title: Optional[str] = None,
    description: Optional[str] = None,
    topics: Optional[List[str]] = None,
    threshold: float = 0.3
) -> str:
    """
    Classify a channel based on its title, description, and associated topics.

    Args:
        title: Channel title
        description: Channel description
        topics: List of topics associated with the channel
        threshold: Minimum score threshold for classification (0-1)

    Returns:
        Channel category: 'Work', 'Housing', 'Family', 'Business', 'Legal', 'Lifestyle', or 'General'
    """
    if not any([title, description, topics]):
        return 'General'

    # Combine all text for analysis
    text_to_analyze = ' '.join(filter(None, [
        title or '',
        description or '',
        ' '.join(topics or [])
    ])).lower()

    if not text_to_analyze.strip():
        return 'General'

    # Score each category
    category_scores = {}

    for category, keywords in CATEGORY_KEYWORDS.items():
        score = 0
        total_keywords = len(keywords['en']) + len(keywords['ru'])

        # Check English keywords
        for keyword in keywords['en']:
            if keyword.lower() in text_to_analyze:
                score += 1

        # Check Russian keywords
        for keyword in keywords['ru']:
            if keyword.lower() in text_to_analyze:
                score += 1

        # Normalize score
        category_scores[category] = score / total_keywords if total_keywords > 0 else 0

    # Find the best matching category
    if category_scores:
        best_category = max(category_scores.items(), key=lambda x: x[1])
        if best_category[1] >= threshold:
            return best_category[0]

    return 'General'


def get_channel_priority(channel_type: str) -> int:
    """
    Get display priority for a channel type (lower number = higher priority).

    Args:
        channel_type: The channel category

    Returns:
        Priority number (1-7)
    """
    priority_map = {
        'Work': 1,
        'Housing': 2,
        'Family': 3,
        'Business': 4,
        'Legal': 5,
        'Lifestyle': 6,
        'General': 7
    }
    return priority_map.get(channel_type, 7)


def calculate_engagement_score(
    views: int,
    forwards: int,
    comments: int,
    members: int,
    weight_views: float = 1.0,
    weight_forwards: float = 2.0,
    weight_comments: float = 3.0
) -> float:
    """
    Calculate engagement score as a percentage.

    Args:
        views: Average views per post
        forwards: Average forwards per post
        comments: Average comments per post
        members: Total channel members
        weight_views: Weight for views in calculation
        weight_forwards: Weight for forwards in calculation
        weight_comments: Weight for comments in calculation

    Returns:
        Engagement percentage (0-100)
    """
    if members <= 0:
        return 0.0

    # Calculate weighted interaction score
    interactions = (
        views * weight_views +
        forwards * weight_forwards +
        comments * weight_comments
    )

    # Calculate engagement as percentage of members
    engagement = (interactions / members) * 100

    # Cap at 100% and ensure non-negative
    return min(max(engagement, 0), 100)


def calculate_growth_rate(
    current_period_posts: int,
    previous_period_posts: int
) -> float:
    """
    Calculate growth rate between two periods.

    Args:
        current_period_posts: Number of posts in current period
        previous_period_posts: Number of posts in previous period

    Returns:
        Growth percentage (-100 to +∞)
    """
    if previous_period_posts <= 0:
        # If no posts in previous period but posts in current, show 100% growth
        return 100.0 if current_period_posts > 0 else 0.0

    growth = ((current_period_posts - previous_period_posts) / previous_period_posts) * 100
    return round(growth, 1)