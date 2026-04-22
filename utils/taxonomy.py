from __future__ import annotations

"""Canonical taxonomy and compatibility aliases for topic normalization and prompting."""

from typing import Iterable

TAXONOMY_VERSION = "v3.0.0"

# Domain -> Category -> Topics
TAXONOMY_DOMAINS: dict[str, dict[str, list[str]]] = {
    "Security & Geopolitics": {
        "Nagorno-Karabakh & Artsakh": [
            "Nagorno-Karabakh Conflict",
            "Karabakh Refugee Integration",
            "Artsakh Sovereignty",
            "Lachin Corridor Blockade",
            "Karabakh War Veterans",
            "POW And Hostage Crisis",
        ],
        "Regional Security": [
            "Armenian Border Security",
            "Turkish-Armenian Relation",
            "Azerbaijan Aggression",
            "Iranian-Armenian Relation",
            "Georgia-Armenia Corridor",
            "South Caucasus Stability",
        ],
        "Global Conflict": [
            "Russia-Ukraine War",
            "Middle East Conflict",
            "Syria Armenian Community",
            "NATO Expansion",
        ],
        "Military & Defense": [
            "Armenian Armed Force",
            "Military Conscription",
            "Defense Spending",
            "CSTO Membership",
            "Arms Procurement",
            "Ceasefire Monitoring",
        ],
        "Geopolitical Alignment": [
            "Pro-Russian Orientation",
            "Pro-Western Orientation",
            "EU Membership Aspiration",
            "Multi-Vector Foreign Policy",
            "Russian Influence",
            "BRICS Dynamics",
        ],
    },
    "Domestic Politics": {
        "Government & Leadership": [
            "Armenian Government Performance",
            "Prime Minister Policy",
            "Cabinet Appointment",
            "Parliament Activity",
            "Municipal Governance",
        ],
        "Opposition & Protest": [
            "Armenian Opposition Movement",
            "Political Protest",
            "Street Demonstration",
            "Political Prisoner",
            "Opposition Leadership",
        ],
        "Democracy & Reform": [
            "Electoral Integrity",
            "Constitutional Reform",
            "Judicial Independence",
            "Anti-Corruption Effort",
            "Government Transparency",
            "Media Freedom",
        ],
        "Political Ideology": [
            "Armenian Nationalism",
            "Pan-Armenian Movement",
            "Revolutionary Discourse",
            "Conservative Movement",
            "Liberal Reform",
        ],
    },
    "Economy & Finance": {
        "Macroeconomic Condition": [
            "Economic Growth",
            "Inflation And Prices",
            "Armenian Dram Exchange",
            "Currency Volatility",
            "GDP Performance",
            "Foreign Investment",
        ],
        "Employment": [
            "Unemployment Rate",
            "Job Market Condition",
            "Labor Migration",
            "Salary Dynamics",
            "Technology Sector Job",
            "Youth Unemployment",
        ],
        "Business & Enterprise": [
            "Business Opportunity",
            "Tech Startup",
            "Import Export Trade",
            "Investment Opportunity",
            "Small Business Support",
            "Agricultural Economy",
        ],
        "Financial System": [
            "Banking Sector",
            "Tax Policy",
            "Cryptocurrency Trend",
            "Russian Sanctions Impact",
            "Financial Regulation",
        ],
        "Cost Of Living": [
            "Housing Affordability",
            "Rental Market",
            "Food Price",
            "Utility Price",
            "Transportation Cost",
        ],
    },
    "Society & Daily Life": {
        "Education": [
            "School System Quality",
            "Higher Education",
            "Private Tutoring",
            "Vocational Training",
            "Educational Reform",
            "Student Life",
        ],
        "Healthcare": [
            "Healthcare Access",
            "Hospital Quality",
            "Pharmaceutical Price",
            "Mental Health",
            "Health Insurance",
            "Medical Tourism",
        ],
        "Housing & Infrastructure": [
            "Housing Market",
            "Urban Development",
            "Road And Transit",
            "Construction Quality",
            "Public Space",
            "Rural Infrastructure",
        ],
        "Family & Relationships": [
            "Family Life",
            "Childcare Access",
            "Marriage And Dating",
            "Elder Care",
            "Domestic Violence",
            "Gender Dynamics",
        ],
        "Social Services": [
            "Social Safety Net",
            "Disability Support",
            "Refugee Assistance",
            "Poverty Alleviation",
            "Pension System",
        ],
    },
    "Migration & Diaspora": {
        "Emigration": [
            "Migration Intent",
            "Brain Drain",
            "Youth Departure",
            "Visa And Residency",
            "Relocation Planning",
        ],
        "Immigration To Armenia": [
            "Russian Expat Community",
            "Karabakh Displaced Person",
            "Foreign Worker",
            "Digital Nomad Arrival",
        ],
        "Diaspora": [
            "Armenian Diaspora Identity",
            "Diaspora Engagement",
            "Repatriation Effort",
            "Homeland Connection",
            "Diaspora Investment",
        ],
        "Integration": [
            "Cultural Adaptation",
            "Language Barrier",
            "Citizenship Issue",
            "Community Acceptance",
        ],
    },
    "Culture & Identity": {
        "National Identity": [
            "Armenian National Identity",
            "Post-Soviet Identity",
            "Collective Memory",
            "Armenian Genocide Remembrance",
            "Cultural Heritage Preservation",
        ],
        "Religion": [
            "Armenian Apostolic Church",
            "Religious Practice",
            "Church-State Relation",
            "Interfaith Dialogue",
            "Orthodox Christianity",
        ],
        "Language & Communication": [
            "Armenian Language",
            "Russian Language Use",
            "Code-Switching Practice",
            "Language Policy",
        ],
        "Arts & Entertainment": [
            "Armenian Music",
            "Film And Cinema",
            "Literature",
            "Cultural Festival",
            "Sports Event",
            "Football Match",
        ],
        "Community Life": [
            "Community Solidarity",
            "Volunteer Movement",
            "Neighborhood Relation",
            "Local Celebration",
            "Soviet Nostalgia",
        ],
    },
    "Media & Information": {
        "Information Integrity": [
            "Russian Propaganda",
            "Disinformation Campaign",
            "Information Warfare",
            "Fake News Detection",
            "Bot Activity",
        ],
        "Media Landscape": [
            "Media And News",
            "Social Media Trend",
            "Telegram Community",
            "Independent Journalism",
            "Media Censorship",
        ],
        "Narrative & Frame": [
            "War Narrative",
            "Government Narrative",
            "Opposition Narrative",
            "Foreign Media Frame",
            "Conspiracy Theory",
        ],
    },
    "Technology & Digital": {
        "Digital Services": [
            "E-Government Service",
            "Digital Payment",
            "Mobile Application",
            "Internet Access",
            "Telecom Service",
        ],
        "Tech Economy": [
            "Tech Industry Growth",
            "AI Development",
            "Software Export",
            "Startup Ecosystem",
        ],
        "Digital Rights": [
            "Online Privacy",
            "Digital Surveillance",
            "Content Moderation",
            "Platform Regulation",
        ],
    },
}

# Domain/category compatibility aliases during migration window.
DOMAIN_ALIASES: dict[str, str] = {
    "security and geopolitics": "Security & Geopolitics",
    "politics and governance": "Domestic Politics",
    "economy and work": "Economy & Finance",
    "society and daily life": "Society & Daily Life",
    "migration and identity": "Migration & Diaspora",
    "culture and community identity": "Culture & Identity",
    "media and information": "Media & Information",
    "technology and digital": "Technology & Digital",
}

CATEGORY_ALIASES: dict[str, str] = {
    "security and conflict": "Nagorno-Karabakh & Artsakh",
    "politics": "Government & Leadership",
    "economy": "Macroeconomic Condition",
    "society": "Education",
    "diaspora and migration": "Diaspora",
    "culture and identity": "National Identity",
    "media and information": "Media Landscape",
    "religion and faith": "Religion",
    "private education": "Education",
}

# Lowercase alias -> canonical topic.
TOPIC_ALIASES: dict[str, str] = {
    # Sports and culture.
    "sport": "Sports Event",
    "sports": "Sports Event",
    "sport event": "Sports Event",
    "sports event": "Sports Event",
    "sports events": "Sports Event",
    "sporting event": "Sports Event",
    "sporting events": "Sports Event",
    "sport offer": "Sports Event",
    "sports offer": "Sports Event",
    "football": "Football Match",
    "football game": "Football Match",
    "soccer": "Football Match",
    "soccer game": "Football Match",

    # Security and geopolitics.
    "karabakh": "Nagorno-Karabakh Conflict",
    "karabakh war": "Nagorno-Karabakh Conflict",
    "nagorno karabakh": "Nagorno-Karabakh Conflict",
    "nagorno-karabakh": "Nagorno-Karabakh Conflict",
    "artsakh": "Nagorno-Karabakh Conflict",
    "artsakh war": "Nagorno-Karabakh Conflict",
    "lachin corridor": "Lachin Corridor Blockade",
    "pow crisis": "POW And Hostage Crisis",
    "hostage crisis": "POW And Hostage Crisis",
    "border security": "Armenian Border Security",
    "ukraine": "Russia-Ukraine War",
    "ukraine war": "Russia-Ukraine War",
    "war in ukraine": "Russia-Ukraine War",
    "russia-ukraine conflict": "Russia-Ukraine War",
    "military": "Armenian Armed Force",
    "conscription": "Military Conscription",
    "csto": "CSTO Membership",
    "arms supply": "Arms Procurement",
    "ceasefire": "Ceasefire Monitoring",
    "pro russia": "Pro-Russian Orientation",
    "pro west": "Pro-Western Orientation",
    "eu membership": "EU Membership Aspiration",
    "russian influence": "Russian Influence",

    # Politics.
    "armenian politics": "Armenian Government Performance",
    "armenian political situation": "Armenian Government Performance",
    "government": "Armenian Government Performance",
    "armenian government": "Armenian Government Performance",
    "pashinyan": "Prime Minister Policy",
    "nikol": "Prime Minister Policy",
    "cabinet": "Cabinet Appointment",
    "parliament": "Parliament Activity",
    "municipal": "Municipal Governance",
    "opposition": "Armenian Opposition Movement",
    "armenian opposition": "Armenian Opposition Movement",
    "protest": "Political Protest",
    "protests": "Political Protest",
    "rally": "Street Demonstration",
    "demonstration": "Street Demonstration",
    "political prisoner": "Political Prisoner",
    "election": "Electoral Integrity",
    "constitution": "Constitutional Reform",
    "judicial": "Judicial Independence",
    "corruption": "Anti-Corruption Effort",
    "transparency": "Government Transparency",
    "media freedom": "Media Freedom",
    "nationalism": "Armenian Nationalism",
    "pan armenian": "Pan-Armenian Movement",
    "revolution": "Revolutionary Discourse",
    "conservative": "Conservative Movement",
    "liberal": "Liberal Reform",

    # Economy and finance.
    "economy": "Economic Growth",
    "economic situation": "Economic Growth",
    "economic crisis": "Economic Growth",
    "inflation": "Inflation And Prices",
    "prices": "Inflation And Prices",
    "high prices": "Inflation And Prices",
    "cost of living": "Inflation And Prices",
    "dram": "Armenian Dram Exchange",
    "armenian dram": "Armenian Dram Exchange",
    "exchange rate": "Armenian Dram Exchange",
    "currency": "Currency Volatility",
    "gdp": "GDP Performance",
    "foreign investment": "Foreign Investment",
    "employment": "Job Market Condition",
    "job": "Job Market Condition",
    "jobs": "Job Market Condition",
    "unemployment": "Unemployment Rate",
    "salary": "Salary Dynamics",
    "labor migration": "Labor Migration",
    "youth unemployment": "Youth Unemployment",
    "business": "Business Opportunity",
    "startup": "Tech Startup",
    "import export": "Import Export Trade",
    "investment": "Investment Opportunity",
    "small business": "Small Business Support",
    "agriculture": "Agricultural Economy",
    "bank": "Banking Sector",
    "tax": "Tax Policy",
    "crypto": "Cryptocurrency Trend",
    "sanctions": "Russian Sanctions Impact",
    "financial regulation": "Financial Regulation",
    "housing affordability": "Housing Affordability",
    "rent": "Rental Market",
    "rental": "Rental Market",
    "food price": "Food Price",
    "utility": "Utility Price",
    "transportation": "Transportation Cost",

    # Society and daily life.
    "education": "School System Quality",
    "school": "School System Quality",
    "schools": "School System Quality",
    "higher education": "Higher Education",
    "tutor": "Private Tutoring",
    "vocational": "Vocational Training",
    "student": "Student Life",
    "healthcare": "Healthcare Access",
    "health": "Healthcare Access",
    "hospital": "Hospital Quality",
    "medicine": "Pharmaceutical Price",
    "mental health": "Mental Health",
    "insurance": "Health Insurance",
    "medical tourism": "Medical Tourism",
    "housing": "Housing Market",
    "urban development": "Urban Development",
    "road": "Road And Transit",
    "construction": "Construction Quality",
    "public space": "Public Space",
    "rural": "Rural Infrastructure",
    "family": "Family Life",
    "childcare": "Childcare Access",
    "marriage": "Marriage And Dating",
    "elder care": "Elder Care",
    "domestic violence": "Domestic Violence",
    "gender": "Gender Dynamics",
    "social service": "Social Safety Net",
    "disability": "Disability Support",
    "refugee": "Refugee Assistance",
    "poverty": "Poverty Alleviation",
    "pension": "Pension System",

    # Migration and diaspora.
    "migration": "Migration Intent",
    "emigration": "Migration Intent",
    "immigration": "Migration Intent",
    "relocation": "Relocation Planning",
    "visa": "Visa And Residency",
    "brain drain": "Brain Drain",
    "youth departure": "Youth Departure",
    "russians in armenia": "Russian Expat Community",
    "expats": "Russian Expat Community",
    "russian community": "Russian Expat Community",
    "karabakh displaced": "Karabakh Displaced Person",
    "foreign worker": "Foreign Worker",
    "digital nomad": "Digital Nomad Arrival",
    "diaspora": "Armenian Diaspora Identity",
    "diaspora engagement": "Diaspora Engagement",
    "repatriation": "Repatriation Effort",
    "homeland": "Homeland Connection",
    "diaspora investment": "Diaspora Investment",
    "integration": "Cultural Adaptation",
    "language barrier": "Language Barrier",
    "citizenship": "Citizenship Issue",
    "community acceptance": "Community Acceptance",

    # Culture and identity.
    "national identity": "Armenian National Identity",
    "post soviet": "Post-Soviet Identity",
    "collective trauma": "Collective Memory",
    "genocide": "Armenian Genocide Remembrance",
    "armenian genocide": "Armenian Genocide Remembrance",
    "heritage": "Cultural Heritage Preservation",
    "church": "Armenian Apostolic Church",
    "religion": "Religious Practice",
    "church state": "Church-State Relation",
    "interfaith": "Interfaith Dialogue",
    "orthodox": "Orthodox Christianity",
    "language": "Armenian Language",
    "russian language": "Russian Language Use",
    "code switching": "Code-Switching Practice",
    "language policy": "Language Policy",
    "music": "Armenian Music",
    "film": "Film And Cinema",
    "cinema": "Film And Cinema",
    "literature": "Literature",
    "festival": "Cultural Festival",
    "community solidarity": "Community Solidarity",
    "volunteer": "Volunteer Movement",
    "neighborhood": "Neighborhood Relation",
    "celebration": "Local Celebration",
    "nostalgia": "Soviet Nostalgia",

    # Media and information.
    "propaganda": "Russian Propaganda",
    "russian propaganda": "Russian Propaganda",
    "disinformation": "Disinformation Campaign",
    "misinformation": "Disinformation Campaign",
    "information warfare": "Information Warfare",
    "fake news": "Fake News Detection",
    "bot": "Bot Activity",
    "media": "Media And News",
    "news": "Media And News",
    "media and information": "Media And News",
    "social media": "Social Media Trend",
    "telegram": "Telegram Community",
    "journalism": "Independent Journalism",
    "censorship": "Media Censorship",
    "war narrative": "War Narrative",
    "government narrative": "Government Narrative",
    "opposition narrative": "Opposition Narrative",
    "foreign media": "Foreign Media Frame",
    "conspiracy": "Conspiracy Theory",

    # Queue-driven compatibility aliases (staging hardening wave).
    "government performance": "Armenian Government Performance",
    "job opportunity": "Job Market Condition",
    "humanitarian aid": "Refugee Assistance",
    "genocide studie": "Armenian Genocide Remembrance",
    "genocide study": "Armenian Genocide Remembrance",
    "religious freedom violation": "Religious Practice",
    "social media commentary": "Social Media Trend",
    "social media engagement": "Social Media Trend",
    "political discourse": "Revolutionary Discourse",
    "political strategy": "Government Narrative",
    "foreign influence": "Russian Influence",
    "geopolitical tension": "South Caucasus Stability",
    "security threat": "Armenian Border Security",
    "iranian foreign policy": "Iranian-Armenian Relation",
    "iranian leadership": "Iranian-Armenian Relation",
    "american military base": "Armenian Armed Force",
    "us foreign policy": "Multi-Vector Foreign Policy",
    "regional tension": "South Caucasus Stability",
    "ethnic tension": "Armenian National Identity",
    "historical parallel": "Collective Memory",
    "child sacrifice allegation": "Conspiracy Theory",
    "iranian border policy": "Iranian-Armenian Relation",
    "nikol pashinyan": "Prime Minister Policy",
    "culinary tradition": "Cultural Festival",
    "environmental awareness": "Public Space",
    "geopolitical influence": "Russian Influence",
    "iranian general incident": "Iranian-Armenian Relation",
    "water management": "Rural Infrastructure",
    "ethnic diversity": "Armenian National Identity",
    "islamic brotherhood": "Interfaith Dialogue",
    "self-irony": "Community Solidarity",
    "ethnic identity": "Armenian National Identity",
    "constitutional debate": "Constitutional Reform",
    "social critique": "Community Solidarity",
    "social commentary": "Social Media Trend",
    "historical origin": "Collective Memory",
    "interethnic tension": "Armenian National Identity",
    "colonialism debate": "Post-Soviet Identity",
    "civil contract": "Armenian Government Performance",
    "crisis response": "Social Safety Net",
    "cultural reference": "Cultural Heritage Preservation",
    "environmental activism": "Public Space",
    "bot interaction": "Bot Activity",
    "project delay": "Urban Development",
    "blood donation": "Social Safety Net",
    "cultural critique": "Armenian National Identity",
    "genocide recognition": "Armenian Genocide Remembrance",
    "kurdish involvement": "South Caucasus Stability",
    "social dynamic": "Community Solidarity",
    "us-iran relation": "Iranian-Armenian Relation",

    # Technology and digital.
    "egov": "E-Government Service",
    "e government": "E-Government Service",
    "digital payment": "Digital Payment",
    "mobile app": "Mobile Application",
    "internet": "Internet Access",
    "telecom": "Telecom Service",
    "tech industry": "Tech Industry Growth",
    "ai": "AI Development",
    "software export": "Software Export",
    "startup ecosystem": "Startup Ecosystem",
    "privacy": "Online Privacy",
    "surveillance": "Digital Surveillance",
    "moderation": "Content Moderation",
    "platform regulation": "Platform Regulation",

    # Legacy topic compatibility aliases from v2.
    "armenian political situation": "Armenian Government Performance",
    "armenian government": "Armenian Government Performance",
    "armenian opposition": "Armenian Opposition Movement",
    "government corruption": "Anti-Corruption Effort",
    "economic situation": "Economic Growth",
    "currency situation": "Currency Volatility",
    "real estate market": "Housing Market",
    "private education": "Private Tutoring",
    "healthcare quality": "Healthcare Access",
    "armenian culture": "Armenian National Identity",
    "armenian history": "Collective Memory",
    "national identity": "Armenian National Identity",
    "religion and faith": "Religious Practice",
    "russian community": "Russian Expat Community",
}


def _normalize_lookup_key(value: str | None) -> str:
    return str(value or "").strip().lower()


_STRUCTURAL_TOPICS = frozenset(
    {
        "Media And News",
        "Social Media Trend",
        "Telegram Community",
    }
)

_SIGNAL_TOPICS = frozenset(
    {
        "Community Solidarity",
    }
)

_REJECTED_CANONICAL_TOPICS = frozenset()

_REJECTED_TOPIC_KEYS = frozenset(
    {
        "",
        "null",
        "none",
        "unknown",
        "n/a",
        "na",
        "product demand",
        "business enterprise business opportunity",
        "proposed classified marketplace listing",
        "media information media and",
        "tech economy tech industry",
        "tech economy startup ecosystem",
        "society daily life community",
        "housing infrastructure road and",
        "emotional distres",
    }
)


def get_topic_role(topic_name: str | None) -> str:
    """Return the worker-side role for a topic name."""
    normalized = str(topic_name or "").strip()
    if not normalized:
        return "rejected"

    if normalized in _STRUCTURAL_TOPICS:
        return "structural"
    if normalized in _SIGNAL_TOPICS:
        return "signal"
    if normalized in _REJECTED_CANONICAL_TOPICS:
        return "rejected"

    lookup = _normalize_lookup_key(normalized)
    if lookup in _REJECTED_TOPIC_KEYS:
        return "rejected"

    return "issue"


def is_issue_topic(topic_name: str | None) -> bool:
    return get_topic_role(topic_name) == "issue"


def iter_non_issue_topics() -> Iterable[str]:
    for topic in iter_topics():
        if not is_issue_topic(topic):
            yield topic


def canonical_domain_name(value: str | None) -> str:
    """Normalize domain label via compatibility aliases."""
    key = _normalize_lookup_key(value)
    if not key:
        return "General"

    mapped = DOMAIN_ALIASES.get(key)
    if mapped:
        return mapped

    for domain in TAXONOMY_DOMAINS.keys():
        if domain.lower() == key:
            return domain
    return str(value).strip() or "General"


def canonical_category_name(value: str | None) -> str:
    """Normalize category label via compatibility aliases."""
    key = _normalize_lookup_key(value)
    if not key:
        return "General"

    mapped = CATEGORY_ALIASES.get(key)
    if mapped:
        return mapped

    for categories in TAXONOMY_DOMAINS.values():
        for category in categories.keys():
            if category.lower() == key:
                return category
    return str(value).strip() or "General"


def iter_topics() -> Iterable[str]:
    for categories in TAXONOMY_DOMAINS.values():
        for topics in categories.values():
            for topic in topics:
                yield topic


def build_topic_maps() -> tuple[dict[str, str], dict[str, str]]:
    """Build reverse maps for topic -> category and topic -> domain."""
    topic_categories: dict[str, str] = {}
    topic_domains: dict[str, str] = {}
    for domain, categories in TAXONOMY_DOMAINS.items():
        canonical_domain = canonical_domain_name(domain)
        for category, topics in categories.items():
            canonical_category = canonical_category_name(category)
            for topic in topics:
                topic_categories[topic] = canonical_category
                topic_domains[topic] = canonical_domain
    return topic_categories, topic_domains


def compact_taxonomy_prompt(max_topics_per_category: int = 5) -> str:
    """Render compact taxonomy text for AI prompts."""
    lines: list[str] = []
    for domain, categories in TAXONOMY_DOMAINS.items():
        lines.append(f"{domain}:")
        for category, topics in categories.items():
            items = topics[:max_topics_per_category]
            suffix = ", ..." if len(topics) > max_topics_per_category else ""
            lines.append(f"- {category}: {', '.join(items)}{suffix}")
    return "\n".join(lines)
