from __future__ import annotations
"""
topic_normalizer.py — Expert-grade topic canonicalization pipeline.

Applied BEFORE every Neo4j MERGE to prevent graph pollution.

Pipeline:
  raw string → strip → Title Case → singular → synonym map → max 4 words

This ensures "sport offer", "Sports Events", "sporting events" all
resolve to the same canonical "Sports Event" node in Neo4j.
"""
import re

# ── Topic Synonym / Canonical Map ────────────────────────────────────────────
# Keys: lowercase version of any variant
# Values: canonical Title Case form to store in Neo4j

TOPIC_SYNONYMS: dict[str, str] = {
    # ── Sports ──
    "sport":                        "Sports Event",
    "sports":                       "Sports Event",
    "sport event":                  "Sports Event",
    "sports event":                 "Sports Event",
    "sports events":                "Sports Event",
    "sporting event":               "Sports Event",
    "sporting events":              "Sports Event",
    "sport offer":                  "Sports Event",
    "sports offer":                 "Sports Event",
    "football":                     "Football Match",
    "football game":                "Football Match",
    "football match":               "Football Match",
    "soccer":                       "Football Match",
    "soccer game":                  "Football Match",
    "soccer match":                 "Football Match",
    "national team":                "Armenian National Team",

    # ── Geopolitics / Conflicts ──
    "karabakh":                     "Nagorno-Karabakh Conflict",
    "karabakh war":                 "Nagorno-Karabakh Conflict",
    "nagorno karabakh":             "Nagorno-Karabakh Conflict",
    "nagorno-karabakh":             "Nagorno-Karabakh Conflict",
    "artsakh":                      "Nagorno-Karabakh Conflict",
    "artsakh war":                  "Nagorno-Karabakh Conflict",
    "2020 war":                     "Nagorno-Karabakh Conflict",
    "ukraine":                      "Russia-Ukraine War",
    "ukraine war":                  "Russia-Ukraine War",
    "war in ukraine":               "Russia-Ukraine War",
    "russia ukraine":               "Russia-Ukraine War",
    "russia-ukraine conflict":      "Russia-Ukraine War",
    "sanctions":                    "Economic Sanctions",
    "russian sanctions":            "Economic Sanctions",
    "western sanctions":            "Economic Sanctions",

    # ── Armenian Politics ──
    "armenian politics":            "Armenian Political Situation",
    "armenian government":          "Armenian Government",
    "government":                   "Armenian Government",
    "pashinyan":                    "Armenian Government",
    "nikol":                        "Armenian Government",
    "opposition":                   "Armenian Opposition",
    "armenian opposition":          "Armenian Opposition",
    "protest":                      "Political Protest",
    "protests":                     "Political Protest",
    "rally":                        "Political Protest",
    "demonstration":                "Political Protest",
    "corruption":                   "Government Corruption",
    "bribe":                        "Government Corruption",
    "bribes":                       "Government Corruption",
    "bribery":                      "Government Corruption",

    # ── Economy ──
    "inflation":                    "Inflation And Prices",
    "prices":                       "Inflation And Prices",
    "high prices":                  "Inflation And Prices",
    "price rise":                   "Inflation And Prices",
    "cost of living":               "Inflation And Prices",
    "economy":                      "Economic Situation",
    "economic situation":           "Economic Situation",
    "economic crisis":              "Economic Situation",
    "currency":                     "Currency Situation",
    "dram":                         "Armenian Dram",
    "armenian dram":                "Armenian Dram",
    "ruble":                        "Russian Ruble",

    # ── Migration & Diaspora ──
    "migration":                    "Migration Intent",
    "immigration":                  "Migration Intent",
    "emigration":                   "Migration Intent",
    "relocation":                   "Migration Intent",
    "moving abroad":                "Migration Intent",
    "leaving armenia":              "Migration Intent",
    "leaving russia":               "Migration Intent",
    "diaspora":                     "Armenian Diaspora Identity",
    "armenian diaspora":            "Armenian Diaspora Identity",
    "diaspora identity":            "Armenian Diaspora Identity",
    "homeland":                     "Homeland Connection",

    # ── Society / Daily Life ──
    "education":                    "Education System",
    "school":                       "Education System",
    "schools":                      "Education System",
    "university":                   "Higher Education",
    "tutor":                        "Private Education",
    "healthcare":                   "Healthcare Quality",
    "health":                       "Healthcare Quality",
    "medicine":                     "Healthcare Quality",
    "hospital":                     "Healthcare Quality",
    "housing":                      "Housing Market",
    "apartment":                    "Housing Market",
    "rent":                         "Housing Market",
    "real estate":                  "Real Estate Market",
    "family":                       "Family Life",
    "parenting":                    "Family Life",
    "children":                     "Family Life",

    # ── Media & Information ──
    "propaganda":                   "Russian Propaganda",
    "russian propaganda":           "Russian Propaganda",
    "fake news":                    "Disinformation",
    "disinformation":               "Disinformation",
    "misinformation":               "Disinformation",
    "media":                        "Media And News",
    "news":                         "Media And News",
    "social media":                 "Social Media",
    "telegram":                     "Telegram Community",

    # ── Religion & Culture ──
    "religion":                     "Religion And Faith",
    "church":                       "Armenian Apostolic Church",
    "orthodox":                     "Orthodox Christianity",
    "christianity":                 "Orthodox Christianity",
    "culture":                      "Armenian Culture",
    "armenian culture":             "Armenian Culture",
    "history":                      "Armenian History",
    "genocide":                     "Armenian Genocide",
    "armenian genocide":            "Armenian Genocide",

    # ── Business ──
    "business":                     "Business Opportunity",
    "startup":                      "Tech Startup",
    "investment":                   "Investment Opportunity",
    "job":                          "Employment",
    "jobs":                         "Employment",
    "work":                         "Employment",
    "employment":                   "Employment",
    "unemployment":                 "Unemployment",

    # ── Identity ──
    "identity":                     "National Identity",
    "national identity":            "National Identity",
    "russians in armenia":          "Russian Expat Community",
    "expats":                       "Russian Expat Community",
    "russian community":            "Russian Expat Community",
    "russians":                     "Russian Community",
}

# ── Topic Category Map ─────────────────────────────────────────────────────────
# Maps canonical topic names → their parent category (for TopicCategory nodes)

TOPIC_CATEGORIES: dict[str, str] = {
    # Politics
    "Nagorno-Karabakh Conflict":    "Security And Conflict",
    "Russia-Ukraine War":           "Security And Conflict",
    "Armenian Political Situation": "Politics",
    "Armenian Government":          "Politics",
    "Armenian Opposition":          "Politics",
    "Political Protest":            "Politics",
    "Government Corruption":        "Politics",
    "Economic Sanctions":           "Politics",

    # Economy
    "Inflation And Prices":         "Economy",
    "Economic Situation":           "Economy",
    "Currency Situation":           "Economy",
    "Armenian Dram":                "Economy",
    "Russian Ruble":                "Economy",
    "Housing Market":               "Economy",
    "Real Estate Market":           "Economy",
    "Employment":                   "Economy",
    "Unemployment":                 "Economy",
    "Business Opportunity":         "Economy",
    "Investment Opportunity":       "Economy",
    "Tech Startup":                 "Economy",

    # Society
    "Education System":             "Society",
    "Higher Education":             "Society",
    "Private Education":            "Society",
    "Healthcare Quality":           "Society",
    "Family Life":                  "Society",

    # Migration & Diaspora
    "Migration Intent":             "Diaspora And Migration",
    "Armenian Diaspora Identity":   "Diaspora And Migration",
    "Homeland Connection":          "Diaspora And Migration",
    "Russian Expat Community":      "Diaspora And Migration",

    # Culture
    "Armenian Culture":             "Culture And Identity",
    "Armenian History":             "Culture And Identity",
    "Armenian Genocide":            "Culture And Identity",
    "National Identity":            "Culture And Identity",
    "Orthodox Christianity":        "Culture And Identity",
    "Armenian Apostolic Church":    "Culture And Identity",
    "Religion And Faith":           "Culture And Identity",

    # Sports
    "Sports Event":                 "Culture And Identity",
    "Football Match":               "Culture And Identity",
    "Armenian National Team":       "Culture And Identity",

    # Media
    "Russian Propaganda":           "Media And Information",
    "Disinformation":               "Media And Information",
    "Media And News":               "Media And Information",
    "Social Media":                 "Media And Information",
    "Telegram Community":           "Media And Information",

    # Security
    "Russian Community":            "Diaspora And Migration",
}

DEFAULT_CATEGORY = "General"

# ── Keyword-based Category Fallback ──────────────────────────────────────────
# Used when a canonical topic doesn't have an exact entry in TOPIC_CATEGORIES.
# Keys are lowercase keyword fragments; first match wins.

_CATEGORY_KEYWORDS: list[tuple[str, str]] = [
    # Security / Conflict
    ("karabakh",       "Security And Conflict"),
    ("artsakh",        "Security And Conflict"),
    ("war",            "Security And Conflict"),
    ("military",       "Security And Conflict"),
    ("conflict",       "Security And Conflict"),
    # Politics
    ("government",     "Politics"),
    ("opposition",     "Politics"),
    ("protest",        "Politics"),
    ("election",       "Politics"),
    ("political",      "Politics"),
    ("parliament",     "Politics"),
    ("corruption",     "Politics"),
    ("sanction",       "Politics"),
    # Economy
    ("bank",           "Economy"),
    ("payment",        "Economy"),
    ("price",          "Economy"),
    ("money",          "Economy"),
    ("business",       "Economy"),
    ("registration",   "Economy"),
    ("tax",            "Economy"),
    ("economic",       "Economy"),
    ("finance",        "Economy"),
    ("invest",         "Economy"),
    ("market",         "Economy"),
    ("inflation",      "Economy"),
    # Society
    ("education",      "Society"),
    ("school",         "Society"),
    ("health",         "Society"),
    ("hospital",       "Society"),
    ("medicine",       "Society"),
    ("family",         "Society"),
    ("parenting",      "Society"),
    ("housing",        "Society"),
    ("social service", "Society"),
    # Technology
    ("app",            "Technology"),
    ("mobile",         "Technology"),
    ("digital",        "Technology"),
    ("authentication", "Technology"),
    ("signature",      "Technology"),
    ("electronic",     "Technology"),
    ("tech",           "Technology"),
    ("telecom",        "Technology"),
    ("internet",       "Technology"),
    ("platform",       "Technology"),
    # Transport
    ("transport",      "Transportation"),
    ("route",          "Transportation"),
    ("travel",         "Transportation"),
    ("bus",            "Transportation"),
    ("metro",          "Transportation"),
    ("vehicle",        "Transportation"),
    ("drive",          "Transportation"),
    # Culture / Sports
    ("sport",          "Culture And Identity"),
    ("football",       "Culture And Identity"),
    ("hockey",         "Culture And Identity"),
    ("martial art",    "Culture And Identity"),
    ("armenian",       "Culture And Identity"),
    ("culture",        "Culture And Identity"),
    ("humor",          "Culture And Identity"),
    ("religious",      "Culture And Identity"),
    ("maslenitsa",     "Culture And Identity"),
    ("celebration",    "Culture And Identity"),
    ("community",      "Culture And Identity"),
    ("gathering",      "Culture And Identity"),
    # Diaspora / Migration
    ("diaspora",       "Diaspora And Migration"),
    ("migration",      "Diaspora And Migration"),
    ("expat",          "Diaspora And Migration"),
    ("relocation",     "Diaspora And Migration"),
    ("homeland",       "Diaspora And Migration"),
    # Media
    ("media",          "Media And Information"),
    ("news",           "Media And Information"),
    ("propaganda",     "Media And Information"),
    ("advertising",    "Media And Information"),
    ("claim",          "Media And Information"),
    ("telegram",       "Media And Information"),
]


def _infer_category_from_keywords(topic: str) -> str:
    """Keyword-based category fallback for AI-generated topic names."""
    lower = topic.lower()
    for keyword, category in _CATEGORY_KEYWORDS:
        if keyword in lower:
            return category
    return DEFAULT_CATEGORY


_TRAILING_S = re.compile(r"(?<=[a-z]{3})s$", re.IGNORECASE)


def normalize_topic(raw: str) -> str:
    """
    Normalize a raw topic string to its canonical graph node name.

    Steps:
      1. Strip whitespace
      2. Lowercase for synonym lookup
      3. Synonym map lookup (returns canonical if found)
      4. If not in map: Title Case + simple plural collapse
      5. Truncate to max 4 words
    """
    if not raw or not raw.strip():
        return "General"

    stripped = raw.strip()
    lookup_key = stripped.lower()

    # Direct synonym map lookup
    if lookup_key in TOPIC_SYNONYMS:
        return TOPIC_SYNONYMS[lookup_key]

    # Not in map → apply Title Case normalization
    title = stripped.title()

    # Remove trailing 's' from last word for simple plural collapse
    # "Sports Events" → "Sports Event"  (only last word)
    words = title.split()
    if len(words) > 0:
        last = _TRAILING_S.sub("", words[-1])
        words[-1] = last
    normalized = " ".join(words)

    # Check again after normalization
    if normalized.lower() in TOPIC_SYNONYMS:
        return TOPIC_SYNONYMS[normalized.lower()]

    # Truncate to 4 words max
    if len(words) > 4:
        normalized = " ".join(words[:4])

    return normalized


def normalize_topics(topics: list[str]) -> list[str]:
    """Normalize a list of topics, deduplicating after normalization."""
    seen: set[str] = set()
    result: list[str] = []
    for t in topics:
        canonical = normalize_topic(t)
        if canonical not in seen:
            seen.add(canonical)
            result.append(canonical)
    return result


def get_topic_category(canonical_topic: str) -> str:
    """Return the TopicCategory name for a given canonical topic.
    
    First tries exact map lookup, then falls back to keyword inference.
    This handles AI-generated topic names not in the synonym dictionary.
    """
    if canonical_topic in TOPIC_CATEGORIES:
        return TOPIC_CATEGORIES[canonical_topic]
    return _infer_category_from_keywords(canonical_topic)
