# core/topic_classifier.py
"""
Rule-based topic classifier for scraped news articles.

Input:
    - portal: portal id (e.g. "jagonews24", "bbc")
    - url   : full article URL
    - title : article title (can be empty)
    - body  : article body (can be empty)

Output:
    - topic string from TOPICS

Priority:
    1) URL-based rules          (URL_KEYWORDS)
    2) Title-based rules        (TEXT_KEYWORDS)
    3) Body-based rules         (TEXT_KEYWORDS, if body is long enough)
    4) Fallback → "other"
"""

from __future__ import annotations

from typing import Optional, Dict, List
import logging

logger = logging.getLogger("topic_classifier")

# ---------------------------------------------------------------------------
# Topic labels (public contract)
# ---------------------------------------------------------------------------

TOPICS: List[str] = [
    "politics",
    "economy",
    "sports",
    "entertainment",
    "crime",
    "education",
    "religion",
    "international",
    "tech",
    "health",
    "environment",
    "other",
]

# ---------------------------------------------------------------------------
# URL-based hints per topic (English + structural hints)
# ---------------------------------------------------------------------------

URL_KEYWORDS: Dict[str, List[str]] = {
    "sports": [
        "/sports/",
        "/sport/",
        "/cricket/",
        "/football/",
        "/wc-",
        "/world-cup",
        "/t20/",
    ],
    "economy": [
        "/economics/",
        "/business/",
        "/stock-market/",
        "/stockmarket/",
        "/bank/",
        "/corporate/",
        "/economy/",
        "/finance/",
    ],
    "entertainment": [
        "/entertainment/",
        "/showbiz/",
        "/lifestyle/",
        "/arts-entertainment/",
        "/culture/",
    ],
    "crime": [
        "/crime/",
        "/crime-justice/",
        "/law-order/",
        "/law-and-order/",
        "/police/",
        "/court/",
        "/high-court/",
        "/supreme-court/",
    ],
    "education": [
        "/education/",
        "/campus/",
        "/university/",
        "/college/",
        "/school/",
        "/admission/",
    ],
    "religion": [
        "/religion/",
        "/islam/",
        "/religious/",
        "/hajj/",
        "/ramadan/",
    ],
    "politics": [
        "/politics/",
        "/national/",
        "/bangladesh/politics/",
        "/election/",
        "/parliament/",
        "/govt/",
    ],
    "international": [
        "/international/",
        "/world/",
        "/global/",
        "/rohingya-influx/",
        "/middle-east/",
        "/asia/",
        "/europe/",
        "/usa/",
        "/us-news/",
    ],
    "tech": [
        "/technology/",
        "/sci-tech/",
        "/science-technology/",
        "/ict/",
        "/tech/",
        "/startup/",
    ],
    "health": [
        "/health/",
        "/lifestyle/health/",
        "/healthcare/",
        "/coronavirus/",
        "/covid-19/",
        "/covid19/",
        "/health-news/",
        "/health-and-fitness/",
    ],
    "environment": [
        "/environment/",
        "/climate/",
        "/climate-crisis/",
        "/climate-change/",
        "/weather/",
        "/natural-disaster/",
        "/disaster/",
        "/environment-pollution/",
    ],
}

# ---------------------------------------------------------------------------
# Title/body keyword hints (Bangla + English)
# Order matters: environment is placed before crime, etc.
# ---------------------------------------------------------------------------

TEXT_KEYWORDS: Dict[str, List[str]] = {
    "sports": [
        "খেলা",
        "ক্রিকেট",
        "ফুটবল",
        "টেস্ট ম্যাচ",
        "ওয়ানডে",
        "ওডিআই",
        "টি-টোয়েন্টি",
        "টি টোয়েন্টি",
        "goal",
        "match",
        "tournament",
        "world cup",
        "league",
        "টুর্নামেন্ট",
    ],
    "economy": [
        "অর্থনীতি",
        "শেয়ার বাজার",
        "শেয়ার বাজার",
        "শেয়ারবাজার",
        "স্টক",
        "ব্যাংক",
        "ডলার",
        "মুদ্রাস্ফীতি",
        "loan",
        "interest rate",
        "inflation",
        "economic",
        "stock market",
        "অর্থনৈতিক",
        "বিনিয়োগ",
        "বিনিয়োগ",
        "ব্যবসা",
        "কর্পোরেট",
        "বাজেট",
    ],
    "entertainment": [
        "অভিনেতা",
        "অভিনেত্রী",
        "নায়িকা",
        "নায়ক",
        "গানের",
        "সিনেমা",
        "ফিল্ম",
        "মডেল",
        "hero",
        "actress",
        "film",
        "movie",
        "drama",
        "showbiz",
        "টেলিফিল্ম",
        "নাটক",
        "বিনোদন",
        "গান",
        "অ্যালবাম",
    ],
    # Environment BEFORE crime so disasters go here first
    "environment": [
        "বন্যা",
        "প্লাবন",
        "নদী ভাঙন",
        "নদীভাঙন",
        "ভূমিকম্প",
        "earthquake",
        "মাটি কাঁপা",
        "ঘূর্ণিঝড়",
        "ঘূর্ণিঝড়",
        "ঘূর্ণিঝড়ে",
        "ঘূর্ণিঝড়ে",
        "cyclone",
        "সাইক্লোন",
        "টাইফুন",
        "typhoon",
        "tornado",
        "storm",
        "ঝড়",
        "ঝড়",
        "কালবৈশাখী",
        "landslide",
        "ভূমিধস",
        "খরা",
        "drought",
        "heatwave",
        "হিটওয়েভ",
        "হিটওয়েভ",
        "তাপপ্রবাহ",
        "wildfire",
        "বনানলে",
        "বনানল",
        "দূষণ",
        "দূষিত বায়ু",
        "দূষিত বায়ু",
        "air pollution",
        "environment",
        "climate",
        "climate change",
        "global warming",
    ],
    "crime": [
        "খুন",
        "হত্যা",
        "ধর্ষণ",
        "ধর্ষণের",
        "গ্রেপ্তার",
        "গ্রেফতার",
        "ডাকাতি",
        "ছিনতাই",
        "মামলা",
        "জোরপূর্বক",
        "বিস্ফোরণ",
        "বোমা",
        "মর্টার শেল",
        "বিস্ফোরক",
        "অস্ত্রসহ",
        "অস্ত্র",
        "আগ্নেয়াস্ত্র",
        "আগ্নেয়াস্ত্র",
        "গুম",
        "হামলা",
        "লাশ",
        "rape",
        "murder",
        "arrest",
        "police",
        "case filed",
        "case against",
        "bomb blast",
        "explosive",
        "firearms",
        "gun",
        "pistol",
        "rifle",
        "court",
        "high court",
        "supreme court",
    ],
    "health": [
        "হাসপাতাল",
        "হাসপতাল",
        "মেডিকেল কলেজ",
        "চিকিৎসা",
        "রোগী",
        "রোগীকে",
        "ইনজেকশন",
        "বাতের ইনজেকশন",
        "ক্যানসার",
        "ক্যান্সার",
        "ডায়াবেটিস",
        "ডায়াবেটিস",
        "জ্বর",
        "স্বাস্থ্য",
        "স্বাস্থ্যসেবা",
        "চিকিৎসক",
        "ডাক্তার",
        "টিকা",
        "টিকাদান",
        "ভ্যাকসিন",
        "medical college",
        "medical",
        "hospital",
        "health",
        "healthcare",
        "treatment",
        "patients",
        "injection",
        "medicine",
        "medicines",
        "vaccination",
        "vaccine",
        "virus",
        "coronavirus",
        "covid",
        "covid-19",
    ],
    "education": [
        "বিশ্ববিদ্যালয়",
        "বিশ্ববিদ্যালয়",
        "কলেজ",
        "স্কুল",
        "শিক্ষা",
        "ভর্তি",
        "রুয়েট",
        "রুয়েট",
        "ক্যাম্পাস",
        "শিক্ষার্থী",
        "শিক্ষার্থীদের",
        "teacher",
        "teacher recruitment",
        "university",
        "campus",
        "admission",
        "students",
        "exam",
        "পরীক্ষা",
        "বোর্ড পরীক্ষা",
    ],
    "religion": [
        "আল্লাহ",
        "নবী",
        "হজরত",
        "কুরআন",
        "হাদিস",
        "মসজিদ",
        "ওমরাহ",
        "ইসলাম",
        "ধর্মীয়",
        "ধর্মীয়",
        "religion",
        "islam",
        "hajj",
        "eid",
        "রমজান",
        "রোজা",
        "উপাসনা",
    ],
    "politics": [
        "সরকার",
        "সাংসদ",
        "সংসদ",
        "এমপি",
        "নির্বাচন",
        "রাজনীতি",
        "মন্ত্রিসভা",
        "প্রধানমন্ত্রী",
        "মন্ত্রী",
        "দলীয়",
        "দলীয়",
        "দলীয় নেতা",
        "politics",
        "election",
        "parliament",
        "government",
        "cabinet",
        "ruling party",
        "opposition",
        "govt",
        "সভাপতি",
        "সাধারণ সম্পাদক",
    ],
    "international": [
        "জাতিসংঘ",
        "জাতিসংঘের",
        "বিশ্ব",
        "আন্তর্জাতিক",
        "যুক্তরাষ্ট্র",
        "হোয়াইট হাউস",
        "হোয়াইট হাউস",
        "ইউরোপ",
        "মধ্যপ্রাচ্য",
        "united nations",
        "international",
        "global",
        "us president",
        "foreign",
        "world leaders",
        "un chief",
        "বিশ্বব্যাপী",
    ],
    "tech": [
        "প্রযুক্তি",
        "সাইবার",
        "ইন্টারনেট",
        "অ্যাপ",
        "অ্যাপস",
        "স্মার্টফোন",
        "মোবাইল অ্যাপ",
        "গ্যাজেট",
        "technology",
        "software",
        "ai",
        "artificial intelligence",
        "startup",
        "digital",
        "আইটি",
        "আইসিটি",
        "ডাটা",
        "ডেটা",
    ],
}


# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------

def _normalize(text: str) -> str:
    return text.lower().strip()


def _match_keywords(
    text: str,
    mapping: Dict[str, List[str]],
) -> Optional[str]:
    """
    Return the first topic whose keyword appears in the text.
    """
    if not text:
        return None

    t = _normalize(text)
    for topic, words in mapping.items():
        for w in words:
            if w.lower() in t:
                return topic
    return None


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------

def classify_topic(
    portal: str,
    url: str,
    title: Optional[str],
    body: Optional[str],
) -> str:
    """
    Classify topic of an article using simple rules.

    Args:
        portal: portal id (not deeply used now, but included for future ML rules)
        url   : article URL
        title : article title (may be empty)
        body  : article body (may be empty)

    Returns:
        One of TOPICS (e.g. "sports", "politics", "other", ...)
    """
    url = url or ""
    title = title or ""
    body = body or ""

    # 1) URL-based rules (strongest signal)
    topic = _match_keywords(url, URL_KEYWORDS)
    if topic:
        logger.debug("Topic from URL: %s → %s", url, topic)
        return topic

    # 2) Title-based rules (portal + title combined)
    combined_title = f"{portal} {title}".strip()
    topic = _match_keywords(combined_title, TEXT_KEYWORDS)
    if topic:
        logger.debug("Topic from title: %s → %s", combined_title, topic)
        return topic

    # 3) Body-based rules (only if body has some length)
    if body and len(body) > 80:
        topic = _match_keywords(body, TEXT_KEYWORDS)
        if topic:
            logger.debug("Topic from body: %s...", topic)
            return topic

    # 4) Fallback
    return "other"
