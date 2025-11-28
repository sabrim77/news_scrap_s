# scrapers/base/base_scraper.py

import logging
import random
import time
from urllib.parse import urlparse

import requests
from bs4 import BeautifulSoup

logger = logging.getLogger("base_scraper")


class BaseScraper:
    """
    Shared HTTP client:
        - Session reuse
        - Rotating UA
        - Per-domain delay
        - Retry with exponential backoff
        - Detect 403/429 + Retry-After header
        - Detect block-page heuristics (Cloudflare / WAF)
        - Proxy support
    """

    def __init__(
        self,
        timeout: int = 10,
        min_delay: float = 1.5,
        max_delay: float = 4.0,
        max_retries: int = 3,
        proxies: dict | None = None,
    ):
        self.session = requests.Session()
        self.timeout = timeout
        self.min_delay = min_delay
        self.max_delay = max_delay
        self.max_retries = max_retries
        self.proxies = proxies

        self._last_request_ts: dict[str, float] = {}

        self._user_agents = [
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
            "(KHTML, like Gecko) Chrome/120.0 Safari/537.36",
            "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 "
            "(KHTML, like Gecko) Version/17.0 Safari/605.1.15",
            "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 "
            "(KHTML, like Gecko) Chrome/118.0 Safari/537.36",
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:119.0) Gecko/20100101 Firefox/119.0",
        ]

    # ----------------------- Helpers -----------------------

    def _random_ua(self) -> str:
        return random.choice(self._user_agents)

    def _sleep_if_needed(self, netloc: str) -> None:
        last_ts = self._last_request_ts.get(netloc)
        if last_ts is None:
            return

        elapsed = time.time() - last_ts
        target = random.uniform(self.min_delay, self.max_delay)

        if elapsed < target:
            delay = target - elapsed
            logger.debug("Sleeping %.2fs before next request to %s", delay, netloc)
            time.sleep(delay)

    def _update_last_ts(self, netloc: str) -> None:
        self._last_request_ts[netloc] = time.time()

    # ----------------------- HTTP GET -----------------------

    def get(self, url: str) -> requests.Response | None:
        netloc = urlparse(url).netloc

        self._sleep_if_needed(netloc)

        for attempt in range(1, self.max_retries + 1):
            headers = {
                "User-Agent": self._random_ua(),
                "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
                "Accept-Language": "en-US,en;q=0.5",
            }

            try:
                logger.info("GET %s (attempt %d/%d)", url, attempt, self.max_retries)

                resp = self.session.get(
                    url,
                    headers=headers,
                    timeout=(5, self.timeout),
                    proxies=self.proxies,
                )

                self._update_last_ts(netloc)

                status = resp.status_code

                # ----------------------- Redirect -----------------------
                if status in (301, 302, 303, 307, 308):
                    return resp

                # ----------------------- Successful -----------------------
                if status == 200:
                    # Check block-page heuristics
                    txt = resp.text.lower()
                    if any(x in txt for x in [
                        "cloudflare",
                        "attention required",
                        "verify you are human",
                        "are you a robot",
                        "just a moment",
                        "checking your browser",
                    ]):
                        logger.warning("Suspected block page for %s", url)
                        return None

                    return resp

                # ----------------------- Rate limit / block -----------------------
                if status in (403, 429):
                    retry_after = int(resp.headers.get("Retry-After", "0"))
                    if retry_after > 0:
                        logger.warning("Retry-After=%s sec for %s", retry_after, url)
                        time.sleep(retry_after)
                    else:
                        time.sleep(5 * attempt)
                    continue

                # ----------------------- Other status -----------------------
                logger.warning("Status %d from %s, retrying...", status, url)
                time.sleep(2 * attempt)

            except requests.RequestException as exc:
                logger.warning("Request error for %s (%s)", url, exc)
                time.sleep(2 * attempt)

        logger.error("Giving up on %s after %d attempts", url, self.max_retries)
        return None

    # ----------------------- HTML Fetch -----------------------

    def fetch_html(self, url: str):
        resp = self.get(url)
        if resp is None:
            return None

        content_type = resp.headers.get("Content-Type", "")
        if "text/html" not in content_type:
            logger.warning("Unexpected content type for %s: %s", url, content_type)

        return BeautifulSoup(resp.text, "html.parser")
