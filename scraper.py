import json
import os
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional

import requests
from bs4 import BeautifulSoup

BASE_URL = "https://www.kooora.com"
MATCHES_TODAY_URL = f"{BASE_URL}/كرة-القدم/مباريات-اليوم"
OUTPUT_DIR = Path("data")
OUTPUT_FILE = OUTPUT_DIR / "kooora_matches.json"
REQUEST_TIMEOUT = 30
USER_AGENT = (
    "Mozilla/5.0 (X11; Linux x86_64) "
    "AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/123.0.0.0 Safari/537.36"
)


def build_session() -> requests.Session:
    session = requests.Session()
    session.headers.update(
        {
            "User-Agent": USER_AGENT,
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
            "Accept-Language": "ar,en-US;q=0.9,en;q=0.8",
            "Cache-Control": "no-cache",
            "Pragma": "no-cache",
        }
    )
    return session


class KoooraScraper:
    def __init__(self) -> None:
        self.session = build_session()

    def fetch_page(self, url: str) -> str:
        response = self.session.get(url, timeout=REQUEST_TIMEOUT)
        response.raise_for_status()
        return response.text

    def extract_next_data(self, html: str) -> Dict[str, Any]:
        soup = BeautifulSoup(html, "html.parser")
        script = soup.find("script", id="__NEXT_DATA__")
        if not script or not script.string:
            raise RuntimeError("لم يتم العثور على __NEXT_DATA__ داخل الصفحة")
        return json.loads(script.string)

    def normalize_match_url(self, link: Optional[Dict[str, Any]]) -> Optional[str]:
        if not link:
            return None
        if link.get("url"):
            return link["url"]
        slug = link.get("slug")
        match_id = link.get("id")
        if slug and match_id:
            return f"{BASE_URL}/كرة-القدم/مباراة/{slug}/{match_id}"
        return None

    def normalize_team(self, team: Optional[Dict[str, Any]]) -> Dict[str, Any]:
        team = team or {}
        image = team.get("image") or {}
        return {
            "name": team.get("name"),
            "code_name": team.get("codeName"),
            "logo": image.get("url"),
        }

    def normalize_channels(self, tv_channels: Optional[List[Dict[str, Any]]]) -> List[Dict[str, Any]]:
        results: List[Dict[str, Any]] = []
        for channel in tv_channels or []:
            url_link = channel.get("urlLink") or {}
            logo = channel.get("logo") or {}
            results.append(
                {
                    "name": channel.get("name"),
                    "url": url_link.get("url"),
                    "logo": logo.get("url"),
                }
            )
        return results

    def normalize_score(self, value: Any) -> Optional[int]:
        if value is None:
            return None
        try:
            return int(value)
        except (TypeError, ValueError):
            return None

    def parse_matches(self, next_data: Dict[str, Any]) -> Dict[str, Any]:
        page_props = next_data.get("props", {}).get("pageProps", {})
        groups = page_props.get("data", [])
        output_matches: List[Dict[str, Any]] = []

        for group in groups:
            competition = group.get("competition") or {}
            competition_area = competition.get("area") or {}

            for match in group.get("matches", []):
                score = match.get("score") or {}
                item = {
                    "status": match.get("status"),
                    "start_date_utc": match.get("startDate"),
                    "last_updated_at": match.get("lastUpdatedAt"),
                    "competition": {
                        "name": competition.get("name"),
                        "country_or_area": competition_area.get("name"),
                        "country_or_area_code": competition_area.get("code"),
                        "logo": (competition.get("image") or {}).get("url"),
                    },
                    "round": match.get("round"),
                    "team_a": self.normalize_team(match.get("teamA")),
                    "team_b": self.normalize_team(match.get("teamB")),
                    "score": {
                        "team_a": self.normalize_score(score.get("teamA")),
                        "team_b": self.normalize_score(score.get("teamB")),
                    },
                    "aggregate_score": match.get("aggregateScore"),
                    "tv_channels": self.normalize_channels(match.get("tvChannels")),
                    "venue": match.get("venue"),
                    "match_url": self.normalize_match_url(match.get("link")),
                }
                output_matches.append(item)

        now_utc = datetime.now(timezone.utc).isoformat()
        return {
            "source": MATCHES_TODAY_URL,
            "fetched_at_utc": now_utc,
            "groups_count": len(groups),
            "matches_count": len(output_matches),
            "matches": output_matches,
        }

    def run(self) -> Dict[str, Any]:
        html = self.fetch_page(MATCHES_TODAY_URL)
        next_data = self.extract_next_data(html)
        result = self.parse_matches(next_data)
        return result


def save_json(data: Dict[str, Any], file_path: Path) -> None:
    file_path.parent.mkdir(parents=True, exist_ok=True)
    file_path.write_text(json.dumps(data, ensure_ascii=False, indent=2), encoding="utf-8")


if __name__ == "__main__":
    scraper = KoooraScraper()

    retries = int(os.getenv("SCRAPER_RETRIES", "3"))
    last_error: Optional[Exception] = None

    for attempt in range(1, retries + 1):
        try:
            payload = scraper.run()
            save_json(payload, OUTPUT_FILE)
            print(f"Saved {payload['matches_count']} matches to {OUTPUT_FILE}")
            break
        except Exception as exc:
            last_error = exc
            print(f"Attempt {attempt}/{retries} failed: {exc}")
            if attempt < retries:
                time.sleep(2 * attempt)
    else:
        raise SystemExit(f"Scraper failed after {retries} attempts: {last_error}")
