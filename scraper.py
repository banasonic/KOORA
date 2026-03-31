import json
import os
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional

import requests
from bs4 import BeautifulSoup

BASE_URL = "https://kooora.com"
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
    session.headers.update({
        "User-Agent": USER_AGENT,
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        "Accept-Language": "ar,en-US;q=0.9,en;q=0.8",
    })
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
            raise RuntimeError("لم يتم العثور على __NEXT_DATA__")
        return json.loads(script.string)

    def normalize_team(self, team: Optional[Dict[str, Any]]) -> Dict[str, Any]:
        team = team or {}
        image = team.get("image") or {}
        return {
            "name": team.get("name"),
            "code_name": team.get("codeName"),
            "logo": image.get("url"),
        }

    def normalize_channels(self, tv_channels: Optional[List[Dict[str, Any]]]) -> List[Dict[str, Any]]:
        results = []
        for channel in tv_channels or []:
            logo = channel.get("logo") or {}
            results.append({
                "name": channel.get("name"),
                "logo": logo.get("url"),
            })
        return results

    def normalize_score(self, value: Any) -> Optional[int]:
        try:
            return int(value) if value is not None else None
        except (TypeError, ValueError):
            return None

    def parse_matches(self, next_data: Dict[str, Any]) -> Dict[str, Any]:
        page_props = next_data.get("props", {}).get("pageProps", {})
        groups = page_props.get("data", [])
        output_matches = []

        for group in groups:
            competition = group.get("competition") or {}

            for match in group.get("matches", []):
                score = match.get("score") or {}
                
                # التعديلات المطلوبة هنا
                item = {
                    "status": match.get("status"),
                    "start_date_utc": match.get("startDate"),
                    "last_updated_at": match.get("lastUpdatedAt"),
                    "competition": competition.get("name"), # الاسم فقط
                    "round": match.get("round"),
                    "team_a": self.normalize_team(match.get("teamA")),
                    "team_b": self.normalize_team(match.get("teamB")),
                    "score": {
                        "team_a": self.normalize_score(score.get("teamA")),
                        "team_b": self.normalize_score(score.get("teamB")),
                    },
                    "tv_channels": self.normalize_channels(match.get("tvChannels")),
                    "venue": match.get("venue"),
                    # تم حذف aggregate_score و match_url
                }
                output_matches.append(item)

        return {
            "source": MATCHES_TODAY_URL,
            "fetched_at_utc": datetime.now(timezone.utc).isoformat(),
            "matches_count": len(output_matches),
            "matches": output_matches,
        }

    def run(self) -> Dict[str, Any]:
        html = self.fetch_page(MATCHES_TODAY_URL)
        next_data = self.extract_next_data(html)
        return self.parse_matches(next_data)

def save_json(data: Dict[str, Any], file_path: Path) -> None:
    file_path.parent.mkdir(parents=True, exist_ok=True)
    file_path.write_text(json.dumps(data, ensure_ascii=False, indent=2), encoding="utf-8")

if __name__ == "__main__":
    scraper = KoooraScraper()
    try:
        payload = scraper.run()
        save_json(payload, OUTPUT_FILE)
        print(f"تم الحفظ بنجاح: {payload['matches_count']} مباراة.")
    except Exception as e:
        print(f"فشل الاستخراج: {e}")
