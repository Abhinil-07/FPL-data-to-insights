import time
import logging
from typing import Dict, Any, Optional
import requests

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("FPLApiClient")

BASE_URL = "https://fantasy.premierleague.com/api"

class FPLApiClient:
    """
    Robust API Client for official Fantasy Premier League API endpoints.
    Includes rate-limiting, retry logic, and polite request handling.
    """
    def __init__(self, base_url: str = BASE_URL, rate_limit_delay: float = 0.3, max_retries: int = 3):
        self.base_url = base_url.rstrip("/")
        self.rate_limit_delay = rate_limit_delay
        self.max_retries = max_retries
        self.session = requests.Session()
        self.session.headers.update({
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) FPL-Decision-Support-Pipeline/1.0"
        })

    def _get(self, endpoint: str) -> Optional[Dict[str, Any]]:
        url = f"{self.base_url}/{endpoint.lstrip('/')}"
        for attempt in range(1, self.max_retries + 1):
            try:
                time.sleep(self.rate_limit_delay)
                response = self.session.get(url, timeout=15)
                if response.status_code == 200:
                    return response.json()
                elif response.status_code == 404:
                    logger.warning(f"Resource not found (404) at URL: {url}")
                    return None
                else:
                    logger.warning(f"HTTP {response.status_code} for URL: {url} (Attempt {attempt}/{self.max_retries})")
            except requests.RequestException as e:
                logger.warning(f"Request failed for URL {url}: {e} (Attempt {attempt}/{self.max_retries})")
            time.sleep(1.0 * attempt)
        logger.error(f"Failed to fetch endpoint {endpoint} after {self.max_retries} attempts.")
        return None

    def get_bootstrap_static(self) -> Optional[Dict[str, Any]]:
        """Fetch bootstrap-static containing overall players, teams, events, and positions."""
        logger.info("Fetching bootstrap-static data...")
        return self._get("bootstrap-static/")

    def get_fixtures(self) -> Optional[list]:
        """Fetch full season fixture list with fixture difficulty ratings (FDR)."""
        logger.info("Fetching fixtures data...")
        return self._get("fixtures/")

    def get_element_summary(self, player_id: int) -> Optional[Dict[str, Any]]:
        """Fetch gameweek history and upcoming fixtures for a specific player ID."""
        return self._get(f"element-summary/{player_id}/")

    def get_event_live(self, gw_id: int) -> Optional[Dict[str, Any]]:
        """Fetch live GW stats for ALL players in a single call.
        Returns elements[] where each entry has id + stats{} for every player.
        One call per GW replaces the 700-call element-summary loop.
        """
        logger.info(f"Fetching live GW data for GW {gw_id}...")
        return self._get(f"event/{gw_id}/live/")

    def get_my_team(self, team_id: Optional[int]) -> Optional[Dict[str, Any]]:
        """Fetch user's squad data for current gameweek. Returns None gracefully if team_id is missing."""
        if not team_id:
            logger.info("No FPL Team ID provided; skipping personal team endpoint.")
            return None
        logger.info(f"Fetching squad for Team ID: {team_id}")
        return self._get(f"entry/{team_id}/")

    def get_my_team_history(self, team_id: Optional[int]) -> Optional[Dict[str, Any]]:
        """Fetch user's historical rank and points per gameweek."""
        if not team_id:
            logger.info("No FPL Team ID provided; skipping personal team history endpoint.")
            return None
        logger.info(f"Fetching squad history for Team ID: {team_id}")
        return self._get(f"entry/{team_id}/history/")
