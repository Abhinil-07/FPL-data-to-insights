import logging
from typing import Optional
import requests
import pandas as pd
import io

logger = logging.getLogger("GitHubArchiveDownloader")

GITHUB_RAW_BASE = "https://raw.githubusercontent.com/vaastav/Fantasy-Premier-League/master/data"

class GitHubArchiveDownloader:
    """
    Downloader for historical FPL datasets from vaastav/Fantasy-Premier-League GitHub repository.
    """
    def __init__(self, raw_base_url: str = GITHUB_RAW_BASE):
        self.raw_base_url = raw_base_url.rstrip("/")

    def fetch_merged_gw(self, season: str) -> Optional[pd.DataFrame]:
        """
        Fetch merged gameweek level CSV data for a given season (e.g. '2023-24').
        Adds a 'season' column automatically.
        """
        url = f"{self.raw_base_url}/{season}/gws/merged_gw.csv"
        logger.info(f"Downloading historical gameweek data for season {season} from {url}...")
        try:
            res = requests.get(url, timeout=30)
            if res.status_code == 200:
                df = pd.read_csv(io.StringIO(res.text), low_memory=False)
                df['season'] = season
                return df
            else:
                logger.error(f"Failed to fetch {url}, HTTP status: {res.status_code}")
                return None
        except Exception as e:
            logger.error(f"Error fetching archive for season {season}: {e}")
            return None

    def fetch_players_raw(self, season: str) -> Optional[pd.DataFrame]:
        """Fetch seasonal player metadata snapshot."""
        url = f"{self.raw_base_url}/{season}/players_raw.csv"
        logger.info(f"Downloading historical players_raw for season {season}...")
        try:
            res = requests.get(url, timeout=30)
            if res.status_code == 200:
                df = pd.read_csv(io.StringIO(res.text), low_memory=False)
                df['season'] = season
                return df
            else:
                logger.error(f"Failed to fetch {url}, HTTP status: {res.status_code}")
                return None
        except Exception as e:
            logger.error(f"Error fetching players_raw for season {season}: {e}")
            return None
