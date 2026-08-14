import sys
import os
import unittest

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from src.github_archive import GitHubArchiveDownloader

class TestGitHubArchiveDownloader(unittest.TestCase):

    def setUp(self):
        self.downloader = GitHubArchiveDownloader()

    def test_fetch_merged_gw(self):
        df = self.downloader.fetch_merged_gw("2023-24")
        self.assertIsNotNone(df, "Failed to download merged_gw.csv for 2023-24")
        self.assertGreater(len(df), 0, "merged_gw DataFrame is empty")
        self.assertIn("season", df.columns, "Missing 'season' column")
        self.assertEqual(df["season"].iloc[0], "2023-24")

    def test_fetch_players_raw(self):
        df = self.downloader.fetch_players_raw("2023-24")
        self.assertIsNotNone(df, "Failed to download players_raw.csv for 2023-24")
        self.assertGreater(len(df), 0, "players_raw DataFrame is empty")
        self.assertIn("season", df.columns, "Missing 'season' column")

if __name__ == "__main__":
    unittest.main()
