import sys
import os
import unittest

# Ensure src is in python path
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from src.fpl_api import FPLApiClient

class TestFPLApiClient(unittest.TestCase):

    def setUp(self):
        self.client = FPLApiClient()

    def test_get_bootstrap_static(self):
        data = self.client.get_bootstrap_static()
        self.assertIsNotNone(data, "Failed to fetch bootstrap-static data")
        self.assertIn("elements", data, "bootstrap-static response missing 'elements'")
        self.assertIn("teams", data, "bootstrap-static response missing 'teams'")
        self.assertIn("events", data, "bootstrap-static response missing 'events'")
        self.assertGreater(len(data["elements"]), 0, "No players found in elements")

    def test_get_fixtures(self):
        fixtures = self.client.get_fixtures()
        self.assertIsNotNone(fixtures, "Failed to fetch fixtures data")
        self.assertIsInstance(fixtures, list, "Fixtures payload should be a list")
        self.assertGreater(len(fixtures), 0, "No fixtures found")

    def test_get_my_team_none_graceful(self):
        res = self.client.get_my_team(None)
        self.assertIsNone(res, "Expected None when team_id is None")

if __name__ == "__main__":
    unittest.main()
