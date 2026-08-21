import tempfile
import unittest
from pathlib import Path
import sys
import types


# The application dependencies are installed in Railway.  The bundled local
# Python used for this regression test does not include Flask or Requests, and
# this test exercises only the storage/Pusher functions, so minimal stubs keep
# the import isolated from the web server.
if "requests" not in sys.modules:
    sys.modules["requests"] = types.ModuleType("requests")

if "flask" not in sys.modules:
    flask_stub = types.ModuleType("flask")
    flask_stub.Flask = object
    flask_stub.abort = lambda *args, **kwargs: None
    flask_stub.jsonify = lambda *args, **kwargs: None
    flask_stub.request = object()
    flask_stub.send_file = lambda *args, **kwargs: None
    sys.modules["flask"] = flask_stub

import kick_sub_tracker as tracker


class GiftedSubscriptionEventTests(unittest.TestCase):
    def setUp(self):
        self.tmpdir = tempfile.TemporaryDirectory()
        data_dir = Path(self.tmpdir.name)
        self.old_paths = (
            tracker.DATA_DIR,
            tracker.OUT_CSV,
            tracker.NAMES_FILE,
            tracker.RAW_LOG,
            tracker.SEEN_EVENTS_FILE,
        )
        tracker.DATA_DIR = data_dir
        tracker.OUT_CSV = data_dir / "subscribers.csv"
        tracker.NAMES_FILE = data_dir / "subscription_names.txt"
        tracker.RAW_LOG = data_dir / "raw_events.jsonl"
        tracker.SEEN_EVENTS_FILE = data_dir / "seen_events.json"
        tracker.seen_event_keys.clear()
        tracker.gifter_badge_counts.clear()
        tracker.leaderboard_gift_totals.clear()
        tracker.ensure_storage()

    def tearDown(self):
        (
            tracker.DATA_DIR,
            tracker.OUT_CSV,
            tracker.NAMES_FILE,
            tracker.RAW_LOG,
            tracker.SEEN_EVENTS_FILE,
        ) = self.old_paths
        tracker.seen_event_keys.clear()
        self.tmpdir.cleanup()

    def test_direct_gift_event_adds_the_gifter_and_one_ticket_per_gift(self):
        tracker.extract_from_pusher(
            "App\\Events\\GiftedSubscriptionsEvent",
            {
                "chatroom_id": 123,
                "gifter_username": "zuzk_engova",
                "gifted_usernames": ["viewer_one", "viewer_two", "viewer_three"],
            },
        )

        rows = tracker.read_rows()
        self.assertEqual(len(rows), 1)
        self.assertEqual(rows[0]["username"], "zuzk_engova")
        self.assertEqual(rows[0]["type"], "gift_subscription")
        self.assertEqual(rows[0]["quantity"], "3")
        self.assertEqual(
            tracker.NAMES_FILE.read_text(encoding="utf-8").splitlines(),
            ["zuzk_engova", "zuzk_engova", "zuzk_engova"],
        )

    def test_leaderboard_update_uses_the_actual_gift_not_the_historical_total(self):
        tracker.extract_from_pusher(
            "App\\Events\\GiftsLeaderboardUpdated",
            {
                "gifter_username": "theuska",
                "gifted_quantity": 15,
                "leaderboard": [
                    {"username": "theuska", "quantity": 89},
                ],
            },
        )

        rows = tracker.read_rows()
        self.assertEqual(len(rows), 1)
        self.assertEqual(rows[0]["username"], "theuska")
        self.assertEqual(rows[0]["quantity"], "15")
        self.assertEqual(
            tracker.NAMES_FILE.read_text(encoding="utf-8").splitlines(),
            ["theuska"] * 15,
        )


if __name__ == "__main__":
    unittest.main()
