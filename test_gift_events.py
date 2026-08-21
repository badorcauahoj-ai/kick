import tempfile
import unittest
from pathlib import Path
import sys
import types
import os


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
        tracker.recent_text_gift_batches.clear()
        self.old_reconcile_totals = os.environ.pop("GIFT_TOTALS_RECONCILE_JSON", None)
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
        tracker.recent_text_gift_batches.clear()
        if self.old_reconcile_totals is not None:
            os.environ["GIFT_TOTALS_RECONCILE_JSON"] = self.old_reconcile_totals
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

    def test_leaderboard_update_resolves_gifter_name_from_gifter_id(self):
        tracker.extract_from_pusher(
            "App\\Events\\GiftsLeaderboardUpdated",
            {
                "gifter_id": 345,
                "gifted_quantity": 5,
                "leaderboard": [
                    {"user_id": 345, "username": "theuska", "quantity": 94},
                ],
            },
        )

        rows = tracker.read_rows()
        self.assertEqual(len(rows), 1)
        self.assertEqual(rows[0]["username"], "theuska")
        self.assertEqual(rows[0]["quantity"], "5")

    def test_czech_community_and_recipient_notices_record_one_gift_only(self):
        tracker.extract_from_pusher(
            "App\\Events\\ChatMessageEvent",
            {
                "content": "Dejf7 Daroval(a) 1 předplatné komunitě! Celkem daroval(a) 1 předplatné v kanálu.",
                "created_at": "2026-08-21T10:00:00Z",
                "sender": {"username": "system"},
            },
        )
        tracker.extract_from_pusher(
            "App\\Events\\ChatMessageEvent",
            {
                "content": "Dejf7 daroval/a předplatné pro valesh06",
                "created_at": "2026-08-21T10:00:01Z",
                "sender": {"username": "system"},
            },
        )

        rows = tracker.read_rows()
        self.assertEqual(len(rows), 1)
        self.assertEqual(rows[0]["username"], "Dejf7")
        self.assertEqual(rows[0]["quantity"], "1")

    def test_czech_plural_community_notice_from_nested_message_adds_all_tickets(self):
        tracker.extract_from_pusher(
            "App\\Events\\ChatMessageSentEvent",
            {
                "message": {
                    "content": "theuska Daroval(a) 15 předplatných komunitě! Celkem daroval(a) 89 předplatných v kanálu.",
                },
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

    def test_gift_notice_is_not_mistaken_for_a_normal_subscription(self):
        tracker.extract_from_pusher(
            "App\\Events\\SubscriptionEvent",
            {
                "username": "jackass",
                "message": {
                    "id": "gift-notice-1",
                    "content": "jackass Daroval(a) 4 předplatná komunitě!",
                },
            },
        )

        rows = tracker.read_rows()
        self.assertEqual(len(rows), 1)
        self.assertEqual(rows[0]["username"], "jackass")
        self.assertEqual(rows[0]["type"], "gift_subscription")
        self.assertEqual(rows[0]["quantity"], "4")

    def test_gift_payload_is_processed_even_with_an_unexpected_event_name(self):
        tracker.extract_from_pusher(
            "App\\Events\\SubscriptionEvent",
            {
                "gifter_username": "ninja",
                "gifted_quantity": 3,
                "message_id": "gift-payload-1",
            },
        )

        rows = tracker.read_rows()
        self.assertEqual(len(rows), 1)
        self.assertEqual(rows[0]["username"], "ninja")
        self.assertEqual(rows[0]["type"], "gift_subscription")
        self.assertEqual(rows[0]["quantity"], "3")

    def test_reconciliation_adds_only_missing_tickets_and_is_idempotent(self):
        tracker.record_entry("Theushka", "subscription", source="test", event_key="existing")
        os.environ["GIFT_TOTALS_RECONCILE_JSON"] = '{"Theushka": 25, "Dejf7": 1}'

        self.assertEqual(tracker.reconcile_wheel_totals_from_env(), 25)
        self.assertEqual(tracker.reconcile_wheel_totals_from_env(), 0)
        tickets = tracker.NAMES_FILE.read_text(encoding="utf-8").splitlines()
        self.assertEqual(sum(name.casefold() == "theushka" for name in tickets), 25)
        self.assertEqual(sum(name.casefold() == "dejf7" for name in tickets), 1)


if __name__ == "__main__":
    unittest.main()
