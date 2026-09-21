import sys
import types
import unittest

# main.py only needs flask (for routing) and requests (for the Upstash REST
# calls) at import time; every test here replaces redis_cmd with an in-memory
# fake, so neither dependency needs to be the real package.
if "requests" not in sys.modules:
    sys.modules["requests"] = types.ModuleType("requests")

if "flask" not in sys.modules:
    flask_stub = types.ModuleType("flask")
    flask_stub.Flask = lambda *a, **k: types.SimpleNamespace(route=lambda *a, **k: (lambda f: f))
    flask_stub.abort = lambda *a, **k: None
    flask_stub.jsonify = lambda *a, **k: {"jsonify": (a, k)}
    flask_stub.request = object()
    flask_stub.Response = lambda *a, **k: None
    sys.modules["flask"] = flask_stub

import main as tracker


class FakeRedis:
    """In-memory stand-in for the Upstash Redis REST API used by main.py.

    Implements just the commands main.py issues, including a Python
    re-implementation of CAP_TICKETS_LUA for the EVAL branch, so the tests
    exercise the same cap/idempotency semantics the real Lua script provides
    without needing a live Redis instance.
    """

    def __init__(self):
        self.sets: dict[str, set] = {}
        self.lists: dict[str, list] = {}
        self.hashes: dict[str, dict] = {}

    def cmd(self, *args):
        args = [str(a) for a in args]
        op = args[0].upper()
        if op == "SADD":
            _, key, member = args
            s = self.sets.setdefault(key, set())
            if member in s:
                return 0
            s.add(member)
            return 1
        if op == "RPUSH":
            key = args[1]
            self.lists.setdefault(key, []).extend(args[2:])
            return len(self.lists[key])
        if op == "LRANGE":
            key = args[1]
            return list(self.lists.get(key, []))
        if op == "HGET":
            key, field = args[1], args[2]
            return self.hashes.get(key, {}).get(field)
        if op == "HSET":
            key, field, value = args[1], args[2], args[3]
            self.hashes.setdefault(key, {})[field] = value
            return 1
        if op == "HSETNX":
            key, field, value = args[1], args[2], args[3]
            h = self.hashes.setdefault(key, {})
            if field in h:
                return 0
            h[field] = value
            return 1
        if op == "HINCRBY":
            key, field, amount = args[1], args[2], int(args[3])
            h = self.hashes.setdefault(key, {})
            h[field] = str(int(h.get(field, 0)) + amount)
            return int(h[field])
        if op == "HGETALL":
            key = args[1]
            flat = []
            for k, v in self.hashes.get(key, {}).items():
                flat.extend([k, v])
            return flat
        if op == "HDEL":
            key, field = args[1], args[2]
            return self.hashes.get(key, {}).pop(field, None) is not None
        if op == "DEL":
            key = args[1]
            self.lists.pop(key, None)
            self.hashes.pop(key, None)
            self.sets.pop(key, None)
            return 1
        if op == "EVAL":
            _script, numkeys = args[1], int(args[2])
            keys = args[3:3 + numkeys]
            argv = args[3 + numkeys:]
            counts_key, names_key = keys
            userkey, display, weight, cap = argv[0], argv[1], int(argv[2]), int(argv[3])
            current = int(self.hashes.get(counts_key, {}).get(userkey, 0))
            room = cap - current
            if room <= 0:
                return 0
            toadd = min(weight, room)
            self.hashes.setdefault(counts_key, {})[userkey] = str(current + toadd)
            self.hashes.setdefault(names_key, {}).setdefault(userkey, display)
            return toadd
        raise NotImplementedError(op)


class VercelTrackerTests(unittest.TestCase):
    def setUp(self):
        self.fake = FakeRedis()
        self.old_redis_cmd = tracker.redis_cmd
        tracker.redis_cmd = self.fake.cmd
        self.old_url, self.old_token = tracker.UPSTASH_URL, tracker.UPSTASH_TOKEN
        tracker.UPSTASH_URL, tracker.UPSTASH_TOKEN = "fake", "fake"
        self.old_count_anon = tracker.COUNT_ANONYMOUS_GIFTS
        tracker.COUNT_ANONYMOUS_GIFTS = False

    def tearDown(self):
        tracker.redis_cmd = self.old_redis_cmd
        tracker.UPSTASH_URL, tracker.UPSTASH_TOKEN = self.old_url, self.old_token
        tracker.COUNT_ANONYMOUS_GIFTS = self.old_count_anon

    def test_duplicate_event_key_is_not_recorded_twice(self):
        first = tracker.record_entry("bador", "subscription", source="webhook", event_key="k1")
        second = tracker.record_entry("bador", "subscription", source="webhook", event_key="k1")
        self.assertTrue(first)
        self.assertFalse(second)
        self.assertEqual(len(tracker.read_ledger()), 1)

    def test_wheel_ticket_cap_applies_across_subscription_and_gift_types(self):
        cap = tracker.MAX_WHEEL_TICKETS_PER_USER
        tracker.record_entry("nova", "subscription", source="webhook", event_key="sub1")
        tracker.record_entry(
            "nova", "gift_subscription", quantity=cap - 1, source="webhook", event_key="gift1", weight=cap - 1
        )
        self.assertEqual(tracker.wheel_names().count("nova"), cap)

        tracker.record_entry(
            "nova", "gift_subscription", quantity=10, source="webhook", event_key="gift2", weight=10
        )
        self.assertEqual(tracker.wheel_names().count("nova"), cap)
        self.assertEqual(tracker.read_ledger()[-1]["quantity"], 10)

    def test_official_gift_event_uses_giftee_count_as_quantity(self):
        data = {
            "gifter": {"is_anonymous": False, "username": "gifter1"},
            "giftees": [{"username": "a"}, {"username": "b"}, {"username": "c"}],
        }
        recorded = tracker.handle_official_kick_event("channel.subscription.gifts", data)
        self.assertTrue(recorded)
        rows = tracker.read_ledger()
        self.assertEqual(rows[0]["username"], "gifter1")
        self.assertEqual(rows[0]["quantity"], 3)
        self.assertEqual(tracker.wheel_names().count("gifter1"), 3)

    def test_anonymous_gift_is_skipped_by_default(self):
        data = {"gifter": {"is_anonymous": True}, "giftees": [{"username": "a"}]}
        recorded = tracker.handle_official_kick_event("channel.subscription.gifts", data)
        self.assertFalse(recorded)
        self.assertEqual(tracker.read_ledger(), [])

    def test_remove_username_completely_clears_ledger_and_wheel(self):
        tracker.record_entry("ninja", "subscription", source="webhook", event_key="k1")
        removed = tracker.remove_username_completely("ninja")
        self.assertTrue(removed)
        self.assertEqual(tracker.wheel_names(), [])
        self.assertEqual(tracker.read_ledger(), [])

    def test_remove_one_ticket_decrements_without_touching_ledger(self):
        tracker.record_entry(
            "bador", "gift_subscription", quantity=2, source="webhook", event_key="g1", weight=2
        )
        removed, remaining = tracker.remove_one_ticket("bador")
        self.assertTrue(removed)
        self.assertEqual(remaining, 1)
        self.assertEqual(len(tracker.read_ledger()), 1)

    def test_reconciliation_adds_only_missing_and_respects_the_cap(self):
        tracker.record_entry("Theushka", "subscription", source="test", event_key="existing")
        added = tracker.reconcile_wheel_totals({"Theushka": 25, "Dejf7": 1})
        self.assertEqual(added, 25)
        added_again = tracker.reconcile_wheel_totals({"Theushka": 25, "Dejf7": 1})
        self.assertEqual(added_again, 0)
        names = tracker.wheel_names()
        self.assertEqual(names.count("Theushka"), tracker.MAX_WHEEL_TICKETS_PER_USER)
        self.assertEqual(names.count("Dejf7"), 1)


if __name__ == "__main__":
    unittest.main()
