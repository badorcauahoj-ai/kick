#!/usr/bin/env python3
"""
Kick subscriber / gift-sub tracker.

What is improved compared with the pasted version:
- accepts official Kick webhook events at POST /kick/webhook
- records channel.subscription.gifts by gifter.username, not giftee usernames
- keeps the old public Pusher listener as a fallback
- deduplicates by event key instead of blocking the same username forever
- gives gift-sub gifters one wheel ticket per gifted sub
- reloads counts from CSV after restarts

Install:
    pip install -r requirements-kick-sub-tracker.txt

Run:
    python kick_sub_tracker.py tyblaho69

Useful env vars:
    KICK_CHANNEL=tyblaho69
    DATA_DIR=/data
    PORT=8080
    ENABLE_PUSHER=1
    WEBHOOK_TOKEN=some-secret
    ADMIN_TOKEN=some-other-secret

Official webhook URL:
    https://your-app.example.com/kick/webhook?token=some-secret
"""

from __future__ import annotations

import csv
import hashlib
import html
import json
import os
import re
import secrets
import sys
import threading
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import requests
from flask import Flask, abort, jsonify, request, send_file

try:
    import websocket  # pip package: websocket-client
except ImportError:  # Pusher fallback can be disabled or unavailable.
    websocket = None


PUSHER_WS_URL = (
    "wss://ws-us2.pusher.com/app/32cbd69e4b950bf97679"
    "?protocol=7&client=js&version=8.4.0-rc2&flash=false"
)

DATA_DIR = Path(os.environ.get("DATA_DIR", ".")).resolve()
OUT_CSV = DATA_DIR / "subscribers.csv"
NAMES_FILE = DATA_DIR / "subscription_names.txt"
RAW_LOG = DATA_DIR / "raw_events.jsonl"
SEEN_EVENTS_FILE = DATA_DIR / "seen_events.json"

CSV_HEADER = ["timestamp", "username", "type", "quantity", "source", "event_key", "note"]
OFFICIAL_SUB_EVENTS = {
    "channel.subscription.new",
    "channel.subscription.renewal",
    "channel.subscription.gifts",
}

ENABLE_PUSHER = os.environ.get("ENABLE_PUSHER", "1").lower() not in {"0", "false", "no"}
COUNT_ANONYMOUS_GIFTS = os.environ.get("COUNT_ANONYMOUS_GIFTS", "0").lower() in {"1", "true", "yes"}
DUPLICATE_WINDOW_SECONDS = int(os.environ.get("DUPLICATE_WINDOW_SECONDS", "8"))

lock = threading.RLock()
seen_event_keys: set[str] = set()
gifter_badge_counts: dict[str, int] = {}
leaderboard_gift_totals: dict[str, int] = {}
recent_text_gift_batches: dict[str, tuple[float, int]] = {}


RE_GIFT_TO_USER = re.compile(r"gifted a sub(?:scription)? to\s+(\S+)", re.IGNORECASE)
RE_GIFT_COMMUNITY = re.compile(r"gifted\s+(\d+)\s+subscriptions?\s+to\s+the\s+community", re.IGNORECASE)
RE_CZ_GIFT_COMMUNITY = re.compile(
    r"^\s*(?P<gifter>\S+)\s+daroval(?:\(a\)|/a)?\s+(?P<quantity>\d+)\s+"
    r"předplatn(?:é|í|á|ých)\s+komunit(?:ě|e)\b",
    re.IGNORECASE,
)
RE_CZ_GIFT_TO_USER = re.compile(
    r"^\s*(?P<gifter>\S+)\s+daroval(?:\(a\)|/a)?\s+předplatné\s+pro\s+(?P<recipient>\S+)",
    re.IGNORECASE,
)
RE_NEW_SUB = re.compile(r"^(\S+)\s+subscribed(?:\s+for\s+(\d+)\s+months?)?", re.IGNORECASE)
RE_RESUB = re.compile(r"^(\S+)\s+resubscribed(?:\s+for\s+(\d+)\s+months?)?", re.IGNORECASE)


def utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


def truthy_env(name: str, default: str = "0") -> bool:
    return os.environ.get(name, default).lower() in {"1", "true", "yes"}


def stable_json(value: Any) -> str:
    return json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":"), default=str)


def fingerprint(prefix: str, value: Any) -> str:
    digest = hashlib.sha256(stable_json(value).encode("utf-8")).hexdigest()
    return f"{prefix}:{digest}"


def safe_int(value: Any, default: int = 1) -> int:
    try:
        return int(value)
    except (TypeError, ValueError):
        return default


def normalize_username(username: Any) -> str | None:
    if username is None:
        return None
    text = str(username).strip()
    return text or None


def ensure_storage() -> None:
    DATA_DIR.mkdir(parents=True, exist_ok=True)
    if not OUT_CSV.exists():
        with OUT_CSV.open("w", newline="", encoding="utf-8") as f:
            csv.writer(f).writerow(CSV_HEADER)
    else:
        migrate_csv_if_needed()
    # `subscribers.csv` is the durable event ledger.  The text file is a
    # convenient, weighted cache for the wheel and can always be restored
    # from that ledger if a deploy or an accidental file removal loses it.
    names_file_was_missing = not NAMES_FILE.exists()
    if names_file_was_missing:
        NAMES_FILE.touch()
    load_seen_events()
    if names_file_was_missing:
        with lock:
            restored = rebuild_wheel_tickets_unlocked()
        if restored:
            print(f"[i] Restored {restored} wheel tickets from {OUT_CSV}.")


def migrate_csv_if_needed() -> None:
    with OUT_CSV.open(newline="", encoding="utf-8") as f:
        rows = list(csv.reader(f))
    if not rows:
        with OUT_CSV.open("w", newline="", encoding="utf-8") as f:
            csv.writer(f).writerow(CSV_HEADER)
        return
    if rows[0] == CSV_HEADER:
        return

    old_header = rows[0]
    backup = OUT_CSV.with_suffix(f".csv.bak.{int(time.time())}")
    OUT_CSV.replace(backup)

    migrated: list[list[str]] = [CSV_HEADER]
    for raw in rows[1:]:
        row = dict(zip(old_header, raw))
        username = row.get("username") or (raw[1] if len(raw) > 1 else "")
        entry_type = row.get("type") or (raw[2] if len(raw) > 2 else "subscription")
        quantity = row.get("months") or row.get("quantity") or (raw[3] if len(raw) > 3 else "1")
        if not quantity:
            quantity = "1"
        timestamp = row.get("timestamp") or (raw[0] if raw else utc_now())
        note = row.get("gifted_by") or row.get("note") or ""
        event_key = row.get("event_key") or fingerprint("legacy", raw)
        migrated.append([timestamp, username, entry_type, quantity, "legacy", event_key, note])

    with OUT_CSV.open("w", newline="", encoding="utf-8") as f:
        csv.writer(f).writerows(migrated)
    print(f"[i] Migrated old CSV format. Backup: {backup}")


def load_seen_events() -> None:
    with lock:
        seen_event_keys.clear()
        if SEEN_EVENTS_FILE.exists():
            try:
                data = json.loads(SEEN_EVENTS_FILE.read_text(encoding="utf-8"))
                if isinstance(data, list):
                    seen_event_keys.update(str(item) for item in data)
            except json.JSONDecodeError:
                pass

        for row in read_rows_unlocked():
            key = row.get("event_key")
            if key:
                seen_event_keys.add(key)


def save_seen_events_unlocked() -> None:
    data = sorted(seen_event_keys)
    tmp = SEEN_EVENTS_FILE.with_suffix(".tmp")
    tmp.write_text(json.dumps(data, ensure_ascii=False, indent=2), encoding="utf-8")
    tmp.replace(SEEN_EVENTS_FILE)


def read_rows_unlocked() -> list[dict[str, str]]:
    if not OUT_CSV.exists():
        return []
    with OUT_CSV.open(newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        return [dict(row) for row in reader]


def read_rows() -> list[dict[str, str]]:
    with lock:
        return read_rows_unlocked()


def parse_timestamp(value: str | None) -> datetime | None:
    if not value:
        return None
    try:
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
        if parsed.tzinfo is None:
            parsed = parsed.replace(tzinfo=timezone.utc)
        return parsed.astimezone(timezone.utc)
    except ValueError:
        return None


def is_recent_cross_source_duplicate(username: str, entry_type: str, quantity: int, source: str) -> bool:
    cutoff = time.time() - DUPLICATE_WINDOW_SECONDS
    for row in reversed(read_rows_unlocked()):
        if row.get("username") != username:
            continue
        if row.get("type") != entry_type:
            continue
        if safe_int(row.get("quantity"), 1) != quantity:
            continue
        if row.get("source") == source:
            continue
        ts = parse_timestamp(row.get("timestamp"))
        if ts and ts.timestamp() >= cutoff:
            return True
    return False


def append_wheel_tickets_unlocked(username: str, weight: int) -> None:
    with NAMES_FILE.open("a", encoding="utf-8") as f:
        for _ in range(max(1, weight)):
            f.write(username + "\n")


def wheel_ticket_weight(row: dict[str, str]) -> int:
    """Return the number of tickets represented by one ledger entry."""
    if row.get("type") == "gift_subscription":
        return max(1, safe_int(row.get("quantity"), 1))
    return 1


def rebuild_wheel_tickets_unlocked() -> int:
    """Recreate the wheel cache from the CSV ledger without dropping names."""
    tickets: list[str] = []
    for row in read_rows_unlocked():
        username = normalize_username(row.get("username"))
        if username:
            tickets.extend([username] * wheel_ticket_weight(row))

    tmp = NAMES_FILE.with_suffix(".tmp")
    tmp.write_text("\n".join(tickets) + ("\n" if tickets else ""), encoding="utf-8")
    tmp.replace(NAMES_FILE)
    return len(tickets)


def wheel_ticket_counts_unlocked() -> dict[str, int]:
    """Count currently eligible tickets, case-insensitively, from the wheel cache."""
    counts: dict[str, int] = {}
    if not NAMES_FILE.exists():
        return counts
    for value in NAMES_FILE.read_text(encoding="utf-8").splitlines():
        username = normalize_username(value)
        if username:
            key = username.casefold()
            counts[key] = counts.get(key, 0) + 1
    return counts


def record_entry(
    username: Any,
    entry_type: str,
    *,
    quantity: int = 1,
    source: str,
    event_key: str | None = None,
    note: str = "",
    weight: int | None = None,
) -> bool:
    username = normalize_username(username)
    if not username:
        return False

    quantity = max(1, safe_int(quantity, 1))
    if weight is None:
        weight = quantity if entry_type == "gift_subscription" else 1
    weight = max(1, safe_int(weight, 1))

    if not event_key:
        event_key = fingerprint(source, {"username": username, "type": entry_type, "quantity": quantity, "note": note})

    with lock:
        if event_key in seen_event_keys:
            return False

        if is_recent_cross_source_duplicate(username, entry_type, quantity, source):
            seen_event_keys.add(event_key)
            save_seen_events_unlocked()
            print(f"[i] Skipping probable cross-source duplicate: {username} {entry_type} x{quantity}")
            return False

        seen_event_keys.add(event_key)
        with OUT_CSV.open("a", newline="", encoding="utf-8") as f:
            csv.writer(f).writerow([utc_now(), username, entry_type, quantity, source, event_key, note])
        append_wheel_tickets_unlocked(username, weight)
        save_seen_events_unlocked()

    print(f"[+] {entry_type}: {username} x{quantity} ({source})")
    return True


def reconcile_wheel_totals_from_env() -> int:
    """Add only missing tickets from a one-time operator-supplied total list."""
    raw_totals = os.environ.get("GIFT_TOTALS_RECONCILE_JSON", "").strip()
    if not raw_totals:
        return 0
    try:
        totals = json.loads(raw_totals)
    except json.JSONDecodeError:
        print("[!] GIFT_TOTALS_RECONCILE_JSON is not valid JSON; reconciliation skipped.")
        return 0
    if not isinstance(totals, dict):
        print("[!] GIFT_TOTALS_RECONCILE_JSON must be a JSON object; reconciliation skipped.")
        return 0

    added = 0
    with lock:
        current = wheel_ticket_counts_unlocked()
    for raw_username, raw_total in totals.items():
        username = normalize_username(raw_username)
        total = safe_int(raw_total, 0)
        if not username or total <= 0:
            continue
        missing = total - current.get(username.casefold(), 0)
        if missing <= 0:
            continue
        target_key = fingerprint("manual_total", {"username": username.casefold(), "target": total})
        if record_entry(
            username,
            "gift_subscription",
            quantity=missing,
            source="manual_total_reconciliation",
            event_key=target_key,
            note=f"target_total={total}",
            weight=missing,
        ):
            added += missing
            current[username.casefold()] = total
    if added:
        print(f"[i] Reconciled {added} missing wheel tickets from configured totals.")
    return added


def remove_one_ticket(username: str) -> bool:
    username = username.strip()
    if not username:
        return False
    with lock:
        if not NAMES_FILE.exists():
            return False
        lines = NAMES_FILE.read_text(encoding="utf-8").splitlines()
        removed = False
        kept: list[str] = []
        for line in lines:
            if not removed and line == username:
                removed = True
                continue
            kept.append(line)
        if removed:
            NAMES_FILE.write_text("\n".join(kept) + ("\n" if kept else ""), encoding="utf-8")
        return removed


def remove_username_completely(username: str) -> bool:
    username = username.strip()
    if not username:
        return False

    with lock:
        found = False
        rows = read_rows_unlocked()
        kept_rows = [row for row in rows if row.get("username") != username]
        found = len(kept_rows) != len(rows)

        with OUT_CSV.open("w", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(f, fieldnames=CSV_HEADER)
            writer.writeheader()
            writer.writerows(kept_rows)

        if NAMES_FILE.exists():
            names = NAMES_FILE.read_text(encoding="utf-8").splitlines()
            kept_names = [name for name in names if name != username]
            if len(kept_names) != len(names):
                found = True
            NAMES_FILE.write_text("\n".join(kept_names) + ("\n" if kept_names else ""), encoding="utf-8")

        return found


def log_raw(source: str, event_name: str, data: dict[str, Any], headers: dict[str, str] | None = None) -> None:
    entry = {
        "timestamp": utc_now(),
        "source": source,
        "event": event_name,
        "headers": headers or {},
        "data": data,
    }
    with RAW_LOG.open("a", encoding="utf-8") as f:
        f.write(json.dumps(entry, ensure_ascii=False) + "\n")


def webhook_event_key(event_type: str, data: dict[str, Any], headers: dict[str, str]) -> str:
    for name in (
        "Kick-Event-Message-Id",
        "Kick-Event-Id",
        "Kick-Event-Subscription-Id",
        "X-Kick-Event-Id",
    ):
        value = headers.get(name)
        if value:
            return f"webhook:{event_type}:{value}"
    return fingerprint(f"webhook:{event_type}", data)


def handle_official_kick_event(event_type: str, data: dict[str, Any], headers: dict[str, str]) -> bool:
    key = webhook_event_key(event_type, data, headers)

    if event_type == "channel.subscription.new":
        subscriber = data.get("subscriber") or {}
        duration = safe_int(data.get("duration"), 1)
        return record_entry(
            subscriber.get("username"),
            "subscription",
            quantity=1,
            source="webhook",
            event_key=key,
            note=f"duration={duration}",
            weight=1,
        )

    if event_type == "channel.subscription.renewal":
        subscriber = data.get("subscriber") or {}
        duration = safe_int(data.get("duration"), 1)
        return record_entry(
            subscriber.get("username"),
            "resubscription",
            quantity=1,
            source="webhook",
            event_key=key,
            note=f"duration={duration}",
            weight=1,
        )

    if event_type == "channel.subscription.gifts":
        gifter = data.get("gifter") or {}
        username = normalize_username(gifter.get("username"))
        if not username:
            if not COUNT_ANONYMOUS_GIFTS:
                print("[i] Anonymous gift-sub webhook skipped; gifter username is hidden by Kick.")
                return False
            username = "anonymous_gifter"

        giftees = data.get("giftees")
        quantity = len(giftees) if isinstance(giftees, list) else safe_int(data.get("quantity"), 1)
        return record_entry(
            username,
            "gift_subscription",
            quantity=max(1, quantity),
            source="webhook",
            event_key=key,
            note="official_gift",
            weight=max(1, quantity),
        )

    return False


def get_ids(slug: str) -> tuple[int, int]:
    url = f"https://kick.com/api/v2/channels/{slug}"
    headers = {
        "User-Agent": (
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
            "(KHTML, like Gecko) Chrome/124.0 Safari/537.36"
        ),
        "Accept": "application/json",
    }
    resp = requests.get(url, headers=headers, timeout=15)
    resp.raise_for_status()
    data = resp.json()
    channel_id = int(data["id"])
    chatroom_id = int(data["chatroom"]["id"])
    print(f"[i] Channel {slug}: channel_id={channel_id}, chatroom_id={chatroom_id}")
    return channel_id, chatroom_id


def pusher_event_key(event_name: str, data: dict[str, Any]) -> str:
    return fingerprint(f"pusher:{event_name}", data)


def record_pusher_gift(
    event_name: str,
    data: dict[str, Any],
    *,
    recipient_fields: tuple[str, ...],
    source: str,
) -> bool:
    """Record the buyer from Kick's direct gift-subscription Pusher event."""
    gifter = data.get("gifter") or {}
    username = normalize_username(
        data.get("gifter_username")
        or data.get("gifterUsername")
        or (gifter.get("username") if isinstance(gifter, dict) else None)
    )
    if not username:
        return False

    recipients: list[Any] = []
    for field in recipient_fields:
        value = data.get(field)
        if isinstance(value, list):
            recipients = value
            break

    quantity = len(recipients) if recipients else safe_int(
        data.get("gifted_quantity") or data.get("quantity"), 0
    )
    if quantity <= 0:
        return False

    return record_entry(
        username,
        "gift_subscription",
        quantity=quantity,
        source=source,
        event_key=pusher_event_key(event_name, data),
        note="pusher_direct_gift",
        weight=quantity,
    )


def record_gifter_badge(username: str, badge_count: int) -> bool:
    username = username.strip()
    if not username:
        return False

    with lock:
        prev = gifter_badge_counts.get(username)
        gifter_badge_counts[username] = badge_count

    if prev is None:
        return False

    delta = badge_count - prev
    if delta <= 0:
        return False

    return record_entry(
        username,
        "gift_subscription",
        quantity=delta,
        source="pusher_badge",
        event_key=f"pusher_badge:{username}:{prev}->{badge_count}",
        note=f"sub_gifter_badge_total={badge_count}",
        weight=delta,
    )


def record_gifts_leaderboard(username: str, total_quantity: int) -> bool:
    username = username.strip()
    if not username:
        return False

    with lock:
        prev = leaderboard_gift_totals.get(username)
        leaderboard_gift_totals[username] = total_quantity

    if prev is None:
        return False

    delta = total_quantity - prev
    if delta <= 0:
        return False

    return record_entry(
        username,
        "gift_subscription",
        quantity=delta,
        source="pusher_leaderboard",
        event_key=f"pusher_leaderboard:{username}:{prev}->{total_quantity}",
        note=f"leaderboard_total={total_quantity}",
        weight=delta,
    )


def start_text_gift_batch(username: str, quantity: int) -> None:
    """Remember a community-gift message so recipient notices are not double counted."""
    with lock:
        recent_text_gift_batches[username.casefold()] = (
            time.monotonic() + DUPLICATE_WINDOW_SECONDS,
            max(1, quantity),
        )


def consume_text_gift_batch(username: str) -> bool:
    """Return true when a recipient notice belongs to a just-recorded community gift."""
    key = username.casefold()
    with lock:
        batch = recent_text_gift_batches.get(key)
        if not batch:
            return False
        expires_at, remaining = batch
        if time.monotonic() > expires_at or remaining <= 0:
            recent_text_gift_batches.pop(key, None)
            return False
        if remaining == 1:
            recent_text_gift_batches.pop(key, None)
        else:
            recent_text_gift_batches[key] = (expires_at, remaining - 1)
        return True


def pusher_chat_content(data: dict[str, Any]) -> str:
    """Read a chat/system notice from both known Pusher payload layouts."""
    value = data.get("content")
    if not isinstance(value, str):
        message = data.get("message") or {}
        value = message.get("content") if isinstance(message, dict) else ""
    return str(value or "").replace("\u00a0", " ").replace("**", "").strip()


def pusher_chat_sender(data: dict[str, Any]) -> str | None:
    """Find the sender when it exists; Kick system notices have no sender."""
    sender = data.get("sender") or {}
    if not isinstance(sender, dict):
        sender = {}
    if not sender:
        message = data.get("message") or {}
        if isinstance(message, dict):
            nested_sender = message.get("sender") or message.get("user") or {}
            sender = nested_sender if isinstance(nested_sender, dict) else {}
    return normalize_username(sender.get("username"))


def record_czech_gift_notice(event_name: str, data: dict[str, Any]) -> bool:
    """Record Czech gift-system messages, including ones mislabelled as sub events."""
    content = pusher_chat_content(data)
    match = RE_CZ_GIFT_COMMUNITY.match(content)
    if match:
        gifter_username = normalize_username(match.group("gifter"))
        quantity = safe_int(match.group("quantity"), 1)
        if gifter_username:
            start_text_gift_batch(gifter_username, quantity)
            record_entry(
                gifter_username,
                "gift_subscription",
                quantity=quantity,
                source="pusher_text_community",
                event_key=pusher_event_key(event_name, data),
                note="gift_text_community_cs",
                weight=quantity,
            )
        return True

    match = RE_CZ_GIFT_TO_USER.match(content)
    if match:
        gifter_username = normalize_username(match.group("gifter"))
        if gifter_username and not consume_text_gift_batch(gifter_username):
            record_entry(
                gifter_username,
                "gift_subscription",
                quantity=1,
                source="pusher_text_recipient",
                event_key=pusher_event_key(event_name, data),
                note="gift_text_to_user_cs",
                weight=1,
            )
        return True
    return False


def extract_from_pusher(event_name: str, data: dict[str, Any]) -> None:
    # Process actual gift data before a generic subscription event. Kick has
    # occasionally delivered a gift notice under a subscription event name.
    if record_czech_gift_notice(event_name, data):
        return
    if record_pusher_gift(
        event_name,
        data,
        recipient_fields=("gifted_usernames", "usernames", "giftees"),
        source="pusher_gift_payload",
    ):
        return

    if event_name == "App\\Events\\SubscriptionEvent":
        username = data.get("username")
        months = safe_int(data.get("months"), 1)
        record_entry(
            username,
            "subscription",
            quantity=1,
            source="pusher",
            event_key=pusher_event_key(event_name, data),
            note=f"months={months}",
            weight=1,
        )
        return

    if event_name == "App\\Events\\ChannelSubscriptionEvent":
        return

    if event_name == "App\\Events\\GiftedSubscriptionsEvent":
        # This is the primary Pusher event for a gift.  It contains the buyer
        # (`gifter_username`) and the users receiving the subscriptions.
        record_pusher_gift(
            event_name,
            data,
            recipient_fields=("gifted_usernames", "usernames", "giftees"),
            source="pusher_gift",
        )
        return

    if event_name == "App\\Events\\LuckyUsersWhoGotGiftSubscriptionsEvent":
        # Some Kick clients expose the buyer on this companion event instead
        # of the direct GiftedSubscriptionsEvent.  Still record the buyer,
        # never the recipients.  A matching direct event is deduplicated by
        # the short cross-source duplicate window in record_entry().
        record_pusher_gift(
            event_name,
            data,
            recipient_fields=("usernames", "gifted_usernames", "giftees"),
            source="pusher_gift_recipients",
        )
        return

    if event_name == "App\\Events\\GiftsLeaderboardUpdated":
        # Kick sends the actual gift that caused this update at the top level.
        # Reading only `leaderboard` loses a gift from a new/non-top donor and
        # depends on an in-memory previous total, which is reset on restart.
        record_pusher_gift(
            event_name,
            data,
            recipient_fields=("gifted_usernames", "usernames", "giftees"),
            source="pusher_gift_leaderboard_event",
        )
        for entry in data.get("leaderboard", []) or []:
            username = normalize_username(entry.get("username"))
            quantity = safe_int(entry.get("quantity"), 0)
            if username and quantity > 0:
                record_gifts_leaderboard(username, quantity)
        return

    if "chatmessage" not in event_name.lower():
        return

    sender = data.get("sender") or {}
    if not isinstance(sender, dict):
        sender = {}
    sender_username = pusher_chat_sender(data)
    if sender_username:
        badges = ((sender.get("identity") or {}).get("badges") or [])
        for badge in badges:
            if badge.get("type") == "sub_gifter" and isinstance(badge.get("count"), int):
                record_gifter_badge(sender_username, badge["count"])
                break

    content = pusher_chat_content(data)
    if not content:
        return

    if not sender_username:
        return

    if RE_GIFT_TO_USER.search(content):
        record_entry(
            sender_username,
            "gift_subscription",
            quantity=1,
            source="pusher_text",
            event_key=pusher_event_key(event_name, {"sender": sender_username, "content": content, "created_at": data.get("created_at")}),
            note="gift_text_to_user",
            weight=1,
        )
        return

    match = RE_GIFT_COMMUNITY.search(content)
    if match:
        quantity = safe_int(match.group(1), 1)
        record_entry(
            sender_username,
            "gift_subscription",
            quantity=quantity,
            source="pusher_text",
            event_key=pusher_event_key(event_name, {"sender": sender_username, "content": content, "created_at": data.get("created_at")}),
            note="gift_text_community",
            weight=quantity,
        )
        return

    match = RE_NEW_SUB.match(content)
    if match:
        record_entry(
            match.group(1),
            "subscription",
            quantity=1,
            source="pusher_text",
            event_key=pusher_event_key(event_name, {"content": content, "created_at": data.get("created_at")}),
            note=f"months={match.group(2) or 1}",
            weight=1,
        )
        return

    match = RE_RESUB.match(content)
    if match:
        record_entry(
            match.group(1),
            "resubscription",
            quantity=1,
            source="pusher_text",
            event_key=pusher_event_key(event_name, {"content": content, "created_at": data.get("created_at")}),
            note=f"months={match.group(2) or 1}",
            weight=1,
        )


def on_message(ws: Any, message: str) -> None:
    try:
        outer = json.loads(message)
    except json.JSONDecodeError:
        return

    event_name = outer.get("event", "")
    if event_name == "pusher:connection_established":
        return
    if event_name == "pusher:ping":
        ws.send(json.dumps({"event": "pusher:pong", "data": {}}))
        return
    if event_name == "pusher_internal:subscription_succeeded":
        print("[i] Pusher subscription succeeded.")
        return

    data = outer.get("data")
    if isinstance(data, str):
        try:
            data = json.loads(data)
        except json.JSONDecodeError:
            data = {}
    if not isinstance(data, dict):
        data = {}

    log_raw("pusher", event_name, data)
    extract_from_pusher(event_name, data)


def on_open(ws: Any, channel_id: int, chatroom_id: int) -> None:
    channels = [
        f"chatrooms.{chatroom_id}.v2",
        f"channel.{channel_id}",
        f"leaderboards.{channel_id}",
        f"gifts.{channel_id}",
    ]
    for channel in channels:
        ws.send(json.dumps({"event": "pusher:subscribe", "data": {"channel": channel}}))


def on_error(ws: Any, error: Any) -> None:
    print(f"[!] WebSocket error: {error}")


def on_close(ws: Any, close_status_code: Any, close_msg: Any) -> None:
    print(f"[i] WebSocket closed ({close_status_code}).")


def run_pusher_loop(slug: str) -> None:
    if websocket is None:
        print("[!] websocket-client is not installed; Pusher fallback disabled.")
        return

    while True:
        try:
            channel_id, chatroom_id = get_ids(slug)
            break
        except Exception as exc:
            print(f"[!] Could not resolve Kick IDs ({exc}); retrying in 15s.")
            time.sleep(15)

    print(f"[i] Connecting to Kick/Pusher for {slug}...")
    while True:
        ws = websocket.WebSocketApp(
            PUSHER_WS_URL,
            on_message=on_message,
            on_error=on_error,
            on_close=on_close,
        )
        ws.on_open = lambda ws_: on_open(ws_, channel_id, chatroom_id)

        try:
            ws.run_forever(ping_interval=30, ping_timeout=10)
        except KeyboardInterrupt:
            raise
        except Exception as exc:
            print(f"[!] Pusher loop error: {exc}")

        print("[i] Pusher connection dropped; reconnecting in 5s.")
        time.sleep(5)


def maybe_auto_subscribe_official_events() -> None:
    if not truthy_env("AUTO_SUBSCRIBE_EVENTS"):
        return

    token = os.environ.get("KICK_API_ACCESS_TOKEN")
    broadcaster_id = os.environ.get("KICK_BROADCASTER_USER_ID")
    if not token or not broadcaster_id:
        print("[!] AUTO_SUBSCRIBE_EVENTS=1 requires KICK_API_ACCESS_TOKEN and KICK_BROADCASTER_USER_ID.")
        return

    payload = {
        "broadcaster_user_id": safe_int(broadcaster_id, 0),
        "events": [{"name": name, "version": 1} for name in sorted(OFFICIAL_SUB_EVENTS)],
    }
    try:
        resp = requests.post(
            "https://api.kick.com/public/v1/events/subscriptions",
            headers={"Authorization": f"Bearer {token}", "Content-Type": "application/json"},
            json=payload,
            timeout=15,
        )
        if resp.status_code >= 400:
            print(f"[!] Kick event subscription setup failed: {resp.status_code} {resp.text}")
        else:
            print("[i] Official Kick event subscriptions requested.")
    except Exception as exc:
        print(f"[!] Kick event subscription setup failed: {exc}")


def start_file_server(slug: str) -> None:
    app = Flask(__name__)

    def admin_allowed() -> bool:
        token = os.environ.get("ADMIN_TOKEN")
        if not token:
            # A public wheel must never also expose a public delete endpoint.
            return False
        provided = request.args.get("admin") or request.headers.get("X-Admin-Token")
        return bool(provided) and secrets.compare_digest(provided, token)

    def permanent_deletion_enabled() -> bool:
        return bool(os.environ.get("ADMIN_TOKEN")) and truthy_env("ALLOW_PERMANENT_DELETE")

    def admin_qs() -> str:
        token = request.args.get("admin")
        if token and os.environ.get("ADMIN_TOKEN") == token:
            return "?admin=" + html.escape(token, quote=True)
        return ""

    def download_link(filename: str) -> str:
        qs = admin_qs()
        return f"/{filename}{qs}"

    @app.route("/health")
    def health() -> Any:
        return jsonify(ok=True, channel=slug, rows=len(read_rows()))

    @app.route("/kick/webhook", methods=["POST"])
    def kick_webhook() -> Any:
        expected = os.environ.get("WEBHOOK_TOKEN")
        provided = request.args.get("token") or request.headers.get("X-Webhook-Token")
        if expected and provided != expected:
            return jsonify(ok=False, error="forbidden"), 403

        payload = request.get_json(silent=True) or {}
        if not isinstance(payload, dict):
            return jsonify(ok=False, error="invalid_json"), 400

        headers = {k: v for k, v in request.headers.items() if k.lower().startswith(("kick-", "x-kick-"))}
        event_type = request.headers.get("Kick-Event-Type") or request.args.get("event") or payload.get("event")
        event_type = str(event_type or "").strip()
        data = payload.get("data") if isinstance(payload.get("data"), dict) else payload

        log_raw("webhook", event_type or "unknown", data, headers)
        if event_type not in OFFICIAL_SUB_EVENTS:
            return jsonify(ok=True, recorded=False, ignored=event_type)

        recorded = handle_official_kick_event(event_type, data, headers)
        return jsonify(ok=True, recorded=recorded)

    @app.route("/")
    def index() -> str:
        rows = read_rows()
        recent = list(reversed(rows[-50:]))
        if recent:
            row_html = "\n".join(render_table_row(row) for row in recent)
        else:
            row_html = '<div class="empty-row">Zadne jmeno zatim zaznamenane - cekam na prvni sub/gift.</div>'

        qs = admin_qs()
        count = len(rows)
        names_filename = html.escape(NAMES_FILE.name)
        csv_filename = html.escape(OUT_CSV.name)
        raw_filename = html.escape(RAW_LOG.name)

        return f"""<!doctype html>
<html lang="cs">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<meta http-equiv="refresh" content="15">
<title>{html.escape(slug)} - kick-sub-tracker</title>
<style>
:root {{
  --bg:#050505; --panel:#0b0b0b; --border:rgba(255,255,255,.1);
  --soft:rgba(255,255,255,.055); --text:#efefef; --muted:#777; --sub:#aaa;
  --white:#f3f3f3; --green:#55c878; --red:#ff695f;
}}
* {{ box-sizing:border-box; }}
body {{ margin:0; min-height:100vh; background:var(--bg); color:var(--text);
  font-family:Inter, ui-sans-serif, system-ui, -apple-system, Segoe UI, sans-serif;
  display:flex; justify-content:center; padding:72px 20px; }}
main {{ width:100%; max-width:720px; }}
header {{ display:flex; align-items:center; justify-content:space-between; margin-bottom:28px; }}
.brand {{ display:flex; gap:12px; align-items:center; }}
.mark {{ width:34px; height:34px; display:grid; place-items:center; border:1px solid var(--border);
  border-radius:8px; font:600 12px ui-monospace, SFMono-Regular, Consolas, monospace; color:var(--sub); }}
h1 {{ margin:0; font-size:15px; }}
.path {{ color:var(--muted); font:12px ui-monospace, SFMono-Regular, Consolas, monospace; margin-top:2px; }}
.nav {{ color:var(--sub); text-decoration:none; border:1px solid var(--border); border-radius:8px; padding:8px 13px; font-size:13px; }}
.panel {{ background:var(--panel); border:1px solid var(--border); border-radius:12px; overflow:hidden; margin-bottom:16px; }}
.meta {{ display:grid; grid-template-columns:repeat(3, 1fr); }}
.meta > div {{ padding:18px 20px; border-right:1px solid var(--soft); }}
.meta > div:last-child {{ border-right:0; }}
.k {{ color:var(--muted); text-transform:uppercase; letter-spacing:.06em; font-size:11px; margin-bottom:8px; }}
.v {{ font:600 21px ui-monospace, SFMono-Regular, Consolas, monospace; }}
.v.small {{ font-size:14px; }}
.actions {{ display:grid; grid-template-columns:repeat(3, 1fr); gap:8px; margin-bottom:16px; }}
.btn {{ text-align:center; text-decoration:none; padding:11px 14px; border-radius:8px; border:1px solid var(--border); color:var(--text); font-size:13px; }}
.btn.primary {{ background:var(--white); color:#090909; border-color:var(--white); font-weight:600; }}
.head,.row {{ display:grid; grid-template-columns:minmax(120px,1fr) 145px 72px 36px; align-items:center; gap:12px; }}
.head {{ padding:11px 20px; color:var(--muted); text-transform:uppercase; letter-spacing:.06em; font-size:11px; border-bottom:1px solid var(--soft); }}
.row {{ padding:13px 20px; border-bottom:1px solid var(--soft); font-size:13px; }}
.row:last-child {{ border-bottom:0; }}
.name {{ font-family:ui-monospace, SFMono-Regular, Consolas, monospace; overflow:hidden; text-overflow:ellipsis; }}
.type,.qty {{ color:var(--sub); }}
.qty {{ text-align:right; font-family:ui-monospace, SFMono-Regular, Consolas, monospace; }}
.del {{ background:transparent; color:var(--muted); border:0; font-size:18px; cursor:pointer; border-radius:6px; padding:4px; }}
.del:hover {{ color:var(--red); background:rgba(255,105,95,.09); }}
.empty-row {{ color:var(--muted); text-align:center; padding:34px 20px; font-size:13px; }}
footer {{ color:var(--muted); display:flex; justify-content:space-between; font-size:12px; padding:3px 2px; }}
@media (max-width:620px) {{
  body {{ padding:32px 12px; }}
  .meta,.actions {{ grid-template-columns:1fr; }}
  .meta > div {{ border-right:0; border-bottom:1px solid var(--soft); }}
  .head,.row {{ grid-template-columns:minmax(100px,1fr) 90px 48px 32px; gap:8px; padding-left:12px; padding-right:12px; }}
}}
</style>
</head>
<body>
<main>
  <header>
    <div class="brand"><div class="mark">KS</div><div><h1>kick-sub-tracker</h1><div class="path">{html.escape(slug)}</div></div></div>
    <a class="nav" href="/wheel{qs}">Kolo stesti -></a>
  </header>
  <section class="panel"><div class="meta">
    <div><div class="k">Zaznamenano</div><div class="v">{count}</div></div>
    <div><div class="k">Kanal</div><div class="v small">{html.escape(slug)}</div></div>
    <div><div class="k">Zdroj</div><div class="v small">webhook + pusher</div></div>
  </div></section>
  <nav class="actions">
    <a class="btn primary" href="{download_link(names_filename)}">Stahnout jmena (.txt)</a>
    <a class="btn" href="{download_link(csv_filename)}">Export detailu (.csv)</a>
    <a class="btn" href="{download_link(raw_filename)}">Raw eventy</a>
  </nav>
  <section class="panel">
    <div class="head"><div>Jmeno</div><div>Typ</div><div class="qty">Pocet</div><div></div></div>
    {row_html}
  </section>
  <footer><span>auto-refresh 15s</span><span>/kick/webhook ready</span></footer>
</main>
<script>
const adminToken = new URLSearchParams(location.search).get('admin') || '';
document.querySelectorAll('.del').forEach((btn) => {{
  btn.addEventListener('click', async () => {{
    const name = btn.dataset.name;
    if (!confirm('Smazat ' + name + ' uplne i z kola?')) return;
    btn.disabled = true;
    const url = '/delete' + (adminToken ? '?admin=' + encodeURIComponent(adminToken) : '');
    const resp = await fetch(url, {{
      method: 'POST',
      headers: {{ 'Content-Type': 'application/json', 'X-Admin-Token': adminToken }},
      body: JSON.stringify({{ name }})
    }});
    const data = await resp.json().catch(() => ({{ ok:false }}));
    if (data.ok) btn.closest('.row').remove();
    else btn.disabled = false;
  }});
}});
</script>
</body>
</html>"""

    def render_table_row(row: dict[str, str]) -> str:
        name = row.get("username", "")
        name_attr = html.escape(name, quote=True)
        delete_button = (
            f'<button class="del" data-name="{name_attr}" title="Smazat">x</button>'
            if permanent_deletion_enabled()
            else ""
        )
        return (
            '<div class="row">'
            f'<div class="name">{html.escape(name)}</div>'
            f'<div class="type">{html.escape(row.get("type", ""))}</div>'
            f'<div class="qty">{html.escape(row.get("quantity", "1"))}</div>'
            f"{delete_button}"
            "</div>"
        )

    @app.route("/delete", methods=["POST"])
    def delete_name() -> Any:
        if not permanent_deletion_enabled() or not admin_allowed():
            return jsonify(ok=False, error="forbidden"), 403
        payload = request.get_json(silent=True) or {}
        name = (payload.get("name") or "").strip()
        if not name:
            return jsonify(ok=False, error="missing_name"), 400
        return jsonify(ok=remove_username_completely(name))

    @app.route("/wheel")
    def wheel() -> str:
        names = []
        if NAMES_FILE.exists():
            names = [line.strip() for line in NAMES_FILE.read_text(encoding="utf-8").splitlines() if line.strip()]

        # This is deliberately opt-in and visibly labelled in the page.  It
        # exists for rehearsals only; production spins stay random by default.
        test_winners: list[str] = []
        if truthy_env("WHEEL_TEST_MODE"):
            test_winners = [
                username
                for username in (
                    normalize_username(value) for value in os.environ.get("WHEEL_TEST_WINNERS", "").split(",")
                )
                if username
            ]

        qs = admin_qs()
        if not names:
            content = '<div class="empty">Zatim nejsou zadne listky. Jakmile prijde sub nebo gift, kolo se naplni.</div>'
            script = ""
        else:
            names_json = json.dumps(names, ensure_ascii=False).replace("</", "<\\/")
            test_winners_json = json.dumps(test_winners, ensure_ascii=False).replace("</", "<\\/")
            test_notice = (
                '<div class="test-mode">TEST REZIM: predvolene vysledky jsou zapnute.</div>'
                if test_winners
                else ""
            )
            content = """
<div class="wheel-wrap">
  <div class="pointer"></div>
  <canvas id="wheel" width="700" height="700"></canvas>
  <div class="hub"></div>
</div>
<button id="spin" class="spin">Roztocit kolo</button>
<div id="meta" class="meta-line"></div>
""" + test_notice + """
<div id="result" class="result">
  <div class="label">Vitez</div>
  <div id="winner" class="winner"></div>
  <div class="result-actions">
    <button id="remove" class="remove">Odebrat jeden listek</button>
    <button id="keep" class="keep">Nechat na kole</button>
  </div>
  <div id="status" class="status"></div>
</div>
"""
            script = f"""
const names = {names_json};
const testWinners = {test_winners_json};
let testSpin = 0;
const canvas = document.getElementById('wheel');
const ctx = canvas.getContext('2d');
const size = canvas.width;
const center = size / 2;
const radius = center - 4;
let rotation = 0;
document.getElementById('meta').textContent = names.length + ' listku na kole';

function draw() {{
  const n = names.length;
  const arc = Math.PI * 2 / n;
  ctx.clearRect(0, 0, size, size);
  for (let i = 0; i < n; i++) {{
    const start = i * arc;
    ctx.beginPath();
    ctx.moveTo(center, center);
    ctx.arc(center, center, radius, start, start + arc);
    ctx.closePath();
    ctx.fillStyle = i % 2 ? '#101113' : '#1a1b1f';
    ctx.fill();
    ctx.strokeStyle = 'rgba(255,255,255,.06)';
    ctx.stroke();
    if (n <= 42) {{
      ctx.save();
      ctx.translate(center, center);
      ctx.rotate(start + arc / 2);
      ctx.textAlign = 'right';
      ctx.fillStyle = '#efefef';
      ctx.font = (n <= 16 ? '20px' : '16px') + ' ui-monospace, Consolas, monospace';
      ctx.fillText(names[i], radius - 16, 5);
      ctx.restore();
    }}
  }}
}}
draw();

function winnerIndexFor(name) {{
  const tickets = [];
  for (let i = 0; i < names.length; i++) {{
    if (names[i] === name) tickets.push(i);
  }}
  return tickets.length ? tickets[Math.floor(Math.random() * tickets.length)] : -1;
}}

document.getElementById('spin').addEventListener('click', () => {{
  const btn = document.getElementById('spin');
  const result = document.getElementById('result');
  btn.disabled = true;
  result.style.display = 'none';
  const n = names.length;
  const configuredWinner = testWinners[testSpin++] || '';
  const configuredIndex = configuredWinner ? winnerIndexFor(configuredWinner) : -1;
  const winnerIndex = configuredIndex >= 0 ? configuredIndex : Math.floor(Math.random() * n);
  const arcDeg = 360 / n;
  const target = winnerIndex * arcDeg + arcDeg / 2;
  // The pointer is at 12 o'clock (270 degrees in canvas coordinates).
  // Account for the current rotation too, otherwise a second spin lands on
  // another slice than the name we announce as the winner.
  const pointerAngle = 270;
  const currentAngle = ((rotation % 360) + 360) % 360;
  const finishDelta = (pointerAngle - target - currentAngle + 360) % 360;
  rotation += 6 * 360 + finishDelta;
  canvas.style.transform = `rotate(${{rotation}}deg)`;
  setTimeout(() => {{
    document.getElementById('winner').textContent = names[winnerIndex];
    result.style.display = 'block';
    btn.disabled = false;
  }}, 4700);
}});

document.getElementById('keep').addEventListener('click', () => {{
  document.getElementById('result').style.display = 'none';
}});

document.getElementById('remove').addEventListener('click', async () => {{
  const winner = document.getElementById('winner').textContent;
  const adminToken = new URLSearchParams(location.search).get('admin') || '';
  const url = '/wheel/remove' + (adminToken ? '?admin=' + encodeURIComponent(adminToken) : '');
  document.getElementById('status').textContent = 'Odebiram...';
  const resp = await fetch(url, {{
    method: 'POST',
    headers: {{ 'Content-Type': 'application/json', 'X-Admin-Token': adminToken }},
    body: JSON.stringify({{ name: winner }})
  }});
  const data = await resp.json().catch(() => ({{ ok:false }}));
  if (data.ok) {{
    document.getElementById('status').textContent = 'Odebrano, zbyva ' + data.remaining + 'x.';
    setTimeout(() => location.reload(), 900);
  }} else {{
    document.getElementById('status').textContent = 'Nepovedlo se odebrat.';
  }}
}});
"""

        return f"""<!doctype html>
<html lang="cs">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>Kolo stesti - {html.escape(slug)}</title>
<style>
:root {{ --bg:#050505; --panel:#0b0b0b; --border:rgba(255,255,255,.1); --text:#efefef; --muted:#777; --white:#f3f3f3; --green:#55c878; --red:#ff695f; }}
* {{ box-sizing:border-box; }}
body {{ margin:0; min-height:100vh; background:var(--bg); color:var(--text); font-family:Inter, ui-sans-serif, system-ui, -apple-system, Segoe UI, sans-serif; display:flex; align-items:center; flex-direction:column; padding:48px 20px; }}
header {{ width:100%; max-width:560px; display:flex; align-items:center; justify-content:space-between; margin-bottom:28px; }}
h1 {{ margin:0; font-size:16px; }}
.path {{ color:var(--muted); font:12px ui-monospace, SFMono-Regular, Consolas, monospace; margin-top:3px; }}
.nav {{ color:#aaa; text-decoration:none; border:1px solid var(--border); border-radius:8px; padding:8px 13px; font-size:13px; }}
.wheel-wrap {{ width:440px; max-width:88vw; position:relative; margin-bottom:24px; }}
canvas {{ width:100%; height:auto; border-radius:50%; border:1px solid var(--border); transition:transform 4.6s cubic-bezier(.17,.67,.12,.99); }}
.pointer {{ position:absolute; left:50%; top:-3px; transform:translateX(-50%); width:0; height:0; border-left:13px solid transparent; border-right:13px solid transparent; border-top:22px solid var(--white); z-index:2; }}
.hub {{ position:absolute; left:50%; top:50%; transform:translate(-50%, -50%); width:16px; height:16px; border-radius:50%; background:var(--white); }}
.spin {{ background:var(--white); color:#080808; border:0; border-radius:9px; padding:12px 28px; font-weight:700; cursor:pointer; }}
.spin:disabled {{ opacity:.55; cursor:not-allowed; }}
.meta-line {{ margin-top:13px; color:var(--muted); font:12px ui-monospace, SFMono-Regular, Consolas, monospace; }}
.test-mode {{ margin-top:10px; color:#f6c453; font-size:12px; text-align:center; letter-spacing:.04em; }}
.result {{ display:none; margin-top:24px; text-align:center; border:1px solid var(--border); background:var(--panel); border-radius:12px; padding:20px 28px; }}
.label {{ color:var(--muted); text-transform:uppercase; letter-spacing:.07em; font-size:11px; margin-bottom:8px; }}
.winner {{ color:var(--green); font:700 24px ui-monospace, SFMono-Regular, Consolas, monospace; margin-bottom:16px; }}
.result-actions {{ display:flex; gap:8px; }}
.remove,.keep {{ border:1px solid var(--border); background:transparent; color:var(--text); border-radius:8px; padding:9px 13px; cursor:pointer; }}
.remove {{ color:var(--red); border-color:rgba(255,105,95,.45); }}
.status,.empty {{ color:var(--muted); margin-top:12px; font-size:13px; text-align:center; }}
</style>
</head>
<body>
<header><div><h1>Kolo stesti</h1><div class="path">{html.escape(slug)}</div></div><a class="nav" href="/{qs}">&lt;- Prehled</a></header>
{content}
<script>{script}</script>
</body>
</html>"""

    @app.route("/wheel/remove", methods=["POST"])
    def wheel_remove() -> Any:
        if not admin_allowed():
            return jsonify(ok=False, error="forbidden"), 403
        payload = request.get_json(silent=True) or {}
        name = (payload.get("name") or "").strip()
        if not name:
            return jsonify(ok=False, error="missing_name"), 400
        removed = remove_one_ticket(name)
        remaining = 0
        if NAMES_FILE.exists():
            remaining = sum(1 for line in NAMES_FILE.read_text(encoding="utf-8").splitlines() if line == name)
        return jsonify(ok=removed, remaining=remaining)

    @app.route(f"/{NAMES_FILE.name}")
    def names_file() -> Any:
        if not NAMES_FILE.exists():
            abort(404)
        return send_file(NAMES_FILE, as_attachment=True)

    @app.route(f"/{OUT_CSV.name}")
    def csv_file() -> Any:
        if not OUT_CSV.exists():
            abort(404)
        return send_file(OUT_CSV, as_attachment=True)

    @app.route(f"/{RAW_LOG.name}")
    def raw_file() -> Any:
        if not RAW_LOG.exists():
            abort(404)
        return send_file(RAW_LOG, as_attachment=True)

    port = int(os.environ.get("PORT", "8080"))
    thread = threading.Thread(
        target=lambda: app.run(host="0.0.0.0", port=port, use_reloader=False),
        daemon=True,
    )
    thread.start()
    print(f"[i] Web UI running on port {port}. Official webhook: /kick/webhook")


def main() -> None:
    slug = os.environ.get("KICK_CHANNEL") or (sys.argv[1] if len(sys.argv) > 1 else "tyblaho69")
    ensure_storage()
    reconcile_wheel_totals_from_env()
    start_file_server(slug)
    maybe_auto_subscribe_official_events()

    print(f"[i] Data dir: {DATA_DIR}")
    print(f"[i] CSV: {OUT_CSV}")
    print(f"[i] Wheel names: {NAMES_FILE}")
    print(f"[i] Official webhook events: {', '.join(sorted(OFFICIAL_SUB_EVENTS))}")

    if not ENABLE_PUSHER:
        print("[i] ENABLE_PUSHER=0, running webhook/UI only.")
        while True:
            time.sleep(3600)

    try:
        run_pusher_loop(slug)
    except KeyboardInterrupt:
        print("\n[i] Stopped.")


if __name__ == "__main__":
    main()
