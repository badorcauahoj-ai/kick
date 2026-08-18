#!/usr/bin/env python3
"""
Kick.com Subscriber Tracker
----------------------------
Sleduje probíhající stream na Kick.com přes (neoficiální) Pusher websocket
chatroomu a zapisuje jména nových/gift odběratelů do CSV souboru.

Kick nemá oficiálně zdokumentované eventy pro odběry, takže tenhle skript
poslouchá VŠECHNY eventy v chatroom kanálu, loguje je do raw_events.jsonl
(pro debug) a zároveň se je snaží rozpoznat a zapsat jako odběr. Pokud Kick
změní formát zpráv, mrkni do raw_events.jsonl a uprav funkci
extract_subscribers().

Instalace:
    pip install websocket-client requests

Spuštění:
    python kick_sub_tracker.py <channel_slug>
    např.: python kick_sub_tracker.py xqc
"""

import csv
import json
import os
import re
import sys
import time
import threading
from datetime import datetime, timezone

import requests
import websocket  # balíček websocket-client
from flask import Flask, send_file, abort  # jen na Railway - stažení výsledků přes web

PUSHER_WS_URL = (
    "wss://ws-us2.pusher.com/app/32cbd69e4b950bf97679"
    "?protocol=7&client=js&version=8.4.0-rc2&flash=false"
)

OUT_CSV = "subscribers.csv"
NAMES_FILE = "subscription_names.txt"  # jen jména odběratelů, jedno jméno na řádek
RAW_LOG = "raw_events.jsonl"  # syrové eventy pro debug, kdyby Kick změnil formát

sub_count = 0
lock = threading.Lock()


def get_ids(slug: str):
    """Zjistí channel_id a chatroom_id podle slugu kanálu (např. 'xqc')."""
    url = f"https://kick.com/api/v2/channels/{slug}"
    headers = {
        "User-Agent": (
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
            "(KHTML, like Gecko) Chrome/124.0 Safari/537.36"
        ),
        "Accept": "application/json",
    }
    resp = requests.get(url, headers=headers, timeout=10)
    resp.raise_for_status()
    data = resp.json()
    channel_id = data["id"]
    chatroom_id = data["chatroom"]["id"]
    print(f"[i] Kanál '{slug}' -> channel_id = {channel_id}, chatroom_id = {chatroom_id}")
    return channel_id, chatroom_id


def ensure_csv_header():
    if not os.path.exists(OUT_CSV):
        with open(OUT_CSV, "w", newline="", encoding="utf-8") as f:
            writer = csv.writer(f)
            writer.writerow(["timestamp", "username", "type", "months", "gifted_by"])


already_written = set()  # jména, co už byla jednou zapsaná - napodruhé se přeskočí


def write_subscriber(username: str, sub_type: str, months=None, gifted_by=None):
    global sub_count
    with lock:
        if username in already_written:
            return  # tohle jméno už jednou zapsané bylo, přeskakujeme
        already_written.add(username)
        sub_count += 1
        with open(OUT_CSV, "a", newline="", encoding="utf-8") as f:
            writer = csv.writer(f)
            writer.writerow([
                datetime.now(timezone.utc).isoformat(),
                username,
                sub_type,
                months or "",
                gifted_by or "",
            ])
        # samostatný soubor jen se jmény - jedno jméno na řádek
        with open(NAMES_FILE, "a", encoding="utf-8") as f:
            f.write(username + "\n")
        extra = f" (gift od {gifted_by})" if gifted_by else ""
        print(f"[+] #{sub_count} {sub_type}: {username}{extra}")


def log_raw(event_name: str, data: dict):
    with open(RAW_LOG, "a", encoding="utf-8") as f:
        f.write(json.dumps({"event": event_name, "data": data}, ensure_ascii=False) + "\n")


import re

# Vzory podle SKUTEČNÝCH textů systémových zpráv z Kick chatu (ověřeno na screenshotech):
#   "Gifted 5 subscriptions to the community!"
#   "Honzbii gifted a sub to niutn97"
# Necháváme i obecnější vzory pro klasický (negiftovaný) sub, protože Kick
# posílá i tenhle typ jako systémovou zprávu v chatu (ne vždy stejně formulovanou).
RE_GIFT_TO_USER = re.compile(r"gifted a sub(?:scription)? to\s+(\S+)", re.IGNORECASE)
RE_GIFT_COMMUNITY = re.compile(r"[Gg]ifted (\d+) subscriptions? to the community", re.IGNORECASE)
RE_NEW_SUB = re.compile(r"^(\S+)\s+subscribed(?:\s+for\s+(\d+)\s+months?)?", re.IGNORECASE)
RE_RESUB = re.compile(r"^(\S+)\s+resubscribed(?:\s+for\s+(\d+)\s+months?)?", re.IGNORECASE)


def extract_subscribers(event_name: str, data: dict):
    """
    Kick posílá info o subech dvěma způsoby, oba potvrzené reálnými eventy:

      A) Strukturované eventy (spolehlivé, používáme přednostně):
         - App\\Events\\SubscriptionEvent -> {"username": ..., "months": ...}
         - App\\Events\\ChannelSubscriptionEvent -> jen doplňkový/duplicitní
           event ke stejnému subu (obsahuje username + user_ids), IGNORUJEME
           ho, aby se stejný sub nezapsal dvakrát.

      B) Gift suby (a community gify) chodí jen jako text v běžné chat
         zprávě (App\\Events\\ChatMessageEvent) - parsujeme regexem.
    """
    if event_name == "App\\Events\\SubscriptionEvent":
        username = data.get("username")
        months = data.get("months")
        if username:
            write_subscriber(username, "subscription", months=months)
        return

    if event_name == "App\\Events\\ChannelSubscriptionEvent":
        # duplicitní event ke stejnému subu jako SubscriptionEvent výše,
        # záměrně ho přeskakujeme, aby se odběratel nezapsal dvakrát
        return

    if event_name == "App\\Events\\GiftsLeaderboardUpdated":
        # Žebříček dárců gift subů - jméno dárce + jeho celkový počet
        # darovaných subů. write_subscriber si už sama hlídá, že se stejné
        # jméno nezapíše podruhé.
        leaderboard = data.get("leaderboard", [])
        for entry in leaderboard:
            username = entry.get("username")
            quantity = entry.get("quantity")
            if username:
                write_subscriber(username, "gifter", months=quantity)
        return

    if "chatmessage" not in event_name.lower():
        return

    content = (data.get("content") or "").strip()
    sender = data.get("sender", {}) or {}
    sender_username = sender.get("username")
    if not content or not sender_username:
        return

    # "Honzbii gifted a sub to niutn97" - zapisujeme jen dárce (Honzbii),
    # ne jméno obdarovaného
    if RE_GIFT_TO_USER.search(content):
        write_subscriber(sender_username, "gifter")
        return

    # "Gifted 5 subscriptions to the community!" - dárce + počet
    m = RE_GIFT_COMMUNITY.search(content)
    if m:
        write_subscriber(sender_username, "gifter", months=int(m.group(1)))
        return

    # "username subscribed" / "username subscribed for 3 months"
    m = RE_NEW_SUB.match(content)
    if m:
        write_subscriber(m.group(1), "subscription", months=m.group(2))
        return

    # "username resubscribed for 3 months"
    m = RE_RESUB.match(content)
    if m:
        write_subscriber(m.group(1), "resubscription", months=m.group(2))
        return


def on_message(ws, message):
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
        print("[i] Přihlášeno k chatroom kanálu, poslouchám eventy...")
        return

    data = outer.get("data")
    if isinstance(data, str):
        try:
            data = json.loads(data)
        except json.JSONDecodeError:
            data = {}
    if not isinstance(data, dict):
        data = {}

    log_raw(event_name, data)

    extract_subscribers(event_name, data)


def on_open(ws, channel_id, chatroom_id):
    # kanál pro běžné chat zprávy
    ws.send(json.dumps({
        "event": "pusher:subscribe",
        "data": {"channel": f"chatrooms.{chatroom_id}.v2"},
    }))
    # kanál pro eventy na úrovni streamera - subs, gifty, followy, bany
    ws.send(json.dumps({
        "event": "pusher:subscribe",
        "data": {"channel": f"channel.{channel_id}"},
    }))


def on_error(ws, error):
    print(f"[!] WebSocket chyba: {error}")


def on_close(ws, close_status_code, close_msg):
    print(f"[i] Spojení uzavřeno ({close_status_code}).")


def start_file_server():
    """
    Jednoduchý webserver jen na stahování výsledků, když skript běží na
    serveru (Railway) a ne na tvém PC. Lokálně na PC ho vůbec nemusíš
    používat - soubory máš přímo ve složce.
    """
    app = Flask(__name__)

    @app.route("/")
    def index():
        return (
            f"Kick sub tracker běží. Zaznamenáno jmen: {sub_count}<br>"
            f'<a href="/{NAMES_FILE}">{NAMES_FILE}</a><br>'
            f'<a href="/{OUT_CSV}">{OUT_CSV}</a>'
        )

    @app.route(f"/{NAMES_FILE}")
    def names():
        if not os.path.exists(NAMES_FILE):
            abort(404)
        return send_file(NAMES_FILE, as_attachment=True)

    @app.route(f"/{OUT_CSV}")
    def csv_file():
        if not os.path.exists(OUT_CSV):
            abort(404)
        return send_file(OUT_CSV, as_attachment=True)

    port = int(os.environ.get("PORT", 8080))
    thread = threading.Thread(
        target=lambda: app.run(host="0.0.0.0", port=port, use_reloader=False),
        daemon=True,
    )
    thread.start()
    print(f"[i] Webserver pro stažení souborů běží na portu {port} (/, /{NAMES_FILE}, /{OUT_CSV})")


def main():
    slug = os.environ.get("KICK_CHANNEL") or (sys.argv[1] if len(sys.argv) > 1 else None)
    if not slug:
        print("Nastav proměnnou prostředí KICK_CHANNEL, nebo spusť: python kick_sub_tracker.py <channel_slug>")
        sys.exit(1)

    ensure_csv_header()
    start_file_server()  # v Railway/na serveru odsud stahuješ subscribers.csv a subscription_names.txt

    try:
        channel_id, chatroom_id = get_ids(slug)
    except Exception as e:
        print(f"[!] Nepodařilo se zjistit ID automaticky ({e}).")
        if sys.stdin.isatty():
            chatroom_id = int(input(
                "Zadej chatroom_id ručně (najdeš ho na "
                f"https://kick.com/api/v2/channels/{slug} v poli \"chatroom\":{{\"id\":...}}): "
            ))
            channel_id = int(input(
                "Zadej channel_id ručně (najdeš ho na stejné stránce v poli \"id\":...): "
            ))
        else:
            # na serveru (Railway) není terminál pro ruční zadání - zkusíme to znovu za chvíli
            print("[!] Server bez terminálu, zkouším znovu za 15s...")
            time.sleep(15)
            return main()

    print(f"[i] Připojuji se ke streamu '{slug}'... (Ctrl+C pro ukončení)")
    print(f"[i] Odběratelé se budou zapisovat do {OUT_CSV}")
    print(f"[i] Syrové eventy (pro debug) se logují do {RAW_LOG}")

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
            print(f"\n[i] Konec. Celkem zaznamenáno odběrů: {sub_count}")
            break

        print("[i] Spojení spadlo, zkouším znovu za 5s...")
        time.sleep(5)


if __name__ == "__main__":
    main()
