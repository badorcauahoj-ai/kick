#!/usr/bin/env python3
"""
Kick subscriber / gift-sub tracker - Vercel (serverless) build.

Vercel functions are stateless and short-lived and have no writable local
disk, so this version drops two things the Railway build (kick_sub_tracker.py)
relied on:

- The Pusher/WebSocket chat listener. It needs a long-running background
  connection, which a serverless function cannot hold open between requests.
- Local CSV/JSON files. Nothing written to disk survives past the request.

Instead this build is webhook-only (POST /kick/webhook) and stores
everything in Upstash Redis (the "Redis" storage product in Vercel's
Storage tab), reached over its REST API so no extra native dependency is
needed. Kick's official webhook payload is documented and stable, so most
of the heuristic chat-parsing this project used to carry for the Pusher
fallback simply doesn't apply here anymore.

Required environment variables:
    UPSTASH_REDIS_REST_URL, UPSTASH_REDIS_REST_TOKEN
        Injected automatically once a Redis store is connected to the
        Vercel project (Storage tab -> Create Database -> Redis).
    KICK_CHANNEL, WEBHOOK_TOKEN, ADMIN_TOKEN, MAX_TICKETS_PER_USER
        Same meaning as in kick_sub_tracker.py - see README.md.
"""

from __future__ import annotations

import hashlib
import html
import json
import os
from datetime import datetime, timezone
from typing import Any

import requests
from flask import Flask, abort, jsonify, request, Response

app = Flask(__name__)

KICK_CHANNEL = os.environ.get("KICK_CHANNEL", "tyblaho69")
WEBHOOK_TOKEN = os.environ.get("WEBHOOK_TOKEN")
ADMIN_TOKEN = os.environ.get("ADMIN_TOKEN")
COUNT_ANONYMOUS_GIFTS = os.environ.get("COUNT_ANONYMOUS_GIFTS", "0").lower() in {"1", "true", "yes"}
ALLOW_PERMANENT_DELETE = os.environ.get("ALLOW_PERMANENT_DELETE", "0").lower() in {"1", "true", "yes"}
try:
    MAX_WHEEL_TICKETS_PER_USER = max(1, int(os.environ.get("MAX_TICKETS_PER_USER", "3")))
except ValueError:
    MAX_WHEEL_TICKETS_PER_USER = 3

UPSTASH_URL = os.environ.get("UPSTASH_REDIS_REST_URL", "").rstrip("/")
UPSTASH_TOKEN = os.environ.get("UPSTASH_REDIS_REST_TOKEN", "")

OFFICIAL_SUB_EVENTS = {
    "channel.subscription.new",
    "channel.subscription.renewal",
    "channel.subscription.gifts",
}

# Atomically caps a person's wheel tickets while still returning how many of
# `weight` actually fit. Runs inside Redis (single-threaded execution) so two
# webhook deliveries for the same person landing in different, concurrently
# running function instances can't both squeeze past the cap.
CAP_TICKETS_LUA = """
local current = tonumber(redis.call('HGET', KEYS[1], ARGV[1]) or '0')
local cap = tonumber(ARGV[4])
local room = cap - current
if room <= 0 then return 0 end
local weight = tonumber(ARGV[3])
local toadd = weight
if toadd > room then toadd = room end
redis.call('HINCRBY', KEYS[1], ARGV[1], toadd)
redis.call('HSETNX', KEYS[2], ARGV[1], ARGV[2])
return toadd
"""


def utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


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


def stable_json(value: Any) -> str:
    return json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":"), default=str)


def fingerprint(prefix: str, value: Any) -> str:
    digest = hashlib.sha256(stable_json(value).encode("utf-8")).hexdigest()
    return f"{prefix}:{digest}"


class RedisNotConfigured(RuntimeError):
    pass


def redis_cmd(*args: Any) -> Any:
    """Run one Redis command against Upstash's REST API."""
    if not UPSTASH_URL or not UPSTASH_TOKEN:
        raise RedisNotConfigured(
            "UPSTASH_REDIS_REST_URL / UPSTASH_REDIS_REST_TOKEN are not set. "
            "Connect a Redis store to this Vercel project (Storage tab -> "
            "Create Database) and redeploy."
        )
    resp = requests.post(
        UPSTASH_URL,
        json=[str(a) for a in args],
        headers={"Authorization": f"Bearer {UPSTASH_TOKEN}"},
        timeout=10,
    )
    resp.raise_for_status()
    data = resp.json()
    if data.get("error"):
        raise RuntimeError(f"Redis error for {args[:1]}: {data['error']}")
    return data.get("result")


def add_wheel_tickets(username: str, weight: int) -> int:
    added = redis_cmd(
        "EVAL",
        CAP_TICKETS_LUA,
        "2",
        "wheel_counts",
        "wheel_names",
        username.casefold(),
        username,
        weight,
        MAX_WHEEL_TICKETS_PER_USER,
    )
    return safe_int(added, 0)


def bump_diagnostic(key: str, amount: int = 1) -> None:
    redis_cmd("HINCRBY", "diagnostics", key, amount)


def set_diagnostic(key: str, value: Any) -> None:
    redis_cmd("HSET", "diagnostics", key, value)


def get_diagnostics() -> dict[str, Any]:
    flat = redis_cmd("HGETALL", "diagnostics") or []
    return dict(zip(flat[0::2], flat[1::2]))


def record_entry(
    username: Any,
    entry_type: str,
    *,
    quantity: int = 1,
    source: str,
    event_key: str,
    note: str = "",
    weight: int | None = None,
) -> bool:
    """Append one ledger row and give the gifter/subscriber their tickets.

    Idempotency is a single atomic SADD on event_key: Kick (like most
    webhook providers) can redeliver the same event, and this must never
    double count it. There is only one event source here (the official
    webhook), so - unlike the Railway build - no cross-source duplicate
    heuristic is needed.
    """
    username = normalize_username(username)
    if not username:
        return False

    quantity = max(1, safe_int(quantity, 1))
    if weight is None:
        weight = quantity if entry_type == "gift_subscription" else 1
    weight = max(1, safe_int(weight, 1))

    is_new = redis_cmd("SADD", "seen_event_keys", event_key)
    if safe_int(is_new, 0) == 0:
        return False

    entry = {
        "timestamp": utc_now(),
        "username": username,
        "type": entry_type,
        "quantity": quantity,
        "source": source,
        "event_key": event_key,
        "note": note,
    }
    redis_cmd("RPUSH", "ledger", stable_json(entry))
    tickets_added = add_wheel_tickets(username, weight)
    bump_diagnostic("ledger_rows_total")

    if tickets_added < weight:
        print(
            f"[i] {username} capped at {MAX_WHEEL_TICKETS_PER_USER} wheel ticket(s); "
            f"recorded the full x{quantity} in the ledger but added only {tickets_added}."
        )
    print(f"[+] {entry_type}: {username} x{quantity} ({source})")
    return True


def handle_official_kick_event(event_type: str, data: dict[str, Any]) -> bool:
    if event_type == "channel.subscription.new":
        subscriber = data.get("subscriber") or {}
        duration = safe_int(data.get("duration"), 1)
        return record_entry(
            subscriber.get("username"),
            "subscription",
            quantity=1,
            source="webhook",
            event_key=fingerprint("webhook:new", data),
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
            event_key=fingerprint("webhook:renewal", data),
            note=f"duration={duration}",
            weight=1,
        )

    if event_type == "channel.subscription.gifts":
        gifter = data.get("gifter") or {}
        username = normalize_username(gifter.get("username"))
        if not username:
            if not COUNT_ANONYMOUS_GIFTS:
                bump_diagnostic("anonymous_gift_skipped_total")
                print("[i] Anonymous gift-sub webhook skipped; gifter username is hidden by Kick.")
                return False
            username = "anonymous_gifter"

        giftees = data.get("giftees")
        quantity = len(giftees) if isinstance(giftees, list) else safe_int(data.get("quantity"), 1)
        quantity = max(1, quantity)
        return record_entry(
            username,
            "gift_subscription",
            quantity=quantity,
            source="webhook",
            event_key=fingerprint("webhook:gifts", data),
            note="official_gift",
            weight=quantity,
        )

    return False


def read_ledger(limit: int | None = None) -> list[dict[str, Any]]:
    raw = redis_cmd("LRANGE", "ledger", 0, -1) or []
    rows = [json.loads(item) for item in raw]
    return rows[-limit:] if limit else rows


def wheel_names() -> list[str]:
    counts_flat = redis_cmd("HGETALL", "wheel_counts") or []
    names_flat = redis_cmd("HGETALL", "wheel_names") or []
    counts = dict(zip(counts_flat[0::2], counts_flat[1::2]))
    display = dict(zip(names_flat[0::2], names_flat[1::2]))
    names: list[str] = []
    for key, count in counts.items():
        names.extend([display.get(key, key)] * safe_int(count, 0))
    return names


def remove_one_ticket(username: str) -> tuple[bool, int]:
    key = username.strip().casefold()
    current = safe_int(redis_cmd("HGET", "wheel_counts", key), 0)
    if current <= 0:
        return False, 0
    remaining = safe_int(redis_cmd("HINCRBY", "wheel_counts", key, -1), 0)
    return True, max(0, remaining)


def remove_username_completely(username: str) -> bool:
    key = username.strip().casefold()
    existed = safe_int(redis_cmd("HGET", "wheel_counts", key), 0) > 0
    redis_cmd("HDEL", "wheel_counts", key)
    redis_cmd("HDEL", "wheel_names", key)

    rows = read_ledger()
    kept = []
    for row in rows:
        row_username = normalize_username(row.get("username"))
        if row_username and row_username.casefold() == key:
            existed = True
            continue
        kept.append(row)

    if len(kept) != len(rows):
        redis_cmd("DEL", "ledger")
        if kept:
            redis_cmd("RPUSH", "ledger", *[stable_json(row) for row in kept])
    return existed


def reconcile_wheel_totals(totals: dict[str, int]) -> int:
    """Add only the missing wheel tickets for a one-time operator-supplied total list."""
    added = 0
    for raw_username, raw_total in totals.items():
        username = normalize_username(raw_username)
        total = safe_int(raw_total, 0)
        if not username or total <= 0:
            continue
        current = safe_int(redis_cmd("HGET", "wheel_counts", username.casefold()), 0)
        missing = total - current
        if missing <= 0:
            continue
        key = fingerprint("manual_total", {"username": username.casefold(), "target": total})
        if record_entry(
            username,
            "gift_subscription",
            quantity=missing,
            source="manual_total_reconciliation",
            event_key=key,
            note=f"target_total={total}",
            weight=missing,
        ):
            added += missing
    return added


def admin_allowed() -> bool:
    if not ADMIN_TOKEN:
        return False
    provided = request.args.get("admin") or request.headers.get("X-Admin-Token")
    return bool(provided) and provided == ADMIN_TOKEN


def permanent_deletion_enabled() -> bool:
    return bool(ADMIN_TOKEN) and ALLOW_PERMANENT_DELETE


def admin_qs() -> str:
    token = request.args.get("admin")
    if token and ADMIN_TOKEN and token == ADMIN_TOKEN:
        return "?admin=" + html.escape(token, quote=True)
    return ""


@app.route("/health")
def health() -> Any:
    try:
        rows = len(read_ledger())
        diagnostics = get_diagnostics()
        ok = True
    except RedisNotConfigured as exc:
        rows = 0
        diagnostics = {"error": str(exc)}
        ok = False
    return jsonify(ok=ok, channel=KICK_CHANNEL, rows=rows, diagnostics=diagnostics)


@app.route("/kick/webhook", methods=["POST"])
def kick_webhook() -> Any:
    provided = request.args.get("token") or request.headers.get("X-Webhook-Token")
    if WEBHOOK_TOKEN and provided != WEBHOOK_TOKEN:
        bump_diagnostic("webhook_forbidden_total")
        return jsonify(ok=False, error="forbidden"), 403

    payload = request.get_json(silent=True) or {}
    if not isinstance(payload, dict):
        return jsonify(ok=False, error="invalid_json"), 400

    event_type = request.headers.get("Kick-Event-Type") or request.args.get("event") or payload.get("event")
    event_type = str(event_type or "").strip()
    data = payload.get("data") if isinstance(payload.get("data"), dict) else payload

    bump_diagnostic("webhook_calls_total")
    set_diagnostic("webhook_last_received_at", utc_now())
    set_diagnostic("webhook_last_event_type", event_type or "unknown")

    if event_type not in OFFICIAL_SUB_EVENTS:
        bump_diagnostic(f"ignored_event:{event_type or 'unknown'}")
        return jsonify(ok=True, recorded=False, ignored=event_type)

    recorded = handle_official_kick_event(event_type, data)
    set_diagnostic("webhook_last_recorded", str(recorded))
    return jsonify(ok=True, recorded=recorded)


@app.route("/admin/reconcile", methods=["POST"])
def admin_reconcile() -> Any:
    if not admin_allowed():
        return jsonify(ok=False, error="forbidden"), 403
    raw_totals = os.environ.get("GIFT_TOTALS_RECONCILE_JSON", "").strip()
    if not raw_totals:
        return jsonify(ok=False, error="GIFT_TOTALS_RECONCILE_JSON is not set"), 400
    try:
        totals = json.loads(raw_totals)
    except json.JSONDecodeError:
        return jsonify(ok=False, error="GIFT_TOTALS_RECONCILE_JSON is not valid JSON"), 400
    if not isinstance(totals, dict):
        return jsonify(ok=False, error="GIFT_TOTALS_RECONCILE_JSON must be a JSON object"), 400
    added = reconcile_wheel_totals(totals)
    return jsonify(ok=True, added=added)


@app.route("/delete", methods=["POST"])
def delete_name() -> Any:
    if not permanent_deletion_enabled() or not admin_allowed():
        return jsonify(ok=False, error="forbidden"), 403
    payload = request.get_json(silent=True) or {}
    name = (payload.get("name") or "").strip()
    if not name:
        return jsonify(ok=False, error="missing_name"), 400
    return jsonify(ok=remove_username_completely(name))


@app.route("/wheel/remove", methods=["POST"])
def wheel_remove() -> Any:
    if not admin_allowed():
        return jsonify(ok=False, error="forbidden"), 403
    payload = request.get_json(silent=True) or {}
    name = (payload.get("name") or "").strip()
    if not name:
        return jsonify(ok=False, error="missing_name"), 400
    removed, remaining = remove_one_ticket(name)
    return jsonify(ok=removed, remaining=remaining)


@app.route("/subscribers.csv")
def csv_export() -> Any:
    rows = read_ledger()
    lines = ["timestamp,username,type,quantity,source,event_key,note"]
    for row in rows:
        values = [str(row.get(field, "")).replace('"', '""') for field in
                  ("timestamp", "username", "type", "quantity", "source", "event_key", "note")]
        lines.append(",".join(f'"{v}"' for v in values))
    return Response("\n".join(lines) + "\n", mimetype="text/csv",
                     headers={"Content-Disposition": "attachment; filename=subscribers.csv"})


@app.route("/subscription_names.txt")
def names_export() -> Any:
    names = wheel_names()
    body = "\n".join(names) + ("\n" if names else "")
    return Response(body, mimetype="text/plain",
                     headers={"Content-Disposition": "attachment; filename=subscription_names.txt"})


def render_table_row(row: dict[str, Any]) -> str:
    name = str(row.get("username", ""))
    name_attr = html.escape(name, quote=True)
    delete_button = (
        f'<button class="del" data-name="{name_attr}" title="Smazat">x</button>'
        if permanent_deletion_enabled() else ""
    )
    return (
        '<div class="row">'
        f'<div class="name">{html.escape(name)}</div>'
        f'<div class="type">{html.escape(str(row.get("type", "")))}</div>'
        f'<div class="qty">{html.escape(str(row.get("quantity", 1)))}</div>'
        f"{delete_button}"
        "</div>"
    )


def render_diagnostics_panel() -> str:
    try:
        d = get_diagnostics()
        error = None
    except RedisNotConfigured as exc:
        d, error = {}, str(exc)

    def fmt(value: Any) -> str:
        return html.escape(str(value)) if value not in (None, "") else "nikdy"

    if error:
        return (
            '<section class="panel"><div class="diag-errors">'
            f'<div class="k">Redis neni pripojeny</div><div>{html.escape(error)}</div>'
            "</div></section>"
        )

    ignored = {k[len("ignored_event:"):]: v for k, v in d.items() if k.startswith("ignored_event:")}
    ignored_html = ""
    if ignored:
        items = "".join(f"<li>{html.escape(k)}: {html.escape(str(v))}</li>" for k, v in ignored.items())
        ignored_html = f'<div class="diag-errors"><div class="k">Ignorovane typy eventu</div><ul>{items}</ul></div>'

    return f"""
  <section class="panel">
    <div class="diag-grid">
      <div><div class="k">Webhook - posledni volani</div><div class="v small">{fmt(d.get('webhook_last_received_at'))}</div></div>
      <div><div class="k">Webhook - posledni typ</div><div class="v small">{fmt(d.get('webhook_last_event_type'))} (celkem {fmt(d.get('webhook_calls_total', 0))})</div></div>
      <div><div class="k">Anonymni gifty preskoceny</div><div class="v small">{fmt(d.get('anonymous_gift_skipped_total', 0))}</div></div>
      <div><div class="k">Odmitnute (spatny token)</div><div class="v small">{fmt(d.get('webhook_forbidden_total', 0))}</div></div>
    </div>
    {ignored_html}
  </section>
"""


@app.route("/")
def index() -> str:
    try:
        rows = read_ledger()
    except RedisNotConfigured:
        rows = []
    recent = list(reversed(rows[-50:]))
    row_html = (
        "\n".join(render_table_row(row) for row in recent)
        if recent else '<div class="empty-row">Zadne jmeno zatim zaznamenane - cekam na prvni sub/gift.</div>'
    )
    qs = admin_qs()
    diag_html = render_diagnostics_panel()

    return f"""<!doctype html>
<html lang="cs">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<meta http-equiv="refresh" content="15">
<title>{html.escape(KICK_CHANNEL)} - kick-sub-tracker</title>
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
.actions {{ display:grid; grid-template-columns:repeat(auto-fit, minmax(140px, 1fr)); gap:8px; margin-bottom:16px; }}
.btn {{ text-align:center; text-decoration:none; padding:11px 14px; border-radius:8px; border:1px solid var(--border); color:var(--text); font-size:13px; }}
.btn.primary {{ background:var(--white); color:#090909; border-color:var(--white); font-weight:600; }}
.diag-grid {{ display:grid; grid-template-columns:repeat(2, 1fr); }}
.diag-grid > div {{ padding:14px 20px; border-right:1px solid var(--soft); border-bottom:1px solid var(--soft); }}
.diag-grid > div:nth-child(2n) {{ border-right:0; }}
.diag-errors {{ padding:14px 20px; }}
.diag-errors ul {{ margin:6px 0 0; padding-left:18px; color:var(--red); font-size:12px; }}
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
    <div class="brand"><div class="mark">KS</div><div><h1>kick-sub-tracker</h1><div class="path">{html.escape(KICK_CHANNEL)}</div></div></div>
    <a class="nav" href="/wheel{qs}">Kolo stesti -></a>
  </header>
  <section class="panel"><div class="meta">
    <div><div class="k">Zaznamenano</div><div class="v">{len(rows)}</div></div>
    <div><div class="k">Kanal</div><div class="v small">{html.escape(KICK_CHANNEL)}</div></div>
    <div><div class="k">Zdroj</div><div class="v small">ofic. Kick webhook</div></div>
  </div></section>
  <nav class="actions">
    <a class="btn primary" href="/subscription_names.txt{qs}">Stahnout jmena (.txt)</a>
    <a class="btn" href="/subscribers.csv{qs}">Export detailu (.csv)</a>
  </nav>
  {diag_html}
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


@app.route("/wheel")
def wheel() -> str:
    try:
        names = wheel_names()
    except RedisNotConfigured:
        names = []
    qs = admin_qs()

    if not names:
        content = '<div class="empty">Zatim nejsou zadne listky. Jakmile prijde sub nebo gift, kolo se naplni.</div>'
        script = ""
    else:
        names_json = json.dumps(names, ensure_ascii=False).replace("</", "<\\/")
        content = """
<div class="wheel-wrap">
  <div class="pointer"></div>
  <canvas id="wheel" width="700" height="700"></canvas>
  <div class="hub"></div>
</div>
<button id="spin" class="spin">Roztocit kolo</button>
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
const canvas = document.getElementById('wheel');
const ctx = canvas.getContext('2d');
const size = canvas.width;
const center = size / 2;
const radius = center - 4;
let rotation = 0;

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

document.getElementById('spin').addEventListener('click', () => {{
  const btn = document.getElementById('spin');
  const result = document.getElementById('result');
  btn.disabled = true;
  result.style.display = 'none';
  const n = names.length;
  const winnerIndex = Math.floor(Math.random() * n);
  const arcDeg = 360 / n;
  const target = winnerIndex * arcDeg + arcDeg / 2;
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
<title>Kolo stesti - {html.escape(KICK_CHANNEL)}</title>
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
<header><div><h1>Kolo stesti</h1><div class="path">{html.escape(KICK_CHANNEL)}</div></div><a class="nav" href="/{qs}">&lt;- Prehled</a></header>
{content}
<script>{script}</script>
</body>
</html>"""
