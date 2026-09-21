# Kick sub tracker

Tracker for Kick subscriptions and gifted subscriptions for channel `tyblaho69`.

The app records:

- normal subscriptions
- renewals
- gifted subscriptions, using the gifter/buyer name, not the giftee name
- one wheel ticket per gifted sub, up to a per-person cap (default 3, see below)

There are two builds in this repo:

- **`main.py`** - Vercel (serverless), webhook-only, data in Redis. **This is
  the recommended build.**
- **`kick_sub_tracker.py`** - the older always-on build for Railway (or any
  host that can run a long-lived process), with a Pusher/chat fallback and
  local file storage. Kept for reference; see the "Railway" section below if
  you ever go back to it.

## Deploy on Vercel

Vercel Functions are stateless and short-lived with no writable local disk,
so this build only uses Kick's **official webhook** (documented, stable
schema) and stores everything in Redis instead of local files. There is no
Pusher/chat fallback here - it cannot run a persistent background connection.

### 1. Add a Redis store to the Vercel project

Project -> Storage tab -> Create Database -> **Redis**. Pick the free plan.
Connecting it injects `UPSTASH_REDIS_REST_URL` and
`UPSTASH_REDIS_REST_TOKEN` automatically - the app reads those two directly,
nothing else to configure for storage.

### 2. Project environment variables

```txt
KICK_CHANNEL=tyblaho69
WEBHOOK_TOKEN=change-this-to-a-long-secret
ADMIN_TOKEN=change-this-too
MAX_TICKETS_PER_USER=3
```

`ALLOW_PERMANENT_DELETE=1` and `COUNT_ANONYMOUS_GIFTS=1` are optional, same
meaning as below.

### 3. Kick webhook

The app's webhook URL is:

```txt
https://<your-vercel-project>.vercel.app/kick/webhook?token=YOUR_WEBHOOK_TOKEN
```

Kick needs to be told to actually call it, by subscribing to these events:

```txt
channel.subscription.new
channel.subscription.renewal
channel.subscription.gifts
```

This subscription step happens entirely on Kick's side and requires an
OAuth 2.0 Authorization Code + PKCE flow (Kick has no simpler option, even
for a channel owner subscribing their own events). The app automates that
flow so you only have to click a link and log into Kick once:

1. Go to Kick's Developer dashboard, create an app, and set its **Redirect
   URL** to exactly:
   ```txt
   https://<your-vercel-project>.vercel.app/kick/oauth/callback
   ```
2. Add two more environment variables in Vercel from that app's page:
   ```txt
   KICK_CLIENT_ID=...
   KICK_CLIENT_SECRET=...
   ```
   and redeploy.
3. Open `https://<your-vercel-project>.vercel.app/kick/oauth/start?admin=YOUR_ADMIN_TOKEN`
   in a browser (or click "Pripojit Kick webhook" on `/` while logged in as
   admin), log into Kick, and approve it. The page that comes back confirms
   the subscription (or shows exactly what Kick rejected, if anything).

Gifts never showing up at all almost always means this step was skipped, or
the URL/token doesn't match what's subscribed. After a real gift, check
`/health`: if `webhook_last_received_at` stays `null`, Kick is never calling
the app and this subscription step is the thing to fix, not the code.

### Wheel ticket cap per person

Each username can hold at most `MAX_TICKETS_PER_USER` tickets (default `3`)
on the wheel, counted across **all** their subs and gifts combined. So one
self-subscription plus a 2-sub gift already reaches the cap, and a single
person buying 10 gifted subs in one go only ever earns 3 tickets from it.
`subscribers.csv` (`/subscribers.csv`) still records the *real* quantity of
every event - the cap only limits `/wheel` eligibility, never the historical
ledger.

### Diagnosing missing gifts

`/health` (JSON) and the panel on `/` report, without needing to open the
Vercel dashboard's logs:

- when the last webhook call arrived, its event type, and the running total
- how many calls were rejected for a bad `token`
- how many anonymous gifts were skipped (Kick hides the gifter's name when
  they choose to gift anonymously - there is no name to put on the wheel, so
  these are intentionally not recorded unless `COUNT_ANONYMOUS_GIFTS=1`)
- a count per any event type Kick sent that isn't one of the three above (in
  case Kick ever adds/renames an event you'd want to subscribe to)

### One-time count reconciliation

If a stream happened while the tracker missed events, fill in the missing
tickets from a verified list. Set `GIFT_TOTALS_RECONCILE_JSON` to the total
desired ticket count per username:

```txt
GIFT_TOTALS_RECONCILE_JSON={"zuzk_engova":1,"Theushka":25,"t0bias_015":5,"TrnovanskyNinja":1,"veronicaaa_27":1,"Dejf7":1,"simonn43x":1,"lauriii10":1,"josefepegeo":10,"weedie123":1,"rusper_TBO":5}
```

Then trigger it once (there is no boot hook on serverless, so this is a
manual admin call instead of something that runs automatically on deploy):

```txt
curl -X POST "https://<your-vercel-project>.vercel.app/admin/reconcile?admin=YOUR_ADMIN_TOKEN"
```

It adds only the difference between the current ledger total and each listed
total, never deletes a ticket, and rerunning it with the same totals makes no
further changes (the wheel itself still stops at the per-person cap above).
Remove the env var after reconciling.

### Pages

```txt
/                          overview + recent events + diagnostics
/wheel                     the wheel of fortune
/health                    JSON status + diagnostics
/subscribers.csv           full ledger export (uncapped quantities)
/subscription_names.txt    current wheel tickets, one name per line
```

## Railway (legacy build: `kick_sub_tracker.py`)

This build needs a host that can run one long-lived process (Railway, Fly.io,
a VPS, ...) because it also listens to Kick's public Pusher/chat feed as a
fallback and keeps its state in local files (`subscribers.csv`, the wheel
ticket cache, etc.) under a persistent volume.

```txt
KICK_CHANNEL=tyblaho69
DATA_DIR=/data
ENABLE_PUSHER=1
WEBHOOK_TOKEN=change-this-to-a-long-secret
ADMIN_TOKEN=change-this-too
MAX_TICKETS_PER_USER=3
```

Add a volume mounted at `/data` - it's required for persistence and holds
`subscribers.csv` (the durable event ledger), the wheel ticket cache, and
`unmatched_gift_candidates.jsonl` (gift-shaped Pusher/chat events nothing
could parse, for diagnosing a Kick format change). If the ticket cache file
is ever missing, the app recreates it from `subscribers.csv` at startup, and
the per-person cap is re-applied on every startup too. Do not use an
ephemeral filesystem for `DATA_DIR`, or data will be lost on redeploy.

The permanent-delete control is disabled by default, so a public wheel cannot
erase subscriber history. Only enable it intentionally (together with
`ADMIN_TOKEN`) if you really need it:

```txt
ALLOW_PERMANENT_DELETE=1
```

The Kick webhook setup and one-time reconciliation are the same as in the
Vercel section above; use `/kick/webhook?token=...` on this host's URL, and
the reconciliation runs automatically on the next deploy/restart instead of
needing a manual call.

This build's `/health` and the panel on `/` additionally report Pusher
connection state and channel-subscription errors, since that fallback (an
undocumented, best-effort listener) is the extra thing that can silently
break here that the Vercel build doesn't have to worry about.

### Pages

```txt
/
/wheel
/health
```
