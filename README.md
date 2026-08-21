# Kick sub tracker

Tracker for Kick subscriptions and gifted subscriptions for channel `tyblaho69`.

The app records:

- normal subscriptions
- renewals
- gifted subscriptions, using the gifter/buyer name, not the giftee name
- one wheel ticket per gifted sub

## Railway variables

Set these in Railway:

```txt
KICK_CHANNEL=tyblaho69
DATA_DIR=/data
ENABLE_PUSHER=1
WEBHOOK_TOKEN=change-this-to-a-long-secret
ADMIN_TOKEN=change-this-too
```

Add a Railway volume mounted at:

```txt
/data
```

`/data` is required for persistence. It contains `subscribers.csv`, the
durable event ledger, and the wheel ticket cache. If the cache file is ever
missing, the app recreates every ticket from `subscribers.csv` at startup.
Do not use Railway's ephemeral filesystem for `DATA_DIR`, or data will be
lost on a redeploy.

The permanent-delete control is disabled by default, so a public wheel cannot
erase subscriber history. Only enable it intentionally (together with
`ADMIN_TOKEN`) if you really need it:

```txt
ALLOW_PERMANENT_DELETE=1
```

## Kick webhook

Use this webhook URL, replacing the token with your Railway `WEBHOOK_TOKEN`:

```txt
https://kick-subcounter.up.railway.app/kick/webhook?token=YOUR_WEBHOOK_TOKEN
```

Subscribe to these Kick events:

```txt
channel.subscription.new
channel.subscription.renewal
channel.subscription.gifts
```

## Pages

```txt
/
/wheel
/health
```
