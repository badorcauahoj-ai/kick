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
