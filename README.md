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

## One-time count reconciliation

If a stream happened while the tracker missed events, you can safely fill in
the missing tickets from a verified list. In Railway Variables, set
`GIFT_TOTALS_RECONCILE_JSON` to the total desired ticket count per username:

```txt
GIFT_TOTALS_RECONCILE_JSON={"zuzk_engova":1,"Theushka":25,"t0bias_015":5,"TrnovanskyNinja":1,"veronicaaa_27":1,"Dejf7":1,"simonn43x":1,"lauriii10":1,"josefepegeo":10,"weedie123":1,"rusper_TBO":5}
```

On the next deploy/restart the tracker adds only the difference between the
current number of wheel tickets and each listed total. It never deletes a
ticket and rerunning it with the same totals makes no further changes. Remove
the variable after the reconciliation has completed.

## Internal wheel rehearsal

For a rehearsal that is visibly labelled `TEST REZIM` on the wheel page, set:

```txt
WHEEL_TEST_MODE=1
WHEEL_TEST_WINNERS=*,jasmiinaa222,*
```

Each comma-separated position represents one spin. `*` means a normal random
result; a username selects one of that user's existing tickets. The example
therefore makes the second of three spins select `jasmiinaa222`. Remove both
variables after the rehearsal. With no test variables, every spin is random.

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
