# Blue-Green Deployment — `grid_bot.py`

## Overview

The blue-green deployment system allows you to deploy a new version of `grid_bot.py`
without closing any open BTC position or cancelling any live grid orders that are
still valid on the new grid. The only manual action required is starting the new
process — everything else is fully automated.

**Goals achieved:**

- No position liquidation during deploy
- The previous grid's live orders are kept in place wherever the new process's own
  grid still has a level at that exact price — no cancel/re-place for those.
  Orders whose price no longer corresponds to any level in the newly (re)computed
  grid are cancelled and replaced by a fresh order at whatever level took their
  place instead. In practice, if very little time/market movement has occurred
  between deploys, this normally means *all* orders are kept; if the auto-tuner's
  freshly computed range/spacing has drifted, only the orders whose price
  survived are kept and the rest are recreated.
- `long_qty` and cost-basis accounting are correct immediately after takeover,
  and stay correct afterward — inherited orders' fills are actually detected and
  processed going forward (see "Fill routing" below; this was not true in an
  earlier version of this feature).
- Deploy completes end-to-end in under 5 seconds
- The new process automatically falls back to a normal cold start if no peer is
  currently running
- Every deploy after the very first one uses the exact same invocation — no
  manual "become the new blue" step is ever required

---

## How to Deploy

### Step 1 — First-ever launch

When launching the bot for the very first time (nothing running yet to hand off
from), use `--role blue`:

```
python grid_bot.py --role blue
```

This is the only change to your normal startup command. The process registers
itself as the live process in SQLite and starts a background watcher thread that
listens for a handoff request from a future deploy.

### Step 2 — Every deploy after that

Copy the new `grid_bot.py` into the working directory, then start it with
`--role green`:

```
python grid_bot.py --role green
```

Green finds the currently-live process's PID in SQLite, sends the handoff
request, and waits. The live process detects the request, freezes order
activity, exports its state, and exits cleanly (no sell). Green pre-registers
the exported orders, builds its own grid, restores whatever still matches, and
takes over as the live process — and itself becomes the process the *next*
deploy hands off from.

**Use `--role green` for every deploy from here on, indefinitely.** There is no
"blue" process to relaunch after the first one — whichever process is currently
running (regardless of which `--role` it was started with) is the one the next
`--role green` deploy will find and hand off from. See "Why there's no role
swap" below if you're curious why this works.

That's it. No other commands, no sentinel files, no scripts to run.

---

## End-to-End Sequence

```
t = 0s     Live process is running: grid orders live, position accumulating
           Logs: logs_grid/grid_bot_<role>_YYYY-MM-DD.log

t = Xs     Operator copies new grid_bot.py into place and runs:
               python grid_bot.py --role green
           Logs: logs_grid/grid_bot_green_YYYY-MM-DD.log

t = X+0s   Green: OMS.start() — brings up its own live order-update WS
           Green: reads the current bg_lock holder's pid
           Green: writes handoff request (meta.bg_handoff_request = own_pid)
           Green: starts polling meta.bg_handoff_json every 1s

t = X+1s   Live process (watcher thread): detects meta.bg_handoff_request
           Live process: sets GridEngine._handoff_freeze = True
                 → _place_buy() and _place_sell() are now suppressed
           Live process: sleeps 300ms to drain any in-flight REST submissions

t = X+1.3s Live process: acquires GridEngine._lock
           Live process: reads long_qty + all level states atomically
           Live process: looks up exchange_id per level from OMS._orders
           Live process: writes snapshot JSON to meta.bg_handoff_json
           Live process: clears meta.bg_handoff_request, then releases
                 bg_lock — THIS is the actual hand-off moment a waiting
                 green's acquire attempt is racing to catch (see "The
                 single-instance lock" below)
           Live process: sets _handoff_stop = True
           Live process: sets a shutdown-request flag (NOT _stop_event directly —
                 see "Shutdown handoff-safety" below); its own main thread
                 (_run()) observes the flag and calls stop() itself, which:
                 - skips liquidation because _handoff_stop is True
                 - releases bg_lock again (harmless no-op — already released above)
                 - tears down OMS/cmd-poller/DB connection
           Live process: exits

t = X+2s   Green: poll finds meta.bg_handoff_json
           Green: races to acquire bg_lock (a short bounded retry smooths
                  over the few-microsecond gap between the write above and
                  the release, plus ordinary contention) — winning this IS
                  becoming the new active instance; a losing process refuses
                  to start rather than falling back to a cold start
           Green: IMMEDIATELY registers every open order in the snapshot with
                  its own OMS (exchange_id → client_oid mapping + fill queue —
                  see "Fill routing" below). This happens now, right after
                  Green's own WS went live, deliberately BEFORE warmup — not
                  after, which is what left inherited orders' fills
                  undetectable in an earlier version of this feature.
           Green: caches the parsed snapshot in memory. Does NOT delete it
                  from SQLite yet — see "Crash recovery during handoff" below
                  for why that's deferred until the restore is actually
                  applied, further down this sequence
           Green: starts heartbeating bg_lock (every 5s) for as long as it runs

t = X+2s   Green: OMS already started; proceeds to warmup (Phase 1 + Phase 2 ATR seed)
           (normal warmup: Phase 1 waits for first live tick, Phase 2 seeds ATR;
           this can occasionally take much longer than a few seconds if the ATR
           REST seed fails and Phase 2 falls back to live candle accumulation —
           but that no longer matters for fill routing, since registration
           already happened above, before warmup even started; it also no
           longer risks a second process mistaking this one for dead, since
           the heartbeat above keeps refreshing throughout)

t = X+12s  Green: computes its own new grid params (auto-tuner) and, BEFORE
                  placing a single order:
                  • Matches the cached snapshot's open orders against this
                    grid's actual level prices (by price, not index — robust
                    to the level count shifting slightly at either edge)
                  • Builds the set of level indices that already have a live,
                    pre-registered order — those must NOT get a fresh order
                    placed on top of them
           Green: builds the grid, placing fresh orders only for indices that
                  aren't already covered by a matched inherited order
           Green: applies the match: for each matched level, sets its
                  state/client_oid/qty/placed_at (and initial-sell flag) from
                  the snapshot; seeds long_qty from the snapshot; cancels (and
                  un-registers) any snapshot order that didn't match anything
                  in this grid
           Green: ONLY NOW deletes meta.bg_handoff_json — the restore is
                  fully applied to a running grid, so there's nothing left to
                  resume if Green were to crash after this point (bg_lock
                  itself stays held — Green keeps heartbeating it, since it's
                  now the live process)

t = X+12s  Green: _run() starts — fully live
           All matching original grid orders are intact on the exchange, still
           tracked, and their fills will be processed like any other order.
           Position is uninterrupted.
```

Typical total time from `python grid_bot.py --role green` to green being fully live:
**under 15 seconds** (dominated by warmup, not handoff).
The handoff itself (freeze → snapshot → read → pre-register) takes **under 2
seconds** — that pre-registration step is what now bounds the fill-routing race
window (see below), independent of how long warmup happens to take.

---

## Fill routing (why inherited orders' fills are actually detected)

Two things have to both be true for green to notice when one of the orders it
inherited actually fills on the exchange:

1. **OMS bookkeeping.** `OMS.restore_order()` must register both the
   `exchange_id → client_oid` mapping *and* a fill-delivery queue for that
   `client_oid`. Registering only the mapping (without the queue) leaves both
   `wait_fill()` (polled every tick by `GridEngine._poll_live_fills()`) and
   `_deliver_fill()` (called from the WS handler) permanently no-op for that
   order — it can fill for real on the exchange, but this process would never
   know. `restore_order()` now creates both.

2. **Timing.** That registration has to happen before a fill event for the
   order arrives over green's WS. Green's order-update WS goes live the moment
   `OMS.start()` returns, at the very top of `start()` — well before the grid
   is rebuilt. Registration now happens via `_preregister_handoff_orders()`
   right when the snapshot is read (during `_request_and_await_handoff()`),
   which is as early as it can possibly happen, rather than being deferred
   until after full warmup + grid rebuild (which is unbounded in the worst
   case — see the warmup note above).

   This does *not* reduce the race window to zero — green cannot know the
   peer's order IDs before the peer hands them off, so there's an inherent gap
   between "green's WS goes live" and "green has read the snapshot". That gap
   is now bounded by the handoff round-trip (typically low single-digit
   seconds) instead of by warmup duration. If a fill genuinely lands in that
   smaller window, it's no longer silently dropped either — `OMS._handle_order_update()`
   logs a warning for any order-update whose exchange_id it doesn't recognize.

---

## The single-instance lock (`bg_lock`)

Before any of the crash-recovery or role-swap discussion below makes sense,
it helps to know the one mechanism underneath both: `grid_bot.db` has a
dedicated `bg_lock` table (a single row) that enforces **at most one
`grid_bot.py` process is ever the active trading instance for this DB at a
time** — standalone, blue, or green; role doesn't matter.

It's a heartbeat lease, not a plain flag:

- **Acquire** is a single atomic `UPDATE bg_lock SET holder_pid=?, updated_at=?
  WHERE id=1 AND (holder_pid IS NULL OR updated_at < ?)`. SQLite serialises
  writers against the same DB file across processes, so if two processes
  race to acquire at the same instant, exactly one `UPDATE`'s `WHERE` clause
  matches — there's no window where both can believe they hold it.
- **Heartbeat**: the current holder refreshes `updated_at` every
  `BG_LOCK_HEARTBEAT_S` (5s) for as long as it runs.
- **Staleness**: if the holder stops heartbeating for longer than
  `BG_LOCK_STALE_AFTER_S` (20s — roughly a 4x margin over the heartbeat
  interval, so one slow SQLite write or GC pause doesn't look like a crash),
  the row is treated as unheld and a later process can acquire it via the
  exact same atomic `UPDATE`.
- **Release**: `UPDATE bg_lock SET holder_pid=NULL, updated_at=0 WHERE id=1
  AND holder_pid=?` — only clears if the caller is still the recorded
  holder, so a delayed release from a dying process can't clobber a lock a
  newer holder has since legitimately acquired.

Every process acquires this lock exactly once, at the point it's about to
become the active instance — either as part of a handoff hand-off (see
below) or right before `reconcile_on_startup()` on a cold start — and holds
it (via the heartbeat) until it cleanly stops or hands off to a successor.
It's checked with `bg_lock_try_acquire()`/`bg_lock_current_holder()`/
`bg_lock_heartbeat()`/`bg_lock_release()` on `GridStateStore`.

---

## Crash recovery during handoff

`_preregister_handoff_orders()` deliberately does **not** delete
`bg_handoff_json` once it's read it — that's deferred to
`_apply_handoff_restore()`, after the snapshot has actually been applied to a
fully-built grid, not merely read. The gap between those two points is
normally ~10-15s but, in the ATR-REST-seed-failure fallback case, can be much
longer. If the incoming process crashes anywhere in that gap, the snapshot
would otherwise be gone for good, and a plain restart would find no snapshot,
no live peer, and fall through to a cold start — cancelling every resting
order on the exchange and liquidating the inherited position. Keeping the
JSON around that long avoids that outcome, and `bg_lock`'s staleness
detection is what makes it safe to do so:

- On restart, `_request_and_await_handoff()` checks for a leftover
  `bg_handoff_json` *before* even looking for a live peer. If one is present,
  this is Step 0: a previous process read it and started registering orders
  but never finished.
- It then tries `bg_lock_try_acquire()`:
  - **Acquired** → whoever last touched this snapshot is gone (stale past
    `BG_LOCK_STALE_AFTER_S`, or the lock was never even held for it) — safe
    to resume. Call `_preregister_handoff_orders()` directly on the leftover
    snapshot — no peer needed, since there isn't one. This is idempotent:
    `OMS._orders` / `_exid_to_coid` / `_fill_queues` are fresh empty dicts on
    a new process, and `GridLevel` states are freshly built as IDLE by
    `_rebuild_grid()` before `_apply_handoff_restore()` touches them, so
    re-registering and re-applying the same snapshot lands in exactly the
    same state it would have on the first attempt.
  - **Not acquired** → another process holds a *fresh* lock — this is not a
    crash, it's a second process launched while the first is still
    legitimately mid-warmup (e.g. an impatient re-run), or genuinely racing
    for the very same hand-off. `reconcile_on_startup()` cancels every open
    order for the instrument directly on the exchange, account-wide — letting
    a second process cold-start here would cancel orders the still-live
    holder owns, out from under it. The safer choice is to refuse outright
    (log an error naming the holder PID and `sys.exit(1)`) rather than
    silently doing anything that could interfere. If you hit this in
    practice, wait for the first process to finish (or confirm it's actually
    stuck before manually clearing `bg_lock`'s row / `bg_handoff_json` from
    `grid_bot.db`).

The same `bg_lock_try_acquire()` also gates the *normal* (non-recovery) hand-off
— see the sequence above: once a waiting green sees the JSON appear, it races
to acquire the lock the exporting process just released, and only the winner
proceeds. A short bounded retry (`_try_acquire_bg_lock()`, a few attempts ~0.1s
apart) smooths over the few-microsecond gap between the exporting process's
JSON write and its lock release being two separate SQLite statements, plus
ordinary write contention — it does not change the outcome when someone else
is genuinely still running, since that holder's lease keeps refreshing
throughout.

---

## Handoff Snapshot Schema

The snapshot is stored as a JSON string in `meta.bg_handoff_json` in `grid_bot.db`.
It is read as soon as it's found (well before warmup), but not deleted until
`_apply_handoff_restore()` finishes (see "Crash recovery during handoff"
above). Schema version 2:

```json
{
  "schema": 2,
  "exported_at": 1720000000.000,
  "role": "blue",
  "long_qty": 0.1234,
  "params": {
    "lower": 62000.0,
    "upper": 63000.0,
    "levels": 10,
    "spacing": 100.0,
    "stop_price": 61800.0,
    "notional_per_level": 50.0,
    "computed_at": 1720000000.0
  },
  "levels": [
    {
      "index": 0,
      "price": 62100.0,
      "state": "BUY_OPEN",
      "client_oid": "abc123",
      "exchange_id": "exid-aaa",
      "qty": 0.001,
      "placed_at": 1719999900.0,
      "is_initial_sell": false
    },
    {
      "index": 1,
      "price": 62200.0,
      "state": "SELL_OPEN",
      "client_oid": "def456",
      "exchange_id": "exid-bbb",
      "qty": 0.001,
      "placed_at": 1719999910.0,
      "is_initial_sell": true
    },
    {
      "index": 2,
      "price": 62300.0,
      "state": "IDLE",
      "client_oid": "",
      "exchange_id": "",
      "qty": 0.0,
      "placed_at": 0.0,
      "is_initial_sell": false
    }
  ]
}
```

`exchange_id` per level is critical: it's what the incoming process registers in
`OMS._exid_to_coid` (plus a fill queue) so that WS fill events, which carry the
exchange's order ID, are routed to the correct order and actually processed.

`is_initial_sell` (schema 2) matters for cost-basis accounting: a
`SELL_OPEN` level that was placed as one of the exporting process's *initial*
sells (sold from an existing position, above mid, before any BUY fill in that
process's own session) must not decrement `long_qty` again when it eventually
fills — see `GridEngine._on_fill`'s `is_initial_sell` handling. Without this
field the incoming process has no way to know that distinction and would
double-count the position reduction.

`params` is carried over for diagnostics only — it's logged alongside the
incoming process's own freshly computed params at handoff-apply time (`peer
params range=[...] -> this grid's range=[...]`), so a quick look at the logs
tells you how much (if at all) the grid actually moved between deploys. It is
*not* used to decide what matches: matching is done against the incoming
process's own newly computed level prices, since the auto-tuner may have
recomputed something slightly different in the time since export.

---

## SQLite Keys / Tables Used

Coordination state lives in `grid_bot.db`:

| Key / table | Set by | Cleared by | Purpose |
|---|---|---|---|
| `bg_lock` (table, 1 row) | Whichever process is about to become the active trading instance | That process, at handoff export or clean stop (or treated as free once stale — see above) | Single-instance lock + lease; also doubles as "who do I ask for a handoff" |
| `bg_handoff_request` | Incoming process when ready | Live process after reading | IPC signal: "please export now" |
| `bg_handoff_json` | Live process after freeze | Incoming process, once `_apply_handoff_restore()` finishes applying it to a built grid — not just read (see "Crash recovery during handoff" above) | The full state snapshot |

---

## Why there's no role swap

An earlier version of this feature used two separate PID slots
(`bg_blue_pid`/`bg_green_pid`) instead of one, with no staleness detection —
a plain "is someone registered" flag. The process that just took over via
handoff kept running with whatever `--role` it was launched with — usually
`green` — and only ever registered itself under `bg_green_pid`. Since the
documented workflow said to use `--role green` for every deploy, the *next*
deploy's `_request_and_await_handoff()` looked up `bg_blue_pid`, found nothing
(nobody had registered there since the very first launch), and silently fell
back to a full cold start — cancelling every order and reconciling position from
the exchange. Blue-green only actually worked once.

A second iteration collapsed those into a single `bg_live_pid` key, which
fixed that specific problem but introduced another: a plain "is it registered"
check can't tell a crashed holder from one that's merely deep in a slow
warmup, so a naive crash-recovery check risked a second process cold-starting
right on top of a first one that was still very much alive and running
— cancelling its orders out from under it.

`bg_lock`'s CAS + heartbeat lease (described above) fixes both at once. Any
process that finishes startup — whether launched with `--role blue` or
`--role green`, whether it got there via handoff or its own cold start —
holds the same lock and keeps it fresh via a heartbeat thread for as long as
it runs. `--role` only controls one thing: whether *this* process, on the way
up, tries to request a handoff from whoever currently holds the lock
(`green` does; `blue` doesn't, since `blue` is meant for the first-ever
launch when nothing is running yet). Nothing about a process's *ongoing*
participation depends on which role it was started with, so no relabeling
step is ever needed — and a crash mid-handoff resolves itself automatically
once the lease goes stale, without risking a still-alive process's orders.

---

## Shutdown handoff-safety

The live process's watcher thread (a background daemon thread) is the one that
detects a handoff request and calls `export_handoff_snapshot()`. After that, it
does **not** call `self.stop()` directly. `GridBot.stop()` mutates
`self._engine`/`self._oms`/etc. with no top-level lock, and the main thread's
`_run()` loop could be mid-iteration touching that same state — calling `stop()`
concurrently from a second OS thread would be a genuine race. This is different
from the normal SIGINT/SIGTERM path, where the signal handler runs *on* the main
thread (pre-empting `_run()` at a safe point) rather than on a separate thread.

Instead, the watcher thread only sets a flag
(`self._handoff_shutdown_requested`). `_run()`, on the main thread, checks that
flag at the top of every loop iteration and calls `self.stop()` itself once it
sees it — which is what actually runs the skip-liquidation logic (`_handoff_stop`
is already `True` at that point), tears down the OMS/command-poller/DB
connection, and sends the "stopped" alert.

---

## Configuration

One config key in `GRID_CONFIG`:

| Key | Default | Description |
|---|---|---|
| `bg_handoff_timeout_s` | `10` | Seconds the incoming process waits for the live peer to write the snapshot before giving up and falling back to a cold start |

Increase this if the live process takes longer than expected to export (e.g. if
it has many levels and the SQLite write is slow on the machine). In practice the
export takes well under 1 second.

---

## Log Files

When `--role` is specified, each process writes to its own log file so output
never interleaves:

```
logs_grid/grid_bot_blue_YYYY-MM-DD.log
logs_grid/grid_bot_green_YYYY-MM-DD.log
```

Without `--role` (standalone mode), the original naming is used:

```
logs_grid/grid_bot_YYYY-MM-DD.log
```

To follow both logs simultaneously during a deploy:

```powershell
# PowerShell — tail both logs in separate windows
Get-Content (Get-ChildItem logs_grid\*blue*.log | Sort LastWriteTime | Select -Last 1).FullName -Wait
Get-Content (Get-ChildItem logs_grid\*green*.log | Sort LastWriteTime | Select -Last 1).FullName -Wait
```

---

## Incoming Process's Reconcile Logic

Two separate steps, run from `_rebuild_grid()`:

**1. `_match_handoff_levels(new_params)`** — pure matching, no side effects, run
before any order is placed:

```
For each OPEN (BUY_OPEN/SELL_OPEN) level in the cached snapshot:
  ├─ Look up its price against this grid's own level_prices (by price, not
  │  index — a level count/index shift at either edge doesn't break matching
  │  as long as the actual price still exists somewhere in the new grid)
  ├─ No matching price, or that price already claimed by an earlier snapshot
  │  level → orphan
  └─ Otherwise → claim that new-grid index, add to the restore plan

Returns (restore_plan: {new_index: snapshot_level}, orphans: [snapshot_level]).
```

**2. Grid build, then `_apply_handoff_restore(restore_plan, orphans)`:**

```
GridEngine.start(mid, skip_indices=restore_plan.keys()) —
  places a fresh order for every level EXCEPT the ones already covered by a
  matched inherited order (those already have a live order — placing another
  would duplicate it on the exchange and orphan one of the two in memory).

For each level in restore_plan:
  • Restore state, client_oid, qty, placed_at, is_initial_sell on the
    already-built GridLevel (OMS-side registration happened earlier, during
    _preregister_handoff_orders — not repeated here)

For each orphan:
  • Cancel its order on the exchange (a fresh order was already placed at
    whatever new level took its price's place, if any)
  • Un-register it from the OMS (drop the early registration so its fill-queue
    entry doesn't leak for the life of the process)

long_qty seeded from the snapshot, unconditionally (real position, independent
of whether the grid itself matched).
```

The alert on handoff-apply reports the counts:
```
✅ Handoff applied: 0.1234 BTC, 8 orders restored in place, 0 orphans cancelled & recreated fresh. Snapshot age 1243ms.
```

---

## Race-Safety Design (freeze → drain → snapshot → write)

This sequence is designed to prevent `long_qty` from being stale at the point
the JSON is written, and is unchanged from the original design:

1. `_handoff_freeze = True` is set on `GridEngine` first. This suppresses
   `_place_buy()` and `_place_sell()` atomically — no new REST calls will fire.

2. `time.sleep(0.3)` gives OMS worker threads 300ms to finish delivering any
   REST responses that were already in-flight before the freeze.

3. `GridEngine._lock` is held for the entire read of `_long_qty` and all level
   states, so no concurrent `_on_fill()` callback can mutate state mid-snapshot.

4. Only after the JSON is written to SQLite does the live process arm
   `_handoff_stop` and request its own shutdown (see "Shutdown handoff-safety"
   above). This means `stop()` cannot run — and cannot attempt liquidation —
   until after the snapshot is safely in the DB.

---

## Fallback: Cold Start

Before any of this, `_request_and_await_handoff()` always checks first for a
leftover, un-applied snapshot from a previous crashed attempt (Step 0 — see
"Crash recovery during handoff" above) and resumes it directly if it finds
one with a dead claimant. If it finds one with a **live** claimant, it refuses
to start at all (`sys.exit(1)`) rather than risking interference — that's not
a cold start, it's a deliberate refusal to run.

Barring either of those, the incoming process falls back to a normal cold
start (with full `reconcile_on_startup()` and cancel-all-orders — after first
acquiring `bg_lock` itself, since a cold start also needs the single-instance
guarantee, e.g. against two independently-launched cold starts racing each
other) in any of these situations:

- `--role green` is used but `bg_lock` is unheld/stale (nothing is currently
  running — first-ever launch, or the previous process already stopped
  cleanly or crashed long enough ago to be considered dead)
- The live peer's snapshot does not appear within `bg_handoff_timeout_s`
  seconds (e.g. it crashed mid-export)
- The snapshot JSON fails to parse or has an unrecognised schema version (this
  also clears the corrupt JSON and releases the lock so it doesn't keep
  re-failing the same check on every future restart)

In all fallback cases, the incoming process logs a warning, clears any stale
`bg_handoff_request` from SQLite, and proceeds identically to a normal
`python grid_bot.py` launch. The position is reconciled from the exchange and
any orphaned open orders are cancelled.

---

## Code Map

| What | Class / method | Line |
|---|---|---|
| DB schema version / migration | `_GRID_DB_SCHEMA_VERSION` | ~3490 |
| `bg_lock` table DDL | `_GRID_DB_DDL` | ~3551 |
| Lock acquire (CAS) / heartbeat / release / current holder | `GridStateStore.bg_lock_try_acquire()` / `bg_lock_heartbeat()` / `bg_lock_release()` / `bg_lock_current_holder()` | ~3846 |
| Handoff request IPC | `GridStateStore.bg_request_handoff()` / `bg_poll_handoff_request()` | ~3922 |
| Snapshot write/read/clear | `GridStateStore.bg_write_handoff_json()` / `bg_read_handoff_json()` / `bg_clear_handoff_json()` | ~3933 |
| Exchange ID reverse-lookup | `OMS.get_exchange_id()` | ~1172 |
| OMS order restoration (+ fill queue) | `OMS.restore_order()` | ~1178 |
| OMS order un-registration (orphan cleanup) | `OMS.forget_order()` | ~1211 |
| Freeze flag | `GridEngine._handoff_freeze` | ~2273 |
| Freeze enforcement | `GridEngine._place_buy()` / `_place_sell()` | ~2378 |
| Skip-indices placement | `GridEngine.start()` / `_place_initial_orders()` | ~2323 |
| Incoming process: request + crash-recovery Step 0 + lock race | `GridBot._request_and_await_handoff()` / `_try_acquire_bg_lock()` | ~4308 |
| Incoming process: immediate OMS pre-registration | `GridBot._preregister_handoff_orders()` | ~4453 |
| Any process: lock heartbeat thread | `GridBot._start_lock_heartbeat()` | ~4533 |
| Live process: watcher thread (sets flag only) | `GridBot._start_handoff_watcher()` | ~4567 |
| Live process: snapshot export + lock release | `GridBot.export_handoff_snapshot()` | ~5035 |
| Incoming process: price-based level matching | `GridBot._match_handoff_levels()` | ~5132 |
| Incoming process: apply restore to built engine, clear JSON | `GridBot._apply_handoff_restore()` | ~5183 |
| CLI `--role` arg | `_parse_args()` | ~6489 |
| Role-aware log naming | `_init_logging()` | ~653 |
| Config key | `GRID_CONFIG["bg_handoff_timeout_s"]` | ~515 |

(Line numbers are approximate and will drift with future edits — search by name
if they're off.)
