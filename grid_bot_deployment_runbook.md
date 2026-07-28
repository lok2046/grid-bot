# Grid Bot — Live Deployment Runbook

## Overview

The grid bot uses **blue-green deployment** to swap in new code without
liquidating open positions. The outgoing process (blue) saves a snapshot
of its position and orders to SQLite; the incoming process (green) reads
it and resumes seamlessly.

Two processes are involved:
- **`grid_bot.py`** — the trading bot (started/stopped per deploy)
- **`grid_bot_launcher.py`** — always-running manager, never stopped during deploys

---

## Telegram commands

| Command | Handler | Action |
|---|---|---|
| `/handoff` | grid_bot.py | Save state and exit without liquidating |
| `/restart` | launcher | Start new `grid_bot.py --role green` |
| `/status` | grid_bot.py | Grid position, PnL, stop-score, TrendSignal |
| `/pnl` | grid_bot.py | Cumulative PnL, today, funding, 7-day history |
| `/pstatus` | launcher | Process PID, uptime, last 10 log lines |
| `/kill` | launcher | Emergency SIGTERM → clean shutdown + liquidation |
| `/help` | both | List all commands |

---

## Standard deployment (planned, non-urgent)

Use this flow for routine code updates during stable market hours.

```
1. Update grid_bot.py on disk
2. Send /status              → confirm bot is healthy, note current position
3. Send /handoff             → bot saves snapshot and exits
                               Wait for: 🔴 GridBot stopped
4. Send /restart             → launcher starts new --role green
                               Wait for: ✅ Handoff applied: N orders restored
                                         🟢 GridBot started
5. Send /status              → verify grid resumed correctly
```

**Expected Telegram sequence:**
```
🔴 GridBot stopped
🟢 GridBot started — LIVE | BTCUSD-PERP
✅ Handoff applied: 0.0085 BTC, 4 orders restored in place, 0 orphans cancelled & recreated fresh. Snapshot age 4823ms.
📐 Grid set: [64526, 64706] 4 levels spacing=45 stop=64517
```

**If you see `orphans=N` instead of `orphaned=0`:** the new process
recomputed a different grid spacing (BTC moved between /handoff and
/restart). The orphaned orders are cancelled and recreated fresh — no
position risk, but there is a brief gap (1–3 seconds) with no resting
orders. Normal and acceptable.

---

## Urgent deployment (market moving, time-sensitive)

If you need to deploy during active trading with a live position:

```
1. Update grid_bot.py on disk
2. Send /handoff immediately  → snapshot exported, bot exits
                                (position stays open on exchange,
                                 no orders until step 3 completes)
3. Send /restart within 30s  → new process picks up snapshot
```

**The risk window** is the gap between /handoff exiting and /restart
completing (~5–15 seconds). During this window:
- Your position is open on the exchange
- All resting orders have been cancelled (by the outgoing process)
- No stop-loss guard is active

Keep the gap short. If /restart fails (see troubleshooting below), the
exchange position remains open unprotected — send /restart again or
start manually.

---

## Cold start (no previous session / first launch)

```
python grid_bot.py --role green
```

The `--role green` flag is correct for all starts, including the very
first one. If no handoff snapshot exists in the DB, the bot performs a
normal cold start (Phase 2a DB load → Phase 2b ATR seed → Phase 2c
range ticks → grid build).

---

## Troubleshooting

### "An un-applied handoff snapshot is present, but the instance lock is already held"

The bot you're trying to start found a snapshot in the DB but the
previous process is still running. **Do not force-start** — that would
cancel all open orders.

Fix:
```
# Option A: trigger handoff on the running process
Send /handoff, then /restart

# Option B: if the running process is frozen/crashed
tasklist /FI "IMAGENAME eq python.exe"   # find the PID
taskkill /PID <pid> /F                   # force kill
python grid_bot.py --role green          # cold start
```

### /restart shows "Bot is already running"

The previous process didn't exit yet. Wait 5 seconds and try again.
If still stuck, send /kill to terminate it, then /restart.

### Handoff applied but `restored=0 orphaned=N`

All N orders were recreated fresh. This happens when BTC price moved
enough during the snapshot window to compute a different grid. Safe —
the position (long_qty) was still inherited correctly. Verify with
/status.

### Snapshot age > 30,000ms warning in log

The snapshot was stale when the new process picked it up. This can
happen if /restart was sent long after /handoff, or if the previous
process was stuck on 403 errors (no live price). The new process uses
the snapshot's long_qty but recomputes grid parameters from scratch.
The position is correct; the grid may differ from the peer's.

### 🛑 Daily loss limit alert received

The bot has halted after exceeding `daily_loss_limit_usd`. Auto-restart
is blocked. Review the day's activity in the log. When ready to resume:
```
Send /restart   → launcher starts new bot (daily counter resets on next HKT midnight)
```
Consider whether to lower `daily_loss_limit_usd` or review grid config
before resuming.

---

## NSSM service management (launcher only)

```powershell
# Check launcher status
nssm status GridBotLauncher

# Restart launcher (e.g. after OS reboot)
nssm start GridBotLauncher

# Stop launcher (rare — only for maintenance)
nssm stop GridBotLauncher

# View launcher log
type E:\code\python\grid-bot\cdc\grid_bot_launcher.log
```

The launcher is set to auto-restart (`AppExit Default Restart`) with a
2-second delay. It survives OS reboots without manual intervention.

The bot (`grid_bot.py`) is **not** managed by NSSM — the launcher starts
it on demand via /restart.

---

## Pre-live checklist (run before switching to LIVE mode)

- [ ] `TRADING_MODE = "live"` set in `grid_bot.py`
- [ ] Live API key and secret in keyring (`cdc_grid_api_key` / `cdc_grid_api_secret`)
- [ ] Confirmed maker fee rate via `private/get-fee-rate` → `effective_deriv_maker_rate_bps=1`
- [ ] `maker_fee_rate: 0.0001` in GRID_CONFIG matches
- [ ] `notional_per_level` set to intended live amount (start at $500)
- [ ] `daily_loss_limit_usd` set to appropriate live limit (e.g. $50)
- [ ] `/status` confirms bot started and grid is live before walking away
- [ ] `/pnl` shows zero cumulative (fresh DB or verified carry-over)
- [ ] Config summary in startup log verified (mode=live, instrument, rest URL)
