# grid_bot.py — Neutral Futures Grid Bot

A fully standalone Python bot that runs a **neutral grid strategy** on `BTCUSD-PERP`
perpetual futures on Crypto.com Exchange.  
"Standalone" means all infrastructure (WebSocket, OMS, logging, alerting) is copied
directly into `grid_bot.py` — no imports from `trading_bot.py` or `funding_arb/`.

---

## Table of Contents

1. [Trading Strategy](#1-trading-strategy)
2. [Configuration Reference](#2-configuration-reference)
3. [Code Structure](#3-code-structure)
4. [Quick Start](#4-quick-start)
5. [Running as a Service](#5-running-as-a-service)

---

## 1. Trading Strategy

### 1.1 What is a neutral grid?

A grid bot divides a price range `[lower, upper]` into `N` equal levels separated
by a fixed `spacing = (upper − lower) / N`.

```
upper  ──────────────────────  $65,000
         [SELL] [SELL] [SELL]
level N  ────────────────────
         [SELL] [SELL] [SELL]
  ....
mid    ══════════════════════  $60,000  ← current price
  ....
         [BUY ] [BUY ] [BUY ]
level 1  ────────────────────
         [BUY ] [BUY ] [BUY ]
lower  ──────────────────────  $55,000
```

- Levels **below** the current mid price hold resting **BUY** limit orders.
- Levels **above** the current mid price hold resting **SELL** limit orders.
- The grid is *neutral*: it profits from oscillation in **either direction**.
  No directional bet is needed.

### 1.2 Fill cycle mechanics

Every completed cycle (one BUY fill followed by one SELL fill) captures one grid
spacing as gross profit:

```
1. BUY  fills at level[i]   → price dropped to that level
2. Bot immediately places a SELL at level[i+1]  (one spacing higher)
3. SELL fills at level[i+1] → price recovered one spacing
4. Net gain = spacing − (2 × maker_fee × notional_per_level)
```

Symmetrically, when a SELL fills first (price rises through a level), the bot
places a BUY one level lower to catch the pullback.

**Example** with `spacing = $500`, `notional_per_level = $500 USD`, BTC at `$60,000`:

| Item | Value |
|------|-------|
| Qty per level | `$500 / $60,000 ≈ 0.0083 BTC` |
| Gross profit per cycle | `0.0083 × $500 = $4.17` |
| Fee cost (maker 0.01% × 2 sides) | `0.0083 × $60,000 × 0.02% = $0.10` |
| **Net profit per cycle** | **≈ $4.07** |

To reach HKD 1,000/day (~USD 128/day) the bot needs roughly **31 completed cycles
per day** across all active levels — achievable when BTC oscillates actively
within the grid range.

### 1.3 Why this works in a bear market

Unlike a directional strategy (swing trading), a grid bot does not need to predict
where BTC goes. It only needs BTC to *move back and forth* within the range.
Bear markets with sideways consolidation — BTC bouncing between support and
resistance — are ideal conditions.

### 1.4 Stop-loss

The fundamental risk of a grid bot is a sustained directional move **below the
lower bound** with no recovery. In that case, all BUY orders fill one by one as
BTC falls, and the bot accumulates a large underwater long position.

To contain this risk, a hard stop-loss is placed below the lower grid bound:

```
stop_price = lower − stop_buffer_atr × ATR
```

When `mid_price < stop_price`:
1. All open grid orders are cancelled.
2. The entire accumulated long position is market-sold immediately.
3. The bot halts and sends a Telegram alert. Manual restart is required.

This converts an unlimited drawdown into a defined worst-case loss.

### 1.5 Auto-tuning

At startup (and periodically or when price exits the range), the bot recomputes
grid parameters from live market data using ATR (Average True Range):

```
ATR         = average true range of 1-minute candles over lookback window
lower       = mid − atr_multiplier × ATR
upper       = mid + atr_multiplier × ATR
stop        = lower − stop_buffer_atr × ATR
spacing     = max(min_grid_pct × mid,  2 × maker_fee × mid × 1.5)
N (levels)  = clamp(floor(range / spacing),  min_grid_levels, max_grid_levels)
```

The spacing floor guarantees every cycle nets positive after fees.  
A **dead-band** prevents thrashing: the grid is only rebuilt if the new range
differs from the current one by more than `retune_deadband_pct`.

---

## 2. Configuration Reference

All settings live in the `GRID_CONFIG` dict at the top of `grid_bot.py`.

### 2.1 Exchange / credentials

| Key | Default | Description |
|-----|---------|-------------|
| `instrument` | `"BTCUSD-PERP"` | Perpetual futures instrument name on Crypto.com |
| `live_trading` | `False` | `False` = paper mode (no real orders). Set `True` to go live. |
| `api_key` | `""` | Crypto.com API key (required for live mode) |
| `api_secret` | `""` | Crypto.com API secret (required for live mode) |

### 2.2 Fee rates

| Key | Default | Description |
|-----|---------|-------------|
| `maker_fee_rate` | `0.0001` | 0.01% — your confirmed derivatives maker tier |
| `taker_fee_rate` | `0.0003` | 0.03% — used for the stop-loss market order |

### 2.3 Grid geometry (fallback defaults)

These values are used only when auto-tune is disabled or ATR data is unavailable.

| Key | Default | Description |
|-----|---------|-------------|
| `grid_lower` | `55000.0` | Lower bound of price range (USD) |
| `grid_upper` | `65000.0` | Upper bound of price range (USD) |
| `grid_levels` | `20` | Number of grid levels |
| `notional_per_level` | `500.0` | USD notional per grid order. Controls position size. |

> **Sizing guide:** `notional_per_level × grid_levels` = total capital deployed.
> With 20 levels × $500 = $10,000 total exposure. Adjust to your risk appetite.

### 2.4 Auto-tuner

| Key | Default | Description |
|-----|---------|-------------|
| `auto_tune_enabled` | `True` | Enable ATR-based parameter computation |
| `atr_lookback_minutes` | `1440` | Lookback window for ATR (1440 = 24 hours) |
| `atr_multiplier` | `3.0` | `range = mid ± N × ATR`. Higher = wider grid, fewer but larger cycles. |
| `min_grid_pct` | `0.0008` | Minimum grid spacing as a fraction of price (0.08%). Must exceed `2 × maker_fee`. |
| `max_grid_levels` | `50` | Cap on number of levels to prevent over-fragmentation |
| `min_grid_levels` | `5` | Floor on number of levels |
| `retune_interval_hours` | `24` | Re-tune grid even if price stayed in range (periodic reset) |
| `retune_deadband_pct` | `0.10` | Skip re-tune if new range is within 10% of current range |

**Tuning tips:**
- Increase `atr_multiplier` (e.g. 4.0) for a wider, safer range with fewer resets.
- Decrease `atr_multiplier` (e.g. 2.0) for a tighter range that cycles more frequently but resets more often.
- Increase `notional_per_level` to earn more per cycle; also increases risk per level.

### 2.5 Stop-loss

| Key | Default | Description |
|-----|---------|-------------|
| `stop_loss_enabled` | `True` | Enable the stop-loss guard. Do not disable in live mode. |
| `stop_buffer_atr` | `1.0` | Stop is placed `N × ATR` below the grid lower bound |

> With `atr_multiplier=3.0` and `stop_buffer_atr=1.0`, the stop sits 4× ATR below
> mid — roughly a 4-sigma move from the starting price.

### 2.6 WebSocket

| Key | Default | Description |
|-----|---------|-------------|
| `ws_market_url` | Crypto.com stream URL | Public market data WebSocket |
| `ws_stale_threshold_s` | `20` | Reconnect if no message received for this many seconds |
| `ws_reconnect_backoff_s` | `2` | Initial reconnect delay; doubles on each attempt |
| `ws_max_backoff_s` | `60` | Maximum reconnect delay cap |
| `min_warmup_seconds` | `60` | Seconds of price history required before placing first orders |

### 2.7 REST

| Key | Default | Description |
|-----|---------|-------------|
| `rest_base_url` | `https://api.crypto.com/exchange/v1` | REST API base URL |

### 2.8 OMS / order params

| Key | Default | Description |
|-----|---------|-------------|
| `maker_fill_timeout` | `10.0` | Seconds before a live maker order is cancelled and re-placed |
| `tick_size` | `1.0` | Minimum price increment for BTCUSD-PERP |

### 2.9 Risk / circuit breaker

| Key | Default | Description |
|-----|---------|-------------|
| `max_long_qty_btc` | `0.5` | Alert threshold: warn if accumulated long exceeds this |
| `daily_loss_limit_usd` | `500.0` | Reserved for future daily loss circuit breaker |

### 2.10 Telegram alerts

| Key | Default | Description |
|-----|---------|-------------|
| `telegram_bot_token` | `""` | BotFather token. Leave empty to disable. |
| `telegram_chat_id` | `""` | Your Telegram chat / group ID |

Alerts fire on: bot start, grid (re)build, each stop-loss trigger, bot stop.

### 2.11 Logging

| Key | Default | Description |
|-----|---------|-------------|
| `log_dir` | `"logs_grid"` | Directory for daily rotating log files |
| `log_level` | `"INFO"` | Console log level (`DEBUG`, `INFO`, `WARNING`) |
| `log_backup_count` | `30` | Days of log files to keep before pruning |

Log files rotate at **midnight HKT** and are named `grid_bot_YYYY_MM_DD.log`.
Uncaught exceptions are written directly to the log file before the process dies.

---

## 3. Code Structure

`grid_bot.py` is a single self-contained file (~1,870 lines). All infrastructure
is copied inline — no external bot imports. Sections appear in this order:

```
grid_bot.py
│
├── GRID_CONFIG dict                 # all runtime settings (edit here)
│
├── Logging (_HKTDailyRotatingHandler, _SafeQueueListener, _init_logging)
│   Copied from funding_arb/logger_setup.py.
│   Async QueueHandler/QueueListener so log writes never block the trading loop.
│   Rotates at midnight HKT. Installs sys.excepthook crash handler.
│
├── AlertManager
│   Copied from funding_arb/alerting.py.
│   Async Telegram queue with retry. send() returns immediately; worker thread
│   delivers in background. send_sync() used at shutdown only.
│
├── OMS  (Order Management System)
│   Stripped from trading_bot/oms.py.
│   │
│   ├── OrderStatus (Enum)           PENDING / ACTIVE / FILLED / CANCELLED / REJECTED
│   ├── OrderRequest (dataclass)     limit_maker() and market() factory methods
│   ├── FillEvent (dataclass)        result delivered after order resolves
│   ├── _LiveOrder (dataclass)       internal live-order tracking record
│   └── OMS (class)
│       ├── Paper mode               instant fill at limit price; correct maker fee
│       ├── Live mode                REST submit → WS fill notification
│       ├── _signed_post()           HMAC-SHA256 signed REST calls
│       └── _cancel_all_dangling()   called on stop() to clean up live orders
│
├── _ReconnectingWS
│   Copied from funding_arb/ws_manager.py.
│   Generation-tagged reconnect loop. Each new connection gets a generation
│   counter; stale callbacks from old connections are silently dropped.
│   │
│   ├── DOA detection                if on_open fires but no message arrives within
│   │                                10s, the connection is "Dead On Arrival"
│   ├── Stale watchdog               separate thread monitors last-message timestamp;
│   │                                signals abandon if > ws_stale_threshold_s
│   └── Exponential backoff          2s → 4s → 8s … capped at ws_max_backoff_s
│
├── PriceCache
│   Thread-safe L1 price store. Written by WS thread, read by GridBot loop.
│   │
│   ├── update_l1(bid, ask)          called on every ticker message
│   ├── compute_atr(lookback_min)    builds 1-min candles from tick history,
│   │                                computes average true range (ATR)
│   └── warmup_complete(min_s)       True once enough history has been collected
│
├── GridParams (dataclass)
│   Immutable snapshot of computed grid geometry.
│   Fields: lower, upper, levels, spacing, stop_price, notional_per_level.
│   level_prices property returns the list of N+1 price points.
│
├── GridAutoTuner
│   Derives GridParams from live ATR + config multipliers.
│   │
│   ├── compute()                    main entry point; returns GridParams or None
│   ├── _from_config()               fallback if ATR unavailable
│   └── should_retune()              True if price exited range or interval elapsed
│
├── LevelState (Enum)                IDLE / BUY_OPEN / SELL_OPEN
├── GridLevel (dataclass)            one level: index, price, state, client_oid, qty
│
├── GridEngine
│   Owns the array of GridLevels; places and replaces orders via OMS.
│   │
│   ├── start(mid)                   build levels, place initial orders, start fill thread
│   ├── stop()                       cancel all orders, stop fill thread
│   ├── check_price_fills(mid)       called every tick by GridBot main loop
│   │   ├── paper mode               _simulate_paper_fills: detects cross by price tick
│   │   └── live mode                _poll_live_fills: polls OMS wait_fill(timeout=0)
│   ├── _on_fill(idx, fill)          routes fill → counter-order + accounting update
│   │   ├── BUY  fill at [i]  →      place SELL at [i+1]
│   │   └── SELL fill at [i]  →      place BUY  at [i-1], record realized PnL
│   └── get_stats()                  snapshot of open orders, long qty, PnL, cycles
│
├── StopLossGuard
│   Single-check guard. Latches permanently on first breach.
│   check(mid) → True triggers GridBot._emergency_halt().
│
├── _price_cache  (module-level singleton)
│   Shared PriceCache instance used by all components.
│
└── GridBot  (top-level controller)
    │
    ├── __init__()        creates OMS, AlertManager, GridAutoTuner, _ReconnectingWS
    ├── start()           OMS.start() → WS.start() → warmup → _rebuild_grid() → _run()
    ├── stop()            signals all threads, calls OMS.stop(), sends final alert
    ├── _run()            main loop: stop-loss → fill poll → retune check → status log
    ├── _rebuild_grid()   auto-tune → dead-band check → GridEngine.start()
    └── _emergency_halt() GridEngine.stop() → market SELL all long → halt + alert
```

### Thread map

| Thread | Name | Owned by | Purpose |
|--------|------|----------|---------|
| Main | `MainThread` | OS | `GridBot.start()` → `_run()` loop |
| WS reconnect | `WSLoop-MarketWS` | `_ReconnectingWS` | Reconnect loop |
| WS worker | `WSWorker-MarketWS-gN` | `_ReconnectingWS` | `ws.run_forever()` per connection |
| WS watchdog | `WSWatchdog-MarketWS` | `_ReconnectingWS` | Stale-data detection |
| OMS worker | `OMS-worker` | `OMS` | Processes `_order_queue`, calls paper/live fill |
| OMS WS | `OMS-ws` | `OMS` | Live-mode user channel (fill notifications) |
| Grid fills | `Grid-fills` | `GridEngine` | Drains `_fill_queue`, calls `_on_fill()` |
| Alerts | `GridAlerts` | `AlertManager` | Async Telegram delivery with retry |
| Logger | `cp_QueueListener-N` | `_SafeQueueListener` | Async log write to file |

### Data flow

```
Crypto.com WS (ticker)
        │
        ▼
_ReconnectingWS._on_raw_message()
        │
        ▼
PriceCache.update_l1(bid, ask)          ← shared singleton _price_cache
        │
        ▼
GridBot._run()  [main loop, 100ms tick]
        │
        ├──► StopLossGuard.check(mid)
        │         └── breach → GridBot._emergency_halt()
        │                   → OMS.submit(market SELL)
        │
        ├──► GridEngine.check_price_fills(mid)
        │         ├── paper: _simulate_paper_fills()  detect cross → FillEvent
        │         └── live:  _poll_live_fills()        OMS.wait_fill(timeout=0)
        │                   └── FillEvent → _fill_queue → _fill_loop thread
        │                             └── _on_fill() → _place_buy/_place_sell
        │                                           → OMS.submit(limit_maker)
        │
        └──► GridAutoTuner.should_retune()
                  └── yes → GridBot._rebuild_grid()
                          → GridEngine.stop() → GridEngine.start()
```

---

## 4. Quick Start

### Prerequisites

```bash
pip install websocket-client requests
```

### Paper trading (default)

No API keys needed. The bot simulates fills based on live price ticks.

```bash
# 1. Set min_warmup_seconds to something short for a quick test
#    (default 60s — bot waits for 60s of price history before placing orders)

python grid_bot.py
```

You will see log output like:

```
2026-06-28 10:00:00 [INFO] [GridBot] Starting
2026-06-28 10:00:00 [INFO] [OMS] Started (live=False)
2026-06-28 10:00:00 [INFO] [MarketWS] connecting (gen=1)
2026-06-28 10:00:01 [INFO] [MarketWS] connected (gen=1)
2026-06-28 10:01:00 [INFO] [GridBot] Warmup complete
2026-06-28 10:01:00 [INFO] [AutoTuner] mid=60123.45 ATR=52.3 range=[59653,60594] levels=12 spacing=78.5 stop=59601
2026-06-28 10:01:00 [INFO] [GridBot] Grid live: [59653,60594] levels=12 spacing=78.50 stop=59601
2026-06-28 10:02:30 [INFO] [GridEngine] FILL BUY  [3] @ 59800.00 qty=0.0083 fee=0.049680 long=0.0083 BTC
2026-06-28 10:02:45 [INFO] [GridEngine] FILL SELL [4] @ 59878.50 qty=0.0083 fee=0.049740 | cycle #1 gross=+0.6515 net=+0.5520 cumulative_net=+0.55 USD
```

### Going live

1. Obtain API keys from Crypto.com with **trade** permission (no withdrawal permission needed).
2. Edit `GRID_CONFIG` in `grid_bot.py`:
   ```python
   "live_trading": True,
   "api_key":      "your-api-key",
   "api_secret":   "your-api-secret",
   ```
3. Optionally add Telegram credentials for alerts.
4. Run:
   ```bash
   python grid_bot.py
   ```

> **Recommended:** Paper trade for at least 24–48 hours first to verify cycle
> frequency and fee costs against expectations before going live.

---

## 5. Running as a Service

Use NSSM (Non-Sucking Service Manager) to run the bot as a Windows service,
consistent with your other bots.

```bat
nssm install GridBot "C:\Python312\python.exe" "C:\bots\grid_bot.py"
nssm set GridBot AppDirectory "C:\bots"
nssm set GridBot AppStdout "C:\bots\logs_grid\service_stdout.log"
nssm set GridBot AppStderr "C:\bots\logs_grid\service_stderr.log"
nssm set GridBot Start SERVICE_AUTO_START
nssm start GridBot
```

SIGTERM from NSSM triggers `_shutdown()` → `GridBot.stop()` → graceful order
cancellation (live mode) and final Telegram alert.
