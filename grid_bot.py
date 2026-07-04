"""
grid_bot.py — Neutral Futures Grid Bot for BTCUSD-PERP on Crypto.com
Standalone: all dependencies copied in; no imports from trading_bot.py or funding_arb/.

Architecture
============
  PriceCache       — tick-level L1 cache + ATR computation from 1-min candles
  GridAutoTuner    — derives range/levels/stop from live ATR; dead-band re-tune
  GridEngine       — manages the limit-order ladder; routes fills to counter-orders
  StopLossGuard    — halts + liquidates on price breach below stop
  _ReconnectingWS  — generation-tagged WS with DOA detection + stale watchdog
                     (copied from funding_arb/ws_manager.py)
  LoggerSetup      — async QueueHandler/QueueListener with HKT rotation + crash hook
                     (copied from funding_arb/logger_setup.py)
  AlertManager     — async Telegram queue with retry
                     (copied from funding_arb/alerting.py)
  OMS              — copied from trading_bot/oms.py (standalone REST+WS order manager)
  GridBot          — top-level controller

Neutral grid logic
==================
  Price range [lower, upper] divided into N equal levels.
  Levels below mid → BUY limits; levels above mid → SELL limits.
  BUY fill at level[i]  → place SELL at level[i+1]  (take-profit one level up)
  SELL fill at level[i] → place BUY  at level[i-1]  (take-profit one level down)
  Each completed cycle captures one grid spacing as gross profit.
  Net profit per cycle ≈ spacing/mid - 2 × maker_fee_rate  (fraction of notional)

Stop-loss
=========
  stop_price = lower - stop_buffer_atr × ATR
  On breach: cancel all grid orders → market-SELL entire accumulated long → halt.

Auto-tuner
==========
  lower = mid - atr_multiplier × ATR
  upper = mid + atr_multiplier × ATR
  stop  = lower - stop_buffer_atr × ATR
  N     = floor(range / min_spacing);  min_spacing > 2 × maker_fee × mid (with buffer)
  Re-tune triggers: price exits range OR retune_interval_hours elapsed.
  Dead-band: skip if new range differs < retune_deadband_pct from current.
"""

# ─────────────────────────────────────────────────────────────────────────────
# Stdlib imports
# ─────────────────────────────────────────────────────────────────────────────
import atexit
import collections
import hashlib
import hmac
import json
import logging
import logging.handlers
import math
import os
import queue
import signal
import sys
import threading
import time
import traceback
import uuid
import datetime as _dt
from dataclasses import dataclass, field
from enum import Enum
from typing import Callable, Dict, List, Optional, Tuple

import requests
import websocket

# ─────────────────────────────────────────────────────────────────────────────
# TRADING MODE  ← the ONLY line you need to change when switching environments
# ─────────────────────────────────────────────────────────────────────────────
#
#   "paper"  — No real orders placed. Uses live Production market data for price
#              feed and paper-fill simulation. Safe to run at any time; nothing
#              touches your real account.
#
#   "uat"    — Real orders sent to Crypto.com UAT Sandbox exchange.
#              Uses UAT REST + WS endpoints. Requires UAT API keys
#              (create at https://exchange-uat.crypto.com — separate from prod).
#              Funding rates and prices on UAT are synthetic, not real.
#
#   "live"   — Real orders sent to Production exchange. Real money.
#
TRADING_MODE = "paper"   # ← change this line only

# ─────────────────────────────────────────────────────────────────────────────
# Secrets  (keyring → env var → empty string fallback)
# ─────────────────────────────────────────────────────────────────────────────
#
# Keyring setup (run ONCE in a terminal, same Windows user as the NSSM service):
#   cmdkey /generic:cdc_grid_api_key    /user:api_key    /pass:YOUR_API_KEY
#   cmdkey /generic:cdc_grid_api_secret /user:api_secret /pass:YOUR_API_SECRET
#   cmdkey /generic:cdc_grid_tg_token   /user:token      /pass:YOUR_TG_TOKEN
#   cmdkey /generic:cdc_grid_tg_chatid  /user:chatid     /pass:YOUR_CHAT_ID
#
# UAT keys use the same keyring names — swap them in/out when switching modes.
#
try:
    import keyring as _keyring
except ImportError:
    _keyring = None

def _secret(keyring_name: str, keyring_user: str, env_var: str) -> str:
    if _keyring:
        try:
            val = _keyring.get_password(keyring_name, keyring_user)
            if val:
                return val
        except Exception:
            pass   # no keyring backend (headless / Linux CI) — fall through
    return os.environ.get(env_var, "")

# ─────────────────────────────────────────────────────────────────────────────
# CONFIG  ← all other settings live here; do not touch for mode switching
# ─────────────────────────────────────────────────────────────────────────────

GRID_CONFIG: dict = {
    # ── Identity ──────────────────────────────────────────────────────────────
    "api_key":    _secret("cdc_grid_api_key",    "api_key",    "CDC_GRID_API_KEY"),
    "api_secret": _secret("cdc_grid_api_secret", "api_secret", "CDC_GRID_API_SECRET"),

    # ── Exchange / instrument ─────────────────────────────────────────────────
    "instrument":    "BTCUSD-PERP",
    "trading_mode":  TRADING_MODE,                   # "paper" | "uat" | "live"
    "live_trading":  TRADING_MODE == "live",          # True only for Production

    # ── Fee rates (your verified Crypto.com deriv maker/taker tier) ───────────
    "maker_fee_rate": 0.0001,          # 0.01% deriv maker
    "taker_fee_rate": 0.0003,          # 0.03% deriv taker

    # ── Grid geometry (auto-tuned at startup; these are fallback defaults) ────
    "grid_lower":         55000.0,
    "grid_upper":         65000.0,
    "grid_levels":        20,
    "notional_per_level": 500.0,       # USD notional per grid order

    # ── Auto-tuner ────────────────────────────────────────────────────────────
    "auto_tune_enabled":    True,
    "atr_lookback_minutes": 1440,      # 1-day lookback for ATR
    "atr_multiplier":       3.0,       # range = mid ± N×ATR
    "min_grid_pct":         0.0008,    # min grid spacing as fraction of price
    "max_grid_levels":      50,
    "min_grid_levels":      5,
    "retune_interval_hours": 24,
    "retune_deadband_pct":  0.10,      # skip re-tune if range shifts < 10%

    # ── Trailing Up ───────────────────────────────────────────────────────────
    # When price rises above the grid upper bound, instead of stopping and
    # rebuilding the whole grid (which would reset all orders), the grid shifts
    # up by one spacing interval: the lowest BUY level is cancelled and a new
    # SELL level is added one spacing above the current upper bound.
    # This lets the bot chase an uptrend incrementally without a full rebuild.
    #
    # trailing_up_enabled:   enable/disable the feature
    # trailing_up_price_cap: optional hard ceiling — grid will not trail above
    #                        this price (0.0 = no cap)
    "trailing_up_enabled":   False,
    "trailing_up_price_cap": 0.0,

    # ── Trailing Down ─────────────────────────────────────────────────────────
    # Mirror of Trailing Up for downtrends: when price drops below the lower
    # bound, the grid shifts down by one spacing — the highest SELL level is
    # cancelled and a new BUY level is added one spacing below the current lower
    # bound. Stops at stop_loss_price even if trailing is enabled.
    #
    # WARNING: trailing down accumulates long exposure as BTC falls. Only enable
    # if you accept that risk and have a meaningful stop_loss in place.
    "trailing_down_enabled":   False,
    "trailing_down_price_cap": 0.0,   # optional floor — grid will not trail below
                                       # this price (0.0 = no cap, stop_loss applies)

    # ── Stop-loss ─────────────────────────────────────────────────────────────
    "stop_loss_enabled": True,
    "stop_buffer_atr":   1.0,         # stop = lower - N×ATR

    # Minimum headroom between current mid and the newly-computed stop price,
    # expressed as a multiple of ATR.  If mid < stop + N×ATR at the moment the
    # grid is (re)built, the build is aborted: price is already too close to the
    # stop for the grid to be useful.  This prevents the bot from arming a stop
    # that fires within seconds of startup or auto-restart.
    #
    # How it relates to stop_buffer_atr:
    #   stop = lower - stop_buffer_atr × ATR = (mid - atr_multiplier×ATR) - stop_buffer_atr×ATR
    #   headroom at startup = mid - stop = (atr_multiplier + stop_buffer_atr) × ATR
    #                       = (3.0 + 1.0) × ATR = 4.0 × ATR  (normal case)
    # Setting min_stop_headroom_atr = 0.5 means we require at least 0.5×ATR of
    # buffer beyond the stop — a very light sanity check that only blocks the build
    # when price has already drifted to within 0.5×ATR of the stop.
    "min_stop_headroom_atr": 0.5,

    # ── Auto-restart after stop-loss ──────────────────────────────────────────
    # After a stop-loss halt, the bot monitors price and automatically rebuilds
    # the grid when market conditions are stable again.
    #
    # ALL four conditions must be true simultaneously before restarting:
    #   1. Cooldown: at least auto_restart_cooldown_minutes since halt.
    #      Prevents restarting into a dead-cat bounce.
    #   2. Price recovered: mid > halt_stop_price − auto_restart_recovery_atr_buffer × ATR.
    #      The buffer (default 0.5×ATR ≈ half a minute's noise) prevents the bot from
    #      staying pinned when BTC consolidates $1–2 below the exact stop cent value.
    #      The _rebuild_grid stop-proximity guard (headroom > 0.5×ATR) acts as a second
    #      line of defence: if price is genuinely too close to the stop, the rebuild
    #      aborts and the bot reverts to halted.  Set buffer=0.0 for strict behaviour.
    #   3. Stable range: hi-lo over last auto_restart_stability_minutes
    #      < auto_restart_stability_atr_mult × ATR.
    #      Confirms BTC is oscillating in a tight band, not still crashing.
    #      ATR is a per-1-minute figure; the stability window is 60 minutes.
    #      The multiplier must scale accordingly: sqrt(stability_minutes) ≈ 7.75
    #      so that the threshold represents the expected random-walk range over
    #      the window.  Setting mult=1.0 makes the gate permanently unsatisfiable.
    #   4. Flat/rising trend: current mid >= mean(prices over stability window).
    #      Rejects a slow bleed where range is small but price drifts lower.
    #
    # Set auto_restart_enabled=False to keep the original "halt until manual
    # restart" behaviour.
    "auto_restart_enabled":           True,
    "auto_restart_cooldown_minutes":  30,    # minimum wait after halt
    "auto_restart_stability_minutes": 60,    # look-back window for stability check
    "auto_restart_stability_atr_mult": 7.75, # hi-lo < N × ATR; 7.75 = sqrt(60) scales 1-min ATR to 60-min window
    "auto_restart_recovery_atr_buffer": 0.5, # price gate: allow restart when mid > halt_stop - N×ATR
                                             # 0.5×ATR ≈ half a minute's typical move — genuine noise, not a
                                             # resumed downtrend.  The _rebuild_grid stop-proximity guard
                                             # (headroom > 0.5×ATR) is the second line of defence: if price
                                             # is truly too close to the new stop, the rebuild aborts and the
                                             # bot reverts to halted state.  Set to 0.0 to keep the old
                                             # strict behaviour (mid must exceed halt_stop exactly).
    "auto_restart_max_attempts":      3,     # give up after N failed attempts; 0 = unlimited

    # ── Endpoints (auto-selected by TRADING_MODE — do not edit) ───────────────
    "rest_base_url": {
        "paper": "https://api.crypto.com/exchange/v1",
        "uat":   "https://uat-api.3ona.co/exchange/v1",
        "live":  "https://api.crypto.com/exchange/v1",
    }[TRADING_MODE],
    "ws_market_url": {
        "paper": "wss://stream.crypto.com/exchange/v1/market",
        "uat":   "wss://uat-stream.3ona.co/exchange/v1/market",
        "live":  "wss://stream.crypto.com/exchange/v1/market",
    }[TRADING_MODE],
    "ws_user_url": {
        "paper": "wss://stream.crypto.com/exchange/v1/user",
        "uat":   "wss://uat-stream.3ona.co/exchange/v1/user",
        "live":  "wss://stream.crypto.com/exchange/v1/user",
    }[TRADING_MODE],

    # ── WebSocket tuning ──────────────────────────────────────────────────────
    "ws_stale_threshold_s":   20,
    "ws_reconnect_backoff_s":  2,
    "ws_max_backoff_s":       60,
    # Minimum seconds to wait for the first live price tick (Phase 1 warmup).
    # Phase 2 waits separately inside start() until compute_atr() returns a
    # valid value (MIN_ATR_CANDLES=30 one-minute candles, ~30 min wall time).
    # 10s is enough to detect a dead WS at startup; no need to set this large.
    "min_warmup_seconds":     10,

    # ── OMS / order params ────────────────────────────────────────────────────
    "maker_fill_timeout":  10.0,
    "paper_latency_ms":    50.0,
    "tick_size":            1.0,       # BTCUSD-PERP min price increment
    "rtt_degraded_p95_ms": 300.0,

    # ── Risk / circuit breaker ────────────────────────────────────────────────
    "max_long_qty_btc":    0.5,        # alert if accumulated long exceeds this
    "daily_loss_limit_usd": 500.0,

    # ── Telegram (optional) ───────────────────────────────────────────────────
    "telegram_bot_token": _secret("cdc_grid_tg_token",  "token",  "CDC_GRID_TG_BOT_TOKEN"),
    "telegram_chat_id":   _secret("cdc_grid_tg_chatid", "chatid", "CDC_GRID_TG_CHAT_ID"),

    # ── Logging ───────────────────────────────────────────────────────────────
    "log_dir":          "logs_grid",
    "log_level":        "INFO",
    "log_backup_count": 30,

    # ── SQLite persistence ────────────────────────────────────────────────────
    "db_path": "grid_bot.db",     # fills, daily PnL, and accumulated PnL survive restarts

    # ── Trend signal (Phase 1 — read-only observer, no grid side-effects) ────
    # Dual-EMA trend detection on hourly closes derived from PriceCache history.
    # The signal is logged every STATUS_INTERVAL_S seconds and included in the
    # /status Telegram reply.  It does NOT change grid behaviour yet.
    #
    # trend_signal_ema_fast_h        fast EMA period in hours (default 4h)
    # trend_signal_ema_slow_h        slow EMA period in hours (default 24h)
    # trend_signal_slope_window_h    hours over which fast-EMA slope is measured
    # trend_signal_min_history_h     minimum hours of data before any signal is
    #                                emitted (default slow_h + 2 = 26h)
    # trend_signal_confirm_periods   consecutive evaluations before UP/DOWN is
    #                                committed (hysteresis; default 3 × 60s = 3 min)
    # trend_signal_slope_threshold_pct  minimum fast-EMA slope as % of slow-EMA
    #                                to count as directional (default 0.05%)
    "trend_signal_ema_fast_h":           4,
    "trend_signal_ema_slow_h":           24,
    "trend_signal_slope_window_h":       2,
    "trend_signal_min_history_h":        26,   # slow_h + 2h EMA warm-up buffer
    "trend_signal_confirm_periods":      3,
    "trend_signal_slope_threshold_pct":  0.05,
}

INSTRUMENT = GRID_CONFIG["instrument"]
_HKT_TZ    = _dt.timezone(_dt.timedelta(hours=8))

# ─────────────────────────────────────────────────────────────────────────────
# Logging  (copied from funding_arb/logger_setup.py)
# ─────────────────────────────────────────────────────────────────────────────

class _HKTDailyRotatingHandler(logging.handlers.BaseRotatingHandler):
    """Rotates at midnight HKT; keeps last backup_count files."""
    def __init__(self, log_dir: str, base_name: str = "grid_bot", backup_count: int = 30):
        self.log_dir      = log_dir
        self.base_name    = base_name
        self.backup_count = backup_count
        self._current_date = self._hkt_date()
        os.makedirs(log_dir, exist_ok=True)
        super().__init__(self._build_path(self._current_date),
                         mode="a", encoding="utf-8", delay=False)

    def _hkt_date(self) -> str:
        return _dt.datetime.now(_HKT_TZ).strftime("%Y_%m_%d")

    def _build_path(self, date_str: str) -> str:
        return os.path.join(self.log_dir, f"{self.base_name}_{date_str}.log")

    def shouldRollover(self, record) -> bool:
        return self._hkt_date() != self._current_date

    def doRollover(self) -> None:
        if self.stream:
            self.stream.close()
            self.stream = None
        self._current_date = self._hkt_date()
        self.baseFilename   = self._build_path(self._current_date)
        self.stream         = self._open()
        self._prune_old_logs()

    def _prune_old_logs(self) -> None:
        try:
            files = sorted(f for f in os.listdir(self.log_dir)
                           if f.startswith(self.base_name) and f.endswith(".log"))
            for old in files[:-self.backup_count]:
                os.remove(os.path.join(self.log_dir, old))
        except Exception:
            pass

    def emit(self, record) -> None:
        if self.shouldRollover(record):
            self.doRollover()
        super().emit(record)


class _SafeQueueListener(logging.handlers.QueueListener):
    """QueueListener resilient to individual handler failures."""
    def handle(self, record: logging.LogRecord) -> None:
        record = self.prepare(record)
        for handler in self.handlers:
            if not self.respect_handler_level or record.levelno >= handler.level:
                try:
                    handler.handle(record)
                except Exception as exc:
                    print(f"[logger] handler {handler!r} error: {exc}", file=sys.stderr)

    def _monitor(self) -> None:
        q = self.queue
        has_task_done = hasattr(q, "task_done")
        while True:
            try:
                record = self.dequeue(True)
                if record is self._sentinel:
                    if has_task_done:
                        q.task_done()
                    break
                try:
                    self.handle(record)
                except Exception as exc:
                    print(f"[logger] QueueListener.handle error: {exc}", file=sys.stderr)
                if has_task_done:
                    q.task_done()
            except queue.Empty:
                break


_log_queue:   queue.Queue        = queue.Queue(-1)
_listener:    Optional[_SafeQueueListener] = None
_file_handler: Optional[_HKTDailyRotatingHandler] = None


def _init_logging(config: dict) -> logging.Logger:
    global _listener, _file_handler
    log_dir      = config.get("log_dir", "logs_grid")
    backup_count = config.get("log_backup_count", 30)
    level        = getattr(logging, config.get("log_level", "INFO").upper(), logging.INFO)
    fmt          = logging.Formatter("%(asctime)s [%(levelname)s] %(message)s",
                                     datefmt="%Y-%m-%d %H:%M:%S")

    _file_handler = _HKTDailyRotatingHandler(log_dir, backup_count=backup_count)
    _file_handler.setLevel(logging.DEBUG)
    _file_handler.setFormatter(fmt)

    console = logging.StreamHandler()
    console.setLevel(level)
    console.setFormatter(fmt)

    _listener = _SafeQueueListener(_log_queue, _file_handler, console,
                                    respect_handler_level=True)
    _listener.start()

    # Crash hook — write uncaught exceptions directly to file before process dies
    _orig_excepthook = sys.excepthook
    def _excepthook(exc_type, exc_value, exc_tb):
        msg  = "".join(traceback.format_exception(exc_type, exc_value, exc_tb))
        ts   = _dt.datetime.now(_HKT_TZ).strftime("%Y-%m-%d %H:%M:%S")
        line = f"{ts} [CRITICAL] [UNCAUGHT] {msg.rstrip()}\n"
        if _file_handler and _file_handler.stream:
            try:
                _file_handler.stream.write(line)
                _file_handler.stream.flush()
            except Exception:
                pass
        print(line, file=sys.stderr, end="")
        _orig_excepthook(exc_type, exc_value, exc_tb)
    sys.excepthook = _excepthook

    atexit.register(lambda: _listener.stop() if _listener else None)

    log = logging.getLogger("GridBot")
    log.setLevel(logging.DEBUG)
    log.propagate = False
    qh = logging.handlers.QueueHandler(_log_queue)
    qh.setLevel(logging.DEBUG)
    log.addHandler(qh)
    return log


# Initialise immediately so logger is available for everything below
logger = _init_logging(GRID_CONFIG)


# ─────────────────────────────────────────────────────────────────────────────
# Telegram AlertManager  (copied from funding_arb/alerting.py)
# ─────────────────────────────────────────────────────────────────────────────

class AlertManager:
    """Async Telegram alerter. send() never blocks the caller."""
    _TG_API      = "https://api.telegram.org"
    _MAX_RETRIES = 3
    _RETRY_DELAY = 5

    def __init__(self, config: dict) -> None:
        self._token   = config.get("telegram_bot_token", "")
        self._chat_id = config.get("telegram_chat_id",   "")
        self._enabled = bool(self._token and self._chat_id)
        self._stop    = threading.Event()
        self._queue: queue.Queue = queue.Queue(maxsize=200)
        self._thread  = threading.Thread(target=self._worker,
                                         name="GridAlerts", daemon=True)
        self._thread.start()
        if self._enabled:
            logger.info("[AlertManager] Telegram enabled")
        else:
            logger.warning("[AlertManager] Telegram not configured — log only")

    def send(self, text: str) -> None:
        logger.info(f"[Alert] {text[:120].replace(chr(10), ' ')}")
        if self._enabled:
            try:
                self._queue.put_nowait((text, "Markdown"))
            except queue.Full:
                pass

    def send_sync(self, text: str) -> bool:
        logger.info(f"[Alert][sync] {text[:120].replace(chr(10), ' ')}")
        return self._post(text, "Markdown") if self._enabled else False

    def stop(self) -> None:
        self._stop.set()
        self._queue.put(None)
        self._thread.join(timeout=15)

    def _worker(self) -> None:
        while True:
            item = self._queue.get()
            if item is None:
                break
            self._post(*item)

    def _post(self, text: str, parse_mode: str = "Markdown") -> bool:
        url     = f"{self._TG_API}/bot{self._token}/sendMessage"
        payload = {"chat_id": self._chat_id, "text": text,
                   "disable_web_page_preview": True}
        if parse_mode:
            payload["parse_mode"] = parse_mode
        for attempt in range(1, self._MAX_RETRIES + 1):
            try:
                resp = requests.post(url, json=payload, timeout=10)
                if resp.status_code == 200:
                    return True
                if resp.status_code == 429:
                    ra = resp.json().get("parameters", {}).get("retry_after", 5)
                    time.sleep(ra)
                    continue
            except Exception as e:
                logger.warning(f"[AlertManager] attempt {attempt} error: {e}")
            if attempt < self._MAX_RETRIES:
                time.sleep(self._RETRY_DELAY)
        logger.error("[AlertManager] failed to deliver after retries")
        return False


# ─────────────────────────────────────────────────────────────────────────────
# Telegram command poller
# ─────────────────────────────────────────────────────────────────────────────

class TelegramCommandPoller:
    """
    Long-polls the Telegram Bot API for incoming messages and dispatches
    registered command handlers.

    Commands are registered via register(command, handler) where `command`
    is a string like "/status" (case-insensitive) and `handler` is a callable
    that returns a str (the reply text).  The reply is sent back to the same
    chat_id that issued the command.

    Only messages from the configured chat_id are processed; others are silently
    dropped to prevent unauthorised control of the bot.

    Uses long-polling (timeout=30s) so the thread blocks mostly in the HTTP
    request rather than spinning.  A fresh offset is tracked after each batch
    so acknowledged messages are never re-delivered.
    """
    _TG_API      = "https://api.telegram.org"
    _POLL_TIMEOUT = 30       # Telegram long-poll window in seconds
    _HTTP_TIMEOUT = 40       # requests timeout > poll timeout to avoid spurious errors
    _RETRY_DELAY  = 5        # seconds to wait after a failed poll before retrying

    def __init__(self, token: str, allowed_chat_id: str) -> None:
        self._token          = token
        self._allowed_chat   = str(allowed_chat_id).strip()
        self._enabled        = bool(token and allowed_chat_id)
        self._handlers: Dict[str, Callable[[], str]] = {}
        self._offset: Optional[int] = None
        self._stop    = threading.Event()
        self._thread  = threading.Thread(target=self._poll_loop,
                                          name="TgCmdPoller", daemon=True)

    def register(self, command: str, handler: Callable[[], str]) -> None:
        """Register a handler for a bot command (e.g. '/status')."""
        self._handlers[command.lower().strip()] = handler

    def start(self) -> None:
        if not self._enabled:
            logger.warning("[TgPoller] Token/chat_id not configured — command polling disabled")
            return
        self._thread.start()
        logger.info("[TgPoller] Command polling started")

    def stop(self) -> None:
        self._stop.set()
        self._thread.join(timeout=self._HTTP_TIMEOUT + 5)

    # ── Internal ──────────────────────────────────────────────────────────────

    def _poll_loop(self) -> None:
        while not self._stop.is_set():
            try:
                updates = self._get_updates()
                if updates is None:
                    # Network error — back off before retrying
                    for _ in range(self._RETRY_DELAY):
                        if self._stop.is_set():
                            return
                        time.sleep(1)
                    continue
                for update in updates:
                    self._dispatch(update)
            except Exception as e:
                logger.error(f"[TgPoller] Unexpected error in poll loop: {e}", exc_info=True)
                time.sleep(self._RETRY_DELAY)

    def _get_updates(self) -> Optional[list]:
        params: dict = {"timeout": self._POLL_TIMEOUT, "allowed_updates": ["message"]}
        if self._offset is not None:
            params["offset"] = self._offset
        url = f"{self._TG_API}/bot{self._token}/getUpdates"
        try:
            resp = requests.get(url, params=params, timeout=self._HTTP_TIMEOUT)
            resp.raise_for_status()
            data = resp.json()
            if not data.get("ok"):
                logger.warning(f"[TgPoller] getUpdates not ok: {data}")
                return None
            updates = data.get("result", [])
            if updates:
                self._offset = updates[-1]["update_id"] + 1
            return updates
        except requests.RequestException as e:
            logger.warning(f"[TgPoller] getUpdates request error: {e}")
            return None

    def _dispatch(self, update: dict) -> None:
        msg = update.get("message", {})
        if not msg:
            return

        # Security: only accept messages from the configured chat
        chat_id = str(msg.get("chat", {}).get("id", ""))
        if chat_id != self._allowed_chat:
            logger.warning(f"[TgPoller] Ignoring message from unknown chat_id={chat_id}")
            return

        text = (msg.get("text") or "").strip()
        if not text.startswith("/"):
            return

        # Strip bot username suffix (e.g. /status@MyBot → /status)
        command = text.split()[0].split("@")[0].lower()
        handler = self._handlers.get(command)
        if handler is None:
            logger.debug(f"[TgPoller] No handler for command: {command}")
            return

        logger.info(f"[TgPoller] Dispatching command: {command}")
        try:
            reply = handler()
        except Exception as e:
            logger.error(f"[TgPoller] Handler error for {command}: {e}", exc_info=True)
            reply = f"⚠️ Error handling {command}: {e}"

        self._send_reply(chat_id, reply)

    def _send_reply(self, chat_id: str, text: str) -> None:
        url = f"{self._TG_API}/bot{self._token}/sendMessage"
        payload = {
            "chat_id":                  chat_id,
            "text":                     text,
            "parse_mode":               "Markdown",
            "disable_web_page_preview": True,
        }
        try:
            resp = requests.post(url, json=payload, timeout=10)
            if resp.status_code != 200:
                logger.warning(f"[TgPoller] sendMessage failed: {resp.text[:200]}")
        except requests.RequestException as e:
            logger.warning(f"[TgPoller] sendMessage error: {e}")


# ─────────────────────────────────────────────────────────────────────────────
# OMS — Order Management System
# Copied from trading_bot/oms.py; stripped to essentials needed by grid bot.
# Kept: paper fill (instant, no realistic simulation needed for limit grid orders),
#       live REST+WS fill, _sign, _params_to_str, FillEvent, OrderStatus.
# Removed: _paper_fill_realistic, RTT tracking, reconcile_positions, smoke_test.
# ─────────────────────────────────────────────────────────────────────────────

class OrderStatus(Enum):
    PENDING   = "PENDING"
    ACTIVE    = "ACTIVE"
    FILLED    = "FILLED"
    PARTIAL   = "PARTIAL"
    CANCELLED = "CANCELLED"
    REJECTED  = "REJECTED"


@dataclass
class OrderRequest:
    side:        str
    qty:         float
    instrument:  str
    order_type:  str
    price:       Optional[float]
    exec_inst:   List[str]
    purpose:     str
    client_oid:  str = field(default_factory=lambda: str(uuid.uuid4()))

    @classmethod
    def limit_maker(cls, side: str, qty: float, price: float,
                    instrument: str, purpose: str = "grid") -> "OrderRequest":
        """POST_ONLY limit — maker fee, rests on book."""
        return cls(side=side, qty=qty, instrument=instrument,
                   order_type="LIMIT", price=price,
                   exec_inst=["POST_ONLY"], purpose=purpose)

    @classmethod
    def market(cls, side: str, qty: float,
               instrument: str, purpose: str = "stop") -> "OrderRequest":
        """Market order — taker, immediate fill."""
        return cls(side=side, qty=qty, instrument=instrument,
                   order_type="MARKET", price=None,
                   exec_inst=[], purpose=purpose)


@dataclass
class FillEvent:
    client_oid:  str
    order_id:    str
    status:      OrderStatus
    filled_qty:  float = 0.0
    avg_price:   float = 0.0
    fee:         float = 0.0
    purpose:     str   = ""

    @property
    def is_filled(self) -> bool:
        return self.status == OrderStatus.FILLED

    @property
    def is_cancelled(self) -> bool:
        return self.status == OrderStatus.CANCELLED

    @property
    def is_rejected(self) -> bool:
        return self.status == OrderStatus.REJECTED


@dataclass
class _LiveOrder:
    req:          OrderRequest
    exchange_id:  str           = ""
    status:       OrderStatus   = OrderStatus.PENDING
    filled_qty:   float         = 0.0
    avg_price:    float         = 0.0
    fee:          float         = 0.0
    submit_time:  float         = field(default_factory=time.time)
    fill_event:   Optional[FillEvent] = None
    cancel_delivered: bool      = False


_OMS_REST_BASE   = GRID_CONFIG["rest_base_url"]
_OMS_WS_USER_URL = GRID_CONFIG["ws_user_url"]
_MAKER_FILL_TIMEOUT = 30.0    # grid orders rest longer than entry orders


class OMS:
    """
    Minimal OMS for grid bot.
    Paper mode: instant fill at req.price with correct fee.
    Live mode:  REST submit + WS fill notification.
    """

    def __init__(self, api_key: str, api_secret: str, instrument: str,
                 live_trading: bool = False, config: Optional[dict] = None):
        self.api_key      = api_key
        self.api_secret   = api_secret
        self.instrument   = instrument
        self.live_trading = live_trading
        self._cfg         = config or {}

        self._order_queue: queue.Queue = queue.Queue()
        self._fill_queues: Dict[str, queue.Queue] = {}
        self._fill_queues_lock = threading.Lock()
        self._orders: Dict[str, _LiveOrder] = {}
        self._orders_lock = threading.Lock()
        self._exid_to_coid: Dict[str, str] = {}

        self._stop_event  = threading.Event()
        self._ws_app      = None
        self._ws_thread   = None
        self._worker_thread = None
        self._ws_ready    = threading.Event()

        self._qty_decimals   = 4
        self._price_decimals = 2

    def start(self):
        self._load_instrument_spec()
        if self.live_trading:
            self._start_ws()
            if not self._ws_ready.wait(timeout=10.0):
                raise RuntimeError("[OMS] WS auth timed out")
            logger.info("[OMS] WS authenticated")
        self._worker_thread = threading.Thread(
            target=self._worker_loop, name="OMS-worker", daemon=True)
        self._worker_thread.start()
        logger.info(f"[OMS] Started (live={self.live_trading})")

    def stop(self):
        self._stop_event.set()
        if self.live_trading:
            self._cancel_all_dangling()
        if self._ws_app:
            try:
                self._ws_app.close()
            except Exception:
                pass
        if self._worker_thread:
            self._worker_thread.join(timeout=5)

    def submit(self, req: OrderRequest):
        with self._fill_queues_lock:
            self._fill_queues[req.client_oid] = queue.Queue(maxsize=1)
        self._order_queue.put(req)

    def wait_fill(self, client_oid: str, timeout: float = 3.0) -> Optional[FillEvent]:
        with self._fill_queues_lock:
            q = self._fill_queues.get(client_oid)
        if q is None:
            return None
        try:
            return q.get(timeout=timeout)
        except queue.Empty:
            return None
        finally:
            with self._fill_queues_lock:
                self._fill_queues.pop(client_oid, None)

    # ── Worker ────────────────────────────────────────────────────────────────

    def _worker_loop(self):
        while not self._stop_event.is_set():
            try:
                req = self._order_queue.get(timeout=0.5)
            except queue.Empty:
                continue
            try:
                self._process_order(req)
            except Exception as e:
                logger.error(f"[OMS] order error {req.client_oid[:8]}: {e}", exc_info=True)
                self._deliver_fill(FillEvent(
                    client_oid=req.client_oid, order_id="",
                    status=OrderStatus.REJECTED, purpose=req.purpose))

    def _process_order(self, req: OrderRequest):
        qty = self._round_qty(req.qty)
        if qty <= 0:
            self._deliver_fill(FillEvent(client_oid=req.client_oid, order_id="",
                                         status=OrderStatus.REJECTED, purpose=req.purpose))
            return
        req.qty = qty
        if not self.live_trading:
            self._paper_fill(req)
        else:
            self._live_fill(req)

    # ── Paper fill (instant at limit price) ───────────────────────────────────

    def _paper_fill(self, req: OrderRequest):
        """
        Instant fill at req.price for grid orders.
        Grid limit orders are resting POST_ONLY makers — we don't need the
        realistic queue-position simulation used for entry signals; the order
        just waits until price crosses its level, which the GridEngine tracks
        via live price ticks.  Fee: maker for limit, taker for market.
        Market orders (price=None) fill at the current live mid price.
        """
        if req.price is not None:
            fill_price = req.price
        else:
            fill_price = _price_cache.get_mid() or 0.0
        is_maker   = bool(req.exec_inst)
        maker_fee  = self._cfg.get("maker_fee_rate", 0.0001)
        taker_fee  = self._cfg.get("taker_fee_rate", 0.0003)
        fee_rate   = maker_fee if is_maker else taker_fee
        fee        = fill_price * req.qty * fee_rate

        logger.debug(
            f"[OMS][PAPER] FILL {req.purpose} {req.side} {req.qty:.4f} @ "
            f"{fill_price:.2f} fee={fee:+.6f} ({'maker' if is_maker else 'taker'}) "
            f"[{req.client_oid[:8]}]"
        )
        self._deliver_fill(FillEvent(
            client_oid=req.client_oid,
            order_id=f"paper-{req.client_oid[:8]}",
            status=OrderStatus.FILLED,
            filled_qty=req.qty,
            avg_price=fill_price,
            fee=fee,
            purpose=req.purpose,
        ))

    # ── Live fill (REST + WS) ─────────────────────────────────────────────────

    def _live_fill(self, req: OrderRequest):
        live_order = _LiveOrder(req=req, submit_time=time.time())
        with self._orders_lock:
            self._orders[req.client_oid] = live_order

        ok, exchange_id, err = self._rest_create_order(req)
        if not ok:
            logger.error(f"[OMS] REST rejected: {err} [{req.client_oid[:8]}]")
            with self._orders_lock:
                del self._orders[req.client_oid]
            self._deliver_fill(FillEvent(client_oid=req.client_oid, order_id="",
                                         status=OrderStatus.REJECTED, purpose=req.purpose))
            return

        live_order.exchange_id = exchange_id
        with self._orders_lock:
            self._exid_to_coid[exchange_id] = req.client_oid

        logger.info(
            f"[OMS] Submitted: {req.purpose} {req.side} {req.qty:.4f} @ "
            f"{req.price or 'MKT'} exid={exchange_id} [{req.client_oid[:8]}]"
        )

        if req.exec_inst:
            self._maker_timeout_handler(req.client_oid, exchange_id, req)

    def _maker_timeout_handler(self, client_oid: str, exchange_id: str, req: OrderRequest):
        deadline = time.time() + _MAKER_FILL_TIMEOUT
        while time.time() < deadline:
            time.sleep(0.5)
            with self._orders_lock:
                order = self._orders.get(client_oid)
                if order is None or order.fill_event is not None:
                    return
                if order.status in (OrderStatus.FILLED, OrderStatus.CANCELLED,
                                    OrderStatus.REJECTED):
                    return

        logger.info(f"[OMS] Maker timeout — cancelling exid={exchange_id} [{client_oid[:8]}]")
        self._rest_cancel_order(exchange_id)
        fill_to_deliver = None
        with self._orders_lock:
            order = self._orders.pop(client_oid, None)
            if order and order.exchange_id:
                self._exid_to_coid.pop(order.exchange_id, None)
            if order and not order.cancel_delivered:
                order.cancel_delivered = True
                fill_to_deliver = FillEvent(
                    client_oid=client_oid, order_id=exchange_id,
                    status=OrderStatus.CANCELLED, filled_qty=order.filled_qty,
                    avg_price=order.avg_price, fee=order.fee, purpose=req.purpose)
        if fill_to_deliver:
            self._deliver_fill(fill_to_deliver)

    # ── REST helpers ──────────────────────────────────────────────────────────

    def _rest_create_order(self, req: OrderRequest):
        params = {"instrument_name": req.instrument, "side": req.side,
                  "type": req.order_type, "quantity": str(req.qty),
                  "client_oid": req.client_oid}
        if req.price is not None:
            params["price"] = str(req.price)
        if req.exec_inst:
            params["exec_inst"] = req.exec_inst
        resp = self._signed_post("private/create-order", params)
        if resp is None:
            return False, "", "network error"
        if resp.get("code", -1) != 0:
            return False, "", f"code={resp.get('code')} msg={resp.get('message')}"
        order_id = str(resp.get("result", {}).get("order_id", ""))
        return True, order_id, ""

    def _rest_cancel_order(self, order_id: str) -> bool:
        resp = self._signed_post("private/cancel-order", {"order_id": order_id})
        if resp is None:
            return False
        return resp.get("code") in (0, 316)   # 316 = already gone

    def _signed_post(self, method: str, params: dict) -> Optional[dict]:
        req_id = int(time.time() * 1000) % 1_000_000
        nonce  = int(time.time() * 1000)
        body   = {"id": req_id, "method": method, "params": params,
                  "api_key": self.api_key, "nonce": nonce}
        body["sig"] = self._sign(method, req_id, params, nonce)
        url = f"{_OMS_REST_BASE}/{method}"
        try:
            resp = requests.post(url, json=body, timeout=5.0)
            resp.raise_for_status()
            return resp.json()
        except requests.RequestException as e:
            logger.error(f"[OMS] REST error {method}: {e}")
            return None

    def _sign(self, method: str, req_id: int, params: dict, nonce: int) -> str:
        param_str = self._params_to_str(params, level=0)
        payload   = f"{method}{req_id}{self.api_key}{param_str}{nonce}"
        return hmac.new(self.api_secret.encode("utf-8"),
                        payload.encode("utf-8"), hashlib.sha256).hexdigest()

    @staticmethod
    def _params_to_str(obj, level: int, max_level: int = 3) -> str:
        if level >= max_level:
            return str(obj)
        if isinstance(obj, dict):
            result = ""
            for k in sorted(obj.keys()):
                v = obj[k]
                if v is None:
                    result += k + "null"
                elif isinstance(v, (dict, list)):
                    result += k + OMS._params_to_str(v, level + 1, max_level)
                else:
                    result += k + str(v)
            return result
        if isinstance(obj, list):
            return "".join(OMS._params_to_str(i, level + 1, max_level) for i in obj)
        return str(obj)

    # ── WS (live mode) ────────────────────────────────────────────────────────

    def _start_ws(self):
        self._ws_thread = threading.Thread(
            target=self._ws_run_forever, name="OMS-ws", daemon=True)
        self._ws_thread.start()

    def _ws_run_forever(self):
        delay = 2.0
        while not self._stop_event.is_set():
            try:
                self._ws_app = websocket.WebSocketApp(
                    _OMS_WS_USER_URL,
                    on_open    = self._on_ws_open,
                    on_message = self._on_ws_message,
                    on_error   = lambda ws, e: logger.error(f"[OMS] WS error: {e}"),
                    on_close   = lambda ws, c, m: (logger.info(f"[OMS] WS closed {c}"),
                                                    self._ws_ready.clear()),
                )
                self._ws_app.run_forever(ping_interval=0)
            except Exception as e:
                logger.error(f"[OMS] WS exception: {e}")
            if self._stop_event.is_set():
                break
            logger.info(f"[OMS] WS reconnecting in {delay:.1f}s")
            self._ws_ready.clear()
            time.sleep(delay)
            delay = min(delay * 2, 60.0)

    def _on_ws_open(self, ws):
        time.sleep(1.0)
        nonce  = int(time.time() * 1000)
        req_id = 10001
        body   = {"id": req_id, "method": "public/auth",
                  "api_key": self.api_key, "nonce": nonce}
        body["sig"] = self._sign("public/auth", req_id, {}, nonce)
        ws.send(json.dumps(body))

    def _on_ws_message(self, ws, raw: str):
        try:
            msg = json.loads(raw)
        except json.JSONDecodeError:
            return
        method = msg.get("method", "")
        if method == "public/heartbeat":
            ws.send(json.dumps({"id": msg["id"], "method": "public/respond-heartbeat"}))
            return
        if method == "public/auth":
            if msg.get("code", -1) == 0:
                ws.send(json.dumps({
                    "id": 10002, "method": "subscribe",
                    "params": {"channels": [f"user.order.{self.instrument}"]},
                    "nonce": int(time.time() * 1000),
                }))
                self._ws_ready.set()
            else:
                logger.error(f"[OMS] WS auth FAILED code={msg.get('code')}")
            return
        if method == "subscribe":
            result  = msg.get("result", {})
            channel = result.get("channel", "")
            if channel == "user.order":
                for item in result.get("data", []):
                    self._handle_order_update(item)

    def _handle_order_update(self, data: dict):
        exchange_id = str(data.get("order_id", ""))
        ws_status   = data.get("status", "")
        filled_qty  = float(data.get("cumulative_quantity", 0))
        avg_price   = float(data.get("avg_price", 0))
        cum_fee     = float(data.get("cumulative_fee", 0))

        with self._orders_lock:
            client_oid = self._exid_to_coid.get(exchange_id)
            if client_oid is None:
                return
            order = self._orders.get(client_oid)
            if order is None:
                return

        order.filled_qty = filled_qty
        order.avg_price  = avg_price
        order.fee        = cum_fee

        if ws_status == "FILLED":
            order.status = OrderStatus.FILLED
            fill = FillEvent(client_oid=client_oid, order_id=exchange_id,
                             status=OrderStatus.FILLED, filled_qty=filled_qty,
                             avg_price=avg_price, fee=cum_fee, purpose=order.req.purpose)
            order.fill_event = fill
            with self._orders_lock:
                self._orders.pop(client_oid, None)
                self._exid_to_coid.pop(exchange_id, None)
            self._deliver_fill(fill)

        elif ws_status == "CANCELED":
            fill_to_deliver = None
            with self._orders_lock:
                live = self._orders.get(client_oid)
                if live and not live.cancel_delivered:
                    live.cancel_delivered = True
                    self._orders.pop(client_oid, None)
                    self._exid_to_coid.pop(exchange_id, None)
                    fill_to_deliver = FillEvent(
                        client_oid=client_oid, order_id=exchange_id,
                        status=OrderStatus.CANCELLED, filled_qty=filled_qty,
                        avg_price=avg_price, fee=cum_fee, purpose=live.req.purpose)
            if fill_to_deliver:
                self._deliver_fill(fill_to_deliver)

        elif ws_status == "REJECTED":
            with self._orders_lock:
                self._orders.pop(client_oid, None)
                self._exid_to_coid.pop(exchange_id, None)
            self._deliver_fill(FillEvent(client_oid=client_oid, order_id=exchange_id,
                                         status=OrderStatus.REJECTED, purpose=order.req.purpose))

        elif ws_status in ("NEW", "ACTIVE"):
            order.status = OrderStatus.ACTIVE

    # ── Helpers ───────────────────────────────────────────────────────────────

    def _deliver_fill(self, fill: FillEvent):
        with self._fill_queues_lock:
            q = self._fill_queues.get(fill.client_oid)
        if q is not None:
            try:
                q.put_nowait(fill)
            except queue.Full:
                pass
        logger.debug(
            f"[OMS] Fill delivered: {fill.purpose} {fill.status.value} "
            f"qty={fill.filled_qty:.4f} avg={fill.avg_price:.2f} "
            f"fee={fill.fee:+.6f} [{fill.client_oid[:8]}]"
        )

    def _round_qty(self, qty: float) -> float:
        factor = 10 ** self._qty_decimals
        return round(round(qty * factor) / factor, self._qty_decimals)

    def _load_instrument_spec(self):
        try:
            url  = f"{_OMS_REST_BASE}/public/get-instruments"
            resp = requests.get(url, params={"instrument_name": self.instrument}, timeout=5.0)
            resp.raise_for_status()
            data = resp.json()
            for inst in data.get("result", {}).get("data", []):
                if inst.get("symbol") == self.instrument:
                    qty_tick = float(inst.get("qty_tick_size", 0.0001))
                    tick_str = f"{qty_tick:.10f}".rstrip("0")
                    if "." in tick_str:
                        self._qty_decimals = len(tick_str.split(".")[1])
                    self._price_decimals = int(inst.get("quote_decimals", 2))
                    logger.info(
                        f"[OMS] Instrument spec: {self.instrument} "
                        f"qty_tick={qty_tick} qty_dec={self._qty_decimals} "
                        f"price_dec={self._price_decimals}"
                    )
                    return
        except Exception as e:
            logger.warning(f"[OMS] Could not load instrument spec: {e} — using defaults")

    def _cancel_all_dangling(self):
        with self._orders_lock:
            dangling = list(self._orders.items())
        for client_oid, order in dangling:
            if (order.exchange_id and
                    order.status in (OrderStatus.PENDING, OrderStatus.ACTIVE)):
                logger.info(f"[OMS] Cancelling dangling order {order.exchange_id}")
                self._rest_cancel_order(order.exchange_id)

    def reconcile_on_startup(self) -> float:
        """
        Called once at startup (after OMS.start()) to detect leftover state
        from a previous run (crash, hard-kill, or clean stop that did not liquidate).

        Live mode:
          1. Cancel all open orders for the instrument on the exchange.
             Prevents the new grid from conflicting with orphaned orders.
          2. Fetch the real position from private/get-positions.
             Returns the net long qty so the caller can decide to close it.

        Paper mode:
          Nothing to do — paper state is in-memory only, so a restart is always
          a clean slate.  Returns 0.0.
        """
        if not self.live_trading:
            return 0.0

        # Step 1: cancel all open orders for this instrument
        logger.info("[OMS] Startup reconcile: cancelling all open orders on exchange...")
        try:
            resp = self._signed_post("private/cancel-all-orders",
                                     {"instrument_name": self.instrument})
            if resp is not None and resp.get("code", -1) == 0:
                logger.info("[OMS] Startup reconcile: all open orders cancelled")
            else:
                code = resp.get("code") if resp else "N/A"
                logger.warning(f"[OMS] Startup reconcile: cancel-all-orders returned code={code}")
        except Exception as e:
            logger.error(f"[OMS] Startup reconcile: cancel-all-orders error: {e}")

        # Step 2: fetch current position
        long_qty = 0.0
        try:
            resp = self._signed_post("private/get-positions",
                                     {"instrument_name": self.instrument})
            if resp is not None and resp.get("code", -1) == 0:
                positions = resp.get("result", {}).get("data", [])
                for pos in positions:
                    if pos.get("instrument_name") == self.instrument:
                        qty  = float(pos.get("quantity", 0))
                        side = pos.get("side", "")   # "BUY" = long, "SELL" = short
                        if side == "BUY" and qty > 0:
                            long_qty = qty
                            logger.warning(
                                f"[OMS] Startup reconcile: found existing long "
                                f"{long_qty:.4f} {self.instrument} from previous run"
                            )
                        elif side == "SELL" and qty > 0:
                            logger.warning(
                                f"[OMS] Startup reconcile: found existing short "
                                f"{qty:.4f} {self.instrument} — unexpected for grid bot"
                            )
                        else:
                            logger.info("[OMS] Startup reconcile: no open position found")
            else:
                code = resp.get("code") if resp else "N/A"
                logger.warning(f"[OMS] Startup reconcile: get-positions returned code={code}")
        except Exception as e:
            logger.error(f"[OMS] Startup reconcile: get-positions error: {e}")

        return long_qty


# ─────────────────────────────────────────────────────────────────────────────
# WebSocket market feed  (copied from funding_arb/ws_manager.py _ReconnectingWS)
# ─────────────────────────────────────────────────────────────────────────────

class _ReconnectingWS:
    """
    Generation-tagged WS with DOA detection + stale-data watchdog.
    Copied from funding_arb/ws_manager.py.
    """
    _DOA_THRESHOLD_S  = 10
    _DOA_BACKOFF_STEP = 60
    _DOA_MAX_BACKOFF  = 300
    _DOA_LONG_STREAK  = 5
    _DOA_LONG_PAUSE   = 1800

    def __init__(self, name: str, url: str,
                 subscribe_msg_fn: Callable[[], List[dict]],
                 on_message_fn: Callable[[dict], None],
                 stale_s: float, backoff_init: float, backoff_max: float,
                 stop_event: threading.Event) -> None:
        self._name             = name
        self._url              = url
        self._subscribe_msg_fn = subscribe_msg_fn
        self._on_message_fn    = on_message_fn
        self._stale_s          = stale_s
        self._backoff_init     = backoff_init
        self._backoff_max      = backoff_max
        self._stop             = stop_event

        self._gen_lock  = threading.Lock()
        self._gen       = 0
        self._ws_app: Optional[websocket.WebSocketApp] = None

        self._last_msg_time = time.time()
        self._last_msg_lock = threading.Lock()

        self._consecutive_doa   = 0
        self._doa_lock          = threading.Lock()
        self._connect_time      = 0.0
        self._connect_time_lock = threading.Lock()
        self._first_msg_event   = threading.Event()
        self._reconnect_pending = threading.Event()
        self._abandon_event     = threading.Event()

    def start(self) -> None:
        threading.Thread(target=self._reconnect_loop,
                         name=f"WSLoop-{self._name}", daemon=True).start()
        threading.Thread(target=self._watchdog,
                         name=f"WSWatchdog-{self._name}", daemon=True).start()

    def stop(self) -> None:
        self._stop.set()
        self._abandon_event.set()
        with self._gen_lock:
            if self._ws_app:
                try:
                    self._ws_app.close()
                except Exception:
                    pass

    def _reconnect_loop(self) -> None:
        backoff = self._backoff_init
        while not self._stop.is_set():
            with self._connect_time_lock:
                self._connect_time = time.time()
            self._first_msg_event.clear()
            self._abandon_event.clear()
            self._reconnect_pending.clear()

            with self._gen_lock:
                self._gen += 1
                my_gen = self._gen
                app = websocket.WebSocketApp(
                    self._url,
                    on_open    = lambda ws:               self._on_open(ws, my_gen),
                    on_message = lambda ws, msg:          self._on_raw_message(ws, msg, my_gen),
                    on_error   = lambda ws, err:          self._on_error(ws, err, my_gen),
                    on_close   = lambda ws, code, reason: self._on_close(ws, code, reason, my_gen),
                )
                app._gen     = my_gen
                self._ws_app = app

            logger.info(f"[{self._name}] connecting (gen={my_gen})")

            def _worker(a=app, g=my_gen):
                try:
                    a.run_forever(ping_interval=0)
                except Exception as e:
                    if g == self._gen:
                        logger.error(f"[{self._name}] run_forever error (gen={g}): {e}")
                    else:
                        logger.debug(f"[{self._name}] run_forever error in orphaned worker (gen={g}): {e}")

            worker = threading.Thread(target=_worker,
                                       name=f"WSWorker-{self._name}-g{my_gen}", daemon=True)
            worker.start()

            doa = False
            while True:
                worker.join(timeout=1.0)
                if not worker.is_alive():
                    break
                if self._stop.is_set():
                    break
                with self._connect_time_lock:
                    ct = self._connect_time
                if (ct > 0 and not self._first_msg_event.is_set()
                        and (time.time() - ct) > self._DOA_THRESHOLD_S):
                    logger.warning(
                        f"[{self._name}] DOA gen={my_gen}: "
                        f"no messages {self._DOA_THRESHOLD_S}s after on_open")
                    doa = True
                    self._reconnect_pending.set()
                    try:
                        app.close()
                    except Exception:
                        pass
                    worker.join(timeout=5.0)
                    break
                if self._abandon_event.is_set():
                    logger.warning(f"[{self._name}] gen={my_gen} abandoned by watchdog")
                    self._reconnect_pending.set()
                    try:
                        app.close()
                    except Exception:
                        pass
                    worker.join(timeout=5.0)
                    break

            if self._stop.is_set():
                break

            if doa:
                with self._doa_lock:
                    self._consecutive_doa += 1
                    streak = self._consecutive_doa
                if streak >= self._DOA_LONG_STREAK:
                    sleep_s = self._DOA_LONG_PAUSE
                    logger.warning(f"[{self._name}] {streak} DOAs — long pause {sleep_s}s")
                else:
                    sleep_s = min(self._backoff_init + self._DOA_BACKOFF_STEP * streak,
                                  self._DOA_MAX_BACKOFF)
                    logger.warning(f"[{self._name}] DOA streak={streak} — backoff {sleep_s}s")
            else:
                with self._doa_lock:
                    self._consecutive_doa = 0
                sleep_s = backoff
                backoff  = min(backoff * 2, self._backoff_max)
                logger.info(f"[{self._name}] disconnected — reconnecting in {sleep_s}s")

            for _ in range(int(sleep_s)):
                if self._stop.is_set():
                    break
                time.sleep(1)

    def _is_current(self, ws) -> bool:
        return getattr(ws, "_gen", None) == self._gen

    def _on_open(self, ws, gen: int) -> None:
        if not self._is_current(ws):
            return
        logger.info(f"[{self._name}] connected (gen={gen})")
        with self._connect_time_lock:
            self._connect_time = time.time()
        with self._last_msg_lock:
            self._last_msg_time = time.time()
        time.sleep(1.0)
        for msg in self._subscribe_msg_fn():
            ws.send(json.dumps(msg))

    def _on_raw_message(self, ws, raw: str, gen: int) -> None:
        if not self._is_current(ws):
            return
        with self._last_msg_lock:
            self._last_msg_time = time.time()
        if not self._first_msg_event.is_set():
            self._first_msg_event.set()
            with self._doa_lock:
                self._consecutive_doa = 0
        try:
            data = json.loads(raw)
        except json.JSONDecodeError:
            return
        method = data.get("method", "")
        if method == "public/heartbeat":
            ws.send(json.dumps({"id": data.get("id"),
                                 "method": "public/respond-heartbeat"}))
            return
        if method == "subscribe":
            code = data.get("code", -1)
            if code != 0:
                logger.error(f"[{self._name}] subscription FAILED code={code} "
                              f"msg={data.get('message','')} (gen={gen})")
                return
            result_block = data.get("result", {})
            if not result_block.get("data"):
                sub = (result_block.get("subscription", "")
                       or result_block.get("channel", "") or repr(result_block))
                logger.debug(f"[{self._name}] subscribed: {sub} (gen={gen})")
                return
        try:
            self._on_message_fn(data)
        except Exception as e:
            logger.error(f"[{self._name}] on_message_fn error: {e}", exc_info=True)

    def _on_error(self, ws, error, gen: int) -> None:
        if self._is_current(ws):
            logger.warning(f"[{self._name}] WS error (gen={gen}): {error}")

    def _on_close(self, ws, code, reason, gen: int) -> None:
        if self._is_current(ws):
            logger.info(f"[{self._name}] disconnected (gen={gen}) code={code}")

    def _watchdog(self) -> None:
        while not self._stop.is_set():
            time.sleep(5)
            if self._reconnect_pending.is_set():
                continue
            with self._last_msg_lock:
                age = time.time() - self._last_msg_time
            if age > self._stale_s:
                logger.warning(
                    f"[{self._name}] stale data ({age:.0f}s > {self._stale_s}s)"
                    f" — signalling reconnect")
                self._reconnect_pending.set()
                self._abandon_event.set()
                with self._last_msg_lock:
                    self._last_msg_time = time.time()


# ─────────────────────────────────────────────────────────────────────────────
# Price cache
# ─────────────────────────────────────────────────────────────────────────────

class PriceCache:
    """Thread-safe L1 cache + rolling tick history for ATR computation."""
    HISTORY_WINDOW_S = 86400   # keep 24h

    def __init__(self):
        self._lock    = threading.Lock()
        self._bid: Optional[float] = None
        self._ask: Optional[float] = None
        self._mid: Optional[float] = None
        self._history: collections.deque = collections.deque(maxlen=30000)

    def update_l1(self, bid: float, ask: float):
        with self._lock:
            self._bid = bid
            self._ask = ask
            self._mid = (bid + ask) / 2.0
            now = time.time()
            self._history.append((now, self._mid))
            cutoff = now - self.HISTORY_WINDOW_S
            while self._history and self._history[0][0] < cutoff:
                self._history.popleft()

    def get_mid(self) -> Optional[float]:
        with self._lock:
            return self._mid

    def get_l1(self) -> Tuple[Optional[float], Optional[float], Optional[float]]:
        with self._lock:
            return self._bid, self._ask, self._mid

    # Minimum number of 1-min candles required before compute_atr() is trusted.
    # With only 1-2 candles (e.g. after a 60s warmup) the ATR is ~$20 instead
    # of the ~$300 daily ATR — producing a dangerously tight grid that is almost
    # immediately stopped out.  30 candles = 30 minutes of data, which is a
    # reasonable minimum for a meaningful intra-day ATR.
    MIN_ATR_CANDLES = 30

    def compute_atr(self, lookback_minutes: int = 1440) -> Optional[float]:
        """
        ATR from rolling 1-min candles built from tick history.
        Returns per-minute ATR in price points.
        Returns None if fewer than MIN_ATR_CANDLES candles are available so
        callers fall back to config defaults rather than using a misleadingly
        small ATR derived from only a few ticks.
        """
        with self._lock:
            history = list(self._history)

        if len(history) < 10:
            return None

        cutoff = time.time() - lookback_minutes * 60
        recent = [(ts, mid) for ts, mid in history if ts >= cutoff]
        if len(recent) < 10:
            recent = history[-100:]

        candles: Dict[int, dict] = {}
        for ts, mid in recent:
            k = int(ts // 60)
            if k not in candles:
                candles[k] = {"open": mid, "high": mid, "low": mid, "close": mid}
            else:
                c = candles[k]
                c["high"]  = max(c["high"], mid)
                c["low"]   = min(c["low"],  mid)
                c["close"] = mid

        sorted_c = [candles[k] for k in sorted(candles.keys())]
        if len(sorted_c) < self.MIN_ATR_CANDLES:
            return None

        trs = []
        for i in range(1, len(sorted_c)):
            prev = sorted_c[i - 1]["close"]
            curr = sorted_c[i]
            trs.append(max(curr["high"] - curr["low"],
                           abs(curr["high"] - prev),
                           abs(curr["low"]  - prev)))
        return sum(trs) / len(trs) if trs else None

    def warmup_complete(self, min_seconds: int) -> bool:
        with self._lock:
            if not self._history:
                return False
            return (time.time() - self._history[0][0]) >= min_seconds

    def atr_candle_count(self, lookback_minutes: int = 1440) -> int:
        """Return the number of complete 1-min candle buckets currently in the cache."""
        with self._lock:
            history = list(self._history)
        if not history:
            return 0
        cutoff = time.time() - lookback_minutes * 60
        recent = [(ts, mid) for ts, mid in history if ts >= cutoff]
        if len(recent) < 2:
            recent = history
        buckets = set(int(ts // 60) for ts, _ in recent)
        return len(buckets)

    def compute_stability(self, window_minutes: int) -> dict:
        """
        Compute price stability metrics over the last window_minutes.

        Returns a dict with:
          "hi"        — highest mid price in window
          "lo"        — lowest mid price in window
          "hi_lo"     — hi - lo (range)
          "mean"      — arithmetic mean of mid prices in window
          "current"   — most recent mid price
          "n_ticks"   — number of ticks in window (quality indicator)
          "ok"        — False if insufficient data (< 10 ticks)
        """
        with self._lock:
            history = list(self._history)

        cutoff = time.time() - window_minutes * 60
        window = [mid for ts, mid in history if ts >= cutoff]

        if len(window) < 10:
            return {"ok": False, "hi": 0.0, "lo": 0.0, "hi_lo": 0.0,
                    "mean": 0.0, "current": 0.0, "n_ticks": len(window)}

        hi      = max(window)
        lo      = min(window)
        mean    = sum(window) / len(window)
        current = window[-1]
        return {
            "ok":      True,
            "hi":      hi,
            "lo":      lo,
            "hi_lo":   hi - lo,
            "mean":    mean,
            "current": current,
            "n_ticks": len(window),
        }


# ─────────────────────────────────────────────────────────────────────────────
# Grid geometry
# ─────────────────────────────────────────────────────────────────────────────

@dataclass
class GridParams:
    lower:      float
    upper:      float
    levels:     int
    spacing:    float
    stop_price: float
    notional_per_level: float
    computed_at: float = field(default_factory=time.time)

    @property
    def level_prices(self) -> List[float]:
        return [round(self.lower + i * self.spacing, 2) for i in range(self.levels + 1)]


# ─────────────────────────────────────────────────────────────────────────────
# Auto-tuner
# ─────────────────────────────────────────────────────────────────────────────

class GridAutoTuner:
    def __init__(self, config: dict, cache: PriceCache):
        self._cfg   = config
        self._cache = cache

    def compute(self) -> Optional[GridParams]:
        mid = self._cache.get_mid()
        if mid is None:
            logger.warning("[AutoTuner] No mid price")
            return None

        atr = self._cache.compute_atr(self._cfg.get("atr_lookback_minutes", 1440))
        if atr is None or atr <= 0:
            logger.warning("[AutoTuner] ATR unavailable — using config fallback")
            return self._from_config(mid)

        atr_mult   = self._cfg.get("atr_multiplier", 3.0)
        stop_buf   = self._cfg.get("stop_buffer_atr", 1.0)
        maker_fee  = self._cfg.get("maker_fee_rate", 0.0001)
        min_sp_pct = self._cfg.get("min_grid_pct", 0.0008)
        max_levels = self._cfg.get("max_grid_levels", 50)
        min_levels = self._cfg.get("min_grid_levels", 5)
        notional   = self._cfg.get("notional_per_level", 500.0)

        lower = round(mid - atr_mult * atr, 2)
        upper = round(mid + atr_mult * atr, 2)
        stop  = round(lower - stop_buf * atr, 2)

        min_spacing = max(min_sp_pct * mid, 2.0 * maker_fee * mid * 1.5)
        raw_levels  = int((upper - lower) / min_spacing)
        levels      = max(min_levels, min(max_levels, raw_levels))
        spacing     = round((upper - lower) / levels, 2)

        logger.info(
            f"[AutoTuner] mid={mid:.2f} ATR={atr:.2f} "
            f"range=[{lower:.2f},{upper:.2f}] levels={levels} "
            f"spacing={spacing:.2f} stop={stop:.2f}"
        )
        return GridParams(lower=lower, upper=upper, levels=levels,
                          spacing=spacing, stop_price=stop,
                          notional_per_level=notional)

    def _from_config(self, mid: float) -> GridParams:
        lower   = self._cfg.get("grid_lower",   mid * 0.92)
        upper   = self._cfg.get("grid_upper",   mid * 1.08)
        levels  = self._cfg.get("grid_levels",  20)
        stop    = self._cfg.get("stop_loss_price", lower * 0.97)
        notional = self._cfg.get("notional_per_level", 500.0)
        spacing = round((upper - lower) / max(levels, 1), 2)
        logger.warning(
            f"[AutoTuner] Using config fallback (ATR unavailable): "
            f"range=[{lower:.2f},{upper:.2f}] levels={levels} "
            f"spacing={spacing:.2f} stop={stop:.2f} mid={mid:.2f}"
        )
        return GridParams(lower=lower, upper=upper, levels=levels,
                          spacing=spacing, stop_price=stop,
                          notional_per_level=notional)

    def should_retune(self, current: GridParams, mid: float, last_tune: float) -> bool:
        if mid < current.lower or mid > current.upper:
            logger.info(f"[AutoTuner] Price {mid:.2f} outside range → retune")
            return True
        interval_s = self._cfg.get("retune_interval_hours", 24) * 3600
        if time.time() - last_tune > interval_s:
            logger.info("[AutoTuner] Periodic retune interval elapsed")
            return True
        return False


# ─────────────────────────────────────────────────────────────────────────────
# Grid level state
# ─────────────────────────────────────────────────────────────────────────────

class LevelState(Enum):
    IDLE      = "IDLE"
    BUY_OPEN  = "BUY_OPEN"
    SELL_OPEN = "SELL_OPEN"


@dataclass
class GridLevel:
    index:      int
    price:      float
    state:      LevelState = LevelState.IDLE
    client_oid: str        = ""
    qty:        float      = 0.0


# ─────────────────────────────────────────────────────────────────────────────
# Grid engine
# ─────────────────────────────────────────────────────────────────────────────

class GridEngine:
    """
    Manages limit order ladder on BTCUSD-PERP.

    Paper-mode fill detection:
      OMS._paper_fill() returns FillEvent instantly when submit() is called.
      But the engine does NOT actually poll wait_fill() — instead, in paper mode
      the GridBot main loop calls check_price_fills(mid) on every tick:
      if mid crossed a level's limit price, the engine simulates the fill
      directly (avoids background OMS threading complexity for grid orders).

    Live-mode fill detection:
      OMS WS delivers FILLED events; engine polls wait_fill(timeout=0) each tick.
    """

    def __init__(self, params: GridParams, oms: OMS,
                 instrument: str, config: dict,
                 store: Optional["GridStateStore"] = None):
        self._params     = params
        self._oms        = oms
        self._instrument = instrument
        self._cfg        = config
        self._store      = store          # may be None in tests / paper mode without DB
        self._lock       = threading.Lock()
        self._levels: List[GridLevel] = []
        self._stop_event = threading.Event()

        # Accounting — seeded from DB so a restart or re-tune doesn't zero out history.
        # In-memory values are the authoritative running total for this process;
        # the DB is appended to on every fill, and all-time sums are queried from it.
        if store is not None:
            acc = store.get_accumulated()
            self._realized_pnl: float = acc["gross_pnl"]
            self._total_fees:   float = -acc["fees"]        # fees stored negative in DB → flip sign
            self._cycle_count:  int   = acc["cycle_count"]
            logger.info(
                f"[GridEngine] Seeded from DB: gross_pnl={self._realized_pnl:+.4f} "
                f"fees={self._total_fees:.6f} cycles={self._cycle_count}"
            )
        else:
            self._realized_pnl = 0.0
            self._total_fees   = 0.0
            self._cycle_count  = 0

        # long_qty is NOT seeded from DB — it reflects live open orders only.
        # On a fresh start the grid is rebuilt from scratch (all orders re-placed),
        # so the accumulated long starts at 0 and grows as BUY fills come in.
        self._long_qty: float = 0.0

        # Fill queue for _fill_thread
        self._fill_queue: collections.deque = collections.deque()
        self._fill_event  = threading.Event()
        self._fill_thread: Optional[threading.Thread] = None

        self._build_levels()

    def _build_levels(self):
        prices = self._params.level_prices
        with self._lock:
            self._levels = [GridLevel(index=i, price=p) for i, p in enumerate(prices)]
        logger.info(
            f"[GridEngine] {len(self._levels)} levels: "
            f"{self._levels[0].price:.2f} … {self._levels[-1].price:.2f}"
        )

    def start(self, mid: float):
        self._fill_thread = threading.Thread(
            target=self._fill_loop, name="Grid-fills", daemon=True)
        self._fill_thread.start()
        self._place_initial_orders(mid)
        logger.info("[GridEngine] Started")

    def stop(self):
        self._stop_event.set()
        self._fill_event.set()
        if self._fill_thread:
            self._fill_thread.join(timeout=5)
        with self._lock:
            for lv in self._levels:
                lv.state      = LevelState.IDLE
                lv.client_oid = ""

    # ── Initial placement ─────────────────────────────────────────────────────

    def _place_initial_orders(self, mid: float):
        with self._lock:
            levels = list(self._levels)
        for lv in levels:
            if self._stop_event.is_set():
                break
            if lv.price < mid:
                self._place_buy(lv)
            elif lv.price > mid:
                self._place_sell(lv)
            time.sleep(0.05)

    # ── Order placement ───────────────────────────────────────────────────────

    def _qty(self, price: float) -> float:
        raw = self._params.notional_per_level / price
        return round(math.floor(raw * 10000) / 10000, 4)

    def _place_buy(self, lv: GridLevel):
        qty = self._qty(lv.price)
        if qty <= 0:
            return
        req = OrderRequest.limit_maker(
            side="BUY", qty=qty, price=lv.price,
            instrument=self._instrument, purpose="grid_buy")
        with self._lock:
            lv.state      = LevelState.BUY_OPEN
            lv.client_oid = req.client_oid
            lv.qty        = qty
        self._oms.submit(req)
        logger.debug(f"[GridEngine] BUY  [{lv.index}] @ {lv.price:.2f} qty={qty:.4f}")

    def _place_sell(self, lv: GridLevel):
        qty = self._qty(lv.price)
        if qty <= 0:
            return
        req = OrderRequest.limit_maker(
            side="SELL", qty=qty, price=lv.price,
            instrument=self._instrument, purpose="grid_sell")
        with self._lock:
            lv.state      = LevelState.SELL_OPEN
            lv.client_oid = req.client_oid
            lv.qty        = qty
        self._oms.submit(req)
        logger.debug(f"[GridEngine] SELL [{lv.index}] @ {lv.price:.2f} qty={qty:.4f}")

    # ── Fill detection ────────────────────────────────────────────────────────

    def check_price_fills(self, mid: float):
        """
        Called every tick by GridBot.
        Paper mode: detects fill by price crossing; simulates accounting directly.
        Live mode: polls OMS wait_fill(timeout=0) for each open order.
        Also checks trailing up/down conditions.
        """
        if self._oms.live_trading:
            self._poll_live_fills()
        else:
            self._simulate_paper_fills(mid)

        # Trailing checks run after fills so counter-orders are placed first
        self._check_trailing(mid)

    def _simulate_paper_fills(self, mid: float):
        """
        Paper fill simulation:
          BUY  fills when mid drops to/below lv.price (seller crosses our bid)
          SELL fills when mid rises to/above lv.price (buyer crosses our ask)
        This matches real exchange matching: our resting limit is hit by a
        market order on the opposite side.
        """
        with self._lock:
            levels = list(self._levels)

        for lv in levels:
            filled = False
            if lv.state == LevelState.BUY_OPEN  and mid <= lv.price:
                filled = True
            elif lv.state == LevelState.SELL_OPEN and mid >= lv.price:
                filled = True

            if filled:
                maker_fee = self._cfg.get("maker_fee_rate", 0.0001)
                fee = lv.price * lv.qty * maker_fee
                fill = FillEvent(
                    client_oid=lv.client_oid,
                    order_id=f"paper-{lv.client_oid[:8]}",
                    status=OrderStatus.FILLED,
                    filled_qty=lv.qty,
                    avg_price=lv.price,
                    fee=fee,
                    purpose=lv.state.value.lower(),   # "buy_open" → "grid_buy" below
                )
                # Rewrite purpose to match convention
                fill.purpose = ("grid_buy" if lv.state == LevelState.BUY_OPEN
                                else "grid_sell")
                with self._lock:
                    lv.state      = LevelState.IDLE
                    lv.client_oid = ""
                self._fill_queue.append((lv.index, fill))
                self._fill_event.set()

    def _poll_live_fills(self):
        """Live mode: check each open order for OMS fill delivery."""
        with self._lock:
            levels = list(self._levels)

        for lv in levels:
            if lv.state == LevelState.IDLE or not lv.client_oid:
                continue
            fill = self._oms.wait_fill(lv.client_oid, timeout=0.0)
            if fill is None:
                continue
            with self._lock:
                if fill.is_filled:
                    lv.state      = LevelState.IDLE
                    lv.client_oid = ""
                    self._fill_queue.append((lv.index, fill))
                    self._fill_event.set()
                elif fill.is_cancelled:
                    # Timeout cancel — re-place same side
                    lv.state      = LevelState.IDLE
                    lv.client_oid = ""
                    # Will be re-placed by _replace_idle_levels() next tick

        self._replace_idle_levels()

    def _replace_idle_levels(self):
        """Re-place any IDLE levels that should have an order."""
        mid = _price_cache.get_mid()
        if mid is None:
            return
        with self._lock:
            idle = [lv for lv in self._levels if lv.state == LevelState.IDLE]
        for lv in idle:
            if lv.price < mid:
                self._place_buy(lv)
            elif lv.price > mid:
                self._place_sell(lv)

    # ── Trailing ──────────────────────────────────────────────────────────────

    def _check_trailing(self, mid: float):
        """
        Evaluate trailing up/down conditions and shift grid one level if triggered.

        Trailing Up:
          Trigger: mid >= upper + spacing  (price has cleared one full level above grid)
          Action:  cancel lowest BUY level → drop it from grid → append new SELL
                   level one spacing above current upper.
          Cap:     do not trail if new upper would exceed trailing_up_price_cap.

        Trailing Down:
          Trigger: mid <= lower - spacing  (price has dropped one full level below grid)
          Action:  cancel highest SELL level → drop it from grid → prepend new BUY
                   level one spacing below current lower.
          Cap:     do not trail if new lower would go below trailing_down_price_cap
                   (or stop_loss_price, whichever is higher).
        """
        with self._lock:
            if len(self._levels) < 2:
                return
            current_lower   = self._levels[0].price
            current_upper   = self._levels[-1].price
            spacing         = self._params.spacing

        trail_up   = self._cfg.get("trailing_up_enabled",   False)
        trail_down = self._cfg.get("trailing_down_enabled", False)

        if trail_up and mid >= current_upper + spacing:
            cap = self._cfg.get("trailing_up_price_cap", 0.0)
            new_upper = round(current_upper + spacing, 2)
            if cap and new_upper > cap:
                logger.info(
                    f"[GridEngine] Trail-up blocked: new_upper={new_upper:.2f} "
                    f"> cap={cap:.2f}"
                )
            else:
                self._trail_up(current_lower, current_upper, spacing)

        if trail_down and mid <= current_lower - spacing:
            floor = self._cfg.get("trailing_down_price_cap", 0.0)
            stop  = self._params.stop_price
            effective_floor = max(floor, stop) if floor else stop
            new_lower = round(current_lower - spacing, 2)
            if effective_floor and new_lower < effective_floor:
                logger.info(
                    f"[GridEngine] Trail-down blocked: new_lower={new_lower:.2f} "
                    f"< floor={effective_floor:.2f}"
                )
            else:
                self._trail_down(current_lower, current_upper, spacing)

    def _trail_up(self, old_lower: float, old_upper: float, spacing: float):
        """
        Shift grid up by one level:
          1. Cancel the lowest BUY level (old_lower).
          2. Remove it from the levels list.
          3. Append a new SELL level at old_upper + spacing.
        """
        new_upper = round(old_upper + spacing, 2)
        logger.info(
            f"[GridEngine] TRAIL UP: dropping lower={old_lower:.2f}, "
            f"adding upper={new_upper:.2f}"
        )

        with self._lock:
            # Step 1: remove bottom level and cancel its order
            if not self._levels:
                return
            bottom = self._levels[0]
            if bottom.state != LevelState.IDLE and bottom.client_oid:
                # Mark idle so poll_fills won't try to re-place it
                bottom.state      = LevelState.IDLE
                bottom.client_oid = ""
            self._levels.pop(0)
            # Re-index remaining levels
            for i, lv in enumerate(self._levels):
                lv.index = i

            # Step 2: append new SELL level at the top
            new_idx = len(self._levels)
            new_lv  = GridLevel(index=new_idx, price=new_upper)
            self._levels.append(new_lv)
            self._params = GridParams(
                lower=self._levels[0].price,
                upper=new_upper,
                levels=len(self._levels) - 1,
                spacing=spacing,
                stop_price=self._params.stop_price,
                notional_per_level=self._params.notional_per_level,
            )

        # Place the new SELL outside the lock
        self._place_sell(new_lv)
        self._alerter_send(
            f"⬆️ Grid trailed UP → [{self._params.lower:.0f}, {new_upper:.0f}]"
        )

    def _trail_down(self, old_lower: float, old_upper: float, spacing: float):
        """
        Shift grid down by one level:
          1. Cancel the highest SELL level (old_upper).
          2. Remove it from the levels list.
          3. Prepend a new BUY level at old_lower - spacing.
        """
        new_lower = round(old_lower - spacing, 2)
        logger.info(
            f"[GridEngine] TRAIL DOWN: dropping upper={old_upper:.2f}, "
            f"adding lower={new_lower:.2f}"
        )

        with self._lock:
            if not self._levels:
                return
            top = self._levels[-1]
            if top.state != LevelState.IDLE and top.client_oid:
                top.state      = LevelState.IDLE
                top.client_oid = ""
            self._levels.pop()

            # Prepend new BUY level at the bottom
            new_lv = GridLevel(index=0, price=new_lower)
            self._levels.insert(0, new_lv)
            # Re-index
            for i, lv in enumerate(self._levels):
                lv.index = i
            self._params = GridParams(
                lower=new_lower,
                upper=self._levels[-1].price,
                levels=len(self._levels) - 1,
                spacing=spacing,
                stop_price=self._params.stop_price,
                notional_per_level=self._params.notional_per_level,
            )

        # Place the new BUY outside the lock
        self._place_buy(new_lv)
        self._alerter_send(
            f"⬇️ Grid trailed DOWN → [{new_lower:.0f}, {self._params.upper:.0f}]"
        )

    def _alerter_send(self, msg: str):
        """Best-effort alert — engine holds no reference to alerter; uses module global."""
        try:
            _grid_bot_alerter.send(msg)
        except Exception:
            pass

    # ── Fill processing thread ────────────────────────────────────────────────

    def _fill_loop(self):
        while not self._stop_event.is_set():
            self._fill_event.wait(timeout=1.0)
            self._fill_event.clear()
            while self._fill_queue:
                try:
                    idx, fill = self._fill_queue.popleft()
                except IndexError:
                    break
                self._on_fill(idx, fill)

    def _on_fill(self, idx: int, fill: FillEvent):
        # Snapshot the level reference and intent under lock, then release
        # before calling _place_buy/_place_sell (which acquire lock themselves).
        with self._lock:
            if idx < 0 or idx >= len(self._levels):
                return
            is_buy = fill.purpose == "grid_buy"

        self._total_fees += fill.fee
        now = time.time()

        if is_buy:
            self._long_qty += fill.filled_qty
            logger.info(
                f"[GridEngine] FILL BUY  [{idx}] @ {fill.avg_price:.2f} "
                f"qty={fill.filled_qty:.4f} fee={fill.fee:.6f} "
                f"long={self._long_qty:.4f} BTC"
            )
            # Persist to DB (gross_pnl=0 for BUY fills — profit only realised on SELL)
            if self._store is not None:
                try:
                    self._store.record_fill(
                        ts_utc=now, side="BUY", level_idx=idx,
                        price_usd=fill.avg_price, qty_btc=fill.filled_qty,
                        fee_usd=fill.fee, gross_pnl=0.0, cycle_num=self._cycle_count,
                    )
                except Exception as e:
                    logger.error(f"[GridEngine] DB record_fill BUY error: {e}", exc_info=True)

            # Snapshot counter-level under lock, then place outside lock
            sell_lv = None
            sell_idx = idx + 1
            with self._lock:
                if sell_idx < len(self._levels):
                    candidate = self._levels[sell_idx]
                    if candidate.state == LevelState.IDLE:
                        sell_lv = candidate
            if sell_lv is not None:
                self._place_sell(sell_lv)
        else:
            self._long_qty -= fill.filled_qty
            buy_price = self._get_paired_buy_price(idx)
            gross_pnl = (fill.avg_price - buy_price) * fill.filled_qty if buy_price else 0.0
            self._realized_pnl += gross_pnl
            self._cycle_count  += 1
            net_pnl = gross_pnl - fill.fee
            logger.info(
                f"[GridEngine] FILL SELL [{idx}] @ {fill.avg_price:.2f} "
                f"qty={fill.filled_qty:.4f} fee={fill.fee:.6f} | "
                f"cycle #{self._cycle_count} gross={gross_pnl:+.4f} net={net_pnl:+.4f} "
                f"cumulative_net={self._realized_pnl - self._total_fees:+.4f} USD"
            )
            # Persist to DB
            if self._store is not None:
                try:
                    self._store.record_fill(
                        ts_utc=now, side="SELL", level_idx=idx,
                        price_usd=fill.avg_price, qty_btc=fill.filled_qty,
                        fee_usd=fill.fee, gross_pnl=gross_pnl, cycle_num=self._cycle_count,
                    )
                except Exception as e:
                    logger.error(f"[GridEngine] DB record_fill SELL error: {e}", exc_info=True)

            # Snapshot counter-level under lock, then place outside lock
            buy_lv = None
            buy_idx = idx - 1
            with self._lock:
                if buy_idx >= 0:
                    candidate = self._levels[buy_idx]
                    if candidate.state == LevelState.IDLE:
                        buy_lv = candidate
            if buy_lv is not None:
                self._place_buy(buy_lv)

    def _get_paired_buy_price(self, sell_idx: int) -> Optional[float]:
        with self._lock:
            buy_idx = sell_idx - 1
            if 0 <= buy_idx < len(self._levels):
                return self._levels[buy_idx].price
        return None

    # ── Stats ─────────────────────────────────────────────────────────────────

    def get_stats(self) -> dict:
        with self._lock:
            open_buys  = sum(1 for lv in self._levels if lv.state == LevelState.BUY_OPEN)
            open_sells = sum(1 for lv in self._levels if lv.state == LevelState.SELL_OPEN)
        return {
            "levels":       len(self._levels),
            "open_buys":    open_buys,
            "open_sells":   open_sells,
            "long_qty":     round(self._long_qty, 4),
            "realized_pnl": round(self._realized_pnl, 4),
            "total_fees":   round(self._total_fees, 6),
            "net_pnl":      round(self._realized_pnl - self._total_fees, 4),
            "cycles":       self._cycle_count,
        }


# ─────────────────────────────────────────────────────────────────────────────
# Stop-loss guard
# ─────────────────────────────────────────────────────────────────────────────

class StopLossGuard:
    def __init__(self, stop_price: float, config: dict):
        self._stop_price = stop_price
        self._enabled    = config.get("stop_loss_enabled", True)
        self._triggered  = False

    def update_price(self, price: float):
        self._stop_price = price

    def check(self, mid: float) -> bool:
        if self._triggered or not self._enabled:
            return self._triggered
        if self._stop_price > 0 and mid < self._stop_price:
            logger.warning(
                f"[StopLoss] TRIGGERED: mid={mid:.2f} < stop={self._stop_price:.2f}")
            self._triggered = True
        return self._triggered

    @property
    def triggered(self) -> bool:
        return self._triggered


# ─────────────────────────────────────────────────────────────────────────────
# Module-level price cache (shared across components)
# ─────────────────────────────────────────────────────────────────────────────

_price_cache = PriceCache()

# Module-level alerter reference set by GridBot.__init__ so GridEngine can
# send trailing alerts without holding a back-reference to GridBot.
class _NullAlerter:
    def send(self, msg: str): pass
_grid_bot_alerter: AlertManager = _NullAlerter()  # type: ignore


# ─────────────────────────────────────────────────────────────────────────────
# TrendSignal  — read-only mid/long-term trend observer (Phase 1)
# ─────────────────────────────────────────────────────────────────────────────
#
# Computes a dual-EMA trend signal from the 1-minute candle data already held
# in PriceCache._history.  No external REST calls; no side-effects on the grid.
#
# Algorithm  (dual-EMA confirmation with daily band filter)
# ─────────────────────────────────────────────────────────
#   Fast signal  — EMA(fast_h) vs EMA(slow_h) on 1-hour close prices
#     fast_h and slow_h are expressed in hours (default 4h / 24h).
#     Uses the 1-min candle "close" prices already bucketed by PriceCache,
#     then sub-samples every 60 buckets to get 1-hour candles.
#
#     UP   if  EMA_fast > EMA_slow  AND  EMA_fast slope is positive
#     DOWN if  EMA_fast < EMA_slow  AND  EMA_fast slope is negative
#     NEUTRAL otherwise (cross-zone or flat slope)
#
#   Trend strength  (auxiliary, logged only)
#     separation = (EMA_fast - EMA_slow) / EMA_slow × 100  (%)
#     slope_pct  = (EMA_fast_now - EMA_fast_N_hours_ago) / EMA_slow × 100 (%)
#
#   Hysteresis  (prevents flutter at the crossover)
#     A transition NEUTRAL→UP or NEUTRAL→DOWN requires the signal to hold for
#     trend_confirm_periods consecutive evaluation intervals before the regime
#     changes.  A transition back to NEUTRAL is immediate.
#
# Data requirement
# ────────────────
#   min_history_hours (default 26h) of 1-min data in PriceCache before any
#   signal is emitted.  This ensures EMA(24h) has enough warm-up candles.
#   In practice, after the REST ATR seed this is available within seconds.
#
# Output
# ──────
#   TrendSignal.evaluate() returns a dict:
#     "regime"      : "UP" | "DOWN" | "NEUTRAL" | "INSUFFICIENT_DATA"
#     "ema_fast"    : float   current fast EMA (hourly close price)
#     "ema_slow"    : float   current slow EMA
#     "separation"  : float   % gap between fast and slow
#     "slope_pct"   : float   % change in fast EMA over last slope_window_h hours
#     "n_hourly"    : int     number of hourly candles used
#     "changed"     : bool    True if regime changed vs previous call
#     "prev_regime" : str     regime before this call

class TrendSignal:
    """
    Dual-EMA trend observer.  Read-only — no grid side-effects.

    All config comes from GRID_CONFIG under the "trend_signal_*" namespace.
    GridBot hooks this into its periodic status log and /status command.
    """

    REGIME_UP      = "UP"
    REGIME_DOWN    = "DOWN"
    REGIME_NEUTRAL = "NEUTRAL"
    REGIME_NODATA  = "INSUFFICIENT_DATA"

    def __init__(self, config: dict, price_cache: "PriceCache"):
        self._cfg   = config
        self._cache = price_cache

        # EMA periods in hours
        self._fast_h  = config.get("trend_signal_ema_fast_h",  4)
        self._slow_h  = config.get("trend_signal_ema_slow_h",  24)
        self._slope_w = config.get("trend_signal_slope_window_h", 2)

        # Minimum data before we trust the slow EMA  (slow_h + 2h buffer)
        self._min_history_h = config.get("trend_signal_min_history_h",
                                          self._slow_h + 2)

        # Hysteresis: require this many consecutive agreeing periods
        # before committing to UP/DOWN from NEUTRAL
        self._confirm_n = config.get("trend_signal_confirm_periods", 3)

        # Slope threshold: EMA_fast must move at least this many % of
        # EMA_slow over slope_window_h before we call it directional
        self._slope_threshold_pct = config.get(
            "trend_signal_slope_threshold_pct", 0.05
        )

        self._regime: str     = self.REGIME_NEUTRAL  # start neutral; NODATA only when closes is None
        self._pending: str    = self.REGIME_NEUTRAL  # candidate in hysteresis window
        self._pending_count   = 0                    # consecutive periods for candidate

        self._lock = threading.Lock()

    # ── Internal helpers ──────────────────────────────────────────────────────

    def _build_hourly_closes(self) -> Optional[List[float]]:
        """
        Build a list of hourly close prices from PriceCache._history.

        1-min buckets are grouped into 60-min buckets.  The last (open) hourly
        bucket is excluded so all candles are complete.

        Returns None if fewer than (slow_h + 2) hourly candles are available.
        """
        with self._cache._lock:
            history = list(self._cache._history)

        if not history:
            return None

        # Group ticks into 1-min buckets, take the last price as close
        min_buckets: Dict[int, float] = {}
        for ts, mid in history:
            k = int(ts // 60)
            min_buckets[k] = mid          # last write wins → close price

        if not min_buckets:
            return None

        # Group 1-min buckets into hourly buckets (60 mins per hour)
        hour_buckets: Dict[int, float] = {}
        for min_k in sorted(min_buckets.keys()):
            hour_k = min_k // 60
            hour_buckets[hour_k] = min_buckets[min_k]   # last minute is hourly close

        sorted_hours = sorted(hour_buckets.keys())
        current_hour = int(time.time() // 3600)

        # Drop the still-open current hourly candle
        if sorted_hours and sorted_hours[-1] == current_hour:
            sorted_hours = sorted_hours[:-1]

        if len(sorted_hours) < self._min_history_h:
            return None

        return [hour_buckets[h] for h in sorted_hours]

    @staticmethod
    def _compute_ema(prices: List[float], period: int) -> List[float]:
        """
        Classic Wilder/exponential EMA.
        alpha = 2 / (period + 1).
        Returns an EMA series of the same length as prices (warm-up from index 0).
        """
        if not prices or period <= 0:
            return []
        alpha = 2.0 / (period + 1)
        ema = [prices[0]]
        for p in prices[1:]:
            ema.append(alpha * p + (1 - alpha) * ema[-1])
        return ema

    # ── Public API ────────────────────────────────────────────────────────────

    def evaluate(self) -> dict:
        """
        Compute the current trend regime.  Thread-safe; cheap (pure Python
        over ~200-300 floats for a 24h window).  Call from the GridBot main
        loop or status handler.

        Returns a result dict (see class docstring).
        """
        closes = self._build_hourly_closes()

        base = {
            "ema_fast":   0.0,
            "ema_slow":   0.0,
            "separation": 0.0,
            "slope_pct":  0.0,
            "n_hourly":   0,
            "changed":    False,
            "prev_regime": self._regime,
        }

        if closes is None:
            with self._lock:
                prev = self._regime
                self._regime = self.REGIME_NODATA
                changed = (prev != self.REGIME_NODATA)
            return {**base, "regime": self.REGIME_NODATA, "changed": changed,
                    "prev_regime": prev}

        n = len(closes)
        ema_fast_series = self._compute_ema(closes, self._fast_h)
        ema_slow_series = self._compute_ema(closes, self._slow_h)

        ema_fast = ema_fast_series[-1]
        ema_slow = ema_slow_series[-1]
        separation_pct = (ema_fast - ema_slow) / ema_slow * 100.0

        # Slope: change in fast EMA over slope_window_h periods
        slope_idx = max(0, len(ema_fast_series) - 1 - self._slope_w)
        slope_pct = (ema_fast - ema_fast_series[slope_idx]) / ema_slow * 100.0

        # Raw signal before hysteresis
        if (ema_fast > ema_slow and slope_pct > self._slope_threshold_pct):
            raw = self.REGIME_UP
        elif (ema_fast < ema_slow and slope_pct < -self._slope_threshold_pct):
            raw = self.REGIME_DOWN
        else:
            raw = self.REGIME_NEUTRAL

        # Apply hysteresis: instantaneous return to NEUTRAL; UP/DOWN need
        # confirm_n consecutive agreeing evaluations to commit.
        with self._lock:
            prev_regime = self._regime

            if raw == self.REGIME_NEUTRAL:
                # Immediate reset — don't persist UP/DOWN through flat periods
                self._pending       = self.REGIME_NEUTRAL
                self._pending_count = 0
                new_regime          = self.REGIME_NEUTRAL
            elif raw == self._regime:
                # Already in this regime — keep it; reset pending counter
                self._pending       = raw
                self._pending_count = self._confirm_n
                new_regime          = raw
            elif raw == self._pending:
                # Building towards a new regime
                self._pending_count += 1
                if self._pending_count >= self._confirm_n:
                    new_regime = raw
                else:
                    new_regime = self._regime   # not yet confirmed; hold current
            else:
                # New candidate, reset counter
                self._pending       = raw
                self._pending_count = 1
                new_regime          = self._regime   # hold current until confirmed

            self._regime = new_regime
            changed = (new_regime != prev_regime)

        return {
            "regime":      new_regime,
            "ema_fast":    ema_fast,
            "ema_slow":    ema_slow,
            "separation":  separation_pct,
            "slope_pct":   slope_pct,
            "n_hourly":    n,
            "changed":     changed,
            "prev_regime": prev_regime,
        }

    def regime(self) -> str:
        """Return the last confirmed regime without recomputing."""
        with self._lock:
            return self._regime


# ─────────────────────────────────────────────────────────────────────────────
# GridStateStore — SQLite persistence
# ─────────────────────────────────────────────────────────────────────────────
#
# Tables
# ──────
#   grid_fills   — every BUY/SELL fill (permanent audit log)
#   daily_pnl    — pre-aggregated per HKT day; updated incrementally on each fill
#   meta         — schema version + misc key/value (e.g. accumulated counters)
#
# Thread safety
# ─────────────
#   A single threading.Lock() guards all DB access. sqlite3 connections must not
#   be shared across threads without serialisation (check_same_thread=False only
#   disables the built-in guard; it does not make the connection thread-safe).
#   WAL journal mode lets readers proceed concurrently with the single writer.
#
# Schema evolution
# ────────────────
#   SCHEMA_VERSION is stored in meta. Future changes add ALTER TABLE migrations
#   keyed on version; existing data is preserved in-place.
# ─────────────────────────────────────────────────────────────────────────────

import sqlite3 as _sqlite3

_GRID_DB_SCHEMA_VERSION = 1

_GRID_DB_DDL = """
-- Every grid fill: permanent, append-only audit log.
-- gross_pnl is meaningful only for SELL fills (price gain over paired BUY level).
-- fee_usd is always positive (a cost).
CREATE TABLE IF NOT EXISTS grid_fills (
    id          INTEGER PRIMARY KEY AUTOINCREMENT,
    ts_utc      REAL    NOT NULL,          -- Unix timestamp of fill
    hkt_date    TEXT    NOT NULL,          -- 'YYYY-MM-DD' derived from ts_utc (HKT)
    side        TEXT    NOT NULL,          -- 'BUY' | 'SELL'
    level_idx   INTEGER NOT NULL,          -- grid level index
    price_usd   REAL    NOT NULL,
    qty_btc     REAL    NOT NULL,
    fee_usd     REAL    NOT NULL,          -- maker fee paid (positive = cost)
    gross_pnl   REAL    NOT NULL DEFAULT 0.0,  -- (sell_price - buy_price) * qty; 0 for BUY fills
    cycle_num   INTEGER NOT NULL DEFAULT 0     -- monotonic cycle counter at time of fill
);

-- Pre-aggregated daily PnL (HKT date); updated atomically with each fill.
-- gross_pnl_usd: sum of (sell_price - buy_level_price) * qty for completed cycles
-- fees_usd:      total maker fees paid (stored as negative — a cost)
-- net_pnl_usd:   gross_pnl_usd + fees_usd  (fees are negative, so this subtracts)
CREATE TABLE IF NOT EXISTS daily_pnl (
    hkt_date      TEXT PRIMARY KEY,
    gross_pnl_usd REAL NOT NULL DEFAULT 0.0,
    fees_usd      REAL NOT NULL DEFAULT 0.0,
    net_pnl_usd   REAL NOT NULL DEFAULT 0.0,
    fill_count    INTEGER NOT NULL DEFAULT 0,
    cycle_count   INTEGER NOT NULL DEFAULT 0
);

-- Key/value metadata store.
CREATE TABLE IF NOT EXISTS meta (
    key   TEXT PRIMARY KEY,
    value TEXT NOT NULL
);
"""


def _db_hkt_date(ts_utc: float) -> str:
    """Return 'YYYY-MM-DD' in HKT for a Unix timestamp."""
    return _dt.datetime.fromtimestamp(ts_utc, tz=_HKT_TZ).strftime("%Y-%m-%d")


class GridStateStore:
    """
    Thread-safe SQLite wrapper for grid bot persistence.

    Persists every fill, daily PnL buckets, and accumulated totals so that
    a service restart (or re-tune that rebuilds GridEngine) does not lose
    historical accounting.

    Public API used by GridEngine
    ──────────────────────────────
      record_fill(ts, side, idx, price, qty, fee, gross_pnl, cycle_num)
          → called inside _on_fill(); updates grid_fills + daily_pnl atomically

    Public API used by GridBot / /status handler
    ─────────────────────────────────────────────
      get_accumulated()  → {gross_pnl, fees, net_pnl, fill_count, cycle_count}
      get_daily(date)    → same dict for one HKT day (today if None)
      get_recent_daily(n)→ list of last n daily rows, newest first
    """

    def __init__(self, db_path: str = "grid_bot.db") -> None:
        self._db_path = db_path
        self._lock    = threading.Lock()
        self._conn    = _sqlite3.connect(db_path, check_same_thread=False)
        self._conn.row_factory = _sqlite3.Row
        self._conn.execute("PRAGMA journal_mode=WAL")   # readers don't block writer
        self._conn.execute("PRAGMA synchronous=NORMAL") # safe with WAL; faster than FULL
        self._conn.execute("PRAGMA foreign_keys=ON")
        self._apply_schema()
        logger.info(f"[GridStateStore] opened {os.path.abspath(db_path)}")

    # ── Schema bootstrap ─────────────────────────────────────────────────────

    def _apply_schema(self) -> None:
        with self._lock:
            self._conn.executescript(_GRID_DB_DDL)
            row = self._conn.execute(
                "SELECT value FROM meta WHERE key='schema_version'"
            ).fetchone()
            if row is None:
                self._conn.execute(
                    "INSERT INTO meta(key,value) VALUES('schema_version',?)",
                    (str(_GRID_DB_SCHEMA_VERSION),),
                )
                self._conn.commit()
            # Future schema migrations go here (ALTER TABLE guarded by version check)

    # ── Fill recording ────────────────────────────────────────────────────────

    def record_fill(
        self,
        ts_utc:    float,
        side:      str,       # 'BUY' | 'SELL'
        level_idx: int,
        price_usd: float,
        qty_btc:   float,
        fee_usd:   float,     # positive = cost
        gross_pnl: float,     # 0.0 for BUY fills
        cycle_num: int,
    ) -> None:
        """
        Append one fill row and update the daily_pnl bucket atomically.
        Called from GridEngine._on_fill() — must be fast and non-blocking
        (the fill thread processes fills sequentially; a slow DB write here
        delays counter-order placement).  WAL + NORMAL sync keeps writes
        to ~1-2 ms on spinning rust; SSD is faster.
        """
        hkt_date = _db_hkt_date(ts_utc)
        cycles_delta = 1 if side == "SELL" else 0

        with self._lock:
            self._conn.execute(
                """INSERT INTO grid_fills
                   (ts_utc, hkt_date, side, level_idx, price_usd,
                    qty_btc, fee_usd, gross_pnl, cycle_num)
                   VALUES (?,?,?,?,?,?,?,?,?)""",
                (ts_utc, hkt_date, side, level_idx, price_usd,
                 qty_btc, fee_usd, gross_pnl, cycle_num),
            )
            # Update daily bucket — fees stored as negative (cost subtracted from net)
            self._conn.execute(
                """INSERT INTO daily_pnl
                   (hkt_date, gross_pnl_usd, fees_usd, net_pnl_usd, fill_count, cycle_count)
                   VALUES (?, ?, ?, ?, 1, ?)
                   ON CONFLICT(hkt_date) DO UPDATE SET
                       gross_pnl_usd = gross_pnl_usd + excluded.gross_pnl_usd,
                       fees_usd      = fees_usd      + excluded.fees_usd,
                       net_pnl_usd   = net_pnl_usd   + excluded.gross_pnl_usd + excluded.fees_usd,
                       fill_count    = fill_count    + 1,
                       cycle_count   = cycle_count   + excluded.cycle_count""",
                (hkt_date, gross_pnl, -fee_usd, gross_pnl - fee_usd, cycles_delta),
            )
            self._conn.commit()

    # ── Accumulated totals ────────────────────────────────────────────────────

    def get_accumulated(self) -> dict:
        """Sum all rows in daily_pnl → all-time totals."""
        with self._lock:
            row = self._conn.execute(
                """SELECT
                       COALESCE(SUM(gross_pnl_usd), 0.0) AS gross_pnl,
                       COALESCE(SUM(fees_usd),      0.0) AS fees,
                       COALESCE(SUM(net_pnl_usd),   0.0) AS net_pnl,
                       COALESCE(SUM(fill_count),     0)   AS fill_count,
                       COALESCE(SUM(cycle_count),    0)   AS cycle_count
                   FROM daily_pnl"""
            ).fetchone()
        return dict(row) if row else {
            "gross_pnl": 0.0, "fees": 0.0, "net_pnl": 0.0,
            "fill_count": 0,  "cycle_count": 0,
        }

    # ── Daily PnL ─────────────────────────────────────────────────────────────

    def get_daily(self, hkt_date: Optional[str] = None) -> dict:
        """Return the daily_pnl row for hkt_date (today HKT if None)."""
        if hkt_date is None:
            hkt_date = _db_hkt_date(time.time())
        with self._lock:
            row = self._conn.execute(
                "SELECT * FROM daily_pnl WHERE hkt_date=?", (hkt_date,)
            ).fetchone()
        if row:
            return dict(row)
        return {
            "hkt_date": hkt_date, "gross_pnl_usd": 0.0, "fees_usd": 0.0,
            "net_pnl_usd": 0.0, "fill_count": 0, "cycle_count": 0,
        }

    def get_recent_daily(self, days: int = 7) -> list:
        """Return last N HKT-day rows, newest first."""
        with self._lock:
            rows = self._conn.execute(
                "SELECT * FROM daily_pnl ORDER BY hkt_date DESC LIMIT ?", (days,)
            ).fetchall()
        return [dict(r) for r in rows]

    # ── Meta ──────────────────────────────────────────────────────────────────

    def get_meta(self, key: str) -> Optional[str]:
        with self._lock:
            row = self._conn.execute(
                "SELECT value FROM meta WHERE key=?", (key,)
            ).fetchone()
        return row["value"] if row else None

    def set_meta(self, key: str, value: str) -> None:
        with self._lock:
            self._conn.execute(
                "INSERT INTO meta(key,value) VALUES(?,?) "
                "ON CONFLICT(key) DO UPDATE SET value=excluded.value",
                (key, value),
            )
            self._conn.commit()

    # ── Lifecycle ─────────────────────────────────────────────────────────────

    def close(self) -> None:
        with self._lock:
            self._conn.close()
        logger.info("[GridStateStore] database connection closed")


# ─────────────────────────────────────────────────────────────────────────────
# GridBot — top-level controller
# ─────────────────────────────────────────────────────────────────────────────

class GridBot:
    STATUS_INTERVAL_S     = 60.0
    RETUNE_CHECK_INTERVAL = 300.0

    def __init__(self, config: dict):
        self._cfg         = config
        self._stop_event  = threading.Event()
        self._engine:     Optional[GridEngine]    = None
        self._params:     Optional[GridParams]    = None
        self._sl_guard:   Optional[StopLossGuard] = None
        self._alerter:    AlertManager            = AlertManager(config)
        global _grid_bot_alerter
        _grid_bot_alerter = self._alerter
        self._last_tune:  float = 0.0
        self._last_status:float = 0.0
        self._last_retune_check: float = 0.0
        self._halted:     bool  = False
        self._halt_time:  float = 0.0       # timestamp of the last halt
        self._halt_stop_price: float = 0.0  # stop_price that triggered the halt
        self._restart_attempts: int = 0     # number of auto-restart attempts made

        self._oms = OMS(
            api_key      = config.get("api_key", ""),
            api_secret   = config.get("api_secret", ""),
            instrument   = INSTRUMENT,
            live_trading = config.get("live_trading", False),
            config       = config,
        )
        self._auto_tuner = GridAutoTuner(config, _price_cache)

        # ── Trend signal (Phase 1 — read-only observer) ───────────────────────
        self._trend = TrendSignal(config, _price_cache)
        self._last_trend_regime: str   = TrendSignal.REGIME_NODATA
        self._last_trend_log:    float = 0.0   # ts of last trend log line

        # ── SQLite persistence ────────────────────────────────────────────────────
        # Opened once here and shared with every GridEngine instance so that
        # fills survive restarts, re-tunes, and stop-loss rebuilds.
        self._store = GridStateStore(config.get("db_path", "grid_bot.db"))

        # ── Telegram command poller ────────────────────────────────────────────
        self._cmd_poller = TelegramCommandPoller(
            token           = config.get("telegram_bot_token", ""),
            allowed_chat_id = config.get("telegram_chat_id",   ""),
        )
        self._cmd_poller.register("/status", self._handle_status_command)

        # WS market feed
        self._ws_stop = threading.Event()
        self._market_ws = _ReconnectingWS(
            name             = "MarketWS",
            url              = config.get("ws_market_url", "wss://stream.crypto.com/exchange/v1/market"),
            subscribe_msg_fn = self._ws_subscriptions,
            on_message_fn    = self._handle_market_message,
            stale_s          = config.get("ws_stale_threshold_s", 20),
            backoff_init     = config.get("ws_reconnect_backoff_s", 2),
            backoff_max      = config.get("ws_max_backoff_s", 60),
            stop_event       = self._ws_stop,
        )

    # ── WS subscriptions ──────────────────────────────────────────────────────

    def _ws_subscriptions(self) -> List[dict]:
        return [{
            "id": 1,
            "method": "subscribe",
            "params": {"channels": [f"ticker.{INSTRUMENT}"]},
        }]

    def _handle_market_message(self, data: dict) -> None:
        result  = data.get("result", {})
        channel = result.get("subscription", "") or result.get("channel", "")
        items   = result.get("data", [])
        if not items:
            return
        if "ticker" in channel:
            t   = items[0]
            bid = t.get("b")
            ask = t.get("k")
            if bid and ask:
                _price_cache.update_l1(float(bid), float(ask))

    # ── Lifecycle ─────────────────────────────────────────────────────────────

    def start(self):
        logger.info("[GridBot] Starting")
        self._oms.start()
        self._cmd_poller.start()   # start Telegram command polling early so /status works during warmup

        # ── Startup reconciliation ────────────────────────────────────────────
        # Detect and close any position left over from a previous run (crash,
        # hard-kill, or clean stop).  Also cancels all orphaned open orders so
        # the new grid starts from a clean slate.  No-op in paper mode.
        stale_qty = self._oms.reconcile_on_startup()
        if stale_qty > 0:
            self._alerter.send(
                f"⚠️ Startup: found stale long {stale_qty:.4f} BTC from previous run\n"
                f"Closing before building new grid..."
            )
            self._liquidate_position(stale_qty, reason="startup reconcile")

        self._market_ws.start()

        # ── Phase 1: wait for first live price tick ───────────────────────────
        # Just confirms the WS is alive and delivering data.  Typically 2-5s.
        warmup_s = self._cfg.get("min_warmup_seconds", 10)
        logger.info(f"[GridBot] Phase 1 warmup: waiting up to {warmup_s}s for first price tick...")
        deadline = time.time() + warmup_s
        while _price_cache.get_mid() is None:
            if self._stop_event.is_set():
                return
            if time.time() > deadline:
                logger.warning("[GridBot] Phase 1 warmup: no price tick received — WS may be down")
                break
            time.sleep(1)
        mid = _price_cache.get_mid()
        logger.info(f"[GridBot] Phase 1 complete: mid={'%.2f' % mid if mid else 'N/A'}")

        # ── Phase 2: seed ATR from REST historical candles ────────────────────
        # Fetch recent 1-min candles via public/get-candlestick so we don't
        # have to sit idle for ~30 minutes collecting live ticks.  On success
        # the Phase 2 poll loop below exits immediately.  On failure we fall
        # through to the original live-accumulation path with a warning.
        atr_lookback = self._cfg.get("atr_lookback_minutes", 1440)
        self._seed_atr_from_rest()

        # ── Phase 2: wait until ATR is computable ─────────────────────────────
        # Normally instant after _seed_atr_from_rest().  Falls back to the
        # original live-accumulation path if the REST seed failed.
        min_candles  = _price_cache.MIN_ATR_CANDLES
        atr = _price_cache.compute_atr(atr_lookback)
        if atr is None:
            logger.info(
                f"[GridBot] Phase 2 warmup: REST seed insufficient — "
                f"collecting {min_candles} one-minute candles live (~{min_candles} min)..."
            )
        _last_progress = time.time()
        while True:
            if self._stop_event.is_set():
                return
            atr = _price_cache.compute_atr(atr_lookback)
            if atr is not None:
                n = _price_cache.atr_candle_count(atr_lookback)
                logger.info(
                    f"[GridBot] Phase 2 complete: ATR={atr:.2f} from {n} candles"
                )
                break
            now = time.time()
            if now - _last_progress >= 60:
                n = _price_cache.atr_candle_count(atr_lookback)
                logger.info(
                    f"[GridBot] Phase 2 warmup: {n}/{min_candles} candles "
                    f"({n*100//min_candles}%) — ATR not yet ready"
                )
                _last_progress = now
            time.sleep(5)

        logger.info("[GridBot] Warmup complete")
        self._alerter.send(f"🟢 GridBot started — {TRADING_MODE.upper()} | {INSTRUMENT}")

        self._rebuild_grid()
        self._run()

    # ── ATR seeding from REST historical candles ──────────────────────────────

    def _seed_atr_from_rest(self) -> None:
        """
        Fetch recent 1-minute candles from public/get-candlestick and inject
        them into PriceCache._history so that compute_atr() is immediately
        satisfiable without waiting ~30 minutes for live ticks to accumulate.

        Injection strategy
        ──────────────────
        PriceCache._history stores (unix_timestamp, mid_price) tuples.
        compute_atr() groups them into 1-minute buckets via int(ts // 60).
        For each historical candle (open, high, low, close) we inject 4 ticks
        spaced evenly within the candle's 60-second window.  This fully
        satisfies the candle-bucketing logic and gives a realistic OHLC ATR.

        We fetch MIN_ATR_CANDLES + 2 candles (extra slack for the current
        open candle and bucket-boundary edge cases) and inject only those
        strictly older than the current live-tick bucket to avoid mixing
        REST and WS data for the same minute.

        Failure modes
        ─────────────
        Any REST error (network, rate-limit, unexpected response shape) is
        caught and logged; the method returns silently so Phase 2 falls back
        to the original live-accumulation path.
        """
        min_candles = _price_cache.MIN_ATR_CANDLES
        fetch_count = min_candles + 2   # extra slack for open candle + edge cases

        rest_base = self._cfg.get("rest_base_url",
                                   "https://api.crypto.com/exchange/v1")
        url = f"{rest_base}/public/get-candlestick"
        params = {
            "instrument_name": INSTRUMENT,
            "timeframe":       "1m",
            "count":           fetch_count,
        }

        logger.info(
            f"[GridBot] Phase 2: seeding ATR from REST "
            f"(fetching {fetch_count} × 1-min candles)..."
        )
        try:
            resp = requests.get(url, params=params, timeout=10.0)
            resp.raise_for_status()
            body = resp.json()
        except Exception as exc:
            logger.warning(
                f"[GridBot] ATR seed: REST request failed ({exc}) — "
                f"falling back to live candle accumulation"
            )
            return

        if body.get("code", -1) != 0:
            logger.warning(
                f"[GridBot] ATR seed: API returned code={body.get('code')} "
                f"msg={body.get('message', '')} — falling back to live accumulation"
            )
            return

        candles = (body.get("result", {}).get("data", [])
                   or body.get("result", {}).get("instrument_name", {})
                   or [])
        # CDC v1 candlestick response nests data under result.data
        if not candles and isinstance(body.get("result"), dict):
            candles = body["result"].get("data", [])

        if not candles:
            logger.warning(
                "[GridBot] ATR seed: empty candle list in response — "
                "falling back to live accumulation"
            )
            return

        # Current open 1-min bucket — we skip injecting into this bucket
        # because live WS ticks are already filling it; mixing would
        # produce an artificially wide H-L for that minute.
        current_bucket = int(time.time() // 60)

        injected = 0
        synthetic_ticks: list = []

        for c in candles:
            # CDC v1 format: {"t": <ms>, "o": "...", "h": "...", "l": "...", "c": "..."}
            try:
                ts_ms  = int(c.get("t", 0))
                o_px   = float(c.get("o", 0))
                h_px   = float(c.get("h", 0))
                l_px   = float(c.get("l", 0))
                cl_px  = float(c.get("c", 0))
            except (TypeError, ValueError) as exc:
                logger.debug(f"[GridBot] ATR seed: skipping malformed candle {c}: {exc}")
                continue

            if ts_ms <= 0 or any(p <= 0 for p in (o_px, h_px, l_px, cl_px)):
                continue

            ts_s   = ts_ms / 1000.0
            bucket = int(ts_s // 60)

            if bucket >= current_bucket:
                # Skip the live (still-open) bucket
                continue

            # Inject 4 ticks spread across the candle's 60-second window:
            #   t+0s  → open
            #   t+15s → high  (first half peak)
            #   t+45s → low   (second half trough)
            #   t+59s → close
            # The exact intra-candle ordering doesn't affect ATR since
            # compute_atr() only uses each bucket's H/L/close aggregate.
            synthetic_ticks.extend([
                (ts_s +  0.0, o_px),
                (ts_s + 15.0, h_px),
                (ts_s + 45.0, l_px),
                (ts_s + 59.0, cl_px),
            ])
            injected += 1

        if injected == 0:
            logger.warning(
                "[GridBot] ATR seed: no usable historical candles after filtering — "
                "falling back to live accumulation"
            )
            return

        # Inject into PriceCache under its own lock.
        # We extend _history directly (it's a bounded deque); existing live
        # ticks from Phase 1 are already in there and remain untouched.
        # Sort ascending so the deque is in chronological order.
        synthetic_ticks.sort(key=lambda x: x[0])
        with _price_cache._lock:
            # Prepend: historical ticks go before the Phase-1 live tick.
            # We rebuild the deque to maintain chronological order and
            # respect the maxlen cap (30 000 entries).
            existing = list(_price_cache._history)
            merged   = synthetic_ticks + existing
            # Deduplicate by bucket+price is unnecessary — slight overlap
            # in the open bucket is prevented by the current_bucket guard.
            _price_cache._history.clear()
            for item in merged[-(30000):]:    # honour maxlen
                _price_cache._history.append(item)

        n_buckets = _price_cache.atr_candle_count(
            self._cfg.get("atr_lookback_minutes", 1440)
        )
        logger.info(
            f"[GridBot] ATR seed complete: injected {injected} historical candles "
            f"({injected * 4} ticks) → {n_buckets} buckets now in cache"
        )

    def stop(self):
        logger.info("[GridBot] Stopping")
        self._stop_event.set()
        self._ws_stop.set()
        self._market_ws.stop()

        # Liquidate any accumulated long before tearing down the OMS.
        # This mirrors what _emergency_halt does for stop-loss events so that
        # a clean SIGINT/SIGTERM also closes the position rather than leaving
        # it orphaned on the exchange.
        long_qty = 0.0
        if self._engine:
            long_qty = self._engine.get_stats().get("long_qty", 0.0)
            self._engine.stop()
            self._engine = None
        if long_qty > 0:
            self._liquidate_position(long_qty, reason="GridBot stop")

        self._cmd_poller.stop()
        self._oms.stop()
        self._store.close()
        self._alerter.send_sync(f"🔴 GridBot stopped")
        self._alerter.stop()
        logger.info("[GridBot] Stopped")

    # ── Main loop ─────────────────────────────────────────────────────────────

    def _run(self):
        logger.info("[GridBot] Main loop running")
        while not self._stop_event.is_set():
            if self._halted:
                self._check_auto_restart()
                time.sleep(10)   # poll every 10s while halted
                continue

            mid = _price_cache.get_mid()
            if mid is None:
                time.sleep(0.2)
                continue

            # Stop-loss
            if self._sl_guard and self._sl_guard.check(mid):
                self._emergency_halt(mid)
                continue

            # Fill detection
            if self._engine:
                self._engine.check_price_fills(mid)

            now = time.time()

            # Re-tune check
            if (self._cfg.get("auto_tune_enabled", True)
                    and self._params is not None
                    and now - self._last_retune_check > self.RETUNE_CHECK_INTERVAL):
                self._last_retune_check = now
                if self._auto_tuner.should_retune(self._params, mid, self._last_tune):
                    self._rebuild_grid()

            # Periodic status + trend signal (share the same cadence)
            if now - self._last_status > self.STATUS_INTERVAL_S:
                self._last_status = now
                self._log_status(mid)
                self._evaluate_trend()

            time.sleep(0.1)

    # ── Grid management ───────────────────────────────────────────────────────

    def _liquidate_position(self, qty: float, reason: str = ""):
        """
        Submit a market SELL for `qty` BTC and wait for the fill (up to 15s).
        Used by stop(), start() reconcile, and _emergency_halt().
        In paper mode the fill is instant at the live mid price.
        Logs and alerts on both success and timeout.
        """
        tag = f"[{reason}]" if reason else ""
        logger.warning(f"[GridBot]{tag} Liquidating {qty:.4f} BTC long via market SELL")
        req  = OrderRequest.market(side="SELL", qty=qty,
                                   instrument=INSTRUMENT, purpose="liquidate")
        self._oms.submit(req)
        fill = self._oms.wait_fill(req.client_oid, timeout=15.0)
        if fill and fill.is_filled:
            logger.warning(
                f"[GridBot]{tag} Liquidation filled: "
                f"{fill.filled_qty:.4f} @ {fill.avg_price:.2f}"
            )
            self._alerter.send(
                f"🔴 Position closed ({reason})\n"
                f"Sold {fill.filled_qty:.4f} BTC @ {fill.avg_price:.2f}"
            )
        else:
            logger.error(
                f"[GridBot]{tag} Liquidation fill TIMED OUT — "
                f"{qty:.4f} BTC may still be open. MANUAL INTERVENTION REQUIRED."
            )
            self._alerter.send(
                f"🚨 Liquidation TIMED OUT ({reason})\n"
                f"{qty:.4f} BTC position may still be open.\n"
                f"MANUAL INTERVENTION REQUIRED"
            )

    def _rebuild_grid(self):
        mid = _price_cache.get_mid()
        if mid is None:
            logger.warning("[GridBot] No mid price — cannot build grid")
            return

        logger.info("[GridBot] (Re)building grid...")

        if self._engine:
            self._engine.stop()
            self._engine = None

        new_params = self._auto_tuner.compute()
        if new_params is None:
            logger.error("[GridBot] Auto-tuner returned None — keeping existing params")
            new_params = self._params
        if new_params is None:
            logger.error("[GridBot] No grid params available — aborting rebuild")
            return

        # Dead-band check
        if self._params is not None:
            old_width = self._params.upper - self._params.lower
            new_width = new_params.upper - new_params.lower
            if old_width > 0:
                delta = abs(new_width - old_width) / old_width
                deadband = self._cfg.get("retune_deadband_pct", 0.10)
                if delta < deadband:
                    logger.info(
                        f"[GridBot] Re-tune skipped (range shift {delta:.1%} < "
                        f"dead-band {deadband:.1%})"
                    )
                    return

        # ── Stop-proximity guard ──────────────────────────────────────────────
        # Abort the grid build if current mid is already too close to the
        # newly-computed stop.  This prevents arming a StopLossGuard that
        # would fire within seconds of startup or auto-restart because price
        # drifted down while we were computing params.
        #
        # Normal headroom at startup = (atr_multiplier + stop_buffer_atr) × ATR
        # = 4 × ATR.  We only block when headroom < min_stop_headroom_atr × ATR,
        # so this guard is intentionally a light sanity check, not a strategy gate.
        headroom_mult = self._cfg.get("min_stop_headroom_atr", 0.5)
        if headroom_mult > 0:
            atr_now = _price_cache.compute_atr(self._cfg.get("atr_lookback_minutes", 1440))
            if atr_now is not None and atr_now > 0:
                min_headroom = headroom_mult * atr_now
                actual_headroom = mid - new_params.stop_price
                if actual_headroom < min_headroom:
                    logger.warning(
                        f"[GridBot] Grid build aborted: mid={mid:.2f} too close to "
                        f"stop={new_params.stop_price:.2f} "
                        f"(headroom={actual_headroom:.2f} < {headroom_mult}×ATR={min_headroom:.2f}). "
                        f"Waiting for price to recover."
                    )
                    self._alerter.send(
                        f"⚠️ Grid build aborted: price too close to stop\n"
                        f"mid={mid:.2f} stop={new_params.stop_price:.2f} "
                        f"headroom={actual_headroom:.0f} < {min_headroom:.0f} required"
                    )
                    return

        self._params    = new_params
        self._last_tune = time.time()
        self._sl_guard  = StopLossGuard(new_params.stop_price, self._cfg)

        self._engine = GridEngine(
            params=new_params, oms=self._oms,
            instrument=INSTRUMENT, config=self._cfg,
            store=self._store)
        self._engine.start(mid)

        logger.info(
            f"[GridBot] Grid live: [{new_params.lower:.2f},{new_params.upper:.2f}] "
            f"levels={new_params.levels} spacing={new_params.spacing:.2f} "
            f"stop={new_params.stop_price:.2f}"
        )
        self._alerter.send(
            f"📐 Grid set: [{new_params.lower:.0f},{new_params.upper:.0f}] "
            f"{new_params.levels} levels spacing={new_params.spacing:.0f} "
            f"stop={new_params.stop_price:.0f}"
        )

    def _emergency_halt(self, mid: float):
        logger.warning(f"[GridBot] EMERGENCY HALT at mid={mid:.2f}")
        self._halted      = True
        self._halt_time   = time.time()
        self._halt_stop_price = self._params.stop_price if self._params else mid

        long_qty = 0.0
        if self._engine:
            long_qty = self._engine.get_stats().get("long_qty", 0.0)
            self._engine.stop()
            self._engine = None

        _restart_note = ("Monitoring for auto-restart."
                         if self._cfg.get("auto_restart_enabled", True)
                         else "Restart manually.")

        if long_qty > 0:
            # _liquidate_position sends its own fill/timeout alert; we send the
            # STOP-LOSS context alert separately so they're distinct in Telegram.
            self._alerter.send_sync(
                f"🚨 STOP-LOSS TRIGGERED\n"
                f"mid={mid:.2f} < stop={self._halt_stop_price:.2f}\n"
                f"Liquidating {long_qty:.4f} BTC — Bot HALTED — {_restart_note}"
            )
            self._liquidate_position(long_qty, reason="stop-loss")
        else:
            self._alerter.send_sync(
                f"🚨 STOP-LOSS TRIGGERED at mid={mid:.2f}\n"
                f"No long position to liquidate. Bot HALTED — {_restart_note}"
            )

    # ── Auto-restart ──────────────────────────────────────────────────────────

    def _check_auto_restart(self):
        """
        Called every 10s while the bot is halted. Evaluates four stability
        conditions and restarts the grid if all pass.

        Conditions:
          1. auto_restart_enabled = True
          2. max_attempts not exceeded (0 = unlimited)
          3. Cooldown since halt elapsed
          4. Price above the stop-loss level that triggered the halt
          5. Hi-lo range over stability window < stability_atr_mult × ATR
          6. Current price >= mean of stability window (flat or rising)
        """
        if not self._cfg.get("auto_restart_enabled", True):
            return

        max_attempts = self._cfg.get("auto_restart_max_attempts", 3)
        if max_attempts > 0 and self._restart_attempts >= max_attempts:
            # Already exhausted all attempts — stay halted, require manual intervention
            return

        mid = _price_cache.get_mid()
        if mid is None:
            return

        now           = time.time()
        cooldown_s    = self._cfg.get("auto_restart_cooldown_minutes", 30) * 60
        elapsed       = now - self._halt_time

        # Condition 1: cooldown
        if elapsed < cooldown_s:
            remaining = int(cooldown_s - elapsed)
            logger.debug(
                f"[AutoRestart] Cooldown: {remaining}s remaining "
                f"(halt={self._halt_stop_price:.2f} mid={mid:.2f})"
            )
            return

        # Condition 2: price must be above (or within ATR noise of) the stop that
        # triggered the halt.  We fetch ATR now so we can apply the buffer; if ATR
        # is unavailable we fall back to the strict exact comparison.
        atr_for_buffer = _price_cache.compute_atr(self._cfg.get("atr_lookback_minutes", 1440))
        recovery_buffer_mult = self._cfg.get("auto_restart_recovery_atr_buffer", 0.5)
        if atr_for_buffer and atr_for_buffer > 0:
            recovery_floor = self._halt_stop_price - recovery_buffer_mult * atr_for_buffer
        else:
            recovery_floor = self._halt_stop_price   # strict fallback

        if mid <= recovery_floor:
            logger.info(
                f"[AutoRestart] Price {mid:.2f} still at/below halt stop "
                f"{self._halt_stop_price:.2f} — waiting"
            )
            return

        # Condition 3 + 4: stability window
        stab_min = self._cfg.get("auto_restart_stability_minutes", 60)
        stab     = _price_cache.compute_stability(stab_min)

        if not stab["ok"]:
            logger.info(
                f"[AutoRestart] Insufficient price history "
                f"({stab.get('n_ticks', 0)} ticks in {stab_min}m window) — waiting"
            )
            return

        atr = atr_for_buffer   # already fetched for condition 2; reuse it
        if atr is None or atr <= 0:
            logger.info("[AutoRestart] ATR unavailable — waiting")
            return

        atr_mult  = self._cfg.get("auto_restart_stability_atr_mult", 7.75)
        max_range = atr_mult * atr
        hi_lo     = stab["hi_lo"]
        mean      = stab["mean"]

        # Condition 3: range must be tight
        if hi_lo > max_range:
            logger.info(
                f"[AutoRestart] Still volatile: hi-lo={hi_lo:.2f} > "
                f"{atr_mult}×ATR={max_range:.2f} — waiting"
            )
            return

        # Condition 4: price must be flat or rising (not bleeding lower)
        # Allow a small tolerance of 0.1×ATR below mean to avoid false blocks
        # from end-of-sine-wave positioning in a tight oscillation.
        trend_floor = mean - 0.1 * atr
        if mid < trend_floor:
            logger.info(
                f"[AutoRestart] Downtrend in window: mid={mid:.2f} < "
                f"trend_floor={trend_floor:.2f} (mean={mean:.2f} - 0.1×ATR) — waiting"
            )
            return

        # All conditions met — restart
        self._restart_attempts += 1
        logger.info(
            f"[AutoRestart] Stability confirmed: "
            f"hi-lo={hi_lo:.2f} < max={max_range:.2f}, "
            f"mid={mid:.2f} >= mean={mean:.2f}, "
            f"above stop={self._halt_stop_price:.2f} "
            f"(attempt {self._restart_attempts}/{max_attempts if max_attempts else '∞'})"
        )
        self._alerter.send(
            f"🔄 Auto-restart #{self._restart_attempts}: stability confirmed\n"
            f"mid={mid:.2f} | hi-lo={hi_lo:.0f} < {max_range:.0f} ({stab_min}m window)\n"
            f"Rebuilding grid..."
        )

        self._halted = False
        # Reset the stop-loss guard so it can fire again on the new grid
        self._sl_guard = None

        # Rebuild grid with fresh ATR-based params
        self._rebuild_grid()

        if max_attempts > 0 and self._restart_attempts >= max_attempts:
            logger.warning(
                f"[AutoRestart] Max attempts ({max_attempts}) reached. "
                f"If bot halts again it will require manual restart."
            )

    # ── /status Telegram command ──────────────────────────────────────────────

    def _handle_status_command(self) -> str:
        """
        Builds and returns the /status reply string.
        Called by TelegramCommandPoller on the poller thread — must be thread-safe.

        Daily PnL and accumulated PnL are read from GridStateStore (SQLite) so
        they are correct across restarts, re-tunes, and stop-loss rebuilds.

        Reply sections
        ──────────────
        1. Current position  — net long BTC, open buy/sell order counts, live mid price
        2. Daily PnL         — net PnL from DB for today's HKT date
        3. Accumulated PnL   — all-time net PnL summed from daily_pnl table
        4. Last 7 days       — per-day breakdown
        """
        now_hkt = _dt.datetime.now(_HKT_TZ).strftime("%Y-%m-%d %H:%M HKT")

        # ── Engine snapshot (thread-safe via get_stats()) ─────────────────────
        if self._engine is not None:
            stats = self._engine.get_stats()
        else:
            stats = {"long_qty": 0.0, "open_buys": 0, "open_sells": 0, "levels": 0}

        long_qty   = stats.get("long_qty",   0.0)
        open_buys  = stats.get("open_buys",  0)
        open_sells = stats.get("open_sells", 0)
        levels     = stats.get("levels",     0)

        # ── DB queries ────────────────────────────────────────────────────────
        today   = self._store.get_daily(_db_hkt_date(time.time()))
        acc     = self._store.get_accumulated()
        history = self._store.get_recent_daily(7)

        daily_net = today["net_pnl_usd"]
        acc_net   = acc["net_pnl"]
        acc_gross = acc["gross_pnl"]
        acc_fees  = acc["fees"]          # stored as negative in DB
        acc_cycles= acc["cycle_count"]

        # ── Live price ────────────────────────────────────────────────────────
        mid = _price_cache.get_mid()
        mid_str = f"${mid:,.2f}" if mid is not None else "N/A"

        # ── Grid range ────────────────────────────────────────────────────────
        params = self._params
        if params:
            range_str   = f"[{params.lower:,.0f} – {params.upper:,.0f}]  stop={params.stop_price:,.0f}"
            spacing_str = f"{params.spacing:.2f}"
        else:
            range_str   = "N/A (grid not built)"
            spacing_str = "N/A"

        # ── Bot state ─────────────────────────────────────────────────────────
        if self._halted:
            state_line = "🔴 *HALTED* (stop-loss triggered)"
        elif self._engine is None:
            state_line = "🟡 Warming up / building grid..."
        else:
            state_line = f"🟢 Running ({TRADING_MODE.upper()})"

        # ── PnL emoji helper ──────────────────────────────────────────────────
        def _e(v: float) -> str:
            return "🟢" if v > 0 else ("🔴" if v < 0 else "⚪")

        # ── Last 7 days table ─────────────────────────────────────────────────
        hist_lines = []
        for row in history:
            sign = "✅" if row["net_pnl_usd"] >= 0 else "❌"
            hist_lines.append(
                f"  {sign} {row['hkt_date']}  "
                f"net={row['net_pnl_usd']:+.4f}  "
                f"cycles={row['cycle_count']}"
            )
        hist_block = "\n".join(hist_lines) if hist_lines else "  (no data yet)"

        # ── Trend signal snapshot (re-evaluate on demand) ─────────────────────
        tr = self._trend.evaluate()
        tr_regime = tr["regime"]
        regime_icons = {
            TrendSignal.REGIME_UP:      "📈",
            TrendSignal.REGIME_DOWN:    "📉",
            TrendSignal.REGIME_NEUTRAL: "➡️",
            TrendSignal.REGIME_NODATA:  "⏳",
        }
        tr_icon = regime_icons.get(tr_regime, "?")
        if tr_regime == TrendSignal.REGIME_NODATA:
            tr_block = f"  {tr_icon} Insufficient data (need {self._cfg.get('trend_signal_min_history_h', 26)}h)"
        else:
            tr_block = (
                f"  {tr_icon} `{tr_regime}`\n"
                f"  • EMA 4h:  `{tr['ema_fast']:,.2f}`\n"
                f"  • EMA 24h: `{tr['ema_slow']:,.2f}`\n"
                f"  • Sep: `{tr['separation']:+.3f}%`  Slope: `{tr['slope_pct']:+.3f}%`\n"
                f"  • Based on `{tr['n_hourly']}` hourly candles _(read-only)_"
            )

        lines = [
            f"📊 *Grid Bot Status* — {now_hkt}",
            f"_{state_line}_",
            "",
            "━━━━━━━━━━━━━━━━━━━━━",
            "*1️⃣  Current Position*",
            f"  • Net long:   `{long_qty:.4f} BTC`",
            f"  • Mid price:  `{mid_str}`",
            f"  • Open buys:  `{open_buys}` / Open sells: `{open_sells}`",
            f"  • Grid range: `{range_str}`",
            f"  • Levels:     `{levels}` (spacing ≈ {spacing_str})",
            "",
            "━━━━━━━━━━━━━━━━━━━━━",
            f"*2️⃣  Daily PnL* (today {today['hkt_date']} HKT)",
            f"  {_e(daily_net)}  Net:   `{daily_net:+.4f} USD`",
            f"  • Gross: `{today['gross_pnl_usd']:+.4f}`  Fees: `{today['fees_usd']:+.4f}`",
            f"  • Cycles today: `{today['cycle_count']}`",
            "",
            "━━━━━━━━━━━━━━━━━━━━━",
            "*3️⃣  Accumulated PnL* (all-time from DB)",
            f"  {_e(acc_net)}  Net:   `{acc_net:+.4f} USD`",
            f"  • Gross realised: `{acc_gross:+.4f} USD`",
            f"  • Total fees:     `{acc_fees:+.4f} USD`",
            f"  • Total cycles:   `{acc_cycles}`",
            "",
            "━━━━━━━━━━━━━━━━━━━━━",
            "*📅  Last 7 Days*",
            hist_block,
            "",
            "━━━━━━━━━━━━━━━━━━━━━",
            "*📡  Trend Signal* (EMA 4h / 24h — observer only)",
            tr_block,
        ]

        logger.info("[GridBot] /status command served via Telegram")
        return "\n".join(lines)

    # ── Status ────────────────────────────────────────────────────────────────

    def _log_status(self, mid: float):
        stats  = self._engine.get_stats() if self._engine else {}
        params = self._params
        if params:
            logger.info(
                f"[Status] mid={mid:.2f} "
                f"range=[{params.lower:.2f},{params.upper:.2f}] stop={params.stop_price:.2f} | "
                f"buys={stats.get('open_buys',0)} sells={stats.get('open_sells',0)} "
                f"long={stats.get('long_qty',0):.4f} BTC | "
                f"cycles={stats.get('cycles',0)} "
                f"net_pnl={stats.get('net_pnl',0):+.4f} USD"
            )

    # ── Trend signal evaluation ───────────────────────────────────────────────

    def _evaluate_trend(self) -> dict:
        """
        Evaluate the TrendSignal and log/alert on regime changes.
        Called from the main _run loop alongside the periodic status log.
        Returns the latest result dict for use in _handle_status_command.
        """
        result = self._trend.evaluate()
        regime = result["regime"]

        # Always log at INFO so the signal is visible in the daily log file
        regime_icons = {
            TrendSignal.REGIME_UP:      "📈",
            TrendSignal.REGIME_DOWN:    "📉",
            TrendSignal.REGIME_NEUTRAL: "➡️ ",
            TrendSignal.REGIME_NODATA:  "⏳",
        }
        icon = regime_icons.get(regime, "?")

        if regime == TrendSignal.REGIME_NODATA:
            logger.info(
                f"[TrendSignal] {icon} INSUFFICIENT_DATA "
                f"(need {self._cfg.get('trend_signal_min_history_h', 26)}h of 1-min candles)"
            )
        else:
            logger.info(
                f"[TrendSignal] {icon} {regime:7s} | "
                f"EMA4h={result['ema_fast']:,.2f}  EMA24h={result['ema_slow']:,.2f} | "
                f"sep={result['separation']:+.3f}%  slope={result['slope_pct']:+.3f}% | "
                f"n_hourly={result['n_hourly']}"
            )

        # Telegram alert on regime change (not for NODATA transitions)
        if (result["changed"]
                and regime != TrendSignal.REGIME_NODATA
                and result["prev_regime"] != TrendSignal.REGIME_NODATA):
            prev = result["prev_regime"]
            self._alerter.send(
                f"{icon} *Trend regime changed*: `{prev}` → `{regime}`\n"
                f"EMA4h={result['ema_fast']:,.2f}  EMA24h={result['ema_slow']:,.2f}\n"
                f"sep={result['separation']:+.3f}%  slope={result['slope_pct']:+.3f}%\n"
                f"_Read-only signal — grid not affected_"
            )
            logger.info(
                f"[TrendSignal] ⚠️  Regime change: {prev} → {regime} "
                f"(Telegram alert sent)"
            )

        self._last_trend_regime = regime
        return result


# ─────────────────────────────────────────────────────────────────────────────
# Entry point
# ─────────────────────────────────────────────────────────────────────────────

def main():
    bot = GridBot(GRID_CONFIG)

    def _shutdown(sig, frame):
        logger.info(f"[Main] Signal {sig} — shutting down")
        bot.stop()

    signal.signal(signal.SIGINT,  _shutdown)
    signal.signal(signal.SIGTERM, _shutdown)

    bot.start()


if __name__ == "__main__":
    main()
