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
import argparse
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
import shutil
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

    # ── Paper-mode fill realism ────────────────────────────────────────────────
    # A real resting limit order needs at least one exchange round-trip before
    # it can be crossed — it is never eligible to fill in the same instant it
    # is placed. GridEngine._simulate_paper_fills() checks price-crossing on
    # every tick with no such floor, so a freshly-placed level (from an initial
    # build, a trail-up/down, or a same-tick counter-order after a fill) whose
    # price is already crossed by a fast-moving mid can paper-fill on the very
    # next tick — faster than a real exchange would ever ack + match it.
    # This delay makes a level ineligible to paper-fill until it has rested
    # for at least this many seconds after being placed. 0 = disabled (legacy
    # instant-fill behaviour).
    "paper_fill_min_resting_s": 1.5,

    # ── Grid geometry (auto-tuned at startup; these are fallback defaults) ────
    "grid_lower":         55000.0,
    "grid_upper":         65000.0,
    "grid_levels":        20,

    # ── Investment amount ─────────────────────────────────────────────────────
    # Total capital to deploy across all grid levels.  Specify EITHER USD or BTC
    # — exactly one must be non-zero.  BTC is valued at the mid price at each
    # grid build time and converted to a USD notional for order sizing.
    #
    # "total_investment_usd":  deploy this many USD across all levels, e.g. 2000.0
    # "total_investment_btc":  deploy this many BTC across all levels, e.g. 0.03
    #   (BTC mode is natural if you hold BTC in your wallet and want the bot to
    #    cycle it — the grid starts with sell orders above mid, converting BTC→USD,
    #    then buy orders below mid buy it back.)
    #
    # notional_per_level is derived at build time:
    #   notional_per_level = total_investment_usd / levels
    #   (BTC: first converted → USD at mid price)
    #
    # Legacy key "notional_per_level" is still accepted as a direct override
    # (skips total_investment logic entirely) for backwards compatibility.
    "total_investment_usd": 2000.0,   # set to 0.0 if using BTC instead
    "total_investment_btc": 0.0,      # e.g. 0.03 BTC; 0.0 = use USD above

    # ── Auto-tuner ────────────────────────────────────────────────────────────
    "auto_tune_enabled":    True,
    "atr_lookback_minutes": 1440,      # 1-day lookback for ATR
    "atr_multiplier":       3.0,       # range = mid ± N×ATR
    "min_grid_pct":         0.0008,    # min grid spacing as fraction of price
    "max_grid_levels":      50,
    "min_grid_levels":      5,
    "retune_interval_hours": 24,
    "retune_deadband_pct":  0.10,      # skip re-tune if range shifts < 10%

    # ── Dead-band stop-raise: risk-adaptive gating ────────────────────────────
    # When a dead-band retune wants to raise the in-place stop (see
    # GridBot._rebuild_grid()), four mechanisms now govern HOW that raise is
    # applied, and all four are modulated in real time by "trend_risk" — a
    # score in [0,1] computed by StopScoreCalculator.compute_trend_risk()
    # from short-term velocity/volatility (tick-level) plus the TrendSignal
    # hourly regime (macro). Low trend_risk ("looks like noise") → raise
    # slowly and conservatively, to avoid the SL1/SL2 whipsaw pattern where a
    # single volatile print raised the stop right into a retracement. High
    # trend_risk ("looks like a genuine strengthening decline") → raise
    # quickly and closer to the full target, to lock in protection before a
    # real drop gets worse.
    #
    #  1. Cap        — max single-event raise step, in ATR. Interpolated
    #                   between *_base_atr (trend_risk=0) and *_max_atr
    #                   (trend_risk=1).
    "stop_raise_cap_base_atr":    0.5,
    "stop_raise_cap_max_atr":     2.5,
    #  2. Debounce   — seconds the candidate stop must hold (not weaken)
    #                   before the raise commits. Interpolated between
    #                   *_base_s (trend_risk=0, patient) and *_min_s
    #                   (trend_risk=1, act fast). Timer resets whenever the
    #                   candidate weakens (a sign of retracement, exactly the
    #                   SL1 scenario).
    "stop_raise_confirm_base_s":  90,
    "stop_raise_confirm_min_s":   10,
    #  3. EMA damping — smooths the raw auto-tuner stop before it's used as
    #                   the raise candidate, filtering single-sample ATR/mid
    #                   spikes. Interpolated between *_base (slow/heavy
    #                   smoothing at trend_risk=0) and *_max (fast/near
    #                   raw at trend_risk=1).
    "stop_raise_ema_alpha_base":  0.15,
    "stop_raise_ema_alpha_max":   0.60,
    #  4. Urgent bypass — if trend_risk reaches this threshold, the raise is
    #                   allowed to bypass the drift-shift cooldown veto
    #                   entirely (strong, real evidence outweighs the
    #                   "might still be mid-retracement" assumption the
    #                   cooldown was built around).
    "stop_raise_urgent_trend_risk": 0.80,
    #  Debounce noise tolerance — the candidate must weaken by more than this
    #  (in units of ATR) to reset the confirmation timer, so ordinary
    #  sample-to-sample float/EMA jitter doesn't perpetually restart it. Fixed
    #  a 2026-07-10 regression where debounce tracked the CAPPED candidate,
    #  whose ceiling drifts with ATR independent of any real reversal.
    #  0.3xATR chosen from replaying the actual logged sequence: the
    #  EMA-damped candidate (alpha=0.15) still swings 10-20 points call-to-call
    #  from ordinary mid/ATR noise even post-damping (raw_new_stop tracks mid
    #  closely) — a tolerance of 0.05xATR (~2.5pts) still reset almost every
    #  time; 0.3xATR (~15pts) absorbed the noise while still resetting on a
    #  genuinely large single-step decline.
    "stop_raise_confirm_noise_atr": 0.3,

    # trend_risk component weights (normalised to sum to 1.0)
    "trend_risk_weight_velocity":        0.40,  # tick-level EMA of down-moves
    "trend_risk_weight_volatility":      0.25,  # ATR expansion vs recent mean
    "trend_risk_weight_regime":          0.35,  # TrendSignal hourly DOWN regime
    "trend_risk_regime_slope_norm_pct":  0.5,   # |slope_pct| that maps to full regime risk (1.0)

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

    # ── Sell-fill-triggered range shift ───────────────────────────────────────
    # When the top-level SELL order fills, price has risen above the grid — a
    # sign of sustained upward drift.  Setting drift_shift_on_top_sell=True
    # triggers an immediate one-level-up range shift (same as trail_up) without
    # waiting for price to clear a full spacing above the upper bound.
    #
    # This keeps the grid centred on where price actually is, which reduces the
    # risk of the entire grid being below mid (all-long, no sells to collect
    # profit) and prevents the lower bound drifting dangerously close to the stop.
    #
    # Consecutive shifts are throttled by drift_shift_min_interval_s (default 60s)
    # to prevent rapid-fire shifts during a volatile upswing.  The trailing_up_price_cap
    # is also respected: drift shift is blocked if that cap would be breached.
    "drift_shift_on_top_sell":    True,
    "drift_shift_min_interval_s": 60,   # minimum seconds between consecutive shifts

    # ── Stop-loss ─────────────────────────────────────────────────────────────
    "stop_loss_enabled": True,
    # stop = lower − stop_buffer_atr × ATR
    #
    # Observed data (Jul 3-4): halts 2-4 were triggered by moves of only
    # 1.3-2.0×ATR below the grid lower bound.  With buffer=1.0 the stop sat
    # only ~1×ATR below lower, making it trivially reachable by normal BTC noise.
    #
    # stop_buffer_atr = 3.0 means the stop fires when price drops 3×ATR below
    # lower (= 6×ATR below mid for the default atr_multiplier=3.0).  At current
    # ATR≈30-42 this places the stop ~$90-126 below lower, surviving the 1.3-2×ATR
    # noise moves seen in the logs while still stopping a genuine crash.
    #
    # Auto-expansion: if the rolling ATR has expanded by more than
    # stop_buffer_atr_expansion_threshold× its own recent mean, the buffer is
    # scaled up proportionally (capped at stop_buffer_atr_max_mult× the base)
    # to protect against sudden volatility regime shifts.
    "stop_buffer_atr":                    3.0,
    "stop_buffer_atr_expansion_threshold": 1.5,  # ATR/mean_ATR ratio that triggers widening
    "stop_buffer_atr_max_mult":            2.0,  # cap: buffer never exceeds base × this

    # ── ATR floor + recent-range guard ───────────────────────────────────────
    # During rapid directional moves (e.g. a fast 200-pt BTC spike) the 1-min
    # candles are all narrow and directional, which collapses the rolling ATR.
    # A low ATR produces a dangerously tight stop: on 2026-07-10 SL1 the ATR
    # compressed to 28.67 (normal: 35-45), placing the stop only 87 pts below
    # mid, which a 174-pt retracement immediately hit.
    #
    # Two complementary guards prevent this:
    #   1. min_atr_floor_pts — hard floor in price points.  The effective ATR
    #      used for stop/range computation is never allowed below this value,
    #      regardless of what the rolling computation returns.
    #      Set to ~80% of expected quiet-market ATR (e.g. 30 for BTCUSD-PERP).
    #
    #   2. recent_range_atr_factor — the effective ATR also can't drop below
    #      (5-min hi-lo × this factor).  This catches ATR-compression-during-
    #      surge: even if the rolling ATR is low, if price moved 100 pts in the
    #      last 5 minutes the stop must account for that range.  Default 0.5
    #      means the effective ATR is at least half the recent 5-min swing.
    "min_atr_floor_pts":       30.0,  # hard floor for effective ATR (price points)
    "recent_range_atr_factor": 0.5,   # effective ATR >= recent_5min_range × this

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
    "auto_restart_stability_minutes": 60,    # look-back window for the RANGE (hi-lo) check
    "auto_restart_stability_atr_mult": 7.75, # hi-lo < N × ATR; 7.75 = sqrt(60) scales 1-min ATR to 60-min window
    "auto_restart_range_percentile":  0.05,  # hi/lo taken as this/its-complement percentile of the
                                             # 60-min window instead of raw min/max (0.0 = raw min/max).
                                             # 2026-07-09/10 log: hi-lo sat pinned at a single old extreme
                                             # tick's value for long stretches, only dropping once that one
                                             # tick fully aged out of the 60-min window rather than decaying
                                             # smoothly. 0.05 (5th/95th pctile) trims a handful of isolated
                                             # outlier ticks/wicks while still requiring genuinely broad calm
                                             # if the chop is real and sustained across most of the window.
    "auto_restart_trend_minutes":     15,    # SEPARATE, shorter look-back for the mean used by the
                                             # flat/rising-trend check (condition 4 below). 2026-07-10 log:
                                             # after an earlier bounce peak, price was already flat for
                                             # ~25 minutes, but the 60-min mean (shared with the range
                                             # check) kept chasing down toward it, so already-stable price
                                             # kept testing as "below mean" (a fake downtrend) until the
                                             # full 60-min window finally rolled past the old peak. A
                                             # shorter, separate window lets this check track *recent*
                                             # price action instead of an hour-old bounce. Falls back to
                                             # auto_restart_stability_minutes if too sparse.
    "auto_restart_recovery_atr_buffer": 0.5, # price gate: initial buffer at halt time:
                                             # floor = halt_stop - buffer×ATR.  Decays over time
                                             # (see recovery_floor_decay_atr_per_hour below).
                                             # The _rebuild_grid stop-proximity guard is the second
                                             # line of defence: if price is truly too close to the
                                             # new stop after restart, the rebuild aborts.
                                             # Set to 0.0 for strict (mid must exceed halt_stop).
    "auto_restart_recovery_floor_decay_atr_per_hour": 3.0,
                                             # The recovery floor drops by this many ATRs per hour
                                             # of halted time.  After 2h at ATR=35 the floor has
                                             # dropped 210 pts below halt_stop, letting the bot
                                             # restart even if price never fully recovered.
                                             # 2026-07-10 SL2: halt_stop=64415, ATR=34.85, floor
                                             # at t=0: 64398.  BTC dropped to 63990 (-425 pts).
                                             # With decay=3.0: after 4h floor = 64415 - (0.5+12)×35
                                             # = 63977 — bot restarts into stable overnight market.
                                             # Set to 0.0 to disable decay (fixed floor).
    "auto_restart_recovery_floor_min_atr": 15.0,
                                             # The decayed floor is never allowed to drop more than
                                             # this many ATRs below halt_stop (absolute lower bound).
                                             # Default 15 → floor never goes more than 15×ATR below
                                             # halt_stop regardless of how long the bot has been halted.
    "auto_restart_max_attempts":      3,     # give up after N failed attempts; 0 = unlimited
    "auto_restart_attempt_reset_hours": 24,  # if the grid has been running healthily (no halt)
                                             # for this long since the last auto-restart, the
                                             # attempt counter is cleared before counting the
                                             # next halt. Prevents attempts accumulated over
                                             # separate, unrelated halt events (days/weeks apart)
                                             # from permanently exhausting max_attempts. 0 = never
                                             # reset (old lifetime-counter behaviour).

    # ── Proactive stop-score gate ─────────────────────────────────────────────
    # After a SELL fill, before placing the counter-BUY order, the bot computes
    # a composite stop-loss risk score from three real-time signals:
    #
    #   Proximity  (weight 0.40):
    #     (stop_price − mid) / ATR — how many ATRs away is the stop right now?
    #     Clamped to [0, 1] where 1 = mid has reached the stop.
    #
    #   Velocity   (weight 0.35):
    #     EMA of (prev_mid − mid) / ATR over the last N ticks.
    #     Captures the speed and direction of price movement; values > 0 mean
    #     price is falling, scaled by how large the move is relative to ATR.
    #     Clamped to [0, 1].
    #
    #   Volatility (weight 0.25):
    #     (ATR / mean_ATR) − 1, clamped to [0, 1].
    #     Fires when ATR has expanded relative to its recent mean, indicating
    #     a volatility regime shift that elevates stop-loss risk.
    #
    #   score = proximity × 0.40 + velocity × 0.35 + volatility × 0.25
    #
    # If score ≥ stop_score_threshold, the counter-BUY is suppressed: the level
    # is set to SUPPRESSED instead of BUY_OPEN so _replace_idle_levels() skips
    # it.  This lets the position close gradually as remaining sell orders fill,
    # without adding new longs into a deteriorating market.
    #
    # Recovery: when score drops back to ≤ stop_score_resume_threshold, the bot
    # releases one SUPPRESSED level per main-loop tick (every ~100ms), starting
    # from the highest index (closest to mid, least exposed), so position rebuilds
    # slowly and can be re-suppressed if conditions worsen again.
    #
    # Set stop_score_enabled=False to disable entirely (gate becomes a no-op).
    "stop_score_enabled":           True,
    "stop_score_threshold":         0.25,   # suppress buy if score ≥ this
                                            # 0.25 = suppress when mid is within ~2.25×ATR
                                            # of stop (with proximity_atr_scale=3).
                                            # Calibrated from 2026-07-08 log where peak
                                            # score was 0.236 immediately before the SL.
                                            # calibrated from 2026-07-08 log: score peaked
                                            # at 0.236 in the 10 min before SL triggered;
                                            # 0.25 would have suppressed the final buy.
                                            # old default was 0.6 (too conservative — gate
                                            # never fired in practice).
    "stop_score_resume_threshold":  0.10,   # release one suppressed level per tick when ≤ this
                                            # asymmetric gap (0.25 gate vs 0.10 resume) prevents
                                            # rapid oscillation at the boundary.
    "stop_score_velocity_ticks":    30,     # number of recent ticks for velocity EMA (default ~3s at 10Hz)
    "stop_score_proximity_atr_scale": 3.0, # headroom (in ATRs) at which proximity = 1.0 (full danger)
                                            # e.g. 3.0 → score contribution ramps from 0→max over the
                                            # last 3×ATR above the stop.  Lower = more sensitive.
    "stop_score_weight_proximity":  0.40,
    "stop_score_weight_velocity":   0.35,
    "stop_score_weight_volatility": 0.25,
    # ── BuyGate auto-calibration ──────────────────────────────────────────────
    # After each stop-loss event, _calibrate_threshold() records the peak
    # score observed in the N seconds before the halt and uses it to nudge
    # the threshold downward toward (peak × safety_margin).  An EMA damps
    # updates so one extreme event does not over-steer.
    # The threshold is persisted in the meta DB table (key='bugate_threshold')
    # so calibration survives restarts.  It is never lowered below
    # stop_score_threshold_floor or raised above the configured default.
    "stop_score_calib_enabled":       True,
    "stop_score_calib_lookback_s":    120,   # seconds before halt to scan for peak score
    "stop_score_calib_safety_margin": 0.90,  # target = peak_score × margin (< 1 leaves headroom)
    "stop_score_calib_ema_alpha":     0.40,  # EMA weight for new calibration signal (0=ignore, 1=replace)
    "stop_score_threshold_floor":     0.12,  # never auto-lower below this (safety floor)

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
    # Reconnect flood alert: send a Telegram alert when the WS reconnects
    # more than ws_reconnect_alert_count times within ws_reconnect_alert_window_s
    # seconds.  Helps catch persistent upstream feed instability before it
    # causes missed fills or stale price decisions.
    "ws_reconnect_alert_count":    3,     # alert threshold (reconnects in window)
    "ws_reconnect_alert_window_s": 300,   # rolling window in seconds (default 5 min)
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
# Security: Telegram bot tokens live in the URL path itself (api.telegram.org/
# bot<TOKEN>/method), so any exception or response string that echoes the URL
# (connection errors, timeouts, HTTP error bodies) leaks the token straight
# into logs. Every Telegram call site below scrubs its own known token out of
# whatever it logs, via this helper, before the message reaches `logger`.
# ─────────────────────────────────────────────────────────────────────────────

def _redact_secret(text: str, *secrets: str) -> str:
    """Replace any occurrence of a known secret substring with a placeholder
    before it is logged. Plain substring replacement (not regex) since the
    exact secret value is known at the call site — no risk of over/under
    matching unrelated content."""
    for s in secrets:
        if s:
            text = text.replace(s, "***REDACTED***")
    return text


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
                logger.warning(f"[AlertManager] attempt {attempt} error: "
                                f"{_redact_secret(str(e), self._token)}")
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
                logger.error(f"[TgPoller] Unexpected error in poll loop: "
                             f"{_redact_secret(str(e), self._token)}", exc_info=True)
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
            logger.warning(f"[TgPoller] getUpdates request error: "
                            f"{_redact_secret(str(e), self._token)}")
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
                logger.warning(f"[TgPoller] sendMessage failed: "
                                f"{_redact_secret(resp.text[:200], self._token)}")
        except requests.RequestException as e:
            logger.warning(f"[TgPoller] sendMessage error: "
                            f"{_redact_secret(str(e), self._token)}")


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
    _DOA_THRESHOLD_S        = 10
    _DOA_BACKOFF_STEP       = 60
    _DOA_MAX_BACKOFF        = 300
    _DOA_LONG_STREAK        = 5
    _DOA_LONG_PAUSE         = 1800
    # A disconnect after this many seconds of uptime resets the backoff
    # counter, so a brief glitch after hours of stability starts from init.
    _BACKOFF_RESET_STABLE_S = 60

    def __init__(self, name: str, url: str,
                 subscribe_msg_fn: Callable[[], List[dict]],
                 on_message_fn: Callable[[dict], None],
                 stale_s: float, backoff_init: float, backoff_max: float,
                 stop_event: threading.Event,
                 on_reconnect_fn: Optional[Callable[[], None]] = None) -> None:
        self._name             = name
        self._url              = url
        self._subscribe_msg_fn = subscribe_msg_fn
        self._on_message_fn    = on_message_fn
        self._stale_s          = stale_s
        self._backoff_init     = backoff_init
        self._backoff_max      = backoff_max
        self._stop             = stop_event
        # Optional callback fired on every successful reconnect (after the
        # first connect).  Used by GridBot to detect reconnect floods and
        # send a Telegram alert when the rate exceeds configured thresholds.
        self._on_reconnect_fn: Optional[Callable[[], None]] = on_reconnect_fn
        self._first_connect_done = False

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
                # Reset backoff if the connection was stable long enough,
                # so a brief glitch after hours of uptime starts from init.
                with self._connect_time_lock:
                    stable_s = time.time() - self._connect_time
                if stable_s >= self._BACKOFF_RESET_STABLE_S:
                    backoff = self._backoff_init
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
        # Fire reconnect callback for gen > 1 (skip the very first connect).
        if self._first_connect_done and self._on_reconnect_fn is not None:
            try:
                self._on_reconnect_fn()
            except Exception as e:
                logger.warning(f"[{self._name}] on_reconnect_fn error: {e}")
        self._first_connect_done = True
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
    HISTORY_WINDOW_S = 97200   # keep 27h — must exceed trend_signal_min_history_h (26h)

    # BUGFIX (2026-07-14): _history previously used maxlen=30000 as a hard
    # count-based cap alongside the HISTORY_WINDOW_S time-based cutoff below.
    # update_l1() is called on every raw WS tick (not once/minute), so on an
    # active feed the 30000-item cap was reached in well under 27h — e.g. at
    # ~1 tick/sec, 30000 ticks only covers ~8.3h. Once the cap bound, deque's
    # automatic maxlen eviction silently dropped the OLDEST entries (the very
    # DB/REST-seeded history that gave TrendSignal its 26h+ warmup) regardless
    # of whether they were actually older than HISTORY_WINDOW_S. This is what
    # caused TrendSignal to fall back into INSUFFICIENT_DATA hours after a
    # successful warmup, and to get progressively worse during high-volatility
    # periods (more WS ticks/min -> the fixed tick budget covers even less
    # wall-clock time).
    #
    # Fix: size the cap for a realistic worst-case sustained tick rate so the
    # time-based cutoff (popleft loop below) is what actually governs
    # retention, and the count cap only exists as a memory safety ceiling.
    # ~10 ticks/sec sustained for the full 27h window = 27*3600*10 = 972,000.
    # Round up with headroom. At ~50 bytes/tuple this is well under 100MB.
    # If [PriceCache] "_history near maxlen cap" warnings ever appear in the
    # logs, raise this further and/or investigate an unexpectedly high tick
    # rate from the WS feed.
    MAX_TICKS = 1_200_000

    def __init__(self):
        self._lock    = threading.Lock()
        self._bid: Optional[float] = None
        self._ask: Optional[float] = None
        self._mid: Optional[float] = None
        self._history: collections.deque = collections.deque(maxlen=self.MAX_TICKS)

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
            # Safety-net visibility: if we're anywhere near the count cap,
            # the time-based cutoff above is no longer the binding constraint
            # and we're at risk of silently losing history again. This should
            # never fire under MAX_TICKS's sizing assumptions; if it does,
            # the WS feed's tick rate is higher than provisioned for.
            if len(self._history) >= self._history.maxlen - 1000:
                oldest_age_h = (now - self._history[0][0]) / 3600.0
                logger.warning(
                    f"[PriceCache] _history near maxlen cap "
                    f"({len(self._history)}/{self._history.maxlen}) — "
                    f"oldest tick age={oldest_age_h:.2f}h "
                    f"(expected ~27h; count cap may be evicting before time cutoff)"
                )

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

    def compute_stability(self, window_minutes: int,
                           trend_window_minutes: Optional[int] = None,
                           range_percentile: float = 0.0) -> dict:
        """
        Compute price stability metrics used by the auto-restart check.

        Two DIFFERENT questions are being asked here, and one fixed 60-minute
        equal-weighted window answers both badly:

          "Have the big swings genuinely stopped?" — this benefits from a
          long, conservative look-back (window_minutes, default 60): we want
          confidence that wide swings have stopped for a good stretch, not
          just paused for a minute. But a single wide window's raw min/max is
          pinned by whatever the single most extreme tick was, for the WHOLE
          window duration, however calm everything since has been — it only
          drops the instant that one tick ages out, rather than decaying
          smoothly. range_percentile (e.g. 0.05 = 5th/95th percentile instead
          of raw min/max) makes hi/lo robust to one or two isolated outlier
          ticks/wicks while still requiring genuinely broad calm across the
          bulk of the window if the chop is real and sustained.

          "Is price flat-or-rising right now?" — this is inherently a
          SHORTER-horizon question, and reusing the same 60-minute window for
          it causes a real, observed problem: if price spikes/bounces once
          within the window and then goes flat, the 60-min mean keeps
          chasing down toward the new flat level for up to the full 60
          minutes, so already-stable price keeps testing as "below mean" —
          i.e. a fake "downtrend" — until the window finally rolls past the
          old peak. trend_window_minutes (default much shorter, e.g. 15) lets
          this check respond to recent price action instead of an hour-old
          bounce. Falls back to the full window if trend_window_minutes
          isn't given, or if the shorter window is too sparse.

        Returns a dict with:
          "hi"        — highest (or range_percentile-th percentile) mid price
                        over window_minutes
          "lo"        — lowest (or (1-range_percentile)-th percentile) mid
                        price over window_minutes
          "hi_lo"     — hi - lo (range)
          "mean"      — arithmetic mean of mid prices over trend_window_minutes
                        (or window_minutes if not given)
          "current"   — most recent mid price
          "n_ticks"   — number of ticks in the range window (quality indicator)
          "ok"        — False if insufficient data (< 10 ticks)
        """
        with self._lock:
            history = list(self._history)

        now    = time.time()
        cutoff = now - window_minutes * 60
        window = [mid for ts, mid in history if ts >= cutoff]

        if len(window) < 10:
            return {"ok": False, "hi": 0.0, "lo": 0.0, "hi_lo": 0.0,
                    "mean": 0.0, "current": 0.0, "n_ticks": len(window)}

        trend_min     = trend_window_minutes if trend_window_minutes else window_minutes
        trend_cutoff  = now - trend_min * 60
        trend_window  = [mid for ts, mid in history if ts >= trend_cutoff]
        if len(trend_window) < 10:
            trend_window = window   # too sparse — fall back to the full window

        if range_percentile and range_percentile > 0:
            ordered = sorted(window)
            lo_idx  = int(len(ordered) * range_percentile)
            hi_idx  = min(int(len(ordered) * (1 - range_percentile)), len(ordered) - 1)
            lo, hi  = ordered[lo_idx], ordered[hi_idx]
        else:
            hi = max(window)
            lo = min(window)

        mean    = sum(trend_window) / len(trend_window)
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
        self._cfg        = config
        self._cache      = cache
        self._recent_atrs: List[float] = []   # for adaptive stop buffer

    def _resolve_notional(self, levels: int, mid: float) -> float:
        """
        Derive notional_per_level from total_investment_usd or total_investment_btc.

        Priority:
          1. Legacy "notional_per_level" key present and non-zero → use directly
          2. "total_investment_btc" non-zero → convert to USD at current mid
          3. "total_investment_usd" non-zero → use as-is
          4. Fallback: 500.0 USD (keeps existing behaviour)

        Returns the USD notional to deploy per grid level.
        """
        # Legacy override
        legacy = self._cfg.get("notional_per_level", 0.0)
        if legacy and legacy > 0:
            return float(legacy)

        btc_inv = self._cfg.get("total_investment_btc", 0.0)
        usd_inv = self._cfg.get("total_investment_usd", 0.0)

        if btc_inv and btc_inv > 0 and mid > 0:
            total_usd = btc_inv * mid
            notional  = total_usd / max(levels, 1)
            logger.info(
                f"[AutoTuner] Investment: {btc_inv} BTC × {mid:.0f} = "
                f"${total_usd:.0f} / {levels} levels = ${notional:.2f}/level"
            )
            return notional

        if usd_inv and usd_inv > 0:
            notional = usd_inv / max(levels, 1)
            logger.info(
                f"[AutoTuner] Investment: ${usd_inv:.0f} / {levels} levels "
                f"= ${notional:.2f}/level"
            )
            return notional

        logger.warning("[AutoTuner] No investment amount configured — defaulting to $500/level")
        return 500.0

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
        stop_buf   = self._cfg.get("stop_buffer_atr", 3.0)
        maker_fee  = self._cfg.get("maker_fee_rate", 0.0001)
        min_sp_pct = self._cfg.get("min_grid_pct", 0.0008)
        max_levels = self._cfg.get("max_grid_levels", 50)
        min_levels = self._cfg.get("min_grid_levels", 5)

        # ── Effective ATR: floor + recent-range guard ─────────────────────────
        # During fast directional moves (surges/crashes) the rolling 1-min ATR
        # compresses: all candles are narrow and same-direction, so their true-
        # range is tiny.  A compressed ATR produces a dangerously tight stop.
        # 2026-07-10 SL1: ATR compressed to 28.67, stop placed only 87 pts
        # below mid; a 174-pt retracement immediately triggered it.
        #
        # Guard 1 — hard floor in price points:
        #   effective_atr >= min_atr_floor_pts
        # Guard 2 — recent 5-min range scaling:
        #   effective_atr >= hi-lo over last 5 min * recent_range_atr_factor
        # The two guards are independent maximums; either can raise the ATR.
        atr_floor   = self._cfg.get("min_atr_floor_pts", 30.0)
        range_factor = self._cfg.get("recent_range_atr_factor", 0.5)
        effective_atr = atr
        if atr < atr_floor:
            logger.info(
                f"[AutoTuner] ATR {atr:.2f} below floor {atr_floor:.2f} "
                f"— clamping effective ATR to floor"
            )
            effective_atr = atr_floor
        recent_stab = self._cache.compute_stability(5)
        if recent_stab["ok"]:
            range_atr_min = recent_stab["hi_lo"] * range_factor
            if range_atr_min > effective_atr:
                logger.info(
                    f"[AutoTuner] Recent 5-min range {recent_stab['hi_lo']:.2f} "
                    f"× {range_factor} = {range_atr_min:.2f} > effective ATR "
                    f"{effective_atr:.2f} — raising effective ATR to {range_atr_min:.2f}"
                )
                effective_atr = range_atr_min
        if effective_atr != atr:
            logger.info(
                f"[AutoTuner] effective_atr={effective_atr:.2f} "
                f"(raw ATR={atr:.2f})"
            )

        # ── Adaptive stop buffer ──────────────────────────────────────────────
        # If ATR has expanded sharply vs its own recent mean, widen the buffer
        # proportionally to protect against sudden volatility regime shifts.
        expansion_threshold = self._cfg.get("stop_buffer_atr_expansion_threshold", 1.5)
        max_mult            = self._cfg.get("stop_buffer_atr_max_mult", 2.0)
        recent_atrs = self._recent_atrs
        recent_atrs.append(effective_atr)
        if len(recent_atrs) > 20:          # keep last 20 builds (~20 retune events)
            recent_atrs.pop(0)
        if len(recent_atrs) >= 3:
            mean_atr = sum(recent_atrs[:-1]) / len(recent_atrs[:-1])
            if mean_atr > 0:
                expansion_ratio = effective_atr / mean_atr
                if expansion_ratio > expansion_threshold:
                    adaptive_mult = min(expansion_ratio / expansion_threshold, max_mult)
                    old_buf = stop_buf
                    stop_buf = round(stop_buf * adaptive_mult, 2)
                    logger.info(
                        f"[AutoTuner] ATR expansion detected: "
                        f"effective_atr={effective_atr:.2f} vs mean={mean_atr:.2f} "
                        f"(ratio={expansion_ratio:.2f}x) -> "
                        f"stop_buffer {old_buf}xATR -> {stop_buf}xATR"
                    )

        lower = round(mid - atr_mult * effective_atr, 2)
        upper = round(mid + atr_mult * effective_atr, 2)
        stop  = round(lower - stop_buf * effective_atr, 2)

        min_spacing = max(min_sp_pct * mid, 2.0 * maker_fee * mid * 1.5)
        raw_levels  = int((upper - lower) / min_spacing)
        levels      = max(min_levels, min(max_levels, raw_levels))
        spacing     = round((upper - lower) / levels, 2)

        notional = self._resolve_notional(levels, mid)
        logger.info(
            f"[AutoTuner] mid={mid:.2f} ATR={atr:.2f} "
            f"effective_atr={effective_atr:.2f} "
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
        spacing = round((upper - lower) / max(levels, 1), 2)
        notional = self._resolve_notional(levels, mid)
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

    def get_mean_atr(self) -> Optional[float]:
        """
        Return the mean of recent ATR samples collected during compute() calls
        (excluding the most recent sample, same as the adaptive stop-buffer uses).
        Returns None if fewer than 3 samples are available.
        Used by StopScoreCalculator to measure ATR expansion vs its own history.
        """
        if len(self._recent_atrs) < 3:
            return None
        history = self._recent_atrs[:-1]   # exclude current sample, same as adaptive buffer
        return sum(history) / len(history)


# ─────────────────────────────────────────────────────────────────────────────
# Grid level state
# ─────────────────────────────────────────────────────────────────────────────

class LevelState(Enum):
    IDLE       = "IDLE"
    BUY_OPEN   = "BUY_OPEN"
    SELL_OPEN  = "SELL_OPEN"
    SUPPRESSED = "SUPPRESSED"   # buy suppressed by stop-score gate; skipped by _replace_idle_levels


@dataclass
class GridLevel:
    index:      int
    price:      float
    state:      LevelState = LevelState.IDLE
    client_oid: str        = ""
    qty:        float      = 0.0
    placed_at:  float      = 0.0   # epoch time this order was (re)placed;
                                    # used by paper_fill_min_resting_s guard


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
                 store: Optional["GridStateStore"] = None,
                 buy_gate_fn: Optional[Callable[[], bool]] = None):
        self._params     = params
        self._oms        = oms
        self._instrument = instrument
        self._cfg        = config
        self._store      = store          # may be None in tests / paper mode without DB
        # buy_gate_fn: optional callable → bool.  Called before every counter-BUY
        # placement after a SELL fill.  Return True to ALLOW the buy, False to
        # SUPPRESS it (level is set to SUPPRESSED instead of placing an order).
        # None = no gate (legacy behaviour, always allow).
        self._buy_gate_fn: Optional[Callable[[], bool]] = buy_gate_fn
        self._lock       = threading.Lock()
        self._levels: List[GridLevel] = []
        self._stop_event = threading.Event()
        self._last_drift_shift: float = 0.0   # epoch time of last sell-triggered shift
        self._needs_rebuild:   bool  = False  # set by drift-shift when mid is far OOB

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

        # Track client_oids of SELL orders placed at grid startup (_place_initial_orders).
        # These SELLs have no corresponding BUY fill in this session — they are the
        # initial resting asks placed above mid, not exits from a real long position.
        # When they fill, _long_qty must NOT be decremented (there is nothing to close).
        # Without this guard, startup SELLs filling before their counter-BUYs makes
        # _long_qty go negative, which shows "short=" in the Status line even though
        # no actual short exists.
        self._initial_sell_oids: set = set()
        self._placing_initial:   bool = False   # True only during _place_initial_orders

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
        self._placing_initial = True
        try:
            for lv in levels:
                if self._stop_event.is_set():
                    break
                if lv.price < mid:
                    self._place_buy(lv)
                elif lv.price > mid:
                    self._place_sell(lv)
                time.sleep(0.05)
        finally:
            self._placing_initial = False

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
            lv.placed_at  = time.time()
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
            lv.placed_at  = time.time()
            # Mark as an initial sell so _on_fill skips the long_qty decrement.
            if self._placing_initial:
                self._initial_sell_oids.add(req.client_oid)
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

        min_resting_s = self._cfg.get("paper_fill_min_resting_s", 1.5)
        now = time.time()

        for lv in levels:
            filled = False
            if lv.state == LevelState.BUY_OPEN  and mid <= lv.price:
                filled = True
            elif lv.state == LevelState.SELL_OPEN and mid >= lv.price:
                filled = True

            if filled and min_resting_s > 0 and (now - lv.placed_at) < min_resting_s:
                # Order hasn't rested long enough to be a realistic fill yet —
                # a real exchange needs at least one round-trip before a resting
                # limit order can be crossed. Defer to a later tick; re-checked
                # every tick until either it fills (once aged past the floor)
                # or price moves away and the crossing condition no longer holds.
                filled = False

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
        """Re-place any IDLE levels that should have an order.
        SUPPRESSED levels are intentionally skipped — they are managed by
        GridBot._run() via release_one_suppressed_level() once the stop-score
        recovers, so they must not be re-queued here."""
        mid = _price_cache.get_mid()
        if mid is None:
            return
        with self._lock:
            idle = [lv for lv in self._levels
                    if lv.state == LevelState.IDLE]     # SUPPRESSED excluded
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
            net = self._long_qty
            net_label = f"long={net:.4f}" if net >= 0 else f"short={-net:.4f}"
            logger.info(
                f"[GridEngine] FILL BUY  [{idx}] @ {fill.avg_price:.2f} "
                f"qty={fill.filled_qty:.4f} fee={fill.fee:.6f} "
                f"{net_label} BTC"
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
            # Skip long_qty decrement for SELLs placed at grid startup.
            # Those orders were placed above mid before any BUY fill existed in
            # this session — decrementing would drive long_qty negative ("short=")
            # even though no real short position exists.
            is_initial_sell = fill.client_oid in self._initial_sell_oids
            if is_initial_sell:
                self._initial_sell_oids.discard(fill.client_oid)
                logger.debug(
                    f"[GridEngine] SELL [{idx}] @ {fill.avg_price:.2f} is an initial"
                    f" sell — skipping long_qty decrement (was {self._long_qty:.4f})"
                )
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
            suppress = False
            buy_idx = idx - 1
            with self._lock:
                if buy_idx >= 0:
                    candidate = self._levels[buy_idx]
                    if candidate.state == LevelState.IDLE:
                        # Run the buy gate before committing to place the order.
                        # Gate returns True = allow, False = suppress.
                        if self._buy_gate_fn is not None and not self._buy_gate_fn():
                            candidate.state = LevelState.SUPPRESSED
                            suppress = True
                            logger.info(
                                f"[GridEngine] BUY [{buy_idx}] suppressed by stop-score gate "
                                f"(sell fill at [{idx}] @ {fill.avg_price:.2f})"
                            )
                        else:
                            buy_lv = candidate
            if suppress:
                self._alerter_send(
                    f"🛡 Buy [{buy_idx}] suppressed — stop-score gate active"
                )
            elif buy_lv is not None:
                self._place_buy(buy_lv)

            # ── Drift-shift: top-level sell → shift range up one spacing ──────
            # If this fill was the top-level SELL, price has drifted above the
            # grid.  Shift the whole range up immediately via _trail_up so the
            # grid stays centred on price rather than accumulating all-long
            # exposure as the lower bound creeps toward the stop.
            if self._cfg.get("drift_shift_on_top_sell", True):
                with self._lock:
                    is_top        = len(self._levels) > 0 and idx == len(self._levels) - 1
                    current_lower = self._levels[0].price  if self._levels else 0.0
                    current_upper = self._levels[-1].price if self._levels else 0.0
                    spacing       = self._params.spacing
                if is_top:
                    min_interval = self._cfg.get("drift_shift_min_interval_s", 60)
                    now_t = time.time()
                    if now_t - self._last_drift_shift >= min_interval:
                        # Guard: if mid is already above the price where the new
                        # SELL level would be placed (current_upper + spacing),
                        # the trail step would immediately fill the new level in
                        # paper mode — and if mid is multiple spacings above, this
                        # cascades into several instant fills and a phantom-negative
                        # long_qty.  Request a full rebuild instead so the grid
                        # re-centres cleanly on the current price.
                        mid_now   = _price_cache.get_mid() or current_upper
                        new_upper = current_upper + spacing
                        far_oor   = mid_now > new_upper
                        if far_oor:
                            logger.info(
                                f"[GridEngine] Top-sell fill at [{idx}] — "
                                f"mid={mid_now:.2f} already above new-upper={new_upper:.2f} "
                                f"→ requesting full rebuild instead of drift-shift"
                            )
                            self._needs_rebuild = True
                        else:
                            self._last_drift_shift = now_t
                            logger.info(
                                f"[GridEngine] Top-sell fill at [{idx}] → "
                                f"drift-shift UP: [{current_lower:.2f},{current_upper:.2f}] "
                                f"+{spacing:.2f}"
                            )
                            self._trail_up(current_lower, current_upper, spacing)
                    else:
                        logger.info(
                            f"[GridEngine] Top-sell fill at [{idx}] — drift-shift "
                            f"throttled ({now_t - self._last_drift_shift:.0f}s < "
                            f"{min_interval}s interval)"
                        )

    def release_one_suppressed_level(self) -> bool:
        """
        Release the highest-index SUPPRESSED level (closest to mid, least
        exposed) by placing its BUY order.  Returns True if a level was
        released, False if none were suppressed.

        Called by GridBot._run() once per tick when stop-score has recovered
        below the resume threshold, so position rebuilds gradually rather than
        all at once.
        """
        with self._lock:
            # Find the highest-index SUPPRESSED level (closest to mid)
            target = None
            for lv in reversed(self._levels):
                if lv.state == LevelState.SUPPRESSED:
                    target = lv
                    break
            if target is None:
                return False
            # Reset to IDLE before releasing the lock — _place_buy() will
            # re-acquire the lock to set it to BUY_OPEN.
            target.state = LevelState.IDLE

        self._place_buy(target)
        logger.info(
            f"[GridEngine] BUY [{target.index}] @ {target.price:.2f} "
            f"released from SUPPRESSED (stop-score recovered)"
        )
        return True

    def count_suppressed(self) -> int:
        """Return number of levels currently in SUPPRESSED state."""
        with self._lock:
            return sum(1 for lv in self._levels if lv.state == LevelState.SUPPRESSED)

    def pop_needs_rebuild(self) -> bool:
        """
        Return True (and clear the flag) if the engine has requested a full
        grid rebuild.  Called once per _run() tick; GridBot calls _rebuild_grid()
        if this returns True.

        Currently set by drift-shift when mid has moved so far above the grid
        that a single trail step would immediately fill the new SELL level and
        leave the grid still misaligned — a cascade of instant paper fills that
        produces a phantom-negative long_qty.
        """
        flag = self._needs_rebuild
        self._needs_rebuild = False
        return flag

    def _get_paired_buy_price(self, sell_idx: int) -> Optional[float]:
        with self._lock:
            buy_idx = sell_idx - 1
            if 0 <= buy_idx < len(self._levels):
                return self._levels[buy_idx].price
        return None

    def get_cost_basis(self) -> Tuple[float, float]:
        """
        Weighted-average cost basis of the net long position, expressed as
        (qty, avg_price), where qty matches _long_qty (the running counter
        of filled BUYs minus filled SELLs) rather than the raw count of
        SELL_OPEN levels.

        Why the two can diverge: _long_qty can go negative during a rapid
        price rally (SELL fills outpace BUY counter-fills).  When it later
        recovers through zero back into positive territory the BUY fills
        that covered the short are absorbed first; only the remaining qty
        represents a genuine long entry.  Summing ALL SELL_OPEN levels in
        that state would overcount and inflate the cost basis.

        Algorithm: collect SELL_OPEN levels sorted lowest-index first
        (i.e. lowest buy price first, matching the most recent entries),
        accumulate until total_qty == _long_qty, then stop.  If _long_qty
        <= 0 there is no net long and we return (0.0, 0.0).

        Must be called before stop() tears down level state.
        """
        with self._lock:
            net_long = self._long_qty
            if net_long <= 0.0:
                return 0.0, 0.0

            # Collect SELL_OPEN levels in ascending index order (lowest buy
            # price first = most recently entered positions when price fell).
            candidates = []
            for lv in self._levels:
                if lv.state == LevelState.SELL_OPEN and lv.qty > 0:
                    buy_idx   = lv.index - 1
                    buy_price = (self._levels[buy_idx].price
                                 if 0 <= buy_idx < len(self._levels) else lv.price)
                    candidates.append((lv.index, buy_price, lv.qty))
            candidates.sort(key=lambda x: x[0])

            # Accumulate only enough levels to match net_long.
            total_qty  = 0.0
            total_cost = 0.0
            for _, buy_price, qty in candidates:
                take = min(qty, net_long - total_qty)
                total_cost += buy_price * take
                total_qty  += take
                if total_qty >= net_long - 1e-9:
                    break

        avg_price = (total_cost / total_qty) if total_qty > 0 else 0.0
        return total_qty, avg_price

    # ── Stats ─────────────────────────────────────────────────────────────────

    def get_stats(self) -> dict:
        with self._lock:
            open_buys   = sum(1 for lv in self._levels if lv.state == LevelState.BUY_OPEN)
            open_sells  = sum(1 for lv in self._levels if lv.state == LevelState.SELL_OPEN)
            suppressed  = sum(1 for lv in self._levels if lv.state == LevelState.SUPPRESSED)
        return {
            "levels":       len(self._levels),
            "open_buys":    open_buys,
            "open_sells":   open_sells,
            "suppressed":   suppressed,
            "long_qty":     round(self._long_qty, 4),
            "realized_pnl": round(self._realized_pnl, 4),
            "total_fees":   round(self._total_fees, 6),
            "net_pnl":      round(self._realized_pnl - self._total_fees, 4),
            "cycles":       self._cycle_count,
        }

    def get_params(self) -> "GridParams":
        """Return the engine's current GridParams.

        _trail_up / _trail_down mutate self._params in place, so this always
        reflects the live grid boundaries — unlike GridBot._params which is
        only updated on full rebuilds.  Thread-safe: GridParams is a value
        object and Python attribute reads are atomic; no lock needed.
        """
        return self._params

    def update_stop_price(self, new_stop: float):
        """Update stop_price on the engine's own GridParams in place.

        Needed because the dead-band stop-raise in GridBot._rebuild_grid()
        only updates GridBot._params and the StopLossGuard — it never touches
        the engine's copy of GridParams. Since _log_status() (and the
        Telegram /status handler's engine-derived fields) read from
        self._engine.get_params(), the stop shown there kept lagging behind
        the real, active stop after every dead-band raise. Call this
        immediately after updating GridBot._params.stop_price so both copies
        stay in sync. Rebuilds a new GridParams (value object) under lock,
        mirroring the _trail_up / _trail_down pattern above.
        """
        with self._lock:
            self._params = GridParams(
                lower=self._params.lower,
                upper=self._params.upper,
                levels=self._params.levels,
                spacing=self._params.spacing,
                stop_price=new_stop,
                notional_per_level=self._params.notional_per_level,
            )


# ─────────────────────────────────────────────────────────────────────────────
# Stop-score calculator  (proactive buy-gate signal)
# ─────────────────────────────────────────────────────────────────────────────

class StopScoreCalculator:
    """
    Computes a composite stop-loss risk score in [0, 1] from three real-time
    signals.  Used by GridEngine._on_fill() to decide whether to suppress the
    counter-BUY after a SELL fill, and by GridBot._run() to decide when to
    release suppressed levels as conditions recover.

    Three components (all independently clamped to [0, 1]):

      Proximity  (default weight 0.40)
        How close is the current mid to the stop price, in ATR units.
        raw = max(0, (stop_price - mid) / ATR)
        Equals 0 when mid is at the stop or above; ramps toward 1 as mid
        approaches the stop.  Hard-clamped at 1.0.

      Velocity   (default weight 0.35)
        Exponential moving average of per-tick price drops, normalised by ATR.
        Computed over the last stop_score_velocity_ticks price updates.
        raw = EMA(max(0, prev_mid - mid) / ATR)
        Positive only on falling ticks; rising ticks contribute 0.  Clamped at 1.

      Volatility (default weight 0.25)
        ATR expansion relative to its own recent mean (from GridAutoTuner's
        _recent_atrs list, exposed via get_mean_atr()).
        raw = max(0, ATR / mean_ATR - 1)  clamped at 1.
        Fires when the current ATR is meaningfully above its recent mean, which
        often precedes or accompanies a directional breakdown.

    score = proximity × w_prox + velocity × w_vel + volatility × w_vol
    """

    def __init__(self, config: dict, cache: PriceCache,
                 auto_tuner: "GridAutoTuner"):
        self._cfg       = config
        self._cache     = cache
        self._tuner     = auto_tuner
        self._enabled   = config.get("stop_score_enabled", True)

        # Velocity EMA state
        vel_ticks         = max(2, config.get("stop_score_velocity_ticks", 30))
        self._vel_alpha   = 2.0 / (vel_ticks + 1)   # standard EMA smoothing factor
        self._vel_ema:    float = 0.0
        self._prev_mid:   Optional[float] = None

        # Weights (normalised to sum to 1.0 for safety)
        w_prox = config.get("stop_score_weight_proximity",  0.40)
        w_vel  = config.get("stop_score_weight_velocity",   0.35)
        w_vol  = config.get("stop_score_weight_volatility", 0.25)
        total  = w_prox + w_vel + w_vol
        if total > 0:
            self._w_prox = w_prox / total
            self._w_vel  = w_vel  / total
            self._w_vol  = w_vol  / total
        else:
            self._w_prox, self._w_vel, self._w_vol = 0.40, 0.35, 0.25

    def compute(self, mid: float, stop_price: float) -> float:
        """
        Returns score in [0, 1].  0.0 if disabled or ATR is unavailable.
        Updates internal velocity EMA as a side effect — call once per tick.
        """
        if not self._enabled:
            return 0.0

        atr = self._cache.compute_atr(self._cfg.get("atr_lookback_minutes", 1440))
        if atr is None or atr <= 0:
            return 0.0

        # ── Proximity ────────────────────────────────────────────────────────
        # Measures how close the current mid is to the stop, normalised by ATR.
        #
        # Formula: 1 - ((mid - stop) / (atr × proximity_atr_scale))
        #   • When mid is far above stop: (mid-stop)/denom >> 1 → clamped to 0 (safe)
        #   • When mid == stop:           (mid-stop)/denom = 0  → proximity = 1 (danger)
        #   • proximity_atr_scale controls how many ATRs of headroom = "full danger"
        #     default 3 → proximity reaches 1.0 when mid is within 3×ATR of stop
        #
        # The old formula (stop-mid)/atr was INVERTED: it returned values > 1
        # when mid was safely above stop (clamped to 1.0 = max danger, always!),
        # making the proximity component useless as a discriminator.
        prox_scale = self._cfg.get("stop_score_proximity_atr_scale", 3.0)
        headroom   = max(0.0, mid - stop_price)          # 0 if mid already at/below stop
        proximity  = max(0.0, 1.0 - headroom / (atr * prox_scale))

        # ── Velocity (EMA of per-tick downward moves normalised by ATR) ──────
        if self._prev_mid is not None:
            drop = max(0.0, self._prev_mid - mid)
            raw_vel = min(1.0, drop / atr)
            self._vel_ema = (self._vel_alpha * raw_vel
                             + (1.0 - self._vel_alpha) * self._vel_ema)
        self._prev_mid = mid
        velocity = min(1.0, self._vel_ema)

        # ── Volatility (ATR expansion vs recent mean) ─────────────────────
        mean_atr = self._tuner.get_mean_atr()
        if mean_atr and mean_atr > 0:
            volatility = min(1.0, max(0.0, atr / mean_atr - 1.0))
        else:
            volatility = 0.0

        score = (self._w_prox * proximity
                 + self._w_vel  * velocity
                 + self._w_vol  * volatility)
        return round(min(1.0, max(0.0, score)), 4)

    def reset_velocity(self) -> None:
        """Reset velocity EMA on grid rebuild so stale fall history doesn't carry over."""
        self._vel_ema  = 0.0
        self._prev_mid = None

    def compute_trend_risk(self, mid: float, trend_regime: str = "NEUTRAL",
                            trend_slope_pct: float = 0.0) -> float:
        """
        Real-time "downtrend-strengthening" risk score in [0, 1] — distinct
        from compute()'s stop-proximity score. Used by GridBot._rebuild_grid()
        to decide, in real time, whether current conditions look like a
        genuine strengthening decline (raise the stop quickly/aggressively to
        lock in protection) or short-term noise (raise slowly/conservatively
        to avoid a whipsaw stop-out like SL1/SL2).

        Three components (independently clamped to [0, 1]):

          Velocity   — the same tick-level EMA of downward moves as compute()
                       (self._vel_ema). NOT recomputed here — call compute()
                       once per tick elsewhere to keep it fresh; this method
                       just reads the current value.

          Volatility — ATR expansion vs its own recent mean, same calculation
                       as compute()'s volatility component.

          Regime     — TrendSignal's hourly dual-EMA regime, passed in by the
                       caller (GridBot holds the TrendSignal instance).
                       Zero unless trend_regime == "DOWN"; when DOWN, scaled
                       by how far the fast EMA has slipped below the slow EMA
                       (trend_slope_pct), normalised by
                       trend_risk_regime_slope_norm_pct.

        score = w_vel × velocity + w_vol × volatility + w_regime × regime_risk
        (weights normalised to sum to 1.0)
        """
        if not self._enabled:
            return 0.0

        atr = self._cache.compute_atr(self._cfg.get("atr_lookback_minutes", 1440))
        if atr is None or atr <= 0:
            return 0.0

        velocity = min(1.0, self._vel_ema)

        mean_atr = self._tuner.get_mean_atr()
        volatility = (min(1.0, max(0.0, atr / mean_atr - 1.0))
                      if mean_atr and mean_atr > 0 else 0.0)

        slope_norm = max(1e-9, self._cfg.get("trend_risk_regime_slope_norm_pct", 0.5))
        regime_risk = (min(1.0, abs(trend_slope_pct) / slope_norm)
                       if trend_regime == "DOWN" else 0.0)

        w_vel = self._cfg.get("trend_risk_weight_velocity",   0.40)
        w_vol = self._cfg.get("trend_risk_weight_volatility", 0.25)
        w_reg = self._cfg.get("trend_risk_weight_regime",     0.35)
        total = w_vel + w_vol + w_reg
        if total <= 0:
            return 0.0

        score = (w_vel * velocity + w_vol * volatility + w_reg * regime_risk) / total
        return round(min(1.0, max(0.0, score)), 4)


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


def _risk_interp(risk: float, low_val: float, high_val: float) -> float:
    """
    Linearly interpolate between low_val (risk=0.0) and high_val (risk=1.0).
    Used to scale the dead-band stop-raise cap/confirm-window/EMA-alpha by
    the real-time trend_risk score — see StopScoreCalculator.compute_trend_risk().
    risk is clamped to [0, 1] before interpolating.
    """
    r = min(1.0, max(0.0, risk))
    return low_val + (high_val - low_val) * r


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

_GRID_DB_SCHEMA_VERSION = 3

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
    cycle_count   INTEGER NOT NULL DEFAULT 0,
    sl_gross_usd  REAL NOT NULL DEFAULT 0.0,  -- stop-loss gross PnL (always ≤ 0)
    sl_count      INTEGER NOT NULL DEFAULT 0   -- number of stop-loss liquidation events
);

-- Key/value metadata store.
CREATE TABLE IF NOT EXISTS meta (
    key   TEXT PRIMARY KEY,
    value TEXT NOT NULL
);

-- Persisted 1-minute candle history for TrendSignal warm-up.
-- On startup the bot loads these rows into PriceCache._history so TrendSignal
-- has 26h of data immediately without waiting for live ticks or hammering REST.
-- Rows older than 27h are pruned on each save to bound table size.
-- ts_bucket is the Unix minute bucket (int(candle_open_time_s // 60)).
-- Storing OHLC lets us reconstruct the same 4-tick injection used by ATR seed.
CREATE TABLE IF NOT EXISTS candle_cache (
    ts_bucket INTEGER PRIMARY KEY,   -- Unix minute number (ts_s // 60)
    open_px   REAL NOT NULL,
    high_px   REAL NOT NULL,
    low_px    REAL NOT NULL,
    close_px  REAL NOT NULL
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
            else:
                db_ver = int(row["value"])
                # v1->v2: candle_cache table added (CREATE IF NOT EXISTS handles DDL).
                if db_ver < 2:
                    logger.info("[GridStateStore] schema migrating v1 -> v2 (candle_cache)")
                # v2->v3: sl_gross_usd + sl_count columns added to daily_pnl.
                if db_ver < 3:
                    for col, typedef in [
                        ("sl_gross_usd", "REAL NOT NULL DEFAULT 0.0"),
                        ("sl_count",     "INTEGER NOT NULL DEFAULT 0"),
                    ]:
                        try:
                            self._conn.execute(
                                f"ALTER TABLE daily_pnl ADD COLUMN {col} {typedef}"
                            )
                        except Exception:
                            pass  # column already exists (idempotent)
                    logger.info("[GridStateStore] schema migrated -> v3 (daily_pnl sl columns)")
                if db_ver < _GRID_DB_SCHEMA_VERSION:
                    self._conn.execute(
                        "UPDATE meta SET value=? WHERE key='schema_version'",
                        (str(_GRID_DB_SCHEMA_VERSION),),
                    )
                    self._conn.commit()

    # ── Fill recording ────────────────────────────────────────────────────────

    def record_fill(
        self,
        ts_utc:         float,
        side:           str,       # 'BUY' | 'SELL'
        level_idx:      int,
        price_usd:      float,
        qty_btc:        float,
        fee_usd:        float,     # positive = cost
        gross_pnl:      float,     # 0.0 for BUY fills
        cycle_num:      int,
        is_liquidation: bool = False,  # True for stop-loss / shutdown liquidations
    ) -> None:
        """
        Append one fill row and update the daily_pnl bucket atomically.
        Called from GridEngine._on_fill() — must be fast and non-blocking
        (the fill thread processes fills sequentially; a slow DB write here
        delays counter-order placement).  WAL + NORMAL sync keeps writes
        to ~1-2 ms on spinning rust; SSD is faster.
        """
        hkt_date = _db_hkt_date(ts_utc)
        # Liquidation SELLs (stop-loss) are not completed grid cycles.
        cycles_delta = 1 if side == "SELL" and not is_liquidation else 0

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
            sl_gross = gross_pnl if is_liquidation else 0.0
            sl_delta = 1         if is_liquidation else 0
            self._conn.execute(
                """INSERT INTO daily_pnl
                   (hkt_date, gross_pnl_usd, fees_usd, net_pnl_usd, fill_count, cycle_count,
                    sl_gross_usd, sl_count)
                   VALUES (?, ?, ?, ?, 1, ?, ?, ?)
                   ON CONFLICT(hkt_date) DO UPDATE SET
                       gross_pnl_usd = gross_pnl_usd + excluded.gross_pnl_usd,
                       fees_usd      = fees_usd      + excluded.fees_usd,
                       net_pnl_usd   = net_pnl_usd   + excluded.gross_pnl_usd + excluded.fees_usd,
                       fill_count    = fill_count    + 1,
                       cycle_count   = cycle_count   + excluded.cycle_count,
                       sl_gross_usd  = sl_gross_usd  + excluded.sl_gross_usd,
                       sl_count      = sl_count      + excluded.sl_count""",
                (hkt_date, gross_pnl, -fee_usd, gross_pnl - fee_usd, cycles_delta,
                 sl_gross, sl_delta),
            )
            self._conn.commit()

    # ── Accumulated totals ────────────────────────────────────────────────────

    def get_accumulated(self) -> dict:
        """Sum all rows in daily_pnl -> all-time totals."""
        with self._lock:
            row = self._conn.execute(
                """SELECT
                       COALESCE(SUM(gross_pnl_usd), 0.0) AS gross_pnl,
                       COALESCE(SUM(fees_usd),      0.0) AS fees,
                       COALESCE(SUM(net_pnl_usd),   0.0) AS net_pnl,
                       COALESCE(SUM(fill_count),     0)   AS fill_count,
                       COALESCE(SUM(cycle_count),    0)   AS cycle_count,
                       COALESCE(SUM(sl_gross_usd),  0.0) AS sl_gross,
                       COALESCE(SUM(sl_count),       0)   AS sl_count
                   FROM daily_pnl"""
            ).fetchone()
        return dict(row) if row else {
            "gross_pnl": 0.0, "fees": 0.0, "net_pnl": 0.0,
            "fill_count": 0,  "cycle_count": 0,
            "sl_gross": 0.0,  "sl_count": 0,
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
            "sl_gross_usd": 0.0, "sl_count": 0,
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

    # ── Reset (fresh-start) ───────────────────────────────────────────────────

    def reset_state(self, backup: bool = True) -> Optional[str]:
        """
        Wipe all persisted fills, daily PnL, and meta rows so the bot behaves
        as if this is the very first startup.

        Does NOT touch anything on the exchange — open orders/positions are
        always independently handled by OMS.reconcile_on_startup() on every
        launch, reset or not, so a stale live position is still detected and
        liquidated exactly as before.

        If backup=True (default), the WAL is checkpointed and the db file is
        copied to "<db_path>.bak-<timestamp>" before anything is wiped, so
        pre-reset history is never silently lost.

        Returns the backup file path, or None if backup=False.
        """
        backup_path = None
        with self._lock:
            if backup:
                self._conn.execute("PRAGMA wal_checkpoint(FULL)")
                self._conn.commit()
                ts = _dt.datetime.now(_HKT_TZ).strftime("%Y%m%d_%H%M%S")
                backup_path = f"{self._db_path}.bak-{ts}"
                try:
                    shutil.copy2(self._db_path, backup_path)
                except OSError as e:
                    logger.error(f"[GridStateStore] Reset backup failed: {e}")
                    backup_path = None

            self._conn.execute("DELETE FROM grid_fills")
            self._conn.execute("DELETE FROM daily_pnl")
            self._conn.execute("DELETE FROM meta")
            # candle_cache is intentionally preserved across reset_state:
            # it contains price history used for TrendSignal warm-up, which
            # has nothing to do with fill accounting.  Clearing it would just
            # force another 26h wait on the next startup for no benefit.
            self._conn.execute(
                "INSERT INTO meta(key,value) VALUES('schema_version',?)",
                (str(_GRID_DB_SCHEMA_VERSION),),
            )
            self._conn.commit()
            try:
                self._conn.execute("VACUUM")
            except _sqlite3.OperationalError as e:
                logger.warning(f"[GridStateStore] VACUUM after reset skipped: {e}")

        logger.warning(
            "[GridStateStore] STATE RESET — all fills, daily PnL, and "
            "accumulated PnL cleared. " +
            (f"Pre-reset backup saved to {os.path.abspath(backup_path)}"
             if backup_path else "No backup taken.")
        )
        return backup_path

    # ── Candle cache persistence ─────────────────────────────────────────────

    def save_candles(self, ticks: list) -> int:
        """
        Persist 1-minute candle OHLC data derived from PriceCache._history.

        `ticks` is the raw list of (unix_ts_s, mid_price) tuples from the
        deque.  We re-bucket them here (same logic as compute_atr) so the
        caller only needs to hand us _history; no extra structures needed.

        Strategy
        --------
        * Group ticks into 1-min buckets, compute O/H/L/C per bucket.
        * Upsert every complete bucket (exclude the currently-open minute
          because live ticks are still updating it).
        * Prune rows older than 27 hours to bound table size.
          (26h required + 1h margin; ~1620 rows max, trivial.)

        Returns the number of rows written/updated.
        """
        if not ticks:
            return 0

        current_bucket = int(time.time() // 60)
        cutoff_bucket  = current_bucket - 27 * 60   # 27 hours ago

        buckets: dict = {}
        for ts_s, mid in ticks:
            k = int(ts_s // 60)
            if k >= current_bucket:
                continue   # skip the still-open minute
            if k not in buckets:
                buckets[k] = {"open": mid, "high": mid, "low": mid, "close": mid}
            else:
                c = buckets[k]
                c["high"]  = max(c["high"], mid)
                c["low"]   = min(c["low"],  mid)
                c["close"] = mid   # last write wins

        if not buckets:
            return 0

        rows = [
            (k, v["open"], v["high"], v["low"], v["close"])
            for k, v in buckets.items()
        ]

        with self._lock:
            self._conn.executemany(
                """INSERT INTO candle_cache(ts_bucket, open_px, high_px, low_px, close_px)
                   VALUES (?,?,?,?,?)
                   ON CONFLICT(ts_bucket) DO UPDATE SET
                       open_px  = excluded.open_px,
                       high_px  = excluded.high_px,
                       low_px   = excluded.low_px,
                       close_px = excluded.close_px""",
                rows,
            )
            self._conn.execute(
                "DELETE FROM candle_cache WHERE ts_bucket < ?",
                (cutoff_bucket,),
            )
            self._conn.commit()

        return len(rows)

    def load_candles(self, max_age_hours: int = 27) -> list:
        """
        Return persisted candle rows as a list of (unix_ts_s, mid_price)
        tick tuples compatible with PriceCache._history.

        Each candle is reconstructed as 4 synthetic ticks using the same
        OHLC injection strategy as _seed_atr_from_rest:
          t+0s  -> open
          t+15s -> high
          t+45s -> low
          t+59s -> close

        Only rows within the last max_age_hours are returned.
        max_age_hours is capped to HISTORY_WINDOW_S // 3600 (27h) so we
        never load more than the deque can hold.

        Returns an empty list if the table is empty (fresh DB).
        """
        cutoff_bucket = int(time.time() // 60) - max_age_hours * 60
        with self._lock:
            rows = self._conn.execute(
                """SELECT ts_bucket, open_px, high_px, low_px, close_px
                   FROM candle_cache
                   WHERE ts_bucket >= ?
                   ORDER BY ts_bucket ASC""",
                (cutoff_bucket,),
            ).fetchall()

        ticks = []
        for row in rows:
            ts_s = float(row["ts_bucket"]) * 60.0
            ticks.extend([
                (ts_s +  0.0, row["open_px"]),
                (ts_s + 15.0, row["high_px"]),
                (ts_s + 45.0, row["low_px"]),
                (ts_s + 59.0, row["close_px"]),
            ])
        return ticks

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
    CANDLE_SAVE_INTERVAL_S = 300.0   # snapshot PriceCache history to DB every 5 min

    def __init__(self, config: dict, reset_state: bool = False):
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
        self._last_candle_save:  float = 0.0
        self._halted:     bool  = False
        self._halt_time:  float = 0.0       # timestamp of the last halt
        self._halt_stop_price: float = 0.0  # stop_price that triggered the halt
        self._restart_attempts: int = 0     # number of auto-restart attempts made
        self._last_restart_time: float = 0.0  # timestamp of the last successful auto-restart
                                               # (0.0 = no auto-restart has happened yet)

        # ── BuyGate auto-calibration state ────────────────────────────────────
        # Rolling buffer of (timestamp, score) for the last N seconds of ticks.
        # Scanned at each SL event to find the peak pre-halt score, which is
        # used to nudge the threshold downward via EMA.
        self._score_history: list = []          # list of (float_ts, float_score)
        self._calib_threshold: Optional[float] = None  # persisted calibrated threshold
                                                        # loaded from DB on first use

        # ── Dead-band stop-raise: EMA damping + debounce state ────────────────
        # See _rebuild_grid()'s dead-band block. Persisted across calls since
        # the debounce timer must accumulate real wall-clock time across
        # separate (irregularly-spaced) invocations of the dead-band check.
        self._stop_raise_ema:  Optional[float] = None  # damped candidate stop
        self._pending_raise_candidate: Optional[float] = None  # stop awaiting confirm
        self._pending_raise_since:     float = 0.0      # when the candidate was first seen

        self._oms = OMS(
            api_key      = config.get("api_key", ""),
            api_secret   = config.get("api_secret", ""),
            instrument   = INSTRUMENT,
            live_trading = config.get("live_trading", False),
            config       = config,
        )
        self._auto_tuner   = GridAutoTuner(config, _price_cache)
        self._stop_scorer  = StopScoreCalculator(config, _price_cache, self._auto_tuner)

        # ── Trend signal (Phase 1 — read-only observer) ───────────────────────
        self._trend = TrendSignal(config, _price_cache)
        self._last_trend_regime: str   = TrendSignal.REGIME_NODATA
        self._last_trend_log:    float = 0.0   # ts of last trend log line
        self._last_trend_slope_pct: float = 0.0  # cached for compute_trend_risk(),
                                                  # refreshed every _evaluate_trend() call

        # ── SQLite persistence ────────────────────────────────────────────────────
        # Opened once here and shared with every GridEngine instance so that
        # fills survive restarts, re-tunes, and stop-loss rebuilds.
        self._store = GridStateStore(config.get("db_path", "grid_bot.db"))

        if reset_state:
            # Fresh-start requested via --reset-state: wipe fill history, daily
            # PnL, and accumulated PnL so /status reports as if this is the
            # very first launch. A pre-reset backup of the db is kept on disk.
            # Note: this only clears local bookkeeping — it does NOT touch the
            # exchange. Any real open orders/position are still independently
            # detected and liquidated by OMS.reconcile_on_startup() below, same
            # as on every normal launch.
            backup_path = self._store.reset_state()
            note = (f" (backup: {os.path.abspath(backup_path)})"
                    if backup_path else " (no backup — see log)")
            logger.warning(f"[GridBot] --reset-state: persisted PnL/fill history cleared{note}")
            self._alerter.send_sync(
                f"🧹 State reset requested — fill history, daily PnL, and "
                f"accumulated PnL cleared{note}.\nBot starting fresh."
            )

        # ── Telegram command poller ────────────────────────────────────────────
        self._cmd_poller = TelegramCommandPoller(
            token           = config.get("telegram_bot_token", ""),
            allowed_chat_id = config.get("telegram_chat_id",   ""),
        )
        self._cmd_poller.register("/status", self._handle_status_command)

        # WS market feed
        self._ws_stop = threading.Event()
        # Rolling deque of reconnect timestamps for flood detection.
        # Kept on GridBot (not _ReconnectingWS) so alerting and config live in one place.
        self._ws_reconnect_times: collections.deque = collections.deque()

        self._market_ws = _ReconnectingWS(
            name             = "MarketWS",
            url              = config.get("ws_market_url", "wss://stream.crypto.com/exchange/v1/market"),
            subscribe_msg_fn = self._ws_subscriptions,
            on_message_fn    = self._handle_market_message,
            stale_s          = config.get("ws_stale_threshold_s", 20),
            backoff_init     = config.get("ws_reconnect_backoff_s", 2),
            backoff_max      = config.get("ws_max_backoff_s", 60),
            stop_event       = self._ws_stop,
            on_reconnect_fn  = self._on_ws_reconnect,
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

    # ── WS reconnect flood detection ─────────────────────────────────────────

    def _on_ws_reconnect(self) -> None:
        """
        Called by _ReconnectingWS._on_open() on every reconnect after the first.
        Maintains a rolling deque of reconnect timestamps and fires a Telegram
        alert when the reconnect rate exceeds ws_reconnect_alert_count events
        within ws_reconnect_alert_window_s seconds.

        The alert fires ONCE when the threshold is first crossed, then rearms
        after the window rolls clear, preventing alert spam during a sustained
        outage while still notifying on the next flood if it recurs.
        """
        now      = time.time()
        window_s = self._cfg.get("ws_reconnect_alert_window_s", 300)
        threshold = self._cfg.get("ws_reconnect_alert_count", 3)

        self._ws_reconnect_times.append(now)
        # Prune events outside the rolling window
        cutoff = now - window_s
        while self._ws_reconnect_times and self._ws_reconnect_times[0] < cutoff:
            self._ws_reconnect_times.popleft()

        count = len(self._ws_reconnect_times)
        logger.info(
            f"[MarketWS] Reconnect #{count} in last {window_s:.0f}s "
            f"(threshold={threshold})"
        )

        if count >= threshold:
            # Only alert on the exact threshold crossing, not every subsequent
            # reconnect within the same flood, to avoid repeated Telegram messages.
            if count == threshold:
                logger.warning(
                    f"[MarketWS] Reconnect flood: {count} reconnects in "
                    f"{window_s:.0f}s — sending alert"
                )
                window_min = int(window_s / 60)
                self._alerter.send(
                    f"⚠️ MarketWS reconnect flood: {count} reconnects in "
                    f"{window_min}min\n"
                    f"Check upstream CDC WebSocket feed stability."
                )
            else:
                logger.warning(
                    f"[MarketWS] Reconnect flood continuing: {count} in "
                    f"{window_s:.0f}s (alert already sent)"
                )

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

        # ── Phase 2a: restore candles from SQLite ────────────────────────────
        # On restarts (deploy, crash recovery) we load the candle history that
        # was snapshotted to the DB during the previous run.  This gives
        # TrendSignal its full 26h warm-up immediately, with zero REST calls.
        atr_lookback = self._cfg.get("atr_lookback_minutes", 1440)
        if self._store is not None:
            self._load_candles_from_db()

        # ── Phase 2b: seed ATR from REST historical candles ───────────────────
        # Fetch recent 1-min candles via public/get-candlestick so we don't
        # have to sit idle for ~30 minutes collecting live ticks.  On success
        # the Phase 2 poll loop below exits immediately.  On failure we fall
        # through to the original live-accumulation path with a warning.
        # If Phase 2a already provided enough candles this call becomes a cheap
        # top-up (fetches only the gap since last shutdown, ~seconds of data).
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

    # ── Candle cache: DB load / save ─────────────────────────────────────────

    def _load_candles_from_db(self) -> None:
        """
        Load persisted 1-min candle ticks from GridStateStore.candle_cache
        into PriceCache._history.

        This is called once at startup (Phase 2a), before _seed_atr_from_rest,
        so that TrendSignal has its full 26h history from the very first tick
        after a restart — no waiting, no extra REST calls beyond the small ATR
        top-up that _seed_atr_from_rest performs.

        Ticks are merged with any live ticks that Phase 1 already deposited,
        sorted in chronological order, and capped to the deque's maxlen (30000).
        Only ticks within PriceCache.HISTORY_WINDOW_S (27h) are loaded to
        match the deque's retention window.
        """
        ticks = self._store.load_candles(
            max_age_hours=min(27, _price_cache.HISTORY_WINDOW_S // 3600)
        )
        if not ticks:
            logger.info("[GridBot] Phase 2a: no persisted candles in DB (first run?)")
            return

        with _price_cache._lock:
            existing = list(_price_cache._history)
            merged   = ticks + existing
            merged.sort(key=lambda x: x[0])
            _price_cache._history.clear()
            for item in merged[-30000:]:
                _price_cache._history.append(item)

        n_buckets = _price_cache.atr_candle_count(
            self._cfg.get("atr_lookback_minutes", 1440)
        )
        trend_h = len(ticks) // 4 // 60   # rough hourly candle count
        logger.info(
            f"[GridBot] Phase 2a: loaded {len(ticks)//4} candles from DB "
            f"(~{trend_h}h of history) -> {n_buckets} ATR buckets in cache"
        )

    def _save_candles_to_db(self) -> None:
        """
        Snapshot PriceCache._history to GridStateStore.candle_cache.

        Called periodically from _run() (every CANDLE_SAVE_INTERVAL_S seconds)
        so that a restart always has recent history available.  Each call is
        idempotent (upsert) and fast (~1-2 ms for ~1600 rows on SSD).
        Old rows (> 27h) are pruned by save_candles() automatically.
        """
        if self._store is None:
            return
        with _price_cache._lock:
            ticks = list(_price_cache._history)
        if not ticks:
            return
        n = self._store.save_candles(ticks)
        span_h = (ticks[-1][0] - ticks[0][0]) / 3600.0 if len(ticks) > 1 else 0.0
        logger.debug(
            f"[GridBot] Candle snapshot: {n} buckets written to DB "
            f"(in-memory span={span_h:.1f}h)"
        )

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
        # How many candles do we already have from the DB cache?
        # If we have enough for TrendSignal (26h × 60 = 1560) only fetch the
        # small gap since the last snapshot.  Otherwise fetch the full 26h so
        # TrendSignal can warm up on first run (or after a DB wipe).
        existing_buckets = _price_cache.atr_candle_count(
            self._cfg.get("atr_lookback_minutes", 1440)
        )
        trend_min_h   = self._cfg.get("trend_signal_min_history_h", 26)
        trend_min_can = trend_min_h * 60          # 1560 candles for 26h
        if existing_buckets < trend_min_can:
            # First run or thin cache: fetch a full 26h + 2 slack
            fetch_count = trend_min_can + 2
        else:
            # Cache is already warm: just top up the last ~2 candles
            fetch_count = min_candles + 2

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
        cost_basis_price = None
        if self._engine:
            long_qty = self._engine.get_stats().get("long_qty", 0.0)
            if long_qty > 0:
                _, cost_basis_price = self._engine.get_cost_basis()
            self._engine.stop()
            self._engine = None
        if long_qty > 0:
            self._liquidate_position(long_qty, reason="GridBot stop",
                                      cost_basis_price=cost_basis_price)

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

            # Stop-score tick — update velocity EMA on every price tick so the
            # score stays fresh even between fills.  Also drives gradual release
            # of SUPPRESSED levels when the score recovers.
            if self._stop_scorer is not None and self._params is not None:
                score = self._stop_scorer.compute(mid, self._params.stop_price)
                resume_thr = self._cfg.get("stop_score_resume_threshold", 0.35)
                if (score <= resume_thr
                        and self._engine is not None
                        and self._engine.count_suppressed() > 0):
                    released = self._engine.release_one_suppressed_level()
                    if released:
                        logger.info(
                            f"[GridBot] Released one suppressed level "
                            f"(score={score:.4f} ≤ resume={resume_thr})"
                        )

            # Fill detection
            if self._engine:
                self._engine.check_price_fills(mid)

            # Engine-requested rebuild (e.g. drift-shift detected mid far OOR)
            if self._engine and self._engine.pop_needs_rebuild():
                logger.info("[GridBot] Engine requested full rebuild (drift far OOR)")
                self._rebuild_grid()
                continue

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

            # Periodic candle snapshot — persists PriceCache history to DB so
            # TrendSignal warm-up survives service restarts.
            if now - self._last_candle_save > self.CANDLE_SAVE_INTERVAL_S:
                self._last_candle_save = now
                self._save_candles_to_db()

            time.sleep(0.1)

    # ── Grid management ───────────────────────────────────────────────────────

    def _liquidate_position(self, qty: float, reason: str = "",
                             cost_basis_price: Optional[float] = None,
                             is_liquidation: bool = False):
        """
        Submit a market SELL for `qty` BTC and wait for the fill (up to 15s).
        Used by stop(), start() reconcile, and _emergency_halt().
        In paper mode the fill is instant at the live mid price.
        Logs and alerts on both success and timeout.

        cost_basis_price, if provided, is the weighted-average entry price
        of the qty being closed (see GridEngine.get_cost_basis()). It's used
        to compute and persist this fill's realized gross PnL, so daily and
        accumulated PnL actually include stop-loss / shutdown losses instead
        of only completed grid-cycle round trips.

        If omitted (e.g. startup reconcile of a stale position left over
        from a previous process, with no local record of its entry price),
        gross_pnl is recorded as 0.0 — the fee is still captured, which is
        strictly better than not persisting the fill at all.

        is_liquidation=True only for actual stop-loss events (_emergency_halt).
        Planned shutdown (stop()) and startup reconcile must leave this False
        so sl_count / sl_gross_usd in daily_pnl are not polluted by non-SL
        position closes.
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
            gross_pnl = (
                (fill.avg_price - cost_basis_price) * fill.filled_qty
                if cost_basis_price else 0.0
            )
            if self._store is not None:
                try:
                    # level_idx=-1 / cycle_num=-1: sentinel marking this as a
                    # liquidation fill rather than a normal numbered grid level/cycle.
                    self._store.record_fill(
                        ts_utc=time.time(), side="SELL", level_idx=-1,
                        price_usd=fill.avg_price, qty_btc=fill.filled_qty,
                        fee_usd=fill.fee, gross_pnl=gross_pnl, cycle_num=-1,
                        is_liquidation=is_liquidation,
                    )
                except Exception as e:
                    logger.error(
                        f"[GridBot]{tag} DB record_fill (liquidation) error: {e}",
                        exc_info=True,
                    )
            pnl_note = f" | realized {gross_pnl:+.4f} USD" if cost_basis_price else ""
            self._alerter.send(
                f"🔴 Position closed ({reason})\n"
                f"Sold {fill.filled_qty:.4f} BTC @ {fill.avg_price:.2f}{pnl_note}"
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

    def _update_pending_raise(self, candidate_stop: float, noise_tolerance: float = 0.0) -> bool:
        """
        Track a candidate dead-band stop-raise across separate invocations of
        _rebuild_grid()'s dead-band check, for the stop_raise_confirm_*
        debounce (see that block below).

        If the candidate weakens by more than noise_tolerance (drops below
        the value we're already tracking) the confirmation timer resets —
        SL1's root cause was a raise that committed instantly on a single
        strong sample and the market was already retracing by the time it
        triggered, so a genuine weakening of the candidate is treated as a
        possible sign the retrace has begun. If it holds (within tolerance)
        or strengthens, the original since-timestamp is kept (so genuine
        sustained moves aren't penalised) and the tracked value is updated
        upward.

        IMPORTANT: callers must pass the pre-cap EMA-damped candidate here,
        not the capped one. The cap's ceiling (cur_stop + cap_atr × ATR)
        recomputes every call from the current ATR, which drifts on its own
        (e.g. slowly declining as volatility normalises) independent of
        whether the underlying raise signal is holding — tracking the capped
        value caused exactly that drift to look like "weakening" on every
        call, permanently resetting the timer to 0 with zero chance to ever
        confirm. The cap should only limit the size of the jump actually
        committed, applied once, at commit time — never enter the
        persistence tracking that decides IF a raise is warranted at all.

        Returns True if this call (re)started the tracking window.
        """
        if (self._pending_raise_candidate is None
                or candidate_stop < self._pending_raise_candidate - noise_tolerance):
            self._pending_raise_candidate = candidate_stop
            self._pending_raise_since     = time.time()
            return True
        self._pending_raise_candidate = max(self._pending_raise_candidate, candidate_stop)
        return False


    def _rebuild_grid(self):
        mid = _price_cache.get_mid()
        if mid is None:
            logger.warning("[GridBot] No mid price — cannot build grid")
            return

        logger.info("[GridBot] (Re)building grid...")

        new_params = self._auto_tuner.compute()
        if new_params is None:
            logger.error("[GridBot] Auto-tuner returned None — keeping existing params")
            new_params = self._params
        if new_params is None:
            logger.error("[GridBot] No grid params available — aborting rebuild")
            return

        # ── Dead-band check (BEFORE tearing down the existing grid) ──────────
        # Must happen first: if the shift is too small we return immediately
        # without disrupting the running grid.  The original code did this AFTER
        # engine.stop(), leaving the bot orderless until the next retune trigger.
        #
        # Skip if the engine is already None (halt, startup, etc.) — there is
        # no live grid to protect, so always proceed with the rebuild.
        if self._params is not None and self._engine is not None:
            old_width = self._params.upper - self._params.lower
            new_width = new_params.upper - new_params.lower

            # CRITICAL: the width-delta check below is blind to POSITION shifts.
            # Root cause of the 2026-07-09 21:32 stop-loss: mid drifted below the
            # entire current range ("Price 62594.35 outside range -> retune"),
            # so the auto-tuner correctly computed a repositioned range/stop
            # ([62464.32,62724.38], stop=62334.29) — but its WIDTH (260.06) was
            # only 0.4% different from the current range's width (258.98), so
            # the dead-band check below treated it as "too small to rebuild" and
            # took the in-place-only path. That path can only RAISE the stop,
            # never lower it — so the grid was left sitting at its old position
            # (range=[62624.06,62883.04], stop=62494.57) while price kept
            # falling, with no mechanism able to either reposition it down or
            # correctly hold it — until price fell through the stale stop
            # 27 minutes later. A same-width RANGE TRANSLATION is exactly the
            # case a width-only delta check cannot see. Whenever mid is outside
            # the current live range, a full reposition is mandatory regardless
            # of width similarity — bypass the dead-band entirely in that case.
            mid_outside_current_range = (
                mid < self._params.lower or mid > self._params.upper
            )

            if old_width > 0 and not mid_outside_current_range:
                delta = abs(new_width - old_width) / old_width
                deadband = self._cfg.get("retune_deadband_pct", 0.10)
                if delta < deadband:
                    # Range shift is too small to justify a full grid rebuild,
                    # but the newly computed stop_price may be meaningfully
                    # different from the current one (ATR expanded, mid drifted).
                    # Update the StopLossGuard and params.stop_price in-place ONLY
                    # if the new stop is HIGHER (tighter) than the current one.
                    # Never move the stop downward: a lower stop during a falling
                    # market just delays the halt and increases potential loss.
                    #
                    # Root cause of the 2026-07-08 and 2026-07-09 stop-outs: a
                    # single dead-band retune jumped the stop straight to the
                    # freshly-computed ATR target (+442pts, then +168pts), right
                    # before the market retraced back through it. The fixed
                    # drift-shift cooldown alone wasn't enough (SL1's raise fired
                    # 7 minutes after the drift-shift, past the 60s cooldown, but
                    # still mid-retracement). Four gates now apply, all scaled in
                    # real time by trend_risk — see
                    # StopScoreCalculator.compute_trend_risk() — a [0,1] score
                    # built from tick-level velocity/volatility plus the
                    # TrendSignal hourly regime:
                    #   1. Cap      — max raise step in ATR (small if trend_risk
                    #                 low/looks like noise, larger if high/looks
                    #                 like a genuine strengthening decline)
                    #   2. Debounce — candidate must hold (not weaken) for a
                    #                 risk-scaled confirm window before committing
                    #   3. EMA      — the raw auto-tuner stop is damped before
                    #                 being treated as a candidate at all, so one
                    #                 volatile print can't swing it on its own
                    #   4. Urgent   — trend_risk above a threshold can bypass the
                    #                 drift-shift cooldown veto (strong, real
                    #                 evidence outweighs the "might still be
                    #                 mid-retracement" assumption behind it)
                    #
                    # Order matters: EMA damp -> Debounce (on the UNCAPPED
                    # damped value) -> cooldown veto -> Cap (applied once, at
                    # commit). The cap must NOT be part of what debounce
                    # tracks — its ceiling (cur_stop + cap_atr×ATR) recomputes
                    # from current ATR every call, which drifts on its own
                    # (e.g. ATR quietly declining as volatility normalises)
                    # independent of whether the raise signal is holding.
                    # Bug found in the 2026-07-10 logs: with the cap value
                    # debounce-tracked, that ATR drift alone reset the
                    # confirmation timer to 0 on every single 5-min retune,
                    # forever — the candidate sat at "0s/90s held" for 50+
                    # minutes while the real target climbed to 63000+ and the
                    # live stop stayed stuck 500+ points below price.
                    raw_new_stop = new_params.stop_price
                    cur_stop     = self._params.stop_price

                    trend_risk = 0.0
                    if self._stop_scorer is not None:
                        trend_risk = self._stop_scorer.compute_trend_risk(
                            mid, self._last_trend_regime, self._last_trend_slope_pct
                        )

                    if raw_new_stop <= cur_stop:
                        # No raise candidate this round — clear any pending
                        # debounce state so a stale candidate doesn't linger.
                        self._pending_raise_candidate = None
                        self._pending_raise_since     = 0.0
                        if self._sl_guard is not None and raw_new_stop < cur_stop:
                            logger.info(
                                f"[GridBot] Re-tune skipped (range shift {delta:.1%} < "
                                f"dead-band {deadband:.1%}) — stop NOT lowered "
                                f"(new={raw_new_stop:.2f} < current={cur_stop:.2f})"
                            )
                        else:
                            logger.info(
                                f"[GridBot] Re-tune skipped (range shift {delta:.1%} < "
                                f"dead-band {deadband:.1%}) — existing grid kept running"
                            )
                        return

                    atr_now = _price_cache.compute_atr(
                        self._cfg.get("atr_lookback_minutes", 1440))

                    # ── 3. EMA damping ────────────────────────────────────────
                    ema_alpha = _risk_interp(
                        trend_risk,
                        self._cfg.get("stop_raise_ema_alpha_base", 0.15),
                        self._cfg.get("stop_raise_ema_alpha_max",  0.60),
                    )
                    if self._stop_raise_ema is None:
                        self._stop_raise_ema = raw_new_stop
                    else:
                        self._stop_raise_ema = (
                            ema_alpha * raw_new_stop
                            + (1.0 - ema_alpha) * self._stop_raise_ema
                        )
                    damped_stop = self._stop_raise_ema

                    if damped_stop <= cur_stop:
                        # Clear pending state too — the damped signal itself
                        # (not just the capped derivative) has weakened back
                        # to/below current stop, a genuine reason to reset.
                        self._pending_raise_candidate = None
                        self._pending_raise_since     = 0.0
                        logger.info(
                            f"[GridBot] Re-tune skipped (range shift {delta:.1%} < "
                            f"dead-band {deadband:.1%}) — raw candidate "
                            f"{raw_new_stop:.2f} damped to {damped_stop:.2f} "
                            f"(EMA α={ema_alpha:.2f}, trend_risk={trend_risk:.2f}), "
                            f"not above current stop={cur_stop:.2f} yet"
                        )
                        return

                    # ── 2. Debounce (on the uncapped damped_stop) ────────────
                    noise_tol = (
                        self._cfg.get("stop_raise_confirm_noise_atr", 0.05) * atr_now
                        if atr_now and atr_now > 0 else 0.0
                    )
                    self._update_pending_raise(damped_stop, noise_tol)
                    confirm_s = _risk_interp(
                        trend_risk,
                        self._cfg.get("stop_raise_confirm_base_s", 90),
                        self._cfg.get("stop_raise_confirm_min_s",  10),
                    )
                    elapsed = time.time() - self._pending_raise_since

                    if elapsed < confirm_s:
                        logger.info(
                            f"[GridBot] Re-tune skipped (range shift {delta:.1%} < "
                            f"dead-band {deadband:.1%}) — stop raise pending "
                            f"confirm (candidate={self._pending_raise_candidate:.2f}, "
                            f"{elapsed:.0f}s/{confirm_s:.0f}s held, "
                            f"trend_risk={trend_risk:.2f})"
                        )
                        return

                    # ── 4. Drift-shift cooldown veto (with urgent bypass) ────
                    # Checked only after confirmation, so cooldown time isn't
                    # wasted counting toward the debounce window — but also
                    # doesn't block the debounce timer from running while
                    # waiting for cooldown to clear (next call re-enters here).
                    drift_cooldown = self._cfg.get("drift_shift_min_interval_s", 60)
                    last_drift = (
                        self._engine._last_drift_shift
                        if self._engine is not None else 0.0
                    )
                    since_drift = time.time() - last_drift
                    in_drift_cooldown = last_drift > 0 and since_drift < drift_cooldown
                    urgent_threshold = self._cfg.get("stop_raise_urgent_trend_risk", 0.80)
                    urgent_bypass = in_drift_cooldown and trend_risk >= urgent_threshold

                    if in_drift_cooldown and not urgent_bypass:
                        logger.info(
                            f"[GridBot] Re-tune skipped (range shift {delta:.1%} < "
                            f"dead-band {deadband:.1%}) — stop raise suppressed "
                            f"(drift-shift cooldown {since_drift:.0f}s < "
                            f"{drift_cooldown}s, trend_risk={trend_risk:.2f} < "
                            f"urgent {urgent_threshold})"
                        )
                        return

                    # ── 1. Cap — applied once, here, at commit time ──────────
                    cap_atr = _risk_interp(
                        trend_risk,
                        self._cfg.get("stop_raise_cap_base_atr", 0.5),
                        self._cfg.get("stop_raise_cap_max_atr",  2.5),
                    )
                    if atr_now and atr_now > 0:
                        capped_stop = min(damped_stop, cur_stop + cap_atr * atr_now)
                    else:
                        capped_stop = damped_stop  # ATR unavailable — cap disabled this round

                    if capped_stop <= cur_stop:
                        # Shouldn't normally happen (damped_stop > cur_stop was
                        # already established above), but guard anyway.
                        logger.info(
                            f"[GridBot] Re-tune skipped (range shift {delta:.1%} < "
                            f"dead-band {deadband:.1%}) — raise capped to "
                            f"{cap_atr:.2f}×ATR, no headroom above current "
                            f"stop={cur_stop:.2f} yet (trend_risk={trend_risk:.2f})"
                        )
                        return

                    # ── All gates passed — commit the raise ──────────────────
                    new_stop = capped_stop
                    old_stop = cur_stop
                    self._params = GridParams(
                        lower=self._params.lower,
                        upper=self._params.upper,
                        levels=self._params.levels,
                        spacing=self._params.spacing,
                        stop_price=new_stop,
                        notional_per_level=self._params.notional_per_level,
                    )
                    self._sl_guard = StopLossGuard(new_stop, self._cfg)
                    # Propagate into the engine's own GridParams copy too —
                    # _log_status() and the Telegram /status handler read
                    # self._engine.get_params(), which _trail_up/_trail_down
                    # keep current but which this dead-band raise otherwise
                    # never touches. Without this the status log/alert shows
                    # the pre-raise stop until the next full grid rebuild.
                    if self._engine is not None:
                        self._engine.update_stop_price(new_stop)
                    self._pending_raise_candidate = None
                    self._pending_raise_since     = 0.0
                    logger.info(
                        f"[GridBot] Re-tune skipped (range shift {delta:.1%} < "
                        f"dead-band {deadband:.1%}) — stop raised "
                        f"{old_stop:.2f} → {new_stop:.2f} "
                        f"(raw candidate={damped_stop:.2f}, "
                        f"trend_risk={trend_risk:.2f}"
                        f"{' URGENT-BYPASS' if urgent_bypass else ''}, "
                        f"cap={cap_atr:.2f}×ATR, confirm={confirm_s:.0f}s)"
                    )
                    return

        # Dead-band passed (or first build) — safe to tear down now
        if self._engine:
            self._engine.stop()
            self._engine = None

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

        # Reset velocity EMA so stale fall history from before the rebuild
        # doesn't inflate the velocity component of the new grid's stop score.
        if self._stop_scorer is not None:
            self._stop_scorer.reset_velocity()

        # Reset dead-band stop-raise EMA/debounce state too — both are scaled
        # to the OLD grid's stop_price, which is meaningless once a full
        # rebuild has replaced it with a new range/stop entirely.
        self._stop_raise_ema           = None
        self._pending_raise_candidate  = None
        self._pending_raise_since      = 0.0

        # Build the buy-gate closure: captures stop_price at build time so it
        # doesn't change under the engine when params are updated.
        _stop_price_at_build = new_params.stop_price
        _scorer = self._stop_scorer

        _bot_ref = self   # capture for score history recording in closure

        def _buy_gate() -> bool:
            """Return True (allow buy) or False (suppress buy)."""
            if _scorer is None:
                return True
            mid_now = _price_cache.get_mid()
            if mid_now is None:
                return True
            score = _scorer.compute(mid_now, _stop_price_at_build)
            # Record score in rolling history for auto-calibration at the
            # next SL event.  Prune entries older than lookback+60s slack so
            # the list stays bounded without a separate housekeeping task.
            lookback_s = _bot_ref._cfg.get("stop_score_calib_lookback_s", 120)
            now_ts = time.time()
            _bot_ref._score_history.append((now_ts, score))
            cutoff = now_ts - lookback_s - 60.0
            _bot_ref._score_history = [
                (t, s) for t, s in _bot_ref._score_history if t >= cutoff
            ]
            threshold = _bot_ref._get_threshold()
            allow = score < threshold
            # Always log at INFO so gate decisions are visible in the daily
            # log and calibration history is auditable.
            logger.info(
                f"[BuyGate] score={score:.4f} threshold={threshold:.4f} "
                f"→ {'ALLOW' if allow else 'SUPPRESS'}"
            )
            return allow

        self._engine = GridEngine(
            params=new_params, oms=self._oms,
            instrument=INSTRUMENT, config=self._cfg,
            store=self._store,
            buy_gate_fn=_buy_gate)
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

    # ── BuyGate auto-calibration ──────────────────────────────────────────────

    def _get_threshold(self) -> float:
        """
        Return the active stop_score_threshold, preferring the calibrated
        value persisted in the DB over the config default.  Loads from DB on
        first call; subsequent calls use the in-memory cache.
        """
        if self._calib_threshold is not None:
            return self._calib_threshold
        # First call: try to load from DB
        try:
            raw = self._store.get_meta("bugate_threshold")
            if raw is not None:
                val = float(raw)
                floor = self._cfg.get("stop_score_threshold_floor", 0.12)
                default = self._cfg.get("stop_score_threshold", 0.25)
                self._calib_threshold = max(floor, min(default, val))
                logger.info(
                    f"[BuyGate] Loaded calibrated threshold "
                    f"{self._calib_threshold:.4f} from DB"
                )
                return self._calib_threshold
        except Exception as e:
            logger.warning(f"[BuyGate] Failed to load threshold from DB: {e}")
        # No persisted value — use config default
        self._calib_threshold = self._cfg.get("stop_score_threshold", 0.25)
        return self._calib_threshold

    def _calibrate_threshold(self, halt_time: float) -> None:
        """
        Called immediately after a stop-loss halt.  Scans _score_history for
        the peak score observed in the lookback window before halt_time, then
        nudges the threshold downward using an EMA update:
            target    = peak_score * safety_margin
            new_thr   = old_thr + alpha * (target - old_thr)
            new_thr   = clamp(new_thr, floor, config_default)
        Persists the result to DB so it survives restarts.
        Logs the update at INFO so every calibration step is auditable.
        """
        if not self._cfg.get("stop_score_calib_enabled", True):
            return
        lookback_s     = self._cfg.get("stop_score_calib_lookback_s", 120)
        safety_margin  = self._cfg.get("stop_score_calib_safety_margin", 0.90)
        alpha          = self._cfg.get("stop_score_calib_ema_alpha", 0.40)
        floor_thr      = self._cfg.get("stop_score_threshold_floor", 0.12)
        default_thr    = self._cfg.get("stop_score_threshold", 0.25)

        cutoff = halt_time - lookback_s
        recent = [(ts, sc) for ts, sc in self._score_history if ts >= cutoff]
        if not recent:
            logger.info(
                f"[BuyGate] Calibration skipped: no score history in last "
                f"{lookback_s}s before halt"
            )
            return

        peak_score = max(sc for _, sc in recent)
        target     = peak_score * safety_margin
        old_thr    = self._get_threshold()
        # EMA nudge: only lower the threshold, never raise it via calibration.
        # (Manual config edits can raise it; calibration is one-directional.)
        if target >= old_thr:
            logger.info(
                f"[BuyGate] Calibration: peak_score={peak_score:.4f} "
                f"target={target:.4f} >= current threshold={old_thr:.4f} "
                f"— no downward adjustment needed"
            )
            return

        new_thr = old_thr + alpha * (target - old_thr)
        new_thr = round(max(floor_thr, min(default_thr, new_thr)), 4)

        logger.info(
            f"[BuyGate] Auto-calibration: peak_score={peak_score:.4f} "
            f"target={target:.4f} (peak*{safety_margin}) "
            f"threshold {old_thr:.4f} -> {new_thr:.4f} "
            f"(alpha={alpha}, floor={floor_thr})"
        )
        self._calib_threshold = new_thr
        try:
            self._store.set_meta("bugate_threshold", str(new_thr))
        except Exception as e:
            logger.warning(f"[BuyGate] Failed to persist threshold: {e}")

    def _emergency_halt(self, mid: float):
        logger.warning(f"[GridBot] EMERGENCY HALT at mid={mid:.2f}")
        self._halted      = True
        self._halt_time   = time.time()
        self._halt_stop_price = self._params.stop_price if self._params else mid

        # Grid is being torn down — clear dead-band stop-raise EMA/debounce
        # state so nothing stale carries into whatever grid comes next.
        self._stop_raise_ema           = None
        self._pending_raise_candidate  = None
        self._pending_raise_since      = 0.0

        # If the grid ran healthily for a long stretch since the last auto-restart
        # (or since startup, if no auto-restart has happened yet) before hitting
        # this new, unrelated halt, clear the attempt counter. Otherwise attempts
        # from long-past, unrelated stop-loss events accumulate forever and can
        # silently exhaust auto_restart_max_attempts for good.
        reset_hours = self._cfg.get("auto_restart_attempt_reset_hours", 24)
        if reset_hours > 0 and self._restart_attempts > 0:
            healthy_since = self._last_restart_time or 0.0
            if healthy_since and (self._halt_time - healthy_since) >= reset_hours * 3600:
                logger.info(
                    f"[AutoRestart] Grid ran for "
                    f"{(self._halt_time - healthy_since) / 3600:.1f}h since last "
                    f"auto-restart (≥ {reset_hours}h) — resetting attempt counter "
                    f"from {self._restart_attempts} to 0"
                )
                self._restart_attempts = 0

        long_qty = 0.0
        cost_basis_price = None
        if self._engine:
            long_qty = self._engine.get_stats().get("long_qty", 0.0)
            if long_qty > 0:
                _, cost_basis_price = self._engine.get_cost_basis()
            self._engine.stop()
            self._engine = None

        max_attempts = self._cfg.get("auto_restart_max_attempts", 3)
        attempts_exhausted = (
            max_attempts > 0 and self._restart_attempts >= max_attempts
        )
        if not self._cfg.get("auto_restart_enabled", True):
            _restart_note = "Restart manually."
        elif attempts_exhausted:
            _restart_note = (
                f"⚠️ Max auto-restart attempts ({max_attempts}) already used "
                f"— will NOT auto-restart. Manual restart required."
            )
        else:
            _restart_note = (
                f"Monitoring for auto-restart "
                f"(attempt {self._restart_attempts + 1}/{max_attempts if max_attempts else '∞'})."
            )

        if long_qty > 0:
            # _liquidate_position sends its own fill/timeout alert; we send the
            # STOP-LOSS context alert separately so they're distinct in Telegram.
            self._alerter.send_sync(
                f"🚨 STOP-LOSS TRIGGERED\n"
                f"mid={mid:.2f} < stop={self._halt_stop_price:.2f}\n"
                f"Liquidating {long_qty:.4f} BTC — Bot HALTED — {_restart_note}"
            )
            self._liquidate_position(long_qty, reason="stop-loss",
                                      cost_basis_price=cost_basis_price,
                                      is_liquidation=True)
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

        # Condition 2: price must be above the (time-decayed) recovery floor.
        # The floor starts at halt_stop - base_buffer×ATR and decays downward
        # by decay_atr_per_hour × ATR for every hour the bot has been halted.
        # This prevents the bot from staying halted all night when price drops
        # below halt_stop and then stabilises at a new, lower level.
        #
        # 2026-07-10 SL2: halt_stop=64415, ATR=34.85, BTC dropped to 63990.
        # Fixed floor (64398) was 408 pts above overnight price — bot never
        # restarted.  With decay=3.0×ATR/h after 4h the floor is
        # 64415 - (0.5 + 12) × 35 = 63977, allowing restart into the stable
        # overnight market.
        #
        # The floor is also bounded below by halt_stop - max_drop_atr×ATR so
        # it can't decay to an absurd level during very long halts.
        atr_for_buffer = _price_cache.compute_atr(self._cfg.get("atr_lookback_minutes", 1440))
        base_buffer    = self._cfg.get("auto_restart_recovery_atr_buffer", 0.5)
        decay_per_hour = self._cfg.get("auto_restart_recovery_floor_decay_atr_per_hour", 3.0)
        max_drop_atr   = self._cfg.get("auto_restart_recovery_floor_min_atr", 15.0)
        hours_halted   = elapsed / 3600.0

        if atr_for_buffer and atr_for_buffer > 0:
            total_buffer   = base_buffer + decay_per_hour * hours_halted
            total_buffer   = min(total_buffer, max_drop_atr)   # cap the decay
            recovery_floor = self._halt_stop_price - total_buffer * atr_for_buffer
        else:
            recovery_floor = self._halt_stop_price   # strict fallback

        if mid <= recovery_floor:
            if atr_for_buffer and atr_for_buffer > 0:
                buf_note = (f"buffer={base_buffer:.1f}+{decay_per_hour:.1f}"
                            f"x{hours_halted:.1f}h={total_buffer:.2f}xATR={atr_for_buffer:.2f}")
            else:
                buf_note = "ATR unavailable"
            logger.info(
                f"[AutoRestart] Price {mid:.2f} still below recovery floor "
                f"{recovery_floor:.2f} (halt_stop={self._halt_stop_price:.2f} "
                f"halted={hours_halted:.1f}h {buf_note}) — waiting"
            )
            return

        # Condition 3 + 4: stability window
        # Range (hi-lo) uses the long, conservative window (confidence big
        # swings have genuinely stopped). Trend (mean) uses a separate,
        # shorter window — see config comments for why sharing one window
        # between these two different questions caused a real, observed delay.
        stab_min   = self._cfg.get("auto_restart_stability_minutes", 60)
        trend_min  = self._cfg.get("auto_restart_trend_minutes", 15)
        range_pct  = self._cfg.get("auto_restart_range_percentile", 0.05)
        stab       = _price_cache.compute_stability(stab_min, trend_min, range_pct)

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
            pct_note = f" ({range_pct:.0%}ile" if range_pct > 0 else " (raw min/max"
            logger.info(
                f"[AutoRestart] Still volatile: hi-lo={hi_lo:.2f}{pct_note}, "
                f"{stab_min}m window) > {atr_mult}×ATR={max_range:.2f} — waiting"
            )
            return

        # Condition 4: price must be flat or rising (not bleeding lower)
        # Allow a small tolerance of 0.1×ATR below mean to avoid false blocks
        # from end-of-sine-wave positioning in a tight oscillation.
        trend_floor = mean - 0.1 * atr
        if mid < trend_floor:
            logger.info(
                f"[AutoRestart] Downtrend in window: mid={mid:.2f} < "
                f"trend_floor={trend_floor:.2f} (mean={mean:.2f} over "
                f"{trend_min}m - 0.1×ATR) — waiting"
            )
            return

        # All conditions met — restart
        self._restart_attempts += 1
        # NOTE: condition 2 above only requires mid > recovery_floor (halt_stop_price
        # minus a configurable ATR buffer) — NOT mid > halt_stop_price itself. The
        # previous log line here read "above stop={halt_stop_price}", which was
        # misleading: it implied mid had recovered above the old halt stop when it
        # may still be below it (by design, within recovery_buffer_mult × ATR).
        # Make that explicit so log readers aren't misled about what was checked.
        # (The subsequent _rebuild_grid() stop-proximity guard is what actually
        # protects against arming a new stop too close to current mid.)
        below_halt_stop = mid < self._halt_stop_price
        recovery_note = (
            f"mid={mid:.2f} < halt_stop={self._halt_stop_price:.2f} but > "
            f"recovery_floor={recovery_floor:.2f} "
            f"(halted {hours_halted:.1f}h, decayed floor)"
            if below_halt_stop else
            f"mid={mid:.2f} >= halt_stop={self._halt_stop_price:.2f}"
        )
        logger.info(
            f"[AutoRestart] Stability confirmed: "
            f"hi-lo={hi_lo:.2f} < max={max_range:.2f}, "
            f"mid={mid:.2f} >= mean={mean:.2f}, "
            f"{recovery_note} "
            f"(attempt {self._restart_attempts}/{max_attempts if max_attempts else '∞'})"
        )
        self._alerter.send(
            f"🔄 Auto-restart #{self._restart_attempts}: stability confirmed\n"
            f"mid={mid:.2f} | hi-lo={hi_lo:.0f} < {max_range:.0f} ({stab_min}m window)\n"
            + (f"⚠️ still below halt stop {self._halt_stop_price:.0f} (buffered recovery)\n"
               if below_halt_stop else "")
            + f"Rebuilding grid..."
        )

        self._halted = False
        self._last_restart_time = now
        # Reset the stop-loss guard so it can fire again on the new grid
        self._sl_guard = None

        # Rebuild grid with fresh ATR-based params
        self._rebuild_grid()

        if max_attempts > 0 and self._restart_attempts >= max_attempts:
            logger.warning(
                f"[AutoRestart] Max attempts ({max_attempts}) reached. "
                f"If bot halts again it will require manual restart."
            )
            self._alerter.send(
                f"⚠️ Auto-restart attempts exhausted ({self._restart_attempts}/{max_attempts}).\n"
                f"Grid is running again for now, but if it hits stop-loss and "
                f"halts again, it will NOT auto-restart — manual restart required."
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
        suppressed = stats.get("suppressed", 0)
        levels     = stats.get("levels",     0)

        # Stop-score snapshot for /status
        score_line = ""
        if self._stop_scorer is not None and self._params is not None:
            mid_now = _price_cache.get_mid() or 0.0
            score   = self._stop_scorer.compute(mid_now, self._params.stop_price)
            thr     = self._get_threshold()
            res_thr = self._cfg.get("stop_score_resume_threshold", 0.10)
            if score >= thr:
                score_icon = "🔴"
            elif score >= res_thr:
                score_icon = "🟡"
            else:
                score_icon = "🟢"
            score_line = (
                f"  {score_icon} Stop-score: `{score:.3f}` "
                f"(gate={thr} resume={res_thr})"
                + (f"  🛡 `{suppressed}` suppressed" if suppressed else "")
            )

        # ── DB queries ────────────────────────────────────────────────────────
        today   = self._store.get_daily(_db_hkt_date(time.time()))
        acc     = self._store.get_accumulated()
        history = self._store.get_recent_daily(7)

        daily_net   = today["net_pnl_usd"]
        daily_sl    = today.get("sl_gross_usd", 0.0)
        daily_sl_n  = today.get("sl_count", 0)
        acc_net     = acc["net_pnl"]
        acc_gross   = acc["gross_pnl"]
        acc_fees    = acc["fees"]          # stored as negative in DB
        acc_cycles  = acc["cycle_count"]
        acc_sl      = acc.get("sl_gross", 0.0)
        acc_sl_n    = acc.get("sl_count", 0)

        # ── Live price ────────────────────────────────────────────────────────
        mid = _price_cache.get_mid()
        mid_str = f"${mid:,.2f}" if mid is not None else "N/A"

        # ── Grid range ────────────────────────────────────────────────────────
        params = self._params
        if params:
            _outside = (
                mid is not None
                and (mid > params.upper or mid < params.lower)
            )
            _outside_tag = " ⚠️ price outside range" if _outside else ""
            range_str   = (f"[{params.lower:,.0f} – {params.upper:,.0f}]"
                           f"  stop={params.stop_price:,.0f}{_outside_tag}")
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

        # ── Capital base for % returns ─────────────────────────────────────────
        # Prefer the configured total_investment_usd (stable, config-driven).
        # Fall back to total_investment_btc converted at current mid, then to
        # the live grid's deployed notional, so % still shows if the operator
        # is using BTC-denominated sizing or the config uses the legacy key.
        capital_base = self._cfg.get("total_investment_usd", 0.0)
        if not capital_base:
            btc_inv = self._cfg.get("total_investment_btc", 0.0)
            if btc_inv and mid:
                capital_base = btc_inv * mid
        if not capital_base and params:
            capital_base = params.notional_per_level * params.levels

        def _pct(v: float) -> str:
            if not capital_base:
                return "N/A"
            return f"{(v / capital_base * 100):+.2f}%"

        # ── Last 7 days table ─────────────────────────────────────────────────
        hist_lines = []
        for row in history:
            sign = "✅" if row["net_pnl_usd"] >= 0 else "❌"
            sl_tag = f"  🚨SL={row.get('sl_gross_usd', 0.0):+.4f}" if row.get("sl_count", 0) > 0 else ""
            hist_lines.append(
                f"  {sign} {row['hkt_date']}  "
                f"net={row['net_pnl_usd']:+.4f}  "
                f"cycles={row['cycle_count']}"
                f"{sl_tag}"
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

        _net_label = "long" if long_qty >= 0 else "short"
        lines = [
            f"📊 *Grid Bot Status* — {now_hkt}",
            f"_{state_line}_",
            "",
            "━━━━━━━━━━━━━━━━━━━━━",
            "*1️⃣  Current Position*",
            f"  • Net {_net_label}:    `{abs(long_qty):.4f} BTC`",
            f"  • Mid price:  `{mid_str}`",
            f"  • Open buys:  `{open_buys}` / Open sells: `{open_sells}`",
            score_line,
            f"  • Grid range: `{range_str}`",
            f"  • Levels:     `{levels}` (spacing ≈ {spacing_str})",
            f"  • Notional/level: `${params.notional_per_level:.2f}` "
            f"(total ≈ `${params.notional_per_level * params.levels:.0f}`)" if params else "",
            "",
            "━━━━━━━━━━━━━━━━━━━━━",
            f"*2️⃣  Daily PnL* (today {today['hkt_date']} HKT)",
            f"  {_e(daily_net)}  Net:   `{daily_net:+.4f} USD` (`{_pct(daily_net)}`)",
            f"  • Gross: `{today['gross_pnl_usd']:+.4f}`  Fees: `{today['fees_usd']:+.4f}`",
            # Only inserted when a stop-loss occurred today — no blank line otherwise.
            *(
                [f"  🚨 Stop-loss: `{daily_sl:+.4f} USD` ({daily_sl_n}× today, included in gross)"]
                if daily_sl_n > 0 else []
            ),
            f"  • Cycles today: `{today['cycle_count']}`",
            "",
            "━━━━━━━━━━━━━━━━━━━━━",
            "*3️⃣  Accumulated PnL* (all-time from DB)",
            f"  {_e(acc_net)}  Net:   `{acc_net:+.4f} USD` (`{_pct(acc_net)}`)",
            f"  • Gross realised: `{acc_gross:+.4f} USD`",
            f"  • Total fees:     `{acc_fees:+.4f} USD`",
            # Only inserted when at least one stop-loss is on record — no blank line otherwise.
            *(
                [f"  • Stop-loss losses: `{acc_sl:+.4f} USD` ({acc_sl_n}× total, included in gross)"]
                if acc_sl_n > 0 else []
            ),
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
        # Prefer the engine's own params (stays current after trail_up/trail_down)
        # over GridBot._params which is only updated on full rebuilds.
        params = (self._engine.get_params() if self._engine else None) or self._params
        if params:
            suppressed = stats.get('suppressed', 0)
            score_str  = ""
            if self._stop_scorer is not None:
                score = self._stop_scorer.compute(mid, params.stop_price)
                score_str = f" score={score:.3f}"
            _out = mid > params.upper or mid < params.lower
            _out_tag = " OUTSIDE_RANGE" if _out else ""
            _pos_label = "long" if stats.get('long_qty', 0) >= 0 else "short"
            logger.info(
                f"[Status] mid={mid:.2f} "
                f"range=[{params.lower:.2f},{params.upper:.2f}] stop={params.stop_price:.2f}{_out_tag} | "
                f"buys={stats.get('open_buys',0)} sells={stats.get('open_sells',0)} "
                f"suppressed={suppressed}{score_str} "
                f"{_pos_label}={abs(stats.get('long_qty',0)):.4f} BTC | "
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
        self._last_trend_slope_pct = result.get("slope_pct", 0.0)
        return result


# ─────────────────────────────────────────────────────────────────────────────
# Entry point
# ─────────────────────────────────────────────────────────────────────────────

def _parse_args():
    parser = argparse.ArgumentParser(description="Grid trading bot")
    parser.add_argument(
        "--reset-state", action="store_true",
        help=(
            "Wipe persisted fill history, daily PnL, and accumulated PnL "
            "(grid_fills/daily_pnl/meta tables) so the bot starts fresh, "
            "as if this were the very first launch. A timestamped backup "
            "of the db file is taken automatically before wiping. This does "
            "NOT close or affect any real position/orders on the exchange — "
            "those are independently reconciled on every startup regardless "
            "of this flag."
        ),
    )
    parser.add_argument(
        "--yes", "-y", action="store_true",
        help="Skip the interactive confirmation prompt for --reset-state "
             "(required when running non-interactively, e.g. under NSSM).",
    )
    return parser.parse_args()


def main():
    args = _parse_args()

    if args.reset_state:
        warning = (
            "\n" + "=" * 70 +
            "\n⚠️  --reset-state: this will PERMANENTLY clear all persisted\n"
            "   fill history, daily PnL, and accumulated PnL for this bot.\n"
            "   (A backup of the db file is taken automatically first.)\n"
            "   Live exchange orders/positions are NOT affected.\n" +
            "=" * 70
        )
        print(warning)
        if not args.yes:
            if sys.stdin.isatty():
                reply = input("Type RESET to confirm, anything else to abort: ")
                if reply.strip() != "RESET":
                    print("Aborted — no changes made.")
                    sys.exit(1)
            else:
                print(
                    "Refusing to reset state non-interactively without --yes. "
                    "Re-run with: --reset-state --yes"
                )
                sys.exit(1)

    bot = GridBot(GRID_CONFIG, reset_state=args.reset_state)

    def _shutdown(sig, frame):
        logger.info(f"[Main] Signal {sig} — shutting down")
        bot.stop()

    signal.signal(signal.SIGINT,  _shutdown)
    signal.signal(signal.SIGTERM, _shutdown)

    bot.start()


if __name__ == "__main__":
    main()
