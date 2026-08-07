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
  Price range [lower, upper] divided into N independent cells (2026-08-03
  restructure — see GridLevel/GridEngine docstrings for the full rationale).
  Each cell is a fixed [lower, upper] price pair running its own
  self-contained 2-phase cycle, never touching a neighboring cell's order:
    Cell opens BELOW mid → BUY@lower; cell opens AT/ABOVE mid → SELL@upper.
    BUY  fill at a cell → place that SAME cell's SELL at its own upper.
    SELL fill at a cell → place that SAME cell's BUY  at its own lower.
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
    "total_investment_usd": 0.0,    # set to 0.0 if using BTC instead
    "total_investment_btc": 0.032,  # e.g. 0.03 BTC; 0.0 = use USD above

    # ── Auto-tuner ────────────────────────────────────────────────────────────
    "auto_tune_enabled":    True,
    "atr_lookback_minutes": 1440,      # 1-day lookback for ATR
    "atr_multiplier":       3.0,       # range = mid ± N×ATR
    "min_grid_pct":         0.0008,    # min grid spacing as fraction of price
                                       # (overridden at runtime by SpacingAutoTuner
                                       # if spacing_autotune_enabled and a persisted
                                       # value exists — see load_persisted())
    "max_grid_levels":      50,
    "min_grid_levels":      5,
    "retune_interval_hours": 24,
    "retune_deadband_pct":  0.10,      # skip re-tune if range shifts < 10%

    # ── Spacing auto-tuner ────────────────────────────────────────────────────
    # Periodically widens/narrows min_grid_pct to hold the fee/gross ratio near
    # a target, without letting cycle frequency collapse. See SpacingAutoTuner.
    # 2026-07-15: introduced after observing fee/gross stuck at ~25% because
    # min_grid_pct (0.0008) was the binding floor on spacing: with a fixed
    # maker_fee_rate, fee/gross ≈ 2 × maker_fee_rate / min_grid_pct, so widening
    # spacing is the only lever available to reduce the ratio.
    "spacing_autotune_enabled":            True,  # opt-in; enable once comfortable
    "spacing_autotune_target_fee_pct":     0.15,   # target fee/gross ratio
    "spacing_autotune_band":               0.05,   # +/- hysteresis band around target
    "spacing_autotune_step":               0.0002, # min_grid_pct adjustment per eval
    "spacing_autotune_min_pct":            0.0008, # never tighten below the original default
    "spacing_autotune_max_pct":            0.0025, # safety ceiling (~10% fee/gross at max)
    "spacing_autotune_eval_days":          3,      # trailing complete days used per evaluation
    "spacing_autotune_interval_h":         24,     # how often to re-evaluate
    "spacing_autotune_min_cycles_per_day": 30,     # floor — below this after a widen, back off

    # ── TrendSignal-driven min_grid_levels auto-tuning ────────────────────────
    # min_grid_levels is the binding constraint that defeats SpacingAutoTuner:
    # with ATR ~38pts and a ~240pt grid, min_levels=5 forces 60pt spacing
    # regardless of min_grid_pct.  The solution: auto-tune min_grid_levels
    # based on TrendSignal regime, evaluated in real-time (every retune cycle).
    #
    # Rationale:
    #   DOWN   → fewer, wider levels.  In a downtrend the grid risks being fully
    #            swept by a sustained move.  Fewer levels = less total exposure,
    #            wider spacing = each level captures more price movement.
    #   NEUTRAL → balanced.  Default behaviour.
    #   UP     → more levels.  In an uptrend the grid is less likely to be swept
    #            downward; tighter spacing captures more chop cycles.
    #
    # When the regime changes, a grid rebuild is triggered immediately so the
    # new level count takes effect without waiting for the next natural retune.
    # The tuned value is stored in _cfg["min_grid_levels"] in-memory; the
    # config default ("min_grid_levels": 5) is used as the NEUTRAL baseline.
    "levels_autotune_enabled":        True,
    "levels_autotune_down_levels":    3,   # DOWN regime  → at most 3 levels (wider, safer)
    "levels_autotune_neutral_levels": 4,   # NEUTRAL      → 4 levels (balanced)
    "levels_autotune_up_levels":      5,   # UP regime    → 5 levels (tighter, more cycles)

    # ── reconcile_open_legs: trend-risk buffer + confirm-dwell ────────────────
    # 2026-08-02 01:34 incident: mid ticked ~15pts outside the (wide) live
    # range while TrendSignal read NEUTRAL (sep=+0.030%, slope=-0.140% —
    # essentially flat) and raw ATR was only 12.70 (well below the 30pt
    # floor). should_retune()'s boundary check correctly forced an immediate
    # reposition (that check protects the 2026-07-09 stop-staleness fix and
    # must stay unconditional — see _rebuild_grid()'s dead-band block), but
    # the LOW-volatility effective_atr it produced collapsed the range width
    # ~50% (366pts -> 183pts), which then made reconcile_open_legs()
    # force-liquidate two open legs at market/taker fees purely because they
    # no longer sat inside the newly-narrow range — nothing about the actual
    # price move justified paying for two round trips. Net loss -8.5688 USD
    # in one rebuild, on a NEUTRAL, low-volatility tick.
    #
    # Fix: reconcile_open_legs() no longer treats "outside [lower,upper]" as
    # an instant, unconditional liquidate. A misfit leg first gets:
    #   1. A trend_risk-scaled tolerance buffer (reuses
    #      StopScoreCalculator.compute_trend_risk() — the same [0,1] score
    #      already trusted for stop-raise gating). trend_risk≈0 ("looks like
    #      noise") -> full buffer, tolerate it; trend_risk≈1 ("genuine
    #      strengthening decline") -> buffer shrinks to 0, evict same as
    #      today. Buffer is sized in effective_atr units (the SAME
    #      volatility read the range itself was built from), not a fixed
    #      price offset, so it scales sensibly across regimes.
    #   2. A confirm-dwell window (reuses StopLossGuard.check()'s
    #      candidate-breach-must-hold pattern) for legs that clear a
    #      direction-correct closing level but still fail the buffered
    #      range test: they get a TENTATIVE re-anchor rather than an
    #      immediate market close, and are only force-liquidated once
    #      they've remained a misfit for reconcile_confirm_s.
    # A leg with NO direction-correct closing level at all in the new grid
    # (e.g. a long leg opened well above every level in a grid that has
    # since dropped) has no cell to hold a resting closer on, so it can't
    # get the buffer/dwell treatment above as-is — see the 2026-08-03
    # 16:35 incident: leg #323 (opened BUY 62401.35) was liquidated at
    # market the instant a retune's new upper (62384.50) fell just 16.85
    # below its open price (one third of one spacing), eating the taker
    # fee and the loss on that gap. 13 minutes later a routine TRAIL UP
    # added a cell that would have covered it easily.
    #
    # Fix: this leg is no longer forced straight to market. Instead
    # (unless trend_risk is already urgent — same bypass as above, a
    # genuinely confirmed move still evicts instantly):
    #   1. GridBot immediately hands it to the existing stray-leg chase
    #      (_chase_close_leg — the same POST_ONLY chase trail-drops use)
    #      for a shot at a decent price within leg_chase_max_attempts ×
    #      leg_chase_wait_s (~90s by default) — see "Stray-leg chase"
    #      below.
    #   2. If that chase exhausts unfilled, the leg is NOT force-liquidated
    #      on the spot either. It's left fully tracked (still counted in
    #      _open_legs / exposure / the daily-loss backstop, just with no
    #      resting protective order — a materially higher-risk wait than
    #      the buffered/dwelling case above, which always keeps SOME order
    #      working) and re-checked at every subsequent rebuild: a real
    #      candidate cell may simply reappear (e.g. the next trail), in
    #      which case it re-anchors normally. If not, it's only tolerated
    #      for reconcile_zero_candidate_max_dwell_s, and that cap itself
    #      shrinks toward 0 as trend_risk climbs toward
    #      reconcile_urgent_trend_risk (linear) — a leg stranded during a
    #      strong, confirmed move gets barely any unmanaged wait at all;
    #      one stranded during flat/noisy conditions gets close to the
    #      full cap. Whichever trend_risk reading is current at each
    #      check (rebuild or chase-exhaustion) governs — it is
    #      deliberately re-read every time, not fixed at the moment the
    #      leg first went stranded.
    "reconcile_buffer_atr_mult": 1.5,    # max tolerance = this × effective_atr, at trend_risk=0
    "reconcile_confirm_s":       1800.0, # must persist as a misfit this long (across retunes) before forced liquidation
    # Urgent bypass: trend_risk at/above this skips the confirm-dwell
    # entirely (mirrors the stop-raise system's own urgent-bypass gate).
    # Defaults to stop_raise_urgent_trend_risk if not set separately, so a
    # confirmed genuine decline evicts immediately in both systems at once.
    "reconcile_urgent_trend_risk": 0.80,
    # Max unmanaged (no resting order) wait for a zero-candidate leg once
    # its post-rescue chase (below) has exhausted, at trend_risk=0. Kept
    # well below reconcile_confirm_s on purpose: that dwell always keeps a
    # real order working somewhere, this one doesn't, so it's tolerated
    # for less time by default. Scales to 0 at reconcile_urgent_trend_risk
    # (same pivot as the urgent bypass above), not at trend_risk=1, so the
    # two thresholds agree exactly on what counts as "confirmed" enough to
    # stop waiting.
    "reconcile_zero_candidate_max_dwell_s": 900.0,

    # Pre-chase grace (2026-08-06, REPRICE_UNDERCOUNT_2026_08_05 follow-up):
    # in the legacy behavior (this at 0.0), a leg is handed to the stray-leg
    # chase on the SAME rebuild it's first flagged zero-candidate — no wait
    # at all before paying (at best) a maker-fee loss at/near current
    # market. Backtested against 202 historical loss-realizing zero-
    # candidate/trail closes (24 days of fills, Jul 11 - Aug 4, plus a full
    # day of 1-min candles, Aug 5 - Aug 6; $215.07 total realized loss
    # across them): price crossed back through the leg's own open price —
    # count-based / $-weighted — within 1h in 53.2% of cases (31.2% of $),
    # within 4h in 64.9% (48.8% of $), within 8h in 73.7% (54.2% of $).
    # Net effect (avoided losses MINUS the extra loss on cases that never
    # recovered in the window, since those still eventually chase-close,
    # possibly at a worse price after drifting further) was positive at
    # every window tried: roughly +$31-43 (15-30min), +$35-36 (1-2h),
    # +$52 (4h), +$94 (8h) out of the $215.07 baseline — noisy given the
    # sample is mostly one 4-day episode plus one day, not a broad set of
    # independent trends, so treat these as directional, not precise.
    # Set to a positive value to hold a newly-stranded leg with NO resting
    # order for up to this many seconds before starting the chase — same
    # unmanaged-wait mechanics as reconcile_zero_candidate_max_dwell_s
    # above, just applied before the chase instead of only after it fails,
    # and subject to the same reconcile_urgent_trend_risk bypass (a
    # genuinely urgent move skips the grace and chases immediately).
    # Starting recommendation: 7200 (2h) — meaningful recovery capture
    # without leaving a leg unmanaged for most of a trading day; revisit
    # against 4h/8h (backtested better, but more unmanaged-exposure time
    # than this sample's P&L-only view can price in) once more days of
    # REPRICE_UNDERCOUNT-fixed data accumulate.
    "zero_candidate_pre_chase_grace_s": 7200.0,

    # Cooldown between GridEngine's "orphaned leg — requesting fast rebuild"
    # triggers (see check_price_fills()). Without this, an orphaned/zero-
    # candidate leg that the dead-band check keeps bouncing (range hasn't
    # shifted >= retune_deadband_pct yet) re-requests a rebuild on every
    # single tick with nothing throttling the main loop in between — each
    # attempt calls GridAutoTuner.compute() and immediately bails out in
    # _rebuild_grid()'s dead-band check without ever reaching
    # reconcile_open_legs(), so the orphan condition is never actually
    # cleared and the exact same trigger fires again next tick.
    # 2026-08-06 GEN00037_GREEN incident: leg #816 went zero-candidate at
    # handoff (19:03:12) and wasn't force-liquidated by the dwell cap until
    # 19:19:59 (1007s later, see reconcile_zero_candidate_max_dwell_s).
    # For that entire 1007s window the trigger re-armed and got dead-band-
    # skipped on every tick — 36,614 times — at ~36/s, producing ~290
    # log lines/s (188K of them just from AutoTuner.compute()) and pinning
    # the main loop in a CPU-bound busy-spin the whole time (no sleep on
    # this path). The dwell cap still correctly liquidated the leg on
    # schedule regardless — this cooldown doesn't change that outcome, it
    # only stops the request from re-firing faster than it could possibly
    # do anything, since a dead-band-blocked rebuild can't change outcome
    # tick-to-tick anyway (mid moves far too little between consecutive
    # ticks to cross retune_deadband_pct). Set to 0 to restore the old
    # (broken) every-tick behavior.
    "orphan_leg_rebuild_cooldown_s": 15.0,

    # ── Stray-leg chase (trail-up/trail-down dropped-cell closers) ────────────
    # A leg whose closing cell gets dropped by _trail_up/_trail_down no longer
    # maps onto any remaining cell boundary (see GridEngine._trail_up
    # docstring) — reconcile_open_legs' cell-fit logic isn't the right tool
    # for it. GridBot._chase_close_leg_worker instead posts POST_ONLY at the
    # current best bid/ask on the closing side, leaves it resting for
    # leg_chase_wait_s, and reprices on every unfilled attempt, up to
    # leg_chase_max_attempts, before falling back to a market close.
    # wait_s=30 (not longer): if it hasn't filled in 30s it's very unlikely
    # to fill materially better by just sitting there longer at a now-stale
    # quote — repricing against a fresh quote on the next attempt does more
    # for fill odds than a longer wait on the same one.
    "leg_chase_max_attempts": 3,      # POST_ONLY attempts before market fallback
    "leg_chase_wait_s":       30.0,   # how long each attempt rests

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

    # ── Confirmed-trend catch-up (2026-08-04) ─────────────────────────────────
    # A single drift-shift is a fine reaction to routine drift, but it's
    # always exactly one spacing behind price by construction — the top-sell
    # only fires once price has already cleared the previous top by a full
    # spacing. During a genuinely sustained move this compounds: the 2026-08-03
    # 17:46-17:56 incident saw THREE separate legs (#327-#329) each opened as
    # a fresh short near a then-current top, then evicted (chase-closed at a
    # loss averaging -1.28 USD) a few minutes later as the very next
    # drift-shift dropped their closer. By the time of the FIRST of those
    # three evictions (17:46), FIVE consecutive same-direction drift-shifts
    # had already fired over the prior 58 minutes — strong, direct,
    # already-logged evidence this was a sustained rally, not noise.
    #
    # Fix: track how many top-sell-triggered up-shifts have fired within
    # drift_shift_trend_lookback_s. Once that count reaches
    # drift_shift_trend_confirm_count, the *next* shift catches the range up
    # by drift_shift_trend_catchup_extra additional spacings in the same
    # event, instead of waiting for price to grind further away first and
    # triggering yet another isolated one-spacing shift later. This evicts
    # whatever's sitting on the bottom sooner — closer to its own open price,
    # so usually for less — rather than later, once price has drifted
    # further still.
    #
    # The real tradeoff, stated plainly: this is a genuine bet that a
    # confirmed pattern continues. If price reverses right after catch-up
    # fires, a leg (or several, in one burst) will have been evicted that
    # might otherwise have recovered on its own. There's no version of this
    # that removes that risk — only evidence-gating which side of it you're
    # more willing to take. Set drift_shift_trend_confirm_count higher (or
    # catchup_extra to 0) to make this more conservative; 0 disables catch-up
    # entirely and restores the exact pre-2026-08-04 behaviour.
    #
    # This only applies to the top-sell → trail_up path today — there is no
    # fill-triggered mirror of drift-shift on the bottom-buy → trail_down
    # side to extend (see trailing_down_enabled above, a different,
    # periodic-retune-driven mechanism instead).
    "drift_shift_trend_lookback_s":    1800.0, # window for counting prior same-direction shifts
    "drift_shift_trend_confirm_count": 2,      # prior shifts within the window needed to call it confirmed
    "drift_shift_trend_catchup_extra": 1,      # extra trail_up calls (beyond the usual 1) once confirmed


    # ── Stop-loss ─────────────────────────────────────────────────────────────
    "stop_loss_enabled": True,
    # Confirmation window (2026-07-30): mid must stay continuously below
    # stop_price for this many seconds before StopLossGuard actually latches
    # _triggered, instead of the old instant single-tick trigger. Added after
    # the 2026-07-28 06:40:47 halt, where mid touched 64113.05 (one tick below
    # a stop of 64126.45) and had already recovered to 64150-64257 within
    # ~30s — a wick, not a breakdown — but still cost a full liquidation plus
    # ~11.5h of cooldown/recovery-floor downtime. Set to 0 to restore the old
    # instant-trigger behavior. See StopLossGuard.check().
    "stop_loss_confirm_s": 3.0,
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

    # ── Stop-buffer widening: baseline window + absolute floor (2026-07-28) ──
    # 2026-07-28 06:02 rebuild: effective_atr=84.95 vs a mean of the last ~20
    # RETUNE EVENTS (not a fixed time window). Because retunes cluster during
    # volatile stretches (the morning already had multiple regime shifts), that
    # event-count mean was itself already elevated (~60-80), so the relative
    # ratio never crossed the 1.5x threshold and the buffer stayed at the base
    # 3.0x even though 84.95 pts was ~2.4x the raw 24h ATR (34.91). A further
    # ~330-pt move in well under a minute then breached the stop.
    #
    # Two independent additions, both layered on top of the pre-existing
    # relative-expansion check (they only ever WIDEN the buffer further, never
    # narrow it):
    #
    #  1. stop_buffer_baseline_window_hours — the "recent mean" used for the
    #     relative-expansion ratio is now built from samples within a fixed
    #     WALL-CLOCK window instead of the last N retune events, so a burst of
    #     retunes during a choppy stretch can no longer drag the baseline up
    #     to match current conditions. Falls back to the old event-count mean
    #     if there isn't yet enough time-window history (e.g. just after
    #     startup) — see stop_buffer_baseline_min_samples/_min_span_frac.
    #
    #  2. stop_buffer_atr_absolute_widen_mult — a SEPARATE, absolute trigger:
    #     if effective_atr exceeds (min_atr_floor_pts × this), the buffer
    #     widens proportionally regardless of what the recent mean is doing.
    #     This catches the case above: 84.95 is 2.83x the 30.0-pt quiet-market
    #     floor even though it wasn't "expanding" relative to an already-hot
    #     recent baseline.
    #
    #     IMPORTANT — trend-regime gate: on 2026-07-28, TrendSignal had
    #     already flagged DOWN at the same 06:02 rebuild. Checking what
    #     actually happened next: price kept falling for hours afterwards
    #     (mid ~63100-63700 through 11:00, well past the ~64113 flash low that
    #     tripped the stop). A wider stop at that moment would NOT have saved
    #     a whipsaw — it would have kept the position open into a much larger,
    #     ongoing decline. So BOTH widen paths (relative expansion AND the
    #     absolute trigger above) are gated OFF whenever TrendSignal reports
    #     DOWN: widening is meant to protect against noise-driven ATR spikes
    #     in a range-bound/uptrending market, not to loosen the stop during a
    #     confirmed downtrend, which is exactly when the tighter stop is doing
    #     its job. (The gate applies to both paths because the reasoning
    #     doesn't depend on which check would have triggered the widen — an
    #     earlier revision of this fix only gated the absolute path, which
    #     left the relative-expansion path, now more sensitive thanks to the
    #     windowed baseline above, able to widen the buffer during a
    #     downtrend anyway. Corrected before this landed.) Set
    #     stop_buffer_absolute_widen_skip_on_downtrend=False to remove this
    #     gate entirely if you decide you don't want it.
    #
    #     NOTE: this gate originally depended on TrendSignal.regime being read
    #     as a property (self._trend.regime, no parens) at the _rebuild_grid()
    #     call site — TrendSignal.regime is decorated @property specifically
    #     so that bare attribute access returns the confirmed regime string
    #     rather than a bound method; an earlier revision of this patch lacked
    #     that decorator, which made the trend_regime comparison always False
    #     and silently disabled this entire gate. The call site has since
    #     moved to self._effective_trend_regime() (see that method), which
    #     carries the last CONFIRMED regime through INSUFFICIENT_DATA stretches
    #     instead of reading TrendSignal live — but it still ultimately reads
    #     regime strings produced the same way, so the same failure mode
    #     (a bound method compared to a string, always False) is worth
    #     knowing about if this gate ever appears silently disabled again.
    "stop_buffer_baseline_window_hours":      12.0,  # wall-clock window for the relative-expansion mean
    "stop_buffer_baseline_min_samples":        3,    # need >= this many samples in the window to trust it
    "stop_buffer_baseline_min_span_frac":      0.25, # ...and they must span >= this fraction of the window
    "stop_buffer_atr_absolute_widen_mult":     2.0,  # absolute reference = min_atr_floor_pts × this
    "stop_buffer_absolute_widen_skip_on_downtrend": True,

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
                                             #
                                             # 2026-07-28: after a 06:40 stop-loss (halt_stop=64126.45),
                                             # price kept falling to ~63100-63300 and stayed there. The
                                             # floor decay hit its 15×ATR cap at ~4.83h halted (floor
                                             # pinned ~63565) and, since it never decays past that cap,
                                             # the bot was still waiting at 11:33 (~4.9h halted) with
                                             # mid ~300+ pts below the floor and no further easing ever
                                             # coming — i.e. condition 2 (price recovery) could stay
                                             # unsatisfiable indefinitely once the cap is hit, even
                                             # though conditions 3/4 (tight range, flat/rising) might
                                             # otherwise be satisfied by a genuinely calm new price level.
                                             # See auto_restart_max_halt_hours below for the fix.
    "auto_restart_max_halt_hours": 8.0,      # Hard timeout for condition 2 (price-recovery-floor) only.
                                             # Once halted this long, condition 2 is SKIPPED entirely —
                                             # the bot no longer waits for mid to clear the (now-capped)
                                             # recovery floor at all. Conditions 1 (cooldown), 3 (tight
                                             # range) and 4 (flat/rising trend) are NOT skipped: the bot
                                             # still requires the market to have genuinely settled into a
                                             # stable band before restarting, it just stops insisting that
                                             # band be within reach of the OLD stop level. A one-time
                                             # Telegram alert fires the moment the timeout is reached so
                                             # you know the gate has changed, and manual /restart remains
                                             # available at any time regardless of this setting.
                                             # Set to 0 to disable (old behaviour: condition 2 can block
                                             # forever once the floor decay is capped).
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

    # ── TrendSignal gate integration ──────────────────────────────────────────
    # When TrendSignal is DOWN, the effective BuyGate threshold is multiplied
    # by trend_gate_down_threshold_mult (< 1.0), making it easier to suppress
    # buys.  e.g. threshold=0.25 * mult=0.60 → effective 0.15 on DOWN.
    #
    # Additionally, when the regime is DOWN and price is OUTSIDE_RANGE (the
    # grid has been swept and the bot is fully long and most vulnerable),
    # ALL new counter-buys are suppressed regardless of score. This directly
    # addresses the SL2 pattern (2026-07-22 22:05-22:17): the drift-shift
    # moved the grid up, BTC reversed, price went outside range below the
    # grid, 5 buys accumulated into a DOWN regime → stop triggered.
    "trend_gate_enabled":                         True,
    "trend_gate_down_threshold_mult":             0.60,  # 0.25 * 0.60 = 0.15 effective threshold
    "trend_gate_outside_range_block_on_down":     True,  # block ALL buys when DOWN + OUTSIDE_RANGE
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

    # ── SellGate (2026-08-04) ──────────────────────────────────────────────────
    # Mirror of the TrendSignal-gate half of BuyGate above, for the opposite
    # side: BuyGate withholds new buy-side exposure during a confirmed DOWN
    # move; nothing symmetric withheld new short-side exposure during a
    # confirmed UP move. The 2026-08-03 17:46-17:56 incident is a direct
    # consequence — legs #327-#330 were all opened as fresh shorts directly
    # into the same sustained rally that went on to chase-close each of them
    # at a loss. SellGate closes that specific gap.
    #
    # It deliberately does NOT reuse StopScoreCalculator's score/threshold —
    # that score (and its velocity term especially) is explicitly a
    # downtrend-strengthening measure (see compute_trend_risk's docstring):
    # velocity is `max(0.0, prev_mid - mid)`, hard-clamped to 0 on every
    # up-tick. Reusing it here wouldn't gate a rally at all, it would just
    # silently under-react to one. Instead SellGate uses two independently
    # justified, already-available signals:
    #   1. OUTSIDE_RANGE block — exact mirror of BuyGate's DOWN + below-lower
    #      block: TrendSignal UP + mid already above the grid's whole upper
    #      bound (fully short, most exposed to a continued rise) blocks ALL
    #      new sell-side orders — both a fresh short open and a covering/
    #      closing sell on an existing long — same "let a confirmed move run
    #      before rushing to act against or into it" logic BuyGate already
    #      applies on the down side.
    #   2. Confirmed-uptrend block — reuses the SAME empirical
    #      drift_shift_trend_* evidence above (real, already-observed
    #      repeated shifts) rather than a second new asymmetric score.
    # Like BuyGate, a suppressed sell is marked SUPPRESSED and released later
    # by release_one_suppressed_level() once conditions clear — nothing is
    # abandoned, just delayed.
    "sell_gate_enabled":                    True,
    "trend_gate_outside_range_block_on_up": True,  # block ALL sells when UP + OUTSIDE_RANGE (above upper)
    "sell_gate_block_on_confirmed_uptrend": True,  # also suppress once drift_shift_trend_confirm_count is met

    # ── Trail-flip: stop seeding fresh shorts/longs INTO a confirmed move (2026-08-05) ──
    # SellGate (above) only gates the counter-SELL placed after a cell's own
    # BUY leg fills — it never touched GridEngine._trail_up's Step 2, which
    # unconditionally opens a BRAND NEW SELL (a fresh short) at the new top
    # cell every single time the grid trails up, by design ("a trail up
    # always seeds a fresh short as the natural continuation of the
    # breakout that triggered it"). That is exactly backwards during a
    # confirmed, persistent uptrend: each fresh short opened this way has
    # only one spacing of headroom before the NEXT trail_up drops it again,
    # handing it to the stray-leg chase, which force-closes it at an
    # ever-worse (higher) price. 2026-08-05 07:09-13:26: 11 separate
    # trail_up chase-closes this way in one session, -13.25 USD, while
    # SellGate (guarding a different code path) stayed green the whole
    # time. BuyGate/_trail_down has the exact mirror problem on the way
    # down.
    #
    # Fix: when the SAME confirmed-uptrend predicate SellGate already uses
    # (GridBot._sell_gate, via GridEngine._sell_gate_fn) is blocking, the
    # new top cell TRAIL UP creates opens as a BUY (a dip-buy level)
    # instead of a fresh SELL. This is mechanically safe specifically here
    # — unlike a blanket "flip every SELL cell above mid" — because
    # TRAIL UP only fires once mid has already cleared new_upper, so the
    # new cell's entire range (and therefore its lower boundary, where the
    # BUY would rest) is guaranteed to already be BELOW current mid: a
    # legitimate passive resting order, never one that crosses the spread.
    # Mirrored for TRAIL DOWN using GridEngine._buy_gate_fn (BuyGate's
    # confirmed-DOWN read: TrendSignal DOWN regime + score threshold —
    # see GridBot._buy_gate).
    "trail_flip_to_buy_on_confirmed_uptrend":     True,
    "trail_flip_to_sell_on_confirmed_downtrend":  True,

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
    # Connect-failure alert: send a Telegram alert once the WS fails to
    # connect ws_error_alert_count times IN A ROW with no successful reconnect
    # in between (e.g. a Cloudflare/IP block on the handshake). This is
    # separate from the reconnect-flood alert above, which only fires on
    # successful reconnects — a total block would otherwise go unnoticed no
    # matter how long it lasts, since no reconnect ever succeeds to trigger it.
    "ws_error_alert_count": 5,            # consecutive failed attempts before alerting
    "ws_stale_threshold_s":   20,
    "ws_reconnect_backoff_s":  2,
    "ws_max_backoff_s":       60,
    # Blue-green deployment: how long green waits for blue to export its snapshot
    # before falling back to a cold start.  Should be well above the time blue
    # needs to freeze + write JSON (typically < 1s), but short enough that a
    # dead blue process doesn't stall green's startup.
    "bg_handoff_timeout_s":   10,
    # Blue-green: param continuity on handoff. When green's auto-tuner computes
    # params that are "close enough" to the snapshot's, green reuses the
    # snapshot's own lower/upper/spacing verbatim instead of the fresh values.
    # This means every level price is identical to blue's → 100% order match
    # instead of 0% match from a small mid-price drift. stop_price and
    # notional_per_level are always taken from the fresh auto-tuner result.
    #
    # handoff_anchor_max_spacing_drift: how many spacings the new lower can
    #   differ from the snapshot's lower before we treat it as a genuine
    #   structural retune (not just a mid-price drift). Default 2.0 = if
    #   the range shifted by less than 2 × spacing, it's drift, not retune.
    # handoff_anchor_max_spacing_pct: max fractional change in spacing before
    #   we treat the grid structure as genuinely different. Default 0.20 = 20%.
    "handoff_anchor_max_spacing_drift": 2.0,
    "handoff_anchor_max_spacing_pct":   0.20,
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
    "daily_loss_limit_usd":     50.0,   # halt if today's net loss exceeds this USD
    "daily_loss_limit_enabled": True,   # set False to disable without removing limit

    # ── Funding rate (perpetuals) ────────────────────────────────────────────
    # Fetch the 8-hourly funding rate from CDC REST and accumulate estimated
    # funding cost/income based on net long position.
    # Rate is fetched once per funding_rate_fetch_interval_s (default 8h).
    "funding_rate_enabled":          True,
    "funding_rate_fetch_interval_s": 28800,   # 8 hours — matches CDC settlement
    "funding_rate_instrument":       "BTCUSD-PERP",

    # ── Status reporting ─────────────────────────────────────────────────────
    # How often to log Status and evaluate TrendSignal (seconds).
    # 900 = 15 min is recommended for live — frequent enough to catch issues,
    # quiet enough not to flood the log.  Set to 60 during debugging.
    "status_interval_s": 900,

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
        """
        Prune down to the last backup_count files.

        Scans by a fixed shared prefix ("grid_bot"), NOT self.base_name.
        base_name is now unique per live instance (grid_bot_gen{N}_...,
        see _init_logging) specifically so concurrent/successive instances
        never share a file. If this scanned by self.base_name instead, each
        instance would only ever see its own single file and could never
        prune any OTHER generation's old files — they'd accumulate forever.
        Scanning by the family-wide prefix lets any instance's rollover
        clean up everything, regardless of which generation or role wrote it.
        """
        try:
            files = sorted(f for f in os.listdir(self.log_dir)
                           if f.startswith("grid_bot") and f.endswith(".log"))
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
_atexit_registered: bool = False


def _init_logging(config: dict, role: str = "", log_gen: Optional[int] = None) -> logging.Logger:
    """
    Initialise async queue-based logging.

    role: cosmetic only now — included in the filename for human readability
          (which role initiated this instance), but no longer what makes
          filenames unique. Kept as a parameter so early-boot logging (the
          module-import call below, and main()'s call before the bg_lock
          generation is known) can still say "blue"/"green" in the filename
          before log_gen exists.
    log_gen: the bg_lock generation number for THIS live instance (see
          GridStateStore.bg_lock_try_acquire). When given, log files are
          named grid_bot_gen{N:05d}_{role|standalone}_YYYY-MM-DD.log — the
          gen number is what actually guarantees a distinct file per
          instance. Naming purely off role (the old scheme) broke down
          once "no role swap" meant every deploy after the first used
          --role green: all of them piled into the same
          grid_bot_green_YYYY-MM-DD.log, since role no longer tracks which
          process is actually live. When log_gen is None (only true before
          any instance has acquired bg_lock), falls back to the role-only
          name.
    Zero-padded and placed immediately after the "grid_bot_gen" prefix so
    that plain alphabetical filename sort is also chronological order,
    regardless of role or date — see _prune_old_logs, which relies on that.

    Idempotent: this is called once unconditionally at module import
    (role="", log_gen=None, below), again from main() once role is known
    (still log_gen=None — bg_lock hasn't been acquired yet at that point),
    and a third time from GridBot.start() once log_gen is known, so that
    this instance's actual logging lands in its own file rather than
    whatever main()'s early call picked. A prior version of this function
    wasn't safe to call twice — each call spun up a brand new
    _SafeQueueListener thread and attached a brand new QueueHandler to the
    same "GridBot" logger, on top of whatever the previous call had already
    set up, rather than replacing it. Since both listeners shared the same
    module-level _log_queue, the result was two listener threads racing to
    dequeue from the same queue while two QueueHandlers each enqueued every
    record once — non-deterministically splitting and/or duplicating every
    subsequent log line across TWO different files, depending purely on
    which listener thread happened to win the race for the queue item that
    particular time. Confirmed via a standalone reproduction: calling this
    twice reliably produced 0/1/2 copies of a given line split unpredictably
    between the two files.

    Now: any previously-running listener is stopped and any handler this
    function previously attached to the logger is removed before the new
    ones are created, so at most one QueueHandler/listener pair is ever
    active regardless of how many times this is called (now three).
    """
    global _listener, _file_handler

    # Stop and detach whatever a previous call to this function set up, if
    # anything — see docstring above for why this has to happen before
    # creating the new listener/handler, not after.
    if _listener is not None:
        try:
            _listener.stop()
        except Exception:
            pass
    if _file_handler is not None:
        try:
            _file_handler.close()
        except Exception:
            pass
    log = logging.getLogger("GridBot")
    for h in list(log.handlers):
        if isinstance(h, logging.handlers.QueueHandler):
            log.removeHandler(h)
            try:
                h.close()
            except Exception:
                pass

    log_dir      = config.get("log_dir", "logs_grid")
    backup_count = config.get("log_backup_count", 30)
    level        = getattr(logging, config.get("log_level", "INFO").upper(), logging.INFO)
    fmt          = logging.Formatter("%(asctime)s [%(levelname)s] %(message)s",
                                     datefmt="%Y-%m-%d %H:%M:%S")

    if log_gen is not None:
        base_name = f"grid_bot_gen{log_gen:05d}_{role or 'standalone'}"
    else:
        # Pre-acquisition calls (module import, and main() before bg_lock is
        # taken) — no gen number exists yet. This name is transient: it's
        # superseded the moment GridBot.start() re-calls _init_logging with
        # the real log_gen, so nothing meaningful ever accumulates under it
        # beyond a few lines of very-early boot logging.
        base_name = f"grid_bot_{role}" if role else "grid_bot"
    _file_handler = _HKTDailyRotatingHandler(log_dir, base_name=base_name,
                                              backup_count=backup_count)
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

    global _atexit_registered
    if not _atexit_registered:
        def _stop_listener_atexit():
            if _listener is not None:
                try:
                    _listener.stop()
                except Exception:
                    pass
        atexit.register(_stop_listener_atexit)
        _atexit_registered = True

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


def _md_escape(text: str) -> str:
    """
    Escape Telegram legacy-Markdown special characters (_ * ` [) in a
    free-form identifier before it's interpolated into an AlertManager
    message sent with parse_mode="Markdown".

    2026-08-03 fix: close_reason-style identifiers like "rebuild_reprice"
    or "trail_up" carry an odd number of unescaped underscores, which
    Telegram's legacy Markdown parser reads as an opening italic marker
    with no matching close — the whole message is then rejected with
    HTTP 400 "can't parse entities". AlertManager._post() already catches
    that and retries as plain text, so no alert was ever silently lost,
    but every rebuild-reprice/trail-up/trail-down leg closure paid for a
    wasted round trip and a warning log line. Escaping the identifier
    before it goes into the Markdown-mode text avoids that entirely.

    Also covers newer reason values from the zero-candidate dwell system
    (e.g. "rebuild_reprice_pending", "rebuild_reprice_chase_exhausted")
    the same way — this is a general parser-safety fix, not tied to any
    one specific identifier string.

    Not applied to log lines (only real risk there is readability) or to
    values written to the DB (grid_fills.close_reason etc. must stay the
    exact, unescaped identifier) — only to the copy of the string that
    goes into a Markdown-parsed Telegram message.
    """
    for ch in ("_", "*", "`", "["):
        text = text.replace(ch, "\\" + ch)
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
                logger.warning(f"[AlertManager] attempt {attempt} HTTP "
                                f"{resp.status_code}: {resp.text[:200]}")
                if (resp.status_code == 400 and payload.get("parse_mode")
                        and "can't parse entities" in resp.text):
                    logger.warning("[AlertManager] malformed Markdown entities — "
                                    "retrying as plain text")
                    payload.pop("parse_mode")
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
        """Signal stop and wait for the poll thread to drain (up to HTTP_TIMEOUT+5s)."""
        self._stop.set()
        self._thread.join(timeout=self._HTTP_TIMEOUT + 5)

    def stop_nowait(self) -> None:
        """
        Signal stop WITHOUT joining the thread.  Used during blue-green handoff
        export: the outgoing process needs to abort its in-flight getUpdates long-
        poll (which holds the bot token exclusively for up to _POLL_TIMEOUT=30s)
        so the incoming process's TgPoller doesn't get a 409 Conflict.  We
        can't block here for up to 45s waiting for the HTTP request to drain —
        the handoff must complete in under 2s.  The thread will exit on its own
        once _stop is set (checked at the top of every poll iteration and inside
        the retry back-off loop), and the process itself exits ~1-2s after the
        handoff completes, so there is no thread leak.
        """
        self._stop.set()
        logger.info("[TgPoller] stop_nowait — signalled (not joining; process exiting soon)")

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
    # None = use the module default _MAKER_FILL_TIMEOUT (grid-cell orders).
    # Set explicitly by callers that need a POST_ONLY order to rest longer
    # (e.g. GridBot._chase_close_leg_worker's 5-minute chase attempts).
    maker_timeout_s: Optional[float] = None

    @classmethod
    def limit_maker(cls, side: str, qty: float, price: float,
                    instrument: str, purpose: str = "grid",
                    maker_timeout_s: Optional[float] = None) -> "OrderRequest":
        """POST_ONLY limit — maker fee, rests on book."""
        return cls(side=side, qty=qty, instrument=instrument,
                   order_type="LIMIT", price=price,
                   exec_inst=["POST_ONLY"], purpose=purpose,
                   maker_timeout_s=maker_timeout_s)

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
            if req.maker_timeout_s is not None and req.exec_inst:
                # An explicit maker_timeout_s means the caller actually
                # needs the resting-order wait simulated (currently only
                # GridBot._chase_close_leg_worker's leg-chase orders) —
                # _paper_fill's instant fill doesn't apply here. Runs on
                # its own thread, not this one, since it can legitimately
                # take up to maker_timeout_s and this is the shared
                # OMS-worker thread every other order (including ordinary
                # grid cell fills) is queued behind. See
                # _paper_maker_fill_with_timeout()'s docstring.
                threading.Thread(
                    target=self._paper_maker_fill_with_timeout_safe, args=(req,),
                    name=f"paper-maker-timeout-{req.client_oid[:8]}",
                    daemon=True).start()
            else:
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

        Does NOT handle orders with an explicit req.maker_timeout_s — those
        are submitted at the CURRENT touch (not a level price crossed
        already) and genuinely need to rest and wait for the market to move
        back through it, which _process_order routes to
        _paper_maker_fill_with_timeout() instead. See that method's
        docstring for why "instant" stopped being accurate for that case.
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

    # ── Paper fill: resting order with a real maker-timeout wait ───────────────

    def _paper_maker_fill_with_timeout(self, req: OrderRequest):
        """
        Paper-mode counterpart to _maker_timeout_handler() for orders that
        need a realistic resting-order simulation instead of _paper_fill's
        instant fill — currently only GridBot._chase_close_leg_worker's
        leg-chase closes (see OrderRequest.maker_timeout_s).

        _paper_fill's instant-fill simplification is valid for grid cell
        orders specifically because _simulate_paper_fills() never submits
        one until price has already reached that exact level — by the time
        it reaches _paper_fill, "instant" is accurate. A leg-chase order is
        different: it's POST_ONLY at the CURRENT touch, explicitly meant to
        rest for up to maker_timeout_s waiting for the market to move back
        through that price, exactly like a real exchange order would.
        _paper_fill filled that too, unconditionally and instantly, at the
        current touch — every chase attempt 1/3 always filled on attempt 1,
        so the reprice/retry loop and the exhaustion→market-fallback branch
        in _chase_close_leg_worker (both already correctly written to
        handle a real timeout) were never actually exercised in paper mode.
        See the 2026-08-04 "8 legs closed via chase" incident.

        Polls _price_cache.get_l1() for a genuinely NEW tick (an L1 reading
        distinct from the previous poll) that has crossed the resting
        price on the correct side — ask <= price for a BUY, bid >= price
        for a SELL, the same direction a real resting POST_ONLY order would
        need the market to move for it to be marketable. FILLED at
        req.price if that happens within maker_timeout_s (module default
        _MAKER_FILL_TIMEOUT if unset); otherwise CANCELLED, all-or-nothing
        (filled_qty=0) — no partial-fill simulation, since there's no order
        book queue position to partially consume in paper mode.
        _chase_close_leg_worker already treats a zero-fill CANCELLED the
        same as an unfilled timeout (reprice and retry), so this is a
        faithful stand-in for "the maker order never got hit."

        Runs on its own thread (see _process_order), not the shared
        OMS-worker thread — otherwise a single resting leg-chase order
        would stall every other queued order, including ordinary grid
        fills, for up to maker_timeout_s. Live mode had this same shape
        of problem, and worse: _live_fill() called _maker_timeout_handler()
        synchronously on OMS-worker for every POST_ONLY order, not just
        leg-chase, so it stalled the whole pipeline behind any ordinary
        resting grid-cell maker order too — fixed alongside this, see
        _maker_timeout_handler_safe.
        """
        timeout  = req.maker_timeout_s if req.maker_timeout_s is not None else _MAKER_FILL_TIMEOUT
        deadline = time.time() + timeout
        is_buy   = req.side == "BUY"
        # None, not an actual get_l1() reading: if it started as a real
        # snapshot, a market that was ALREADY crossed at that exact instant
        # (a narrow but real window — this thread's first read happens
        # slightly after the price the calling chase attempt priced off
        # of) would never get evaluated, since only READINGS THAT DIFFER
        # FROM last_l1 get checked below — and in a quiet/frozen market
        # that already-crossed state could persist for the whole timeout
        # unevaluated. None guarantees the first real reading always
        # differs from it, so it's always checked.
        last_l1  = None

        while time.time() < deadline:
            time.sleep(0.5)
            l1 = _price_cache.get_l1()
            if l1 == last_l1:
                continue
            last_l1 = l1
            bid, ask, _mid = l1
            crossed = (ask is not None and ask <= req.price) if is_buy \
                else (bid is not None and bid >= req.price)
            if crossed:
                maker_fee = self._cfg.get("maker_fee_rate", 0.0001)
                fee = req.price * req.qty * maker_fee
                logger.debug(
                    f"[OMS][PAPER] Resting fill {req.purpose} {req.side} "
                    f"{req.qty:.4f} @ {req.price:.2f} — market crossed back "
                    f"fee={fee:+.6f} (maker) [{req.client_oid[:8]}]"
                )
                self._deliver_fill(FillEvent(
                    client_oid=req.client_oid,
                    order_id=f"paper-{req.client_oid[:8]}",
                    status=OrderStatus.FILLED,
                    filled_qty=req.qty,
                    avg_price=req.price,
                    fee=fee,
                    purpose=req.purpose,
                ))
                return

        logger.debug(
            f"[OMS][PAPER] Resting order timeout {req.purpose} {req.side} "
            f"{req.qty:.4f} @ {req.price:.2f} after {timeout:.0f}s — "
            f"cancelling [{req.client_oid[:8]}]"
        )
        self._deliver_fill(FillEvent(
            client_oid=req.client_oid,
            order_id=f"paper-{req.client_oid[:8]}",
            status=OrderStatus.CANCELLED,
            filled_qty=0.0,
            avg_price=0.0,
            fee=0.0,
            purpose=req.purpose,
        ))

    def _paper_maker_fill_with_timeout_safe(self, req: OrderRequest):
        """
        Thread entry point for _paper_maker_fill_with_timeout() (see
        _process_order's submission comment for why this runs off
        OMS-worker). Same reasoning as _maker_timeout_handler_safe for the
        live-mode path: this now runs on its own daemon thread instead of
        inline behind _worker_loop's try/except, so an unhandled exception
        here would otherwise just log a traceback and let the thread die —
        no FillEvent ever delivered, and the caller's wait_fill() would
        time out looking identical to an ordinary "never got hit" chase
        attempt (INFO log), silently hiding whatever actually broke.

        Unlike the live-mode wrapper, there's no self._orders/_exid_to_coid
        bookkeeping to unwind — paper mode never registers an order there
        in the first place (see _paper_fill) — so this only needs the
        error log and the REJECTED fallback delivery.
        """
        try:
            self._paper_maker_fill_with_timeout(req)
        except Exception as e:
            logger.error(
                f"[OMS][PAPER] resting-order simulation error "
                f"{req.client_oid[:8]}: {e}", exc_info=True)
            self._deliver_fill(FillEvent(
                client_oid=req.client_oid,
                order_id=f"paper-{req.client_oid[:8]}",
                status=OrderStatus.REJECTED, purpose=req.purpose))

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
            # Off the OMS-worker thread — _maker_timeout_handler polls
            # every 0.5s for up to maker_timeout_s (30s default), and
            # req.exec_inst is set for EVERY POST_ONLY order, not just
            # leg-chase (see OrderRequest.limit_maker) — so calling it
            # synchronously here, on the same single worker thread every
            # other queued order (including ordinary grid cell orders)
            # waits behind, stalled the entire OMS for up to that long on
            # every resting maker order. Same shape of bug as the
            # paper-mode leg-chase issue fixed in _paper_maker_fill_with_
            # timeout() — see that method's docstring — just present here
            # unconditionally instead of only for leg-chase, and masked in
            # practice because live trading hasn't been exercised as
            # heavily as paper mode. See the 2026-08-04 "8 legs closed via
            # chase" incident thread.
            threading.Thread(
                target=self._maker_timeout_handler_safe,
                args=(req.client_oid, exchange_id, req),
                name=f"maker-timeout-{req.client_oid[:8]}",
                daemon=True).start()

    def get_exchange_id(self, client_oid: str) -> str:
        """Return the exchange order-id for a given client_oid, or '' if not known."""
        with self._orders_lock:
            order = self._orders.get(client_oid)
            return order.exchange_id if order else ""

    def restore_order(self, client_oid: str, exchange_id: str, req: "OrderRequest") -> None:
        """
        Re-register a live order that was placed by a previous process (blue)
        into this OMS instance (green) so that incoming WS fill events are
        routed correctly to the right GridLevel.

        IMPORTANT: this must set up the same bookkeeping submit() does — both
        the _orders/_exid_to_coid mapping AND the _fill_queues entry. Without
        the fill queue, wait_fill() (polled by GridEngine._poll_live_fills())
        and _deliver_fill() (called from the WS handler) both silently no-op
        for this client_oid forever, since they treat a missing queue as
        "nothing to deliver" rather than "not yet initialised". A restored
        order without a fill queue can still fill for real on the exchange,
        but this process would never notice.

        Called as soon as the handoff snapshot is read — before the grid is
        rebuilt or any GridLevel exists for it — specifically to close the
        window between the OMS WS coming up and this process knowing about
        the inherited orders. See GridBot._preregister_handoff_orders().
        """
        from copy import deepcopy
        live_order = _LiveOrder(req=deepcopy(req), exchange_id=exchange_id,
                                status=OrderStatus.PENDING)
        with self._orders_lock:
            self._orders[client_oid]         = live_order
            self._exid_to_coid[exchange_id]  = client_oid
        with self._fill_queues_lock:
            self._fill_queues[client_oid] = queue.Queue(maxsize=1)
        logger.debug(
            f"[OMS] Restored order: {req.side} @ {req.price} "
            f"exid={exchange_id} [{client_oid[:8]}]"
        )

    def forget_order(self, client_oid: str) -> None:
        """
        Remove all OMS-side bookkeeping for a client_oid. Used when a
        restored handoff order turns out to be orphaned (its price no longer
        matches any level in the newly-rebuilt grid) and gets cancelled
        before any GridLevel ever references it — otherwise its fill_queue
        entry would leak for the life of the process, since nothing would
        ever call wait_fill() on it to clean it up naturally.
        """
        with self._orders_lock:
            order = self._orders.pop(client_oid, None)
            if order and order.exchange_id:
                self._exid_to_coid.pop(order.exchange_id, None)
        with self._fill_queues_lock:
            self._fill_queues.pop(client_oid, None)

    def _maker_timeout_handler_safe(self, client_oid: str, exchange_id: str, req: OrderRequest):
        """
        Thread entry point for _maker_timeout_handler() (see _live_fill()'s
        submission comment for why this now runs off OMS-worker instead of
        inline). Previously an exception here was caught by _worker_loop's
        try/except around the whole synchronous _process_order call, which
        delivered a REJECTED fallback fill so the order never dangled.
        Running on its own thread loses that safety net — an unhandled
        exception in a thread just logs a traceback and the thread dies,
        nothing resolves the order — so it's rebuilt here instead: same
        cleanup _rest_cancel_order's caller does (drop from _orders/
        _exid_to_coid) plus the same REJECTED fallback, so wait_fill()
        callers still get a result instead of running out their own
        timeout with the order silently leaked in self._orders.
        """
        try:
            self._maker_timeout_handler(client_oid, exchange_id, req)
        except Exception as e:
            logger.error(
                f"[OMS] maker-timeout handler error {client_oid[:8]}: {e}",
                exc_info=True)
            with self._orders_lock:
                order = self._orders.pop(client_oid, None)
                if order and order.exchange_id:
                    self._exid_to_coid.pop(order.exchange_id, None)
            self._deliver_fill(FillEvent(
                client_oid=client_oid, order_id=exchange_id,
                status=OrderStatus.REJECTED, purpose=req.purpose))

    def _maker_timeout_handler(self, client_oid: str, exchange_id: str, req: OrderRequest):
        timeout  = req.maker_timeout_s if req.maker_timeout_s is not None else _MAKER_FILL_TIMEOUT
        deadline = time.time() + timeout
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

    def request_cancel_and_await(self, client_oid: str,
                                  timeout: float = 15.0) -> Optional[FillEvent]:
        """
        Cancel a still-tracked order and wait for its REAL resolution
        before returning, instead of guessing from the cancel response
        alone.

        2026-08-07: this replaces an earlier version that fired the REST
        cancel and then immediately popped _orders/_exid_to_coid/
        _fill_queues itself, on the assumption that a 0/316 ("already
        gone") response meant "safe to discard." That's a real race
        against _handle_order_update (fed by the WS user.order stream):
        "already gone" can mean either "already cancelled" or "already
        filled," and if it was actually filled, _handle_order_update's
        FILLED branch calls _deliver_fill() unconditionally regardless
        of whether _orders/_exid_to_coid were already popped by someone
        else. If this method's own premature pop had already removed
        the _fill_queues entry first, that real fill's FillEvent would
        find nobody home and vanish — worse than doing nothing, since at
        least an uncancelled order's eventual fill was still delivered
        (just to a queue nobody was polling — see the trail-drop
        cancellation comment this method serves).

        Fix: don't guess, and don't touch _orders/_exid_to_coid/
        _fill_queues here at all. _handle_order_update remains the sole
        writer of those three structures no matter which resolution
        actually happens, and wait_fill() below is the sole reader/
        cleaner of _fill_queues — exactly the same plumbing
        _liquidate_position, the leg-chase loop, and every other caller
        already trust. This method just triggers the cancel and then
        observes whatever _handle_order_update eventually reports,
        instead of racing it.

        Returns the resolving FillEvent — status FILLED if the order
        filled before the cancel could land, or CANCELLED (possibly
        with a nonzero filled_qty, if part of it filled first — same
        partial-delivery shape _maker_timeout_handler already produces)
        — or None in either of two different situations this method
        can't distinguish from the return value alone: the order was
        never live-tracked to begin with (paper mode never populates
        _orders), or it genuinely didn't resolve within timeout. Callers
        that need to tell those apart should check their own knowledge
        of whether this was a live order before calling, and treat a
        None after a real cancel attempt as "resolution unknown" —
        never as "safe to assume cancelled."
        """
        with self._orders_lock:
            order = self._orders.get(client_oid)
        if order is None or not order.exchange_id:
            return None
        if order.status not in (OrderStatus.PENDING, OrderStatus.ACTIVE):
            return None
        logger.info(
            f"[OMS] Cancelling {order.exchange_id} [{client_oid[:8]}] — "
            f"grid cell dropped out from under it (trail)"
        )
        self._rest_cancel_order(order.exchange_id)
        return self.wait_fill(client_oid, timeout=timeout)

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

        # ── Step 3: compare live open orders against DB snapshot levels ─────────
        # After a clean handoff the green process re-registers peer orders via
        # _preregister_handoff_orders before calling reconcile_on_startup, so by
        # the time we reach here those orders are in the OMS pending queue.
        # We compare exchange live orders (before cancel) against expected count
        # from the DB snapshot and alert if there is a significant mismatch.
        # Note: cancel-all-orders in Step 1 already cleared exchange orders in
        # live mode; here we query the DB snapshot, not the exchange, to detect
        # cases where the DB and exchange diverged (e.g. partial crash).
        try:
            snap_raw = self._store.get_meta("bg_handoff_json")
            if snap_raw:
                import json as _json
                snap = _json.loads(snap_raw)
                snap_levels = snap.get("levels", [])
                open_snap   = [lv for lv in snap_levels
                               if lv.get("state") in ("BUY_OPEN", "SELL_OPEN")]
                snap_long   = float(snap.get("long_qty", 0.0))
                if abs(snap_long - long_qty) > 0.0001:
                    logger.warning(
                        f"[OMS] Position mismatch: DB snapshot long={snap_long:.4f} "
                        f"but exchange reports long={long_qty:.4f} BTC — "
                        f"delta={long_qty - snap_long:+.4f}. "
                        f"Review carefully before resuming."
                    )
                    if self._alerter:
                        _grid_bot_alerter.send(
                            "⚠️ Position mismatch on startup:\n"
                            f"  DB snapshot: {snap_long:.4f} BTC\n"
                            f"  Exchange:    {long_qty:.4f} BTC\n"
                            f"  Delta: {long_qty - snap_long:+.4f} BTC\n"
                            "Review before resuming — bot will proceed with "
                            "exchange-reported position."
                        )
                else:
                    logger.info(
                        f"[OMS] Position reconciled: exchange long={long_qty:.4f} "
                        f"matches DB snapshot ({len(open_snap)} open orders in snapshot)"
                    )
        except Exception as e:
            logger.warning(f"[OMS] Reconcile snapshot comparison failed: {e}")

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
                 on_reconnect_fn: Optional[Callable[[], None]] = None,
                 on_error_fn: Optional[Callable[[str], None]] = None) -> None:
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
        # Optional callback fired on every FAILED connection attempt (e.g. a
        # handshake 403/other error before on_open ever fires). Deliberately
        # separate from on_reconnect_fn: a total block (WS handshake rejected
        # every single attempt) never produces a successful reconnect, so
        # on_reconnect_fn alone would never fire and a full outage could pass
        # completely silently. Used by GridBot to alert on sustained
        # connect-failure streaks regardless of whether a reconnect ever
        # succeeds.
        self._on_error_fn: Optional[Callable[[str], None]] = on_error_fn
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
            if self._on_error_fn is not None:
                try:
                    self._on_error_fn(str(error))
                except Exception as e:
                    logger.warning(f"[{self._name}] on_error_fn error: {e}")

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
    # The effective_atr this range was built from (post floor-clamp and
    # recent-range guard — see AutoTuner.compute()). 0.0 for the config
    # fallback path, where no ATR was available at all. Exposed so
    # GridEngine.reconcile_open_legs() can size its misfit-tolerance buffer
    # off the SAME volatility read the range itself used, rather than a
    # second, possibly-stale ATR computation.
    effective_atr: float = 0.0

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
        self._recent_atrs: List[float] = []   # for adaptive stop buffer (legacy, event-count based;
                                               # kept as-is for get_mean_atr(), used by StopScoreCalculator)
        self._recent_atr_samples: List[Tuple[float, float]] = []  # (timestamp, effective_atr), wall-clock windowed

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

    def compute(self, trend_regime: Optional[str] = None) -> Optional[GridParams]:
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
        #
        # Two checks feed into the same adaptive_mult (each can only WIDEN the
        # buffer further — neither ever narrows it below the base):
        #   (a) relative — effective_atr vs a wall-clock-windowed mean (falls
        #       back to the legacy event-count mean if the time window doesn't
        #       have enough history yet)
        #   (b) absolute — effective_atr vs a fixed reference (min_atr_floor_pts
        #       × stop_buffer_atr_absolute_widen_mult), independent of recent
        #       history, gated off during a confirmed TrendSignal DOWN regime
        #       (see GRID_CONFIG comment for the 2026-07-28 incident behind
        #       this — a wider stop mid-downtrend would have held the position
        #       into a much larger ongoing decline, not saved a whipsaw).
        expansion_threshold = self._cfg.get("stop_buffer_atr_expansion_threshold", 1.5)
        max_mult            = self._cfg.get("stop_buffer_atr_max_mult", 2.0)

        # Legacy event-count list — kept as-is (also feeds get_mean_atr(), used
        # elsewhere by StopScoreCalculator).
        recent_atrs = self._recent_atrs
        recent_atrs.append(effective_atr)
        if len(recent_atrs) > 20:          # keep last 20 builds (~20 retune events)
            recent_atrs.pop(0)

        now = time.time()
        window_h_cfg = self._cfg.get("stop_buffer_baseline_window_hours", 12.0)
        baseline_mean = self._windowed_baseline_atr(now, effective_atr)
        baseline_source = f"{window_h_cfg:g}h-window"
        if baseline_mean is None and len(recent_atrs) >= 3:
            baseline_mean = sum(recent_atrs[:-1]) / len(recent_atrs[:-1])
            baseline_source = "event-count (fallback, insufficient time-window history)"

        # Downtrend gate: computed once, applied to BOTH the relative and
        # absolute widen checks below. The 2026-07-28 incident showed that a
        # wider stop mid-confirmed-downtrend holds the position into a larger
        # ongoing decline rather than surviving a whipsaw — that reasoning
        # doesn't depend on which check (relative vs absolute) would have
        # triggered the widen, so both are gated the same way.
        skip_on_down = self._cfg.get("stop_buffer_absolute_widen_skip_on_downtrend", True)
        downtrend_gated = skip_on_down and trend_regime == "DOWN"

        adaptive_mult = 1.0
        widen_notes = []
        skipped_notes = []

        if baseline_mean and baseline_mean > 0:
            expansion_ratio = effective_atr / baseline_mean
            if expansion_ratio > expansion_threshold:
                if downtrend_gated:
                    skipped_notes.append(
                        f"relative (effective_atr={effective_atr:.2f} vs "
                        f"{baseline_source} mean={baseline_mean:.2f}, "
                        f"ratio={expansion_ratio:.2f}x)"
                    )
                else:
                    rel_mult = min(expansion_ratio / expansion_threshold, max_mult)
                    if rel_mult > adaptive_mult:
                        adaptive_mult = rel_mult
                    widen_notes.append(
                        f"relative: effective_atr={effective_atr:.2f} vs "
                        f"{baseline_source} mean={baseline_mean:.2f} "
                        f"(ratio={expansion_ratio:.2f}x)"
                    )

        absolute_widen_mult_cfg = self._cfg.get("stop_buffer_atr_absolute_widen_mult", 2.0)
        absolute_reference = atr_floor * absolute_widen_mult_cfg

        if effective_atr > absolute_reference:
            if downtrend_gated:
                skipped_notes.append(
                    f"absolute (effective_atr={effective_atr:.2f} > "
                    f"absolute_ref={absolute_reference:.2f}, "
                    f"{absolute_widen_mult_cfg}x min_atr_floor_pts)"
                )
            else:
                abs_ratio = effective_atr / absolute_reference
                abs_mult = min(abs_ratio, max_mult)
                if abs_mult > adaptive_mult:
                    adaptive_mult = abs_mult
                widen_notes.append(
                    f"absolute: effective_atr={effective_atr:.2f} > "
                    f"absolute_ref={absolute_reference:.2f} "
                    f"({absolute_widen_mult_cfg}x min_atr_floor_pts, ratio={abs_ratio:.2f}x)"
                )

        if skipped_notes:
            logger.info(
                f"[AutoTuner] Stop-buffer widen skipped (TrendSignal=DOWN): "
                f"{' & '.join(skipped_notes)} — keeping tighter stop while "
                f"trend is confirmed down"
            )

        if adaptive_mult > 1.0:
            old_buf = stop_buf
            stop_buf = round(stop_buf * adaptive_mult, 2)
            logger.info(
                f"[AutoTuner] Stop-buffer widened ({' & '.join(widen_notes)}) -> "
                f"stop_buffer {old_buf}xATR -> {stop_buf}xATR (mult={adaptive_mult:.2f}x)"
            )

        lower = round(mid - atr_mult * effective_atr, 2)
        upper = round(mid + atr_mult * effective_atr, 2)
        stop  = round(lower - stop_buf * effective_atr, 2)

        min_spacing = max(min_sp_pct * mid, 2.0 * maker_fee * mid * 1.5)
        raw_levels  = int((upper - lower) / min_spacing)

        # min_grid_levels (config default, or the trend-regime-driven target
        # from SpacingAutoTuner.update_levels_from_trend) is an ASPIRATION,
        # reachable only when the ATR-derived range is wide enough to hold
        # that many levels without spacing falling under min_spacing. It must
        # never PUSH levels past raw_levels — doing so is exactly what
        # breaks the floor it's meant to sit above: e.g. a 206pt range at a
        # 126pt min_spacing floor only supports 1 level, but a NEUTRAL-regime
        # target of 4 would force spacing down to 51pt, well under the floor
        # and under typical 5-min price noise, causing the same level to
        # cross back and forth repeatedly without ever reaching the adjacent
        # (designated-closer) level — real fills, real fees, ~$0 captured
        # spread each time. SpacingAutoTuner._evaluate's "min_levels is
        # likely overriding min_grid_pct" guard already detects the downstream
        # symptom of this (fee/gross stuck high despite widening min_grid_pct)
        # but had no way to fix the actual cause; this does.
        levels = max(1, min(max_levels, raw_levels))
        if min_levels > levels:
            logger.warning(
                f"[AutoTuner] NOT REACHABLE: min_grid_levels={min_levels} target "
                f"(regime-driven) exceeds what the min_grid_pct floor allows at "
                f"current range/ATR — spacing floor takes priority, using "
                f"{levels} level(s) instead of {min_levels} "
                f"(raw_levels={raw_levels}, range width={upper-lower:.2f}, "
                f"min_spacing floor={min_spacing:.2f})"
            )
        spacing = round((upper - lower) / levels, 2)

        notional = self._resolve_notional(levels, mid)
        logger.info(
            f"[AutoTuner] mid={mid:.2f} ATR={atr:.2f} "
            f"effective_atr={effective_atr:.2f} "
            f"range=[{lower:.2f},{upper:.2f}] levels={levels} "
            f"spacing={spacing:.2f} stop={stop:.2f}"
        )
        return GridParams(lower=lower, upper=upper, levels=levels,
                          spacing=spacing, stop_price=stop,
                          notional_per_level=notional,
                          effective_atr=effective_atr)

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

    def _windowed_baseline_atr(self, now: float, effective_atr: float) -> Optional[float]:
        """
        Wall-clock-windowed mean of recent effective_atr samples, for the
        stop-buffer relative-expansion check.

        Unlike the legacy self._recent_atrs (last ~20 RETUNE EVENTS, whatever
        their timing), this prunes by elapsed time so a burst of retunes
        during a choppy stretch can't drag the baseline up to match current
        conditions (see stop_buffer_baseline_window_hours comment in
        GRID_CONFIG for the 2026-07-28 incident this addresses).

        Returns None (caller should fall back to the legacy event-count mean)
        if there isn't yet enough time-window history to trust.
        """
        window_h = self._cfg.get("stop_buffer_baseline_window_hours", 12.0)
        window_s = window_h * 3600.0

        self._recent_atr_samples.append((now, effective_atr))
        cutoff = now - window_s
        self._recent_atr_samples = [
            (t, a) for (t, a) in self._recent_atr_samples if t >= cutoff
        ]

        history = self._recent_atr_samples[:-1]   # exclude current sample
        min_samples = self._cfg.get("stop_buffer_baseline_min_samples", 3)
        if len(history) < min_samples:
            return None

        min_span_frac = self._cfg.get("stop_buffer_baseline_min_span_frac", 0.25)
        span_s = history[-1][0] - history[0][0]
        if span_s < window_s * min_span_frac:
            return None

        return sum(a for _, a in history) / len(history)


# ─────────────────────────────────────────────────────────────────────────────
# Spacing auto-tuner
# ─────────────────────────────────────────────────────────────────────────────

class SpacingAutoTuner:
    """
    Periodically adjusts GRID_CONFIG['min_grid_pct'] to hold the fee/gross
    ratio near a target, without letting cycle frequency collapse.

    Background (2026-07-15): with maker_fee_rate fixed by the exchange,
    fee/gross ratio ≈ 2 × maker_fee_rate / spacing_pct, and GridAutoTuner packs
    in levels up to the min_grid_pct floor — so actual spacing tracks
    min_grid_pct almost exactly. Widening min_grid_pct is the only lever that
    reduces the fee/gross ratio; there is no independent knob for fee drag.
    The tradeoff: too-wide spacing means quiet/choppy periods where price
    oscillates within one spacing band produce *zero* cycles instead of just
    fewer, so cycle frequency is tracked as a guard rail, not just a side
    metric.

    Evaluation (once per spacing_autotune_interval_h, default 24h):
      - Looks at the trailing spacing_autotune_eval_days of *complete* HKT
        days from daily_pnl (today's partial day is always excluded).
      - fee/gross above target+band, and no prior widening has just tanked
        cycle frequency  -> step min_grid_pct UP by spacing_autotune_step
        (wider spacing, fewer/fatter cycles).
      - cycles/day has fallen below spacing_autotune_min_cycles_per_day since
        the last UP step  -> step back DOWN — spacing likely overshot the
        point where typical intraday chop still spans a full grid step.
      - fee/gross below target-band  -> step DOWN (recover trade frequency
        if fees are already comfortably low).
      - otherwise within the target band  -> hold.

    All steps are bounded to [spacing_autotune_min_pct, spacing_autotune_max_pct].
    The tuned value is persisted via GridStateStore.set_meta() so it survives
    restarts instead of resetting to the hardcoded config default.
    """
    META_KEY           = "auto_tuned_min_grid_pct"
    META_KEY_LAST_EVAL = "auto_tuned_last_eval_ts"
    META_KEY_LEVELS    = "auto_tuned_min_grid_levels"

    def __init__(self, config: dict, store: "GridStateStore", alerter: "AlertManager"):
        self._cfg     = config
        self._store   = store
        self._alerter = alerter
        self._last_eval_ts: float = 0.0
        # cycles/day observed at the time of the most recent UP step, so the
        # NEXT evaluation can tell whether that step caused a collapse.
        self._cycles_after_last_widen: Optional[float] = None
        # fee/gross observed at the time of the most recent UP step.  If the
        # ratio hasn't improved meaningfully by the next evaluation, min_levels
        # is likely overriding min_grid_pct and further widening is pointless.
        self._fee_ratio_at_last_widen: Optional[float] = None
        # Set to True when _evaluate changes min_grid_pct or when
        # update_levels_from_trend() changes min_grid_levels; cleared by
        # GridBot._run() via pop_rebuild_requested() so the new value takes
        # effect immediately rather than waiting for the next natural retune.
        self._rebuild_requested: bool = False
        # Track the last regime seen so update_levels_from_trend() only
        # triggers a rebuild when the regime actually changes, not every call.
        self._last_levels_regime: str = ""

    def load_persisted(self) -> None:
        """Restore a previously auto-tuned min_grid_pct on startup, if any.
        Call this once, before the first _rebuild_grid(), so the very first
        grid build already uses the tuned value instead of the raw config
        default."""
        if self._store is None:
            return
        val = self._store.get_meta(self.META_KEY)
        if val is None:
            return
        try:
            pct = float(val)
        except (TypeError, ValueError):
            logger.warning(f"[SpacingAutoTuner] Ignoring unparseable persisted "
                            f"value: {val!r}")
            return
        floor = self._cfg.get("spacing_autotune_min_pct", 0.0008)
        ceil  = self._cfg.get("spacing_autotune_max_pct", 0.0025)
        clamped = max(floor, min(ceil, pct))
        self._cfg["min_grid_pct"] = clamped
        logger.info(
            f"[SpacingAutoTuner] Restored persisted min_grid_pct="
            f"{clamped:.5f} ({clamped*100:.3f}%) from previous auto-tune"
            + ("" if clamped == pct else f" (clamped from {pct:.5f})")
        )

        # Restore last-eval timestamp so a restart within the 24h window
        # doesn't immediately re-fire an evaluation (Bug: _last_eval_ts=0
        # on every cold start caused one widen step per restart).
        raw_ts = self._store.get_meta(self.META_KEY_LAST_EVAL)
        if raw_ts is not None:
            try:
                self._last_eval_ts = float(raw_ts)
                interval_s = self._cfg.get("spacing_autotune_interval_h", 24) * 3600
                remaining_h = max(0.0, self._last_eval_ts + interval_s - time.time()) / 3600
                logger.info(
                    f"[SpacingAutoTuner] Restored last_eval_ts — "
                    f"next eval in {remaining_h:.1f}h"
                )
            except (TypeError, ValueError):
                logger.warning("[SpacingAutoTuner] Ignoring unparseable "
                               f"persisted last_eval_ts: {raw_ts!r}")

    def load_persisted_levels(self) -> None:
        """Restore a previously auto-tuned min_grid_levels on startup.
        Call after load_persisted() so startup uses the last regime-tuned value."""
        if self._store is None:
            return
        val = self._store.get_meta(self.META_KEY_LEVELS)
        if val is None:
            return
        try:
            levels = int(val)
        except (TypeError, ValueError):
            logger.warning(f"[SpacingAutoTuner] Ignoring unparseable persisted "
                           f"levels value: {val!r}")
            return
        down_n    = self._cfg.get("levels_autotune_down_levels",    3)
        up_n      = self._cfg.get("levels_autotune_up_levels",      5)
        clamped   = max(down_n, min(up_n, levels))
        self._cfg["min_grid_levels"] = clamped
        logger.info(
            f"[SpacingAutoTuner] Restored persisted min_grid_levels={clamped} "
            f"from previous regime auto-tune"
        )

    def update_levels_from_trend(self, regime: str) -> None:
        """
        Adjust min_grid_levels in _cfg based on the current TrendSignal regime.
        Called from GridBot._run() on every trend evaluation (every ~60s).

        Design rationale:
          DOWN    → 3 levels: fewer, wider grid levels reduce total long exposure
                    in a falling market and produce a wider stop buffer per level.
                    This breaks the SpacingAutoTuner deadlock: with min_levels=3
                    and a 240pt grid, spacing=120pt which is well above the
                    min_grid_pct floor of 0.0020×66400=133pt.
          NEUTRAL → 4 levels: balanced.  One fewer than the config default (5)
                    to allow SpacingAutoTuner to work without the binding constraint.
          UP      → 5 levels: more levels in an uptrend captures more chop cycles;
                    the grid is less likely to be swept downward in an uptrend.

        Only flags the change (via _rebuild_requested — see 2026-08-02
        deferred-adoption fix at its call site in GridBot._run()) when the
        regime changes — not on every call. The new min_grid_levels value
        is live in config immediately either way; the flag no longer forces
        an out-of-band rebuild, it's picked up whenever a rebuild next
        happens for an actual, price-driven reason. INSUFFICIENT_DATA
        leaves min_grid_levels unchanged (hold the last known-good value).
        """
        if not self._cfg.get("levels_autotune_enabled", True):
            return
        # Ignore INSUFFICIENT_DATA: no information to act on; hold current value
        if regime in (TrendSignal.REGIME_NODATA, "INSUFFICIENT_DATA"):
            return
        if regime == self._last_levels_regime:
            return   # no change — avoid spurious rebuilds

        down_n    = self._cfg.get("levels_autotune_down_levels",    3)
        neutral_n = self._cfg.get("levels_autotune_neutral_levels", 4)
        up_n      = self._cfg.get("levels_autotune_up_levels",      5)

        new_levels = {
            TrendSignal.REGIME_DOWN:    down_n,
            TrendSignal.REGIME_NEUTRAL: neutral_n,
            TrendSignal.REGIME_UP:      up_n,
        }.get(regime, neutral_n)

        old_levels = self._cfg.get("min_grid_levels", neutral_n)
        self._cfg["min_grid_levels"] = new_levels
        self._last_levels_regime = regime

        if new_levels != old_levels:
            logger.info(
                f"[SpacingAutoTuner] Trend-driven levels: "
                f"min_grid_levels {old_levels} → {new_levels} "
                f"(regime={regime})"
            )
            self._rebuild_requested = True
            if self._store is not None:
                try:
                    self._store.set_meta(self.META_KEY_LEVELS, str(new_levels))
                except Exception as e:
                    logger.warning(
                        f"[SpacingAutoTuner] Failed to persist min_grid_levels: {e}"
                    )
        else:
            # Same level count but regime changed (e.g. two regimes share the
            # same configured value); update tracking but no rebuild needed.
            logger.debug(
                f"[SpacingAutoTuner] Regime changed to {regime}, "
                f"min_grid_levels unchanged at {new_levels}"
            )

    def pop_rebuild_requested(self) -> bool:
        """Return True (and clear the flag) if _evaluate changed min_grid_pct
        or update_levels_from_trend() changed min_grid_levels since the last
        call. As of the 2026-08-02 deferred-adoption fix, GridBot._run() no
        longer forces an immediate _rebuild_grid() off this — it only logs
        that a change is pending. The new value is already live in config
        (set directly by _evaluate()/update_levels_from_trend()) and gets
        picked up whenever the grid next rebuilds for an actual, price-driven
        reason (should_retune()'s boundary check or the 24h periodic
        interval), rather than forcing an extra rebuild purely to adopt a
        config target with no accompanying price movement."""
        v, self._rebuild_requested = self._rebuild_requested, False
        return v

    def maybe_evaluate(self) -> None:
        """Called from GridBot._run()'s periodic-task section. No-ops unless
        spacing_autotune_enabled and the interval has elapsed."""
        if not self._cfg.get("spacing_autotune_enabled", False):
            return
        if self._store is None:
            return
        interval_s = self._cfg.get("spacing_autotune_interval_h", 24) * 3600
        now = time.time()
        if now - self._last_eval_ts < interval_s:
            return
        self._last_eval_ts = now
        self._store.set_meta(self.META_KEY_LAST_EVAL, str(now))
        try:
            self._evaluate(now)
        except Exception:
            logger.exception("[SpacingAutoTuner] Evaluation failed — leaving "
                              "min_grid_pct unchanged")

    def _evaluate(self, now: float) -> None:
        eval_days = self._cfg.get("spacing_autotune_eval_days", 3)
        rows = self._store.get_recent_daily(days=eval_days + 1)  # +1: today may be in the slice
        today = _db_hkt_date(now)
        rows = [r for r in rows if r["hkt_date"] != today]        # exclude partial day
        rows = rows[:eval_days]
        if len(rows) < eval_days:
            logger.info(
                f"[SpacingAutoTuner] Only {len(rows)}/{eval_days} complete "
                f"days of history available — skipping this evaluation"
            )
            return

        total_gross    = sum(r["gross_pnl_usd"] for r in rows)
        total_fees     = sum(abs(r["fees_usd"])  for r in rows)
        total_cycles   = sum(r["cycle_count"]     for r in rows)
        cycles_per_day = total_cycles / len(rows)

        if total_gross <= 0:
            logger.info(
                f"[SpacingAutoTuner] Non-positive gross over trailing "
                f"{len(rows)}d (${total_gross:.2f}) — skipping evaluation "
                f"(ratio undefined)"
            )
            return

        fee_ratio  = total_fees / total_gross
        target     = self._cfg.get("spacing_autotune_target_fee_pct", 0.15)
        band       = self._cfg.get("spacing_autotune_band", 0.05)
        step       = self._cfg.get("spacing_autotune_step", 0.0002)
        floor      = self._cfg.get("spacing_autotune_min_pct", 0.0008)
        ceil       = self._cfg.get("spacing_autotune_max_pct", 0.0025)
        min_cycles = self._cfg.get("spacing_autotune_min_cycles_per_day", 30)
        current    = self._cfg.get("min_grid_pct", floor)

        logger.info(
            f"[SpacingAutoTuner] Eval over {len(rows)}d "
            f"({rows[-1]['hkt_date']}..{rows[0]['hkt_date']}): "
            f"fee/gross={fee_ratio:.1%} (target={target:.0%}±{band:.0%}) "
            f"cycles/day={cycles_per_day:.1f} "
            f"current min_grid_pct={current:.5f} ({current*100:.3f}%)"
        )

        new_pct, reason = current, None

        # Back-off check takes priority: did the last widening step visibly
        # tank cycle frequency? If so, undo part of it before considering
        # whether fee_ratio still looks "too high" (it will, transiently).
        if (self._cycles_after_last_widen is not None
                and cycles_per_day < min_cycles
                and current > floor):
            new_pct = max(floor, round(current - step, 6))
            reason = (f"cycles/day={cycles_per_day:.1f} < floor {min_cycles} "
                       f"after prior widening — backing off")
            self._cycles_after_last_widen = None
            self._fee_ratio_at_last_widen = None
        elif fee_ratio > target + band and current < ceil:
            # Guard: if we already widened once and fee/gross hasn't improved
            # meaningfully, min_levels is likely overriding min_grid_pct and
            # further widening will have no effect on actual spacing.
            # Threshold: less than 10% relative improvement from last widen.
            if (self._fee_ratio_at_last_widen is not None
                    and fee_ratio >= self._fee_ratio_at_last_widen * 0.90):
                logger.warning(
                    f"[SpacingAutoTuner] fee/gross={fee_ratio:.1%} still high but "
                    f"unchanged since last widen ({self._fee_ratio_at_last_widen:.1%}) — "
                    f"min_levels is likely overriding min_grid_pct; skipping further widen"
                )
                return
            new_pct = min(ceil, round(current + step, 6))
            reason = f"fee/gross {fee_ratio:.1%} above target+band — widening spacing"
        elif fee_ratio < target - band and current > floor:
            new_pct = max(floor, round(current - step, 6))
            reason = f"fee/gross {fee_ratio:.1%} below target-band — tightening spacing"

        if new_pct == current:
            logger.info(f"[SpacingAutoTuner] No change ({reason or 'within target band'})")
            return

        self._cfg["min_grid_pct"] = new_pct
        self._store.set_meta(self.META_KEY, str(new_pct))
        self._rebuild_requested = True
        if new_pct > current:
            self._cycles_after_last_widen = cycles_per_day
            self._fee_ratio_at_last_widen = fee_ratio
        logger.info(
            f"[SpacingAutoTuner] ADJUST min_grid_pct {current:.5f} -> "
            f"{new_pct:.5f} ({reason})"
        )
        if self._alerter is not None:
            self._alerter.send(
                f"⚙️ Spacing auto-tune: min_grid_pct "
                f"{current*100:.3f}% → {new_pct*100:.3f}%\n{reason}\n"
                f"(fee/gross={fee_ratio:.1%}, cycles/day={cycles_per_day:.1f}, "
                f"over trailing {len(rows)}d)"
            )


# ─────────────────────────────────────────────────────────────────────────────
# Grid level state
# ─────────────────────────────────────────────────────────────────────────────

class LevelState(Enum):
    IDLE       = "IDLE"
    BUY_OPEN   = "BUY_OPEN"
    SELL_OPEN  = "SELL_OPEN"
    SUPPRESSED = "SUPPRESSED"   # buy suppressed by stop-score gate; skipped by _replace_idle_levels


@dataclass
class OpenLeg:
    """
    A position opened by one fill, not yet closed by another.

    Deliberately side-neutral in naming (open_side, not "buy price") — a leg
    can be opened by a SELL (a fresh short, when total_investment_btc allows
    net-short exposure) just as validly as by a BUY. Whichever fill created
    it, gross PnL on close is computed against THIS leg's own open_price and
    qty — never against the price of an unrelated adjacent grid level, which
    was the source of the 2026-07-30 fabricated-PnL bug this replaces.
    """
    leg_id:           int
    open_side:        str      # 'BUY' | 'SELL' — the fill that opened this leg
    open_price:       float
    qty:              float
    opened_ts:        float
    opened_level_idx: int      # diagnostics only; the leg outlives any one level
    open_fee:         float = 0.0  # the opening fill's fee. Mutable: shrinks in
    # lockstep with qty as partial closes each claim their proportional share
    # (see GridBot._record_partial_leg_close) so it always represents "the fee
    # attributable to whatever's still open," and the leg's final close
    # attributes exactly whatever share remains — summing to the full original
    # opening fee across however many chunks the leg took to close, no matter
    # how many. 2026-08-05 fix: previously untracked, so every closing-fill
    # log line's net= silently omitted the opening leg's fee entirely and
    # only subtracted the closing fill's own — cosmetic (record_fill's own
    # fee_usd/gross_pnl, and therefore daily_pnl/cumulative_net/status/
    # alerts, were never affected, since each fill's fee is independently
    # summed there regardless of open/close), but wrong on the log line.


@dataclass
class GridLevel:
    """
    2026-08-03 restructure: despite the name, this is now a grid CELL — a
    fixed [lower, upper] price pair — not a single price point. Each cell
    runs its own fully independent, self-contained 2-phase cycle and never
    touches a neighboring cell's order:

        OPEN phase:  resting BUY @ lower  (if open_side == 'BUY')
                  or resting SELL @ upper (if open_side == 'SELL')
        fills -> opens a new OpenLeg -> places the CLOSER on THIS SAME
                 cell's OTHER boundary (see GridEngine._on_fill)
        CLOSE phase: resting order @ the other boundary, closes_leg_id set
        fills -> realizes PnL -> re-arms OPEN phase, same open_side, same
                 cell — repeat.

    This replaces the old "N+1 independent price points, counter-order
    routed to idx±1, retagged in place if that neighbor was occupied"
    design. That design is what produced the retag/orphan-leg bug class
    (silently dropped closers on a 2-level grid, forced rebuilds) — cells
    eliminate it structurally rather than patching around it further, since
    there's no longer any neighboring slot to collide over.

    price is a derived convenience, not an independent source of truth: it
    always equals lower (while state == BUY_OPEN) or upper (while state ==
    SELL_OPEN), kept in sync by _place_buy/_place_sell. open_side is this
    cell's fixed identity — the side it uses whenever it places a FRESH,
    untagged order (initial placement, cancel-retry of an opener, or
    re-arm after its own leg closes). It is set once, at cell creation or
    when reconcile_open_legs/_apply_handoff_restore repurposes an existing
    cell for a specific leg, and is deliberately NEVER recomputed against
    live mid afterward — see _replace_idle_levels for why that would be
    wrong once a cell is mid-cycle (a short-covering BUY can legitimately
    rest above mid; recomputing from price-vs-mid would pick the wrong side).
    """
    index:       int
    lower:       float
    upper:       float
    open_side:   str        = "BUY"   # 'BUY' | 'SELL' — this cell's fixed identity
    state:       LevelState = LevelState.IDLE
    client_oid:  str        = ""
    exchange_id: str        = ""   # exchange order-id; populated after REST confirm
    qty:         float      = 0.0
    price:       float      = 0.0  # derived — see docstring above
    placed_at:   float      = 0.0  # epoch time this order was (re)placed;
                                    # used by paper_fill_min_resting_s guard
    # Set only when this cell's resting order is a designated closer for a
    # specific OpenLeg (placed by _on_fill's counter-order step, or restored
    # across a rebuild/handoff by reconcile_open_legs / _apply_handoff_restore).
    # None means "this is a fresh open, whatever fills here starts a new leg"
    # — the same role the old _initial_sell_oids set served, generalized to
    # every cell, not just startup sells.
    closes_leg_id: Optional[int] = None
    # Set only while state == SUPPRESSED, to whichever side ("BUY" or
    # "SELL") the counter-order actually was at the moment of suppression —
    # see _on_fill's two gate branches. Needed because a suppressed level's
    # correct side is NOT open_side (that's this cell's fixed *opening*
    # identity, never the counter/closing side — see the class docstring
    # above); before SellGate (2026-08-04) existed, every suppression was
    # BuyGate's and therefore always "BUY", so release_one_suppressed_level()
    # could hardcode _place_buy(). Now that SellGate can also suppress a
    # level (a covering/closing SELL, or a fresh short open), release must
    # place whichever side was actually withheld.
    suppressed_side: Optional[str] = None


# ─────────────────────────────────────────────────────────────────────────────
# Grid engine
# ─────────────────────────────────────────────────────────────────────────────

class GridEngine:
    """
    Manages limit order ladder on BTCUSD-PERP.

    2026-08-03 restructure: self._levels holds N independent CELLS (fixed
    [lower, upper] price pairs), not N+1 independent price points — see the
    GridLevel docstring for the full per-cell cycle. This is what removed
    the retag/orphan-leg bug class from _on_fill's counter-order routing.
    GridBot._match_handoff_levels/_apply_handoff_restore were updated in
    the same pass to match a handoff snapshot's orders by (price, side)
    against the new grid's actual cell boundaries, rather than the old
    flat N+1-point index — see those methods' docstrings.

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
                 buy_gate_fn: Optional[Callable[[], bool]] = None,
                 sell_gate_fn: Optional[Callable[[], bool]] = None,
                 trend_confirm_fn: Optional[Callable[[], int]] = None,
                 stray_leg_fn: Optional[Callable[[Optional["OpenLeg"], str, Optional[str], Optional[str]], None]] = None,
                 uptrend_confirmed_fn: Optional[Callable[[], bool]] = None,
                 downtrend_confirmed_fn: Optional[Callable[[], bool]] = None,
                 down_shift_record_fn: Optional[Callable[[], None]] = None):
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
        # sell_gate_fn (2026-08-04): same shape and calling convention as
        # buy_gate_fn, mirrored for the counter-SELL placed after a BUY
        # fill — see GridBot._sell_gate. None = no gate (always allow),
        # same legacy-safe default as buy_gate_fn.
        self._sell_gate_fn: Optional[Callable[[], bool]] = sell_gate_fn
        # trend_confirm_fn (2026-08-04): optional callable → int. Called
        # once per top-sell-triggered drift-shift decision, right before
        # _trail_up would otherwise fire exactly once. Returns how many
        # EXTRA shifts (beyond the usual 1) to perform in this same event —
        # 0 preserves the exact legacy single-shift behaviour. The callable
        # is also responsible for recording this event as evidence for its
        # own next call (see GridBot._trend_confirm) — GridEngine only
        # reports "a shift is about to happen", it doesn't track history
        # itself, since a full rebuild replaces this engine instance
        # entirely and would silently reset any history kept here (the
        # same reason leg-dwell state lives on GridBot, not GridEngine —
        # see reconcile_open_legs' pending_since docs). None = no catch-up
        # (legacy behaviour).
        self._trend_confirm_fn: Optional[Callable[[], int]] = trend_confirm_fn
        # uptrend_confirmed_fn / downtrend_confirmed_fn / down_shift_record_fn
        # (2026-08-05, "trail-flip" — see GRID_CONFIG comment block):
        #
        # uptrend_confirmed_fn: read-only mirror of GridBot._uptrend_confirmed_now
        # — is the SAME drift_shift_trend_* shift-count evidence
        # trend_confirm_fn already records currently "confirmed"? Unlike
        # sell_gate_fn, this does NOT first require TrendSignal's hourly
        # regime to read UP. That distinction is the whole point: on
        # 2026-08-05 the hourly regime sat at NEUTRAL for the entire
        # 07:05-14:00 window while price still ground out 11 top-sell
        # drift-shifts (5 of them already flagged CONFIRMED by
        # trend_confirm_fn's own shift-count logic) — sell_gate_fn, gated
        # behind the regime read, never once suppressed anything in that
        # window. uptrend_confirmed_fn reads the same underlying evidence
        # trend_confirm_fn was already collecting, just without the
        # regime gate in front of it.
        #
        # downtrend_confirmed_fn / down_shift_record_fn: mirror for the
        # downside. No fill-triggered drift-shift-on-bottom-buy path
        # exists (see drift_shift_trend_* GRID_CONFIG comment), so there
        # was no existing shift-count evidence to read on this side —
        # down_shift_record_fn is GridEngine's own way of contributing
        # that evidence (called once per actual _trail_down, mirroring
        # what the top-sell fill handler already does for
        # trend_confirm_fn), and downtrend_confirmed_fn reads it back.
        #
        # Both confirmed_fn callables are read-only / side-effect-free —
        # safe to call every _trail_up / _trail_down. Their backing state
        # lives on GridBot, not here, for the same reason trend_confirm_fn's
        # does (a full rebuild replaces this GridEngine instance and would
        # silently reset any history kept on it — see that param's own
        # docstring above).
        self._uptrend_confirmed_fn: Optional[Callable[[], bool]] = uptrend_confirmed_fn
        self._downtrend_confirmed_fn: Optional[Callable[[], bool]] = downtrend_confirmed_fn
        self._down_shift_record_fn: Optional[Callable[[], None]] = down_shift_record_fn
        # stray_leg_fn: optional callable(leg, reason, dropped_client_oid,
        # dropped_order_side) -> None. Called by _trail_up/_trail_down
        # whenever a dropped cell was holding a leg's designated closer
        # (leg not None) and/or a still-live resting order of its own
        # (dropped_client_oid not None, live-trading only — see those
        # methods' docstrings for why the remaining cell grid is
        # structurally the wrong home for either). Set by GridBot to
        # GridBot._chase_close_leg, which spawns a background thread and
        # returns immediately; this attribute exists (rather than
        # hardcoding that call here) so GridEngine has no upward
        # dependency on GridBot and stays constructible standalone in
        # tests. None = legacy behaviour (log a warning, rely on the
        # orphan-leg rebuild safety net; the dropped order, if any, is
        # simply left resting — see the trail methods' cancellation
        # comments for why that's live-mode-risky).
        self._stray_leg_fn: Optional[Callable[[Optional["OpenLeg"], str, Optional[str], Optional[str]], None]] = stray_leg_fn
        # Leg ids currently being actively managed by stray_leg_fn (a chase
        # in progress). check_price_fills()'s orphan-leg detector excludes
        # these — they're accounted for, just not by a cell — so it doesn't
        # force a needless rebuild out from under an in-flight chase.
        self._chasing_leg_ids: set = set()
        self._lock       = threading.Lock()
        self._levels: List[GridLevel] = []
        self._stop_event = threading.Event()
        self._last_drift_shift: float = 0.0   # epoch time of last sell-triggered shift
        self._needs_rebuild:   bool  = False  # set by drift-shift when mid is far OOB
        self._handoff_freeze:  bool  = False  # set by GridBot during blue-green handoff
        # Epoch time of the last orphaned-leg "requesting fast rebuild"
        # trigger (see check_price_fills() / orphan_leg_rebuild_cooldown_s).
        # 0.0 so the very first detection always fires immediately.
        self._last_orphan_rebuild_request: float = 0.0

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

        # long_qty is now a derived @property (see below) — computed from
        # _open_legs, not tracked as a separate counter that could drift out
        # of sync with the actual leg ledger.

        # Open-legs ledger: the durable, real cost-basis source of truth,
        # replacing the old "adjacent grid level's price" assumption (see
        # OpenLeg docstring and the 2026-07-30 wash-trade-PnL fix this
        # generalizes). Seeded from DB so a plain process restart resumes
        # with real, correct cost basis instead of starting blind — the
        # same reason _realized_pnl/_total_fees/_cycle_count are seeded above.
        self._open_legs: Dict[int, "OpenLeg"] = {}
        self._local_leg_seq: int = 0   # fallback negative-id source when store is None
        if store is not None:
            for row in store.get_open_legs():
                leg = OpenLeg(
                    leg_id=row["leg_id"], open_side=row["open_side"],
                    open_price=row["open_price"], qty=row["qty"],
                    opened_ts=row["opened_ts"], opened_level_idx=row["opened_level_idx"],
                    open_fee=row.get("open_fee", 0.0),
                )
                self._open_legs[leg.leg_id] = leg
            if self._open_legs:
                logger.info(
                    f"[GridEngine] Seeded {len(self._open_legs)} open leg(s) from DB "
                    f"(net_qty={self._long_qty:+.4f})"
                )

        # Fill queue for _fill_thread
        self._fill_queue: collections.deque = collections.deque()
        self._fill_event  = threading.Event()
        self._fill_thread: Optional[threading.Thread] = None

        # Indices with a fill detected (state already flipped to IDLE) but not
        # yet processed by _on_fill() on the Grid-fills thread. _replace_idle_levels()
        # must skip these — see its docstring for the same-cell race this
        # prevents (fixed 2026-07-30; simplified 2026-08-03 when the cell
        # restructure retired the separate _rearm_eligible allow-list this
        # used to work alongside).
        self._pending_fill_indices: set = set()

        self._build_levels()

    @property
    def _long_qty(self) -> float:
        """
        Net position, derived from _open_legs — a BUY-opened leg is long
        qty, a SELL-opened leg is short qty (negative). Replaces the old
        independently-mutated counter: keeping this derived rather than a
        separately-tracked value that both _on_fill and the leg ledger had
        to remember to update in lockstep removes an entire class of
        desync bugs between "what we think we hold" and "what the ledger
        of actual open legs says we hold."
        """
        total = 0.0
        for leg in self._open_legs.values():
            total += leg.qty if leg.open_side == "BUY" else -leg.qty
        return total

    def _build_levels(self):
        """
        Build one independent GridLevel (cell) per [prices[i], prices[i+1]]
        pair — N cells from N+1 boundary prices, not N+1 independent
        points. open_side is decided later, per cell, in
        _place_initial_orders (or by reconcile_open_legs /
        _apply_handoff_restore for a cell reassigned to an existing leg) —
        this method has no mid to decide it with yet.
        """
        prices = self._params.level_prices
        with self._lock:
            self._levels = [
                GridLevel(index=i, lower=prices[i], upper=prices[i + 1])
                for i in range(len(prices) - 1)
            ]
        logger.info(
            f"[GridEngine] {len(self._levels)} cells: "
            f"{self._levels[0].lower:.2f} … {self._levels[-1].upper:.2f}"
        )

    def start(self, mid: float, skip_indices: Optional[set] = None):
        self._fill_thread = threading.Thread(
            target=self._fill_loop, name="Grid-fills", daemon=True)
        self._fill_thread.start()
        self._place_initial_orders(mid, skip_indices=skip_indices or set())
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

    def _place_initial_orders(self, mid: float, skip_indices: Optional[set] = None):
        """
        Place a BUY or SELL for every level, except those in skip_indices.

        skip_indices has two callers, both meaning "a different code path
        already gave this specific level the order it should have — don't
        overwrite it with a generic one":
          - Blue-green handoff: those indices have a live order inherited
            from the predecessor process (applied afterward by
            GridBot._apply_handoff_restore) — placing a fresh order here
            would create a duplicate resting order at that price.
          - Leg reconciliation at rebuild time (GridBot._rebuild_grid via
            GridEngine.reconcile_open_legs): those indices are about to get
            a designated closer for a specific still-open leg (applied
            afterward by apply_leg_reassignments), sized to that leg's own
            qty rather than the standard per-level notional.

        Every order placed here has no closes_leg_id — each is a fresh
        open, exactly like the old "initial sell" case, just no longer
        special-cased: whatever fills here starts a new OpenLeg (see
        GridEngine._on_fill).
        """
        skip_indices = skip_indices or set()
        with self._lock:
            levels = list(self._levels)
        for lv in levels:
            if self._stop_event.is_set():
                break
            if lv.index in skip_indices:
                continue
            # Defensive: mark eligible for _replace_idle_levels()'s naive
            # re-arm BEFORE attempting placement, so if _place_buy()/
            # _place_sell() raises or this loop is interrupted (_stop_event),
            # the level doesn't end up stuck IDLE with no path back to
            # getting an order. Placement below almost always succeeds and
            # moves the level off IDLE anyway, so this is a fallback, not
            # the normal path.
            # This cell's fixed identity: rests BUY@lower if its lower
            # boundary is below current mid (a normal, non-crossing resting
            # buy); otherwise rests SELL@upper (opens a fresh short — this
            # bot supports net-short exposure, see OpenLeg). Decided once,
            # here — never recomputed against live mid again afterward, see
            # GridLevel.open_side.
            # NOTE: was `elif lv.price > mid`, which silently placed no
            # order at all when lv.price == mid exactly (observed live
            # 2026-07-29 12:38:48 — level [2] never got an order because
            # the AutoTuner-computed level price was bit-for-bit equal to
            # the mid passed in here). `>=` makes the tie an explicit
            # SELL instead of an orphaned level.
            lv.open_side = "BUY" if lv.lower < mid else "SELL"

            # 2026-08-05 ("Trail-flip" — see GRID_CONFIG comment block):
            # this loop previously had NO trend gating at all — every
            # full rebuild placed a fresh SELL on every above-mid cell
            # regardless of trend, even while SellGate was actively
            # blocking the exact same kind of order everywhere else. This
            # is the ONE placement path where an outright side-flip
            # (SELL→BUY at the SAME price boundary) is NOT safe to do the
            # way _trail_up/_trail_down do it: unlike a trail-created
            # cell, this cell's lower boundary can sit AT OR ABOVE current
            # mid by construction (that's the whole reason it was
            # classified SELL), so resting a BUY there would be a
            # marketable/crossing order, not a passive one. Suppress
            # instead — same SUPPRESSED state SellGate/BuyGate already use
            # for exactly this "don't open it now, but don't lose the
            # slot either" situation; release_one_suppressed_level() picks
            # it back up once the confirmed-trend evidence clears, same as
            # any other SellGate/BuyGate suppression.
            if lv.open_side == "SELL":
                shift_evidence_up = (
                    self._uptrend_confirmed_fn is not None
                    and self._uptrend_confirmed_fn()
                )
                regime_blocked_up = (
                    self._sell_gate_fn is not None and not self._sell_gate_fn()
                )
                if shift_evidence_up or regime_blocked_up:
                    lv.state           = LevelState.SUPPRESSED
                    lv.suppressed_side = "SELL"
                    logger.info(
                        f"[GridEngine] SELL [{lv.index}] @ {lv.upper:.2f} "
                        f"suppressed at rebuild — confirmed uptrend "
                        f"(no fresh short opened)"
                    )
                    time.sleep(0.05)
                    continue
            elif lv.open_side == "BUY":
                shift_evidence_down = (
                    self._downtrend_confirmed_fn is not None
                    and self._downtrend_confirmed_fn()
                )
                regime_blocked_down = (
                    self._buy_gate_fn is not None and not self._buy_gate_fn()
                )
                if shift_evidence_down or regime_blocked_down:
                    lv.state           = LevelState.SUPPRESSED
                    lv.suppressed_side = "BUY"
                    logger.info(
                        f"[GridEngine] BUY  [{lv.index}] @ {lv.lower:.2f} "
                        f"suppressed at rebuild — confirmed downtrend "
                        f"(no fresh long opened)"
                    )
                    time.sleep(0.05)
                    continue

            if lv.open_side == "BUY":
                self._place_buy(lv)
            else:
                self._place_sell(lv)
            time.sleep(0.05)

    # ── Order placement ───────────────────────────────────────────────────────

    def _qty(self, price: float) -> float:
        raw = self._params.notional_per_level / price
        return round(math.floor(raw * 10000) / 10000, 4)

    def _place_buy(self, lv: GridLevel, qty_override: Optional[float] = None,
                   closes_leg_id: Optional[int] = None):
        if self._handoff_freeze:
            logger.debug(f"[GridEngine] BUY  [{lv.index}] suppressed — handoff freeze active")
            return
        qty = qty_override if qty_override is not None else self._qty(lv.lower)
        if qty <= 0:
            # 2026-08-02 incident: notional_per_level was ~$1 (a
            # total_investment_btc config value ~2000x smaller than
            # intended), so _qty()'s floor to 4dp (0.0001 BTC) rounded
            # every level to exactly 0 — every level silently skipped
            # placement, for two hours, with zero trace in the log. This
            # warning would have been the only signal available at the
            # time it was happening.
            reason = (f"notional_per_level={self._params.notional_per_level:.4f} "
                      f"too small at this price (needs >= "
                      f"~{lv.lower * 0.0001:.2f} for a non-zero 0.0001 BTC lot)"
                      if qty_override is None else
                      f"qty_override={qty_override} was <= 0 (re-anchoring a "
                      f"zero/negative-qty leg?)")
            logger.warning(
                f"[GridEngine] BUY  [{lv.index}] @ {lv.lower:.2f} SKIPPED — "
                f"qty rounded to {qty} ({reason}). Level stays uncovered "
                f"until this is fixed."
            )
            return
        req = OrderRequest.limit_maker(
            side="BUY", qty=qty, price=lv.lower,
            instrument=self._instrument, purpose="grid_buy")
        with self._lock:
            lv.state         = LevelState.BUY_OPEN
            lv.client_oid    = req.client_oid
            lv.qty           = qty
            lv.price         = lv.lower
            lv.placed_at     = time.time()
            lv.closes_leg_id = closes_leg_id
        if self._oms.live_trading:
            self._oms.submit(req)
        # Paper mode: _simulate_paper_fills() is the sole fill authority for
        # grid orders (checks mid vs lv.price every tick). Routing through
        # OMS.submit()/_paper_fill() here would be dead weight — its FillEvent
        # is never consumed (wait_fill() is only called for market/live
        # orders), so it just leaks a Queue in OMS._fill_queues per order and
        # logs a misleading "FILL" line for an order that hasn't actually
        # crossed price yet.
        tag = f" closes_leg={closes_leg_id}" if closes_leg_id is not None else ""
        logger.debug(f"[GridEngine] BUY  [{lv.index}] @ {lv.lower:.2f} qty={qty:.4f}{tag}")

    def _place_sell(self, lv: GridLevel, qty_override: Optional[float] = None,
                    closes_leg_id: Optional[int] = None):
        if self._handoff_freeze:
            logger.debug(f"[GridEngine] SELL [{lv.index}] suppressed — handoff freeze active")
            return
        qty = qty_override if qty_override is not None else self._qty(lv.upper)
        if qty <= 0:
            reason = (f"notional_per_level={self._params.notional_per_level:.4f} "
                      f"too small at this price (needs >= "
                      f"~{lv.upper * 0.0001:.2f} for a non-zero 0.0001 BTC lot)"
                      if qty_override is None else
                      f"qty_override={qty_override} was <= 0 (re-anchoring a "
                      f"zero/negative-qty leg?)")
            logger.warning(
                f"[GridEngine] SELL [{lv.index}] @ {lv.upper:.2f} SKIPPED — "
                f"qty rounded to {qty} ({reason}). Level stays uncovered "
                f"until this is fixed."
            )
            return
        req = OrderRequest.limit_maker(
            side="SELL", qty=qty, price=lv.upper,
            instrument=self._instrument, purpose="grid_sell")
        with self._lock:
            lv.state         = LevelState.SELL_OPEN
            lv.client_oid    = req.client_oid
            lv.qty           = qty
            lv.price         = lv.upper
            lv.placed_at     = time.time()
            lv.closes_leg_id = closes_leg_id
        if self._oms.live_trading:
            self._oms.submit(req)
        # See _place_buy: paper mode's fill authority is _simulate_paper_fills(),
        # not OMS.submit()/_paper_fill(), so skip it here too.
        tag = f" closes_leg={closes_leg_id}" if closes_leg_id is not None else ""
        logger.debug(f"[GridEngine] SELL [{lv.index}] @ {lv.upper:.2f} qty={qty:.4f}{tag}")

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

        # Catch any level left IDLE with no resting order — e.g. a
        # cancel-timeout re-place, or (see _place_initial_orders) a level
        # whose price tied mid exactly at build time. This used to only run
        # in live mode (called from inside _poll_live_fills), so paper mode
        # never self-healed: an orphaned level could sit unplaced for the
        # rest of that grid's life. Now runs every tick regardless of mode.
        self._replace_idle_levels()

        # Historical note (predates the 2026-08-03 cell restructure): under
        # the old point-per-level design, every level ending up on the same
        # side could leave the grid stuck with no path back except a full
        # rebuild, and _rearm_eligible only changed HOW that happened rather
        # than preventing it. Under the cell model each cell is a fully
        # self-contained 2-phase cycle, so this specific failure mode no
        # longer applies the same way — the check below is kept as a
        # defense-in-depth safety net for the one thing that genuinely still
        # needs a rebuild to fix (see next comment), not as the primary fix
        # for it.
        #
        # Excludes the one legitimate false-positive: LevelState.SUPPRESSED
        # only ever applies to BUY-side levels (the stop-score gate refusing
        # new exposure near a falling stop — see the SELL-fill counter-order
        # branch above). Zero buys while sells remain is expected and correct
        # in that state; forcing a rebuild there would fight the protection
        # this bot already has in place, not fix anything.
        with self._lock:
            open_buys   = sum(1 for lv in self._levels if lv.state == LevelState.BUY_OPEN)
            open_sells  = sum(1 for lv in self._levels if lv.state == LevelState.SELL_OPEN)
            suppressed  = sum(1 for lv in self._levels if lv.state == LevelState.SUPPRESSED)
            # Every cell only ever rests one side at a time by construction
            # now (2026-08-03) — a lopsided buys/sells count across the
            # whole grid is normal, not a symptom of anything. What actually
            # can't self-heal is an open leg with NO cell anywhere tagged to
            # close it (shouldn't happen under the cell model — each cell
            # only ever tags a leg it opened itself — but kept as a
            # defensive check for any bug that manages to leave one
            # untagged).
            tagged_leg_ids = {lv.closes_leg_id for lv in self._levels
                               if lv.closes_leg_id is not None}
            orphaned_legs = (set(self._open_legs.keys()) - tagged_leg_ids
                              - self._chasing_leg_ids)
        buys_are_intentionally_paused = (open_buys == 0 and suppressed > 0)
        # Cooldown-gated (2026-08-06 GEN00037_GREEN incident — see
        # orphan_leg_rebuild_cooldown_s in GRID_CONFIG for the full writeup):
        # without this gate, a leg that stays orphaned across many
        # consecutive ticks re-requests a rebuild every single tick,
        # regardless of whether the previous request could possibly have
        # accomplished anything yet (a dead-band-blocked _rebuild_grid()
        # call bails out before it ever reaches reconcile_open_legs(), so
        # re-requesting faster than mid can plausibly cross
        # retune_deadband_pct just burns CPU and log volume for no benefit).
        # `not self._needs_rebuild` alone doesn't prevent this: the flag is
        # cleared by pop_needs_rebuild() every time _rebuild_grid() runs
        # (dead-band-skipped or not), so it's back to False well within the
        # same tick that set it. The cooldown timestamp survives that reset.
        rebuild_cooldown = self._cfg.get("orphan_leg_rebuild_cooldown_s", 15.0)
        cooldown_elapsed = (
            time.time() - self._last_orphan_rebuild_request >= rebuild_cooldown
        )
        if (not buys_are_intentionally_paused
                and orphaned_legs
                and not self._needs_rebuild
                and cooldown_elapsed):
            side = "SELL" if open_buys == 0 else "BUY"
            logger.warning(
                f"[GridEngine] {len(orphaned_legs)} open leg(s) have no "
                f"assigned closer anywhere on the grid (all {side}, "
                f"buys={open_buys} sells={open_sells}, suppressed={suppressed}) "
                f"— requesting fast rebuild instead of waiting for the next "
                f"retune check"
            )
            self._needs_rebuild = True
            self._last_orphan_rebuild_request = time.time()

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
                    # This cell's own order just filled. _replace_idle_levels()
                    # must not naively re-arm it out from under _on_fill(),
                    # which is about to place the correct next order (tagged
                    # closer or fresh re-arm) on this SAME cell — see
                    # _pending_fill_indices and _on_fill.
                    self._pending_fill_indices.add(lv.index)
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
                    # See _simulate_paper_fills' matching comment — this
                    # cell's own fill must not be naively re-armed by
                    # _replace_idle_levels(); _on_fill() places the correct
                    # next order on this same cell.
                    self._pending_fill_indices.add(lv.index)
                    self._fill_queue.append((lv.index, fill))
                    self._fill_event.set()
                elif fill.is_cancelled:
                    # Timeout cancel — re-place same side/price. Not in
                    # _pending_fill_indices (no fill happened), so
                    # _replace_idle_levels() will pick it straight back up.
                    # closes_leg_id is deliberately left untouched (see
                    # _replace_idle_levels' comment) — a cancel means the
                    # closing fill never happened, so the leg it was tagged
                    # to close is still open and still needs a closer.
                    lv.state      = LevelState.IDLE
                    lv.client_oid = ""
                    # Will be re-placed by check_price_fills()'s unconditional
                    # _replace_idle_levels() call right after this returns.

    def _replace_idle_levels(self):
        """Re-place any IDLE cells that should have an order.
        SUPPRESSED levels are intentionally skipped — they are managed by
        GridBot._run() via release_one_suppressed_level(), once either the
        stop-score recovers (BuyGate-suppressed) or the sell-gate condition
        clears (SellGate-suppressed, 2026-08-04) — so they must not be
        re-queued here.

        Excludes _pending_fill_indices as the sole guard (2026-08-03: the
        older _rearm_eligible ALLOW-list is retired — see history below for
        why it existed and why the cell restructure removes the need for
        it).  A cell in _pending_fill_indices just filled and is waiting for
        _on_fill() to process it on the Grid-fills thread; touching it here
        first would race with _on_fill() placing the correct next order on
        that same cell and could submit a duplicate.  Every OTHER IDLE cell
        — a maker-timeout cancel retry, or a residual unplaced cell from
        _place_initial_orders — is safe to re-arm immediately.

        History:

        2026-07-30 / 2026-08-01 (gen00013/gen00014, wash-trade incidents):
        under the OLD point-per-level design, a level's counter-order after
        its own fill belonged at the ADJACENT level, and this method had to
        be kept from naively re-arming the level that just filled at its
        OWN price while it waited — sometimes for a long time — for that
        neighbor's own eventual fill to cycle back around. _rearm_eligible
        was an allow-list built specifically to enforce that wait.

        2026-08-03 restructure: under the cell model, that whole "wait on a
        neighbor" condition no longer exists — a cell's own re-arm is
        *always* placed by _on_fill() on that SAME cell, essentially
        immediately (the only delay is the brief hand-off to the async
        Grid-fills thread, which _pending_fill_indices alone already
        guards). There is no other case left where an IDLE cell should stay
        unplaced, so the allow-list is gone; _pending_fill_indices is the
        only exclusion needed.

        Side selection: NEVER recomputed from lv.price-vs-mid (that was
        already only safe under the old design because a level's price was
        immutable; under the cell model, a cell mid-CLOSE phase can
        legitimately need to rest on the "wrong" side of live mid — e.g. a
        short-covering BUY resting above mid — and recomputing from mid
        would silently retry the wrong side). Instead this always uses the
        cell's own local, already-correct identity: open_side if this is a
        fresh/untagged retry, or open_side's opposite if closes_leg_id is
        set (a cell only ever closes on the side opposite how it opens —
        see GridLevel docstring)."""
        with self._lock:
            idle = [lv for lv in self._levels
                    if lv.state == LevelState.IDLE          # SUPPRESSED excluded
                    and lv.index not in self._pending_fill_indices]
        for lv in idle:
            # closes_leg_id/qty must be carried through the re-arm. A cell
            # can go IDLE without ever passing through _on_fill — e.g. a
            # maker-timeout cancel in _poll_live_fills, which resets
            # state/client_oid but (correctly) leaves closes_leg_id alone,
            # since that fill never happened. Re-arming it as a plain,
            # untagged, standard-notional order here would silently strip
            # the designated-closer tag from whatever OpenLeg it was placed
            # for — that leg would still be tracked correctly, just with
            # nothing actively targeting it for close until the next
            # rebuild's reconcile_open_legs happens to sweep it up.
            # Re-placing it as the SAME tagged closer, same qty, is a plain
            # retry of the order that just got cancelled, not a fresh open.
            tag_qty = lv.qty if lv.closes_leg_id is not None else None
            tag_leg = lv.closes_leg_id
            side = lv.open_side if tag_leg is None else (
                "SELL" if lv.open_side == "BUY" else "BUY")
            if side == "BUY":
                self._place_buy(lv, qty_override=tag_qty, closes_leg_id=tag_leg)
            else:
                self._place_sell(lv, qty_override=tag_qty, closes_leg_id=tag_leg)

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
            # Structural grid boundaries, not the edge cells' transient
            # .price (which now moves between lower/upper as each cell
            # cycles through its own open/close phases — see GridLevel).
            current_lower   = self._levels[0].lower
            current_upper   = self._levels[-1].upper
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

        dropped_leg = None
        dropped_client_oid = None
        dropped_order_side = None
        with self._lock:
            # Step 1: remove bottom cell and cancel its order
            if not self._levels:
                return
            bottom = self._levels[0]
            if bottom.index in self._pending_fill_indices:
                # Its fill was JUST detected this same tick (mid crossing
                # bottom.upper implies mid also clears current_upper+spacing,
                # since current_upper >= bottom.upper — the trail-up trigger
                # and this cell's own fill are never independent events).
                # _on_fill() hasn't processed it yet. Dropping the cell now
                # would pop it out from under that still-queued fill event —
                # the async Grid-fills thread would then apply it to
                # whatever cell reindexing shifts into this slot instead
                # (wrong leg, wrong PnL) — AND, just as importantly, this
                # cell's own state is still IDLE at this instant, so the
                # closes_leg_id check below would be skipped entirely: the
                # leg would silently miss the stray-leg-chase handoff below
                # too, not just the old orphan-rebuild fallback. Defer one
                # tick instead; _check_trailing() re-evaluates next tick
                # once _on_fill() has placed this cell's correct next order.
                #
                # (2026-08-03: this guard has now been dropped twice across
                # two separate uploads when this method was independently
                # regenerated for the stray-leg-chase / zero-candidate
                # features — see test_trail_defers_while_fill_pending and
                # test_trail_defers_then_hands_off_to_chase in
                # test_cell_model.py, which exist specifically to catch
                # this if it happens again.)
                logger.debug(
                    f"[GridEngine] TRAIL UP deferred one tick — bottom cell "
                    f"[{bottom.index}]'s fill is still pending processing"
                )
                return
            logger.info(
                f"[GridEngine] TRAIL UP: dropping lower={old_lower:.2f}, "
                f"adding upper={new_upper:.2f}"
            )
            # SUPPRESSED cells always have client_oid=="" (a suppressed
            # level never had an order placed for it — see BuyGate/
            # SellGate, and now also the trail-flip fresh-open suppression
            # in _replace_idle_levels), so the plain "state != IDLE and
            # client_oid" check below can never be true for one. Without
            # this OR clause, a cell dropped while sitting SUPPRESSED
            # silently skips the closes_leg_id hand-off entirely: no
            # chase, no warning log, no _chasing_leg_ids entry — the leg
            # just sits in _open_legs untracked until the generic
            # orphan-leg safety net notices on a later tick and forces a
            # full rebuild.
            if bottom.state == LevelState.SUPPRESSED or (
                    bottom.state != LevelState.IDLE and bottom.client_oid):
                if bottom.closes_leg_id is not None:
                    # This cell was mid-CLOSE-phase, holding a leg's
                    # designated closer. Its ideal closing price IS the
                    # boundary being dropped here — every remaining cell
                    # boundary is a full spacing away from it by
                    # construction, so reconcile_open_legs' cell-fit logic
                    # would wedge it into a worse price (or the
                    # buffer/confirm-dwell path meant for genuine range
                    # mismatches, not routine trail noise). Hand it to
                    # stray_leg_fn instead, which manages it independently
                    # of the cell grid.
                    dropped_leg = self._open_legs.get(bottom.closes_leg_id)
                    if dropped_leg is not None:
                        self._chasing_leg_ids.add(dropped_leg.leg_id)
                        logger.info(
                            f"[GridEngine] TRAIL UP dropping bottom cell "
                            f"[{bottom.index}] while it holds leg "
                            f"#{dropped_leg.leg_id}'s closer — handing off "
                            f"to stray-leg chase."
                        )
                    else:
                        # Defensive: closes_leg_id pointing nowhere. Nothing
                        # to hand off; fall back to the old safety net.
                        logger.warning(
                            f"[GridEngine] TRAIL UP dropping bottom cell "
                            f"[{bottom.index}] tagged closes_leg_id="
                            f"{bottom.closes_leg_id}, but that leg isn't in "
                            f"_open_legs — relying on the orphan-leg rebuild "
                            f"safety net."
                        )
                # Mark idle so poll_fills won't try to re-place it. Only
                # capture the client_oid for cancellation when it's a
                # REAL exchange order (live_trading) — in paper mode
                # client_oid is set but nothing was ever submitted to the
                # OMS, so there's nothing to cancel and no phantom-fill
                # risk (see _trail_up's cancellation comment below).
                # Gating here, rather than trying to tell the two cases
                # apart from request_cancel_and_await's return value
                # later, is deliberate: a None back from that call always
                # means "genuinely unresolved," never "wasn't live."
                if bottom.client_oid and self._oms.live_trading:
                    dropped_client_oid = bottom.client_oid
                    dropped_order_side = (
                        "BUY" if bottom.state == LevelState.BUY_OPEN else "SELL"
                    )
                bottom.state      = LevelState.IDLE
                bottom.client_oid = ""
            self._levels.pop(0)
            # Re-index remaining cells
            for i, lv in enumerate(self._levels):
                lv.index = i

            # Step 2: append new cell at the top. Default identity is SELL
            # (a trail up always seeds a fresh short as the natural
            # continuation of the breakout that triggered it) — UNLESS a
            # confirmed uptrend is active, in which case this cell opens
            # as a BUY (dip-buy) instead. See "Trail-flip" GRID_CONFIG
            # comment block (2026-08-05) for why: this specific new cell
            # is guaranteed to sit entirely below current mid at this
            # instant (trail_up only fires once mid >= new_upper), so a
            # resting BUY at its lower boundary is always a valid passive
            # order, never a market-crossing one.
            shift_evidence_up = (
                self._uptrend_confirmed_fn is not None
                and self._uptrend_confirmed_fn()
            )
            regime_blocked_up = (
                self._sell_gate_fn is not None and not self._sell_gate_fn()
            )
            confirmed_up = (
                self._cfg.get("trail_flip_to_buy_on_confirmed_uptrend", True)
                and (shift_evidence_up or regime_blocked_up)
            )
            new_side = "BUY" if confirmed_up else "SELL"
            new_idx = len(self._levels)
            new_lv  = GridLevel(index=new_idx, lower=old_upper, upper=new_upper,
                                 open_side=new_side)
            self._levels.append(new_lv)
            self._params = GridParams(
                lower=self._levels[0].lower,
                upper=new_upper,
                levels=len(self._levels),
                spacing=spacing,
                stop_price=self._params.stop_price,
                notional_per_level=self._params.notional_per_level,
            )

        # Place the new cell outside the lock
        if new_lv.open_side == "BUY":
            self._place_buy(new_lv)
            logger.info(
                f"[GridEngine] TRAIL UP — confirmed uptrend active: new top "
                f"cell [{new_lv.index}] opened as BUY (dip-buy) @ "
                f"{new_lv.lower:.2f} instead of a fresh SELL"
            )
        else:
            self._place_sell(new_lv)
        if (dropped_leg is not None or dropped_client_oid is not None) \
                and self._stray_leg_fn is not None:
            # Single hand-off covering both: a leg that needs chasing,
            # and/or a still-live order that needs cancelling. These
            # used to be two independent calls fired back-to-back here —
            # cancel this function's own order while, in parallel, the
            # chase worker immediately started racing to fill a FRESH
            # order for the same leg. If the original order won its own
            # race against its cancel (filled instead of cancelling) at
            # the same time the chase's new order also filled, the leg
            # would close twice — a real double-execution, not just a
            # bookkeeping gap. Bundling them into one call lets the
            # receiving worker cancel-and-confirm the original order
            # FIRST, and only then decide whether (and for how much
            # remaining qty) the leg still needs chasing — see
            # GridBot._reconcile_dropped_cell_worker.
            self._stray_leg_fn(dropped_leg, "trail_up",
                                dropped_client_oid, dropped_order_side)
        self._alerter_send(
            f"⬆️ Grid trailed UP → [{self._params.lower:.0f}, {new_upper:.0f}]"
            + (" (top cell flipped to BUY — confirmed uptrend)" if new_lv.open_side == "BUY" else "")
        )

    def _trail_down(self, old_lower: float, old_upper: float, spacing: float):
        """
        Shift grid down by one level:
          1. Cancel the highest SELL level (old_upper).
          2. Remove it from the levels list.
          3. Prepend a new BUY level at old_lower - spacing.
        """
        new_lower = round(old_lower - spacing, 2)

        dropped_leg = None
        dropped_client_oid = None
        dropped_order_side = None
        with self._lock:
            if not self._levels:
                return
            top = self._levels[-1]
            if top.index in self._pending_fill_indices:
                # See the matching TRAIL UP comment — same same-tick race,
                # mirrored for the top cell (mid <= current_lower - spacing
                # implies mid has also already cleared top.lower).
                logger.debug(
                    f"[GridEngine] TRAIL DOWN deferred one tick — top cell "
                    f"[{top.index}]'s fill is still pending processing"
                )
                return
            logger.info(
                f"[GridEngine] TRAIL DOWN: dropping upper={old_upper:.2f}, "
                f"adding lower={new_lower:.2f}"
            )
            # See the matching TRAIL UP comment — SUPPRESSED cells always
            # have client_oid=="", so they need this explicit OR to reach
            # the closes_leg_id hand-off below.
            if top.state == LevelState.SUPPRESSED or (
                    top.state != LevelState.IDLE and top.client_oid):
                if top.closes_leg_id is not None:
                    # See the matching TRAIL UP comment — same handoff,
                    # mirrored for the top cell.
                    dropped_leg = self._open_legs.get(top.closes_leg_id)
                    if dropped_leg is not None:
                        self._chasing_leg_ids.add(dropped_leg.leg_id)
                        logger.info(
                            f"[GridEngine] TRAIL DOWN dropping top cell "
                            f"[{top.index}] while it holds leg "
                            f"#{dropped_leg.leg_id}'s closer — handing off "
                            f"to stray-leg chase."
                        )
                    else:
                        logger.warning(
                            f"[GridEngine] TRAIL DOWN dropping top cell "
                            f"[{top.index}] tagged closes_leg_id="
                            f"{top.closes_leg_id}, but that leg isn't in "
                            f"_open_legs — relying on the orphan-leg rebuild "
                            f"safety net."
                        )
                if top.client_oid and self._oms.live_trading:
                    dropped_client_oid = top.client_oid
                    dropped_order_side = (
                        "BUY" if top.state == LevelState.BUY_OPEN else "SELL"
                    )
                top.state      = LevelState.IDLE
                top.client_oid = ""
            self._levels.pop()

            # Prepend new cell at the bottom. Default identity is BUY (a
            # trail down always seeds a fresh long as the natural
            # continuation of the breakdown that triggered it) — UNLESS a
            # confirmed downtrend is active, in which case this cell opens
            # as a SELL (rally-sell) instead. Mirror of the TRAIL UP
            # flip above (see "Trail-flip" GRID_CONFIG comment block,
            # 2026-08-05) — mechanically safe here too: trail_down only
            # fires once mid <= new_lower, so this cell's entire range
            # (and its upper boundary, where the SELL would rest) is
            # guaranteed to already be ABOVE current mid.
            # Record this shift as evidence for downtrend persistence — no
            # fill-triggered path does this for the down side today (see
            # down_shift_record_fn's docstring above), so _trail_down
            # itself is the only place this evidence can be captured.
            if self._down_shift_record_fn is not None:
                try:
                    self._down_shift_record_fn()
                except Exception:
                    logger.exception(
                        "[GridEngine] down_shift_record_fn raised — "
                        "downtrend persistence evidence for this shift lost"
                    )
            shift_evidence_down = (
                self._downtrend_confirmed_fn is not None
                and self._downtrend_confirmed_fn()
            )
            regime_blocked_down = (
                self._buy_gate_fn is not None and not self._buy_gate_fn()
            )
            confirmed_down = (
                self._cfg.get("trail_flip_to_sell_on_confirmed_downtrend", True)
                and (shift_evidence_down or regime_blocked_down)
            )
            new_side = "SELL" if confirmed_down else "BUY"
            new_lv = GridLevel(index=0, lower=new_lower, upper=old_lower,
                                open_side=new_side)
            self._levels.insert(0, new_lv)
            # Re-index
            for i, lv in enumerate(self._levels):
                lv.index = i
            self._params = GridParams(
                lower=new_lower,
                upper=self._levels[-1].upper,
                levels=len(self._levels),
                spacing=spacing,
                stop_price=self._params.stop_price,
                notional_per_level=self._params.notional_per_level,
            )

        # Place the new cell outside the lock
        if new_lv.open_side == "SELL":
            self._place_sell(new_lv)
            logger.info(
                f"[GridEngine] TRAIL DOWN — confirmed downtrend active: new "
                f"bottom cell [{new_lv.index}] opened as SELL (rally-sell) "
                f"@ {new_lv.upper:.2f} instead of a fresh BUY"
            )
        else:
            self._place_buy(new_lv)
        if (dropped_leg is not None or dropped_client_oid is not None) \
                and self._stray_leg_fn is not None:
            # See the matching TRAIL UP comment — same double-execution
            # risk from firing cancel and chase independently, mirrored
            # for the top cell.
            self._stray_leg_fn(dropped_leg, "trail_down",
                                dropped_client_oid, dropped_order_side)
        self._alerter_send(
            f"⬇️ Grid trailed DOWN → [{new_lower:.0f}, {self._params.upper:.0f}]"
            + (" (bottom cell flipped to SELL — confirmed downtrend)" if new_lv.open_side == "SELL" else "")
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

    def _retry_db_write(self, fn, attempts: int = 3, base_delay: float = 0.05) -> Tuple[bool, object]:
        """
        Thin wrapper — the real logic now lives on GridStateStore.execute_with_retry
        so GridBot's liquidation paths can share it too (see that method's
        docstring). Kept here under the original name so existing call sites
        in _on_fill (and existing tests) don't need to change. Every current
        call site already guards `if self._store is not None:` before
        reaching here; this defensive check is just so a future call site
        that forgets that guard gets a clean (False, error) back instead of
        an AttributeError several frames deep in fill processing.
        """
        if self._store is None:
            return False, RuntimeError("_retry_db_write called with no store configured")
        return self._store.execute_with_retry(fn, attempts, base_delay)

    def _on_fill(self, idx: int, fill: FillEvent):
        # Snapshot the level reference and intent under lock, then release
        # before calling _place_buy/_place_sell (which acquire lock themselves).
        with self._lock:
            if idx < 0 or idx >= len(self._levels):
                return
            is_buy = fill.purpose == "grid_buy"
            # Done racing _replace_idle_levels() for this index — from here on
            # the correct counter-order (placed below, on this SAME cell's
            # other boundary — see the 2026-08-03 cell restructure) is what
            # should happen next, not a naive re-arm.
            self._pending_fill_indices.discard(idx)
            lv = self._levels[idx]
            closes_leg_id = lv.closes_leg_id
            lv.closes_leg_id = None   # consumed either way

        self._total_fees += fill.fee
        now = time.time()
        side_str = "BUY" if is_buy else "SELL"

        leg_closed: Optional[OpenLeg] = None
        if closes_leg_id is not None:
            with self._lock:
                leg_closed = self._open_legs.pop(closes_leg_id, None)
            if leg_closed is None:
                logger.error(
                    f"[GridEngine] FILL {side_str} [{idx}] @ {fill.avg_price:.2f} was "
                    f"tagged closes_leg_id={closes_leg_id}, but that leg isn't in "
                    f"_open_legs. Treating as a fresh open so this fill isn't "
                    f"silently dropped from accounting either way."
                )

        new_leg: Optional[OpenLeg] = None
        if leg_closed is not None:
            # ── Closing fill: real PnL against the SPECIFIC leg it closes ──────
            # Same formula either direction — a long leg profits when the close
            # price is higher than the open price, a short leg profits when
            # it's lower. Never the price of an unrelated adjacent grid level.
            if leg_closed.open_side == "BUY":
                gross_pnl = (fill.avg_price - leg_closed.open_price) * fill.filled_qty
            else:
                gross_pnl = (leg_closed.open_price - fill.avg_price) * fill.filled_qty
            self._realized_pnl += gross_pnl
            self._cycle_count  += 1
            # Proportional share of the OPENING fill's fee attributable to
            # the qty closed by THIS fill — see OpenLeg.open_fee docstring.
            # leg_closed.qty here is still its pre-this-close value (this
            # branch doesn't mutate it), and _on_fill's closing path is
            # always a full close (a grid cell's counter-order is sized to
            # the leg's exact remaining qty; partial fills only happen via
            # the leg-chase path, which routes through
            # GridBot._finalize_leg_close instead — see that method's own
            # comment), so this reduces to the leg's whole open_fee in
            # practice. The proportional form is used anyway for
            # correctness rather than assuming, and so the two closing
            # paths compute net_pnl identically.
            open_fee_share = (
                leg_closed.open_fee * (fill.filled_qty / leg_closed.qty)
                if leg_closed.qty > 0 else leg_closed.open_fee
            )
            net_pnl = gross_pnl - fill.fee - open_fee_share
            if self._store is not None:
                ok, err = self._retry_db_write(
                    lambda: self._store.close_leg(leg_closed.leg_id))
                if not ok:
                    # This leg is ALREADY popped from _open_legs and its PnL
                    # already realized in-memory (and in grid_fills) above —
                    # that part is correct and must not be undone. The risk
                    # is purely durability: if this DELETE never lands, the
                    # next engine construction (rebuild OR plain restart)
                    # re-seeds this exact leg from DB as still-open, and its
                    # eventual re-anchored close will realize the SAME PnL a
                    # second time. Loud + distinct from the generic DB-error
                    # log so it doesn't get lost among routine noise —
                    # manual fix is one DELETE statement, but only a human
                    # who saw this will know to run it.
                    logger.critical(
                        f"[GridEngine] PERSISTENT close_leg FAILURE for leg "
                        f"#{leg_closed.leg_id} (opened {leg_closed.open_side} "
                        f"{leg_closed.qty:.4f} @ {leg_closed.open_price:.2f}) "
                        f"after retries: {err}. GHOST-LEG RISK: this leg is "
                        f"closed in memory/grid_fills but the open_legs row "
                        f"may still exist — if so it will be re-seeded and "
                        f"its PnL double-counted at the next rebuild/restart. "
                        f"Manual fix: DELETE FROM open_legs WHERE leg_id={leg_closed.leg_id};",
                        exc_info=err,
                    )
                    self._alerter_send(
                        f"🚨 GHOST-LEG RISK: close_leg DB write failed for leg "
                        f"#{leg_closed.leg_id} after retries — see log. Manual "
                        f"DB cleanup recommended before the next rebuild/restart."
                    )
            logger.info(
                f"[GridEngine] FILL {side_str} [{idx}] @ {fill.avg_price:.2f} "
                f"qty={fill.filled_qty:.4f} fee={fill.fee:.6f} | "
                f"closed leg #{leg_closed.leg_id} (opened {leg_closed.open_side} "
                f"@ {leg_closed.open_price:.2f}) cycle #{self._cycle_count} "
                f"gross={gross_pnl:+.4f} net={net_pnl:+.4f} "
                f"cumulative_net={self._realized_pnl - self._total_fees:+.4f} USD"
            )
            fill_leg_id = leg_closed.leg_id
        else:
            # ── Opening fill: starts a new leg. Nothing realized yet — matches
            # the old BUY-fill behavior (gross_pnl always 0), now applied
            # symmetrically to a SELL that opens a fresh short too, instead of
            # the old is_initial_sell special case that only covered startup.
            gross_pnl = 0.0
            leg_id = None
            db_write_failed = False
            if self._store is not None:
                ok, result = self._retry_db_write(lambda: self._store.open_leg(
                    open_side=side_str, open_price=fill.avg_price,
                    qty=fill.filled_qty, opened_ts=now, opened_level_idx=idx,
                    open_fee=fill.fee,
                ))
                if ok:
                    leg_id = result
                else:
                    db_write_failed = True
                    logger.error(
                        f"[GridEngine] DB open_leg error after retries: {result}",
                        exc_info=result,
                    )
            if leg_id is None:
                # No store (unit tests) or the write failed — a locally unique
                # negative id keeps in-memory accounting correct for the life
                # of this process even without DB backing.
                with self._lock:
                    self._local_leg_seq -= 1
                    leg_id = self._local_leg_seq
                if db_write_failed:
                    # Unlike close_leg failing (where the position is still
                    # correctly tracked, just at risk of a double-count
                    # later), THIS leg has no durable record at all. It's
                    # fine for the rest of THIS process's life (the negative
                    # local id keeps in-memory accounting/reconciliation
                    # correct), but a rebuild or restart constructs a brand
                    # new engine that seeds _open_legs strictly from DB —
                    # this leg, and the real exposure it represents, would
                    # silently vanish from the books at that point.
                    logger.critical(
                        f"[GridEngine] UNTRACKED LEG: open_leg DB write failed "
                        f"after retries for {side_str} {fill.filled_qty:.4f} @ "
                        f"{fill.avg_price:.2f} (local leg #{leg_id}). This "
                        f"position is real and tracked in-memory for now, but "
                        f"will be silently lost from the ledger at the next "
                        f"rebuild or restart. Manual note recommended."
                    )
                    self._alerter_send(
                        f"🚨 UNTRACKED LEG: {side_str} {fill.filled_qty:.4f} @ "
                        f"{fill.avg_price:.2f} not persisted to DB after retries "
                        f"— will be lost from the ledger on next rebuild/restart."
                    )
            new_leg = OpenLeg(leg_id=leg_id, open_side=side_str,
                              open_price=fill.avg_price, qty=fill.filled_qty,
                              opened_ts=now, opened_level_idx=idx,
                              open_fee=fill.fee)
            with self._lock:
                self._open_legs[leg_id] = new_leg
            net = self._long_qty
            net_label = f"long={net:.4f}" if net >= 0 else f"short={-net:.4f}"
            logger.info(
                f"[GridEngine] FILL {side_str} [{idx}] @ {fill.avg_price:.2f} "
                f"qty={fill.filled_qty:.4f} fee={fill.fee:.6f} | "
                f"opened leg #{leg_id} {net_label} BTC"
            )
            fill_leg_id = leg_id

        if self._store is not None:
            ok, err = self._retry_db_write(lambda: self._store.record_fill(
                ts_utc=now, side=side_str, level_idx=idx,
                price_usd=fill.avg_price, qty_btc=fill.filled_qty,
                fee_usd=fill.fee, gross_pnl=gross_pnl, cycle_num=self._cycle_count,
                leg_id=fill_leg_id, close_reason=None,
                is_close=(leg_closed is not None),
            ))
            if not ok:
                # Unlike open_leg/close_leg failing, this doesn't put the
                # ledger itself at risk — _open_legs and _realized_pnl above
                # are already correct in memory, and that's what future
                # decisions (closer placement, reconciliation) actually use.
                # What's lost is this one fill's row in grid_fills and its
                # contribution to daily_pnl/accumulated PnL — the numbers
                # /status reports would permanently undercount by exactly
                # this fill's gross/fee, with no ledger corruption behind
                # it. Still worth a loud, distinct log rather than the
                # generic error that was here before, since "why is my
                # daily PnL missing a fill I can see in the console log"
                # is exactly the kind of silent discrepancy this whole
                # ledger rework exists to eliminate.
                logger.critical(
                    f"[GridEngine] PERSISTENT record_fill FAILURE for "
                    f"{side_str} [{idx}] @ {fill.avg_price:.2f} qty="
                    f"{fill.filled_qty:.4f} gross={gross_pnl:+.4f} after "
                    f"retries: {err}. AUDIT-TRAIL GAP: in-memory accounting "
                    f"is correct, but this fill's grid_fills row and its "
                    f"daily_pnl/accumulated contribution never landed — "
                    f"/status will undercount by exactly this fill's "
                    f"gross={gross_pnl:+.4f} fee={fill.fee:.6f} until "
                    f"manually backfilled.",
                    exc_info=err,
                )
                self._alerter_send(
                    f"⚠️ record_fill DB write failed for {side_str} [{idx}] "
                    f"@ {fill.avg_price:.2f} after retries — see log. "
                    f"Daily/accumulated PnL will undercount by "
                    f"{gross_pnl:+.4f} until backfilled."
                )

        # ── Counter-order: SAME cell, other boundary (2026-08-03) ───────────
        # Every cell is fully self-contained — the order that follows any
        # fill always belongs to THIS SAME cell (idx unchanged), never a
        # neighbor. No adjacent-index lookup, no IDLE/occupied branching, no
        # retag: whichever side just filled, the next order is simply the
        # opposite side, on this cell's other boundary. This is what
        # eliminates the old retag/orphan-leg bug class at the data-structure
        # level (see GridLevel docstring) rather than patching around it
        # further. If this fill just opened `new_leg`, the counter-order is
        # tagged as its designated closer and sized to its exact qty. If
        # this fill just closed `leg_closed` instead, the counter-order is a
        # fresh, untagged re-arm — same cell, same open_side, next cycle.
        with self._lock:
            lv = self._levels[idx]   # same cell — no index arithmetic

        if is_buy:
            # BUY just filled at this cell's lower boundary -> counter is a
            # SELL at this cell's upper boundary, subject to the sell-gate
            # (2026-08-04 — mirror of the buy-gate below: withholds NEW
            # sell-side exposure while a confirmed-uptrend signal is active,
            # whether that SELL is opening a fresh short or covering/closing
            # a long — same "let a confirmed move run before rushing to act
            # against or into it" logic as the buy-gate already applies on
            # the down side. See GridBot._sell_gate).
            suppress = False
            if self._sell_gate_fn is not None and not self._sell_gate_fn():
                with self._lock:
                    lv.state         = LevelState.SUPPRESSED
                    lv.suppressed_side = "SELL"
                    # Preserve the tag/qty this SELL would have carried, so
                    # release_one_suppressed_level() can restore it exactly
                    # rather than silently placing a generic fresh order in
                    # its place once the gate reopens.
                    lv.closes_leg_id = new_leg.leg_id if new_leg is not None else None
                    lv.qty           = new_leg.qty if new_leg is not None else 0.0
                suppress = True
                logger.info(
                    f"[GridEngine] SELL [{idx}] suppressed by sell-gate "
                    f"(buy fill at [{idx}] @ {fill.avg_price:.2f})"
                )
            if suppress:
                self._alerter_send(
                    f"🛡 Sell [{idx}] suppressed — sell-gate active"
                )
            else:
                if new_leg is not None:
                    self._place_sell(lv, qty_override=new_leg.qty,
                                      closes_leg_id=new_leg.leg_id)
                else:
                    self._place_sell(lv)
        else:
            # SELL just filled at this cell's upper boundary -> counter is a
            # BUY at this cell's lower boundary, subject to the buy-gate
            # (same protection as before: it exists to withhold NEW buy-side
            # exposure while a stop-score risk signal is active, whether
            # that BUY is opening a fresh long or covering a short).
            suppress = False
            if self._buy_gate_fn is not None and not self._buy_gate_fn():
                with self._lock:
                    lv.state         = LevelState.SUPPRESSED
                    lv.suppressed_side = "BUY"
                    # Preserve the tag/qty this BUY would have carried, so
                    # release_one_suppressed_level() can restore it exactly
                    # rather than silently placing a generic fresh order in
                    # its place once the gate reopens.
                    lv.closes_leg_id = new_leg.leg_id if new_leg is not None else None
                    lv.qty           = new_leg.qty if new_leg is not None else 0.0
                suppress = True
                logger.info(
                    f"[GridEngine] BUY [{idx}] suppressed by stop-score gate "
                    f"(sell fill at [{idx}] @ {fill.avg_price:.2f})"
                )
            if suppress:
                self._alerter_send(
                    f"🛡 Buy [{idx}] suppressed — stop-score gate active"
                )
            else:
                if new_leg is not None:
                    self._place_buy(lv, qty_override=new_leg.qty,
                                     closes_leg_id=new_leg.leg_id)
                else:
                    self._place_buy(lv)

            # ── Drift-shift: top-cell sell → shift range up one spacing ──────
            # If this fill was the top-cell SELL, price has drifted above the
            # grid.  Shift the whole range up immediately via _trail_up so the
            # grid stays centred on price rather than accumulating all-long
            # exposure as the lower bound creeps toward the stop.
            if self._cfg.get("drift_shift_on_top_sell", True):
                with self._lock:
                    is_top        = len(self._levels) > 0 and idx == len(self._levels) - 1
                    # Structural grid boundaries — the fixed lower/upper of
                    # the edge cells, NOT their transient .price (which now
                    # moves between lower/upper as each cell cycles).
                    current_lower = self._levels[0].lower  if self._levels else 0.0
                    current_upper = self._levels[-1].upper if self._levels else 0.0
                    spacing       = self._params.spacing
                if is_top:
                    min_interval = self._cfg.get("drift_shift_min_interval_s", 60)
                    now_t = time.time()
                    if now_t - self._last_drift_shift >= min_interval:
                        # Confirmed-trend catch-up (2026-08-04): ask how many
                        # EXTRA shifts (beyond the usual 1) this event should
                        # perform. trend_confirm_fn both records this event
                        # as evidence and returns the decision in one call —
                        # see GridBot._trend_confirm and the
                        # drift_shift_trend_* GRID_CONFIG comment block.
                        # None / no callable = 0 extra = exact legacy
                        # single-shift behaviour.
                        extra_shifts = 0
                        if self._trend_confirm_fn is not None:
                            try:
                                extra_shifts = max(0, int(self._trend_confirm_fn()))
                            except Exception:
                                logger.exception(
                                    "[GridEngine] trend_confirm_fn raised — "
                                    "treating this event as 0 extra shifts"
                                )
                                extra_shifts = 0
                        shifts_to_do = 1 + extra_shifts

                        # Guard: if mid is already above the price where the
                        # FINAL new SELL level (after all shifts_to_do shifts)
                        # would be placed, even the accelerated catch-up can't
                        # keep up — the trail step would immediately fill the
                        # new level in paper mode, and if mid is further above
                        # still, this cascades into several instant fills and
                        # a phantom-negative long_qty. Request a full rebuild
                        # instead so the grid re-centres cleanly on the
                        # current price. (This is the same guard as before
                        # catch-up existed, just sized to shifts_to_do instead
                        # of a hardcoded 1.)
                        mid_now         = _price_cache.get_mid() or current_upper
                        new_upper_final = current_upper + shifts_to_do * spacing
                        far_oor         = mid_now > new_upper_final
                        if far_oor:
                            logger.info(
                                f"[GridEngine] Top-sell fill at [{idx}] — "
                                f"mid={mid_now:.2f} already above new-upper="
                                f"{new_upper_final:.2f} (after {shifts_to_do} "
                                f"planned shift(s)) → requesting full rebuild "
                                f"instead of drift-shift"
                            )
                            self._needs_rebuild = True
                        else:
                            self._last_drift_shift = now_t
                            if extra_shifts > 0:
                                logger.info(
                                    f"[GridEngine] Top-sell fill at [{idx}] → "
                                    f"CONFIRMED uptrend catch-up: performing "
                                    f"{shifts_to_do} shift(s) ({extra_shifts} "
                                    f"extra beyond the usual 1) in this event"
                                )
                            for i in range(shifts_to_do):
                                with self._lock:
                                    cur_lower = self._levels[0].lower  if self._levels else 0.0
                                    cur_upper = self._levels[-1].upper if self._levels else 0.0
                                logger.info(
                                    f"[GridEngine] Top-sell fill at [{idx}] → "
                                    f"drift-shift UP ({i + 1}/{shifts_to_do}): "
                                    f"[{cur_lower:.2f},{cur_upper:.2f}] "
                                    f"+{spacing:.2f}"
                                )
                                self._trail_up(cur_lower, cur_upper, spacing)
                                with self._lock:
                                    applied = (
                                        len(self._levels) > 0
                                        and self._levels[-1].upper > cur_upper + 1e-9
                                    )
                                if not applied:
                                    # _trail_up deferred one tick (its own
                                    # pending_fill_indices guard — see that
                                    # method) rather than force further
                                    # shifts through a stale/racing state.
                                    # _check_trailing() picks this up again
                                    # next tick, same as the non-catch-up
                                    # path already relies on.
                                    logger.info(
                                        f"[GridEngine] Top-sell fill at "
                                        f"[{idx}] — catch-up stopped after "
                                        f"{i + 1}/{shifts_to_do} (TRAIL UP "
                                        f"deferred one tick)"
                                    )
                                    break
                    else:
                        logger.info(
                            f"[GridEngine] Top-sell fill at [{idx}] — drift-shift "
                            f"throttled ({now_t - self._last_drift_shift:.0f}s < "
                            f"{min_interval}s interval)"
                        )

    def release_one_suppressed_level(self, eligible_side: Optional[str] = None) -> bool:
        """
        Release the highest-index SUPPRESSED level (closest to mid, least
        exposed) by placing whichever side (BUY or SELL) was actually
        withheld — see GridLevel.suppressed_side. If eligible_side is given
        ("BUY" or "SELL"), only a level suppressed on that side is
        considered — so BuyGate's stop-score-recovered release and
        SellGate's uptrend-cleared release (2026-08-04) each only ever
        touch levels their own gate suppressed, never each other's.
        Returns True if a level was released, False if none matched.

        Called by GridBot._run() once per tick when the corresponding gate's
        release condition is met, so position rebuilds gradually rather than
        all at once.
        """
        with self._lock:
            # Find the highest-index SUPPRESSED level (closest to mid)
            # matching eligible_side (or any, if eligible_side is None).
            target = None
            for lv in reversed(self._levels):
                if lv.state != LevelState.SUPPRESSED:
                    continue
                if eligible_side is not None and lv.suppressed_side != eligible_side:
                    continue
                target = lv
                break
            if target is None:
                return False
            # Reset to IDLE before releasing the lock — _place_buy()/
            # _place_sell() will re-acquire the lock to set it to
            # BUY_OPEN/SELL_OPEN. closes_leg_id/qty were preserved at
            # suppression time (see _on_fill) — carry them through so a
            # suppressed CLOSER is restored exactly, not silently replaced
            # by a generic fresh-open order.
            target.state = LevelState.IDLE
            tag_qty  = target.qty if target.closes_leg_id is not None else None
            tag_leg  = target.closes_leg_id
            # Default to "BUY" for defensiveness (matches every
            # suppression before SellGate existed, so any level that
            # somehow predates this field — e.g. across a hot-reload mid
            # rollout — still releases exactly as it always did).
            release_side = target.suppressed_side or "BUY"
            target.suppressed_side = None

        if release_side == "SELL":
            self._place_sell(target, qty_override=tag_qty, closes_leg_id=tag_leg)
            logger.info(
                f"[GridEngine] SELL [{target.index}] @ {target.upper:.2f} "
                f"released from SUPPRESSED (sell-gate cleared)"
            )
        else:
            self._place_buy(target, qty_override=tag_qty, closes_leg_id=tag_leg)
            logger.info(
                f"[GridEngine] BUY [{target.index}] @ {target.lower:.2f} "
                f"released from SUPPRESSED (stop-score recovered)"
            )
        return True

    def count_suppressed(self, side: Optional[str] = None) -> int:
        """
        Return number of levels currently in SUPPRESSED state. If side is
        given ("BUY" or "SELL"), count only levels suppressed on that side
        (see GridLevel.suppressed_side) — used to drive BuyGate's and
        SellGate's independent release conditions in GridBot._run() without
        either one seeing (and prematurely releasing) the other's.
        """
        with self._lock:
            if side is None:
                return sum(1 for lv in self._levels if lv.state == LevelState.SUPPRESSED)
            return sum(1 for lv in self._levels
                       if lv.state == LevelState.SUPPRESSED and lv.suppressed_side == side)

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

    def get_cost_basis(self) -> Tuple[float, float]:
        """
        Weighted-average cost basis of the net long position, expressed as
        (qty, avg_price).

        Previously this scanned SELL_OPEN levels and assumed each one's cost
        basis was "whatever the adjacent lower level happens to be priced
        at" — the same assumption behind the 2026-07-30 fabricated-PnL bug,
        just on the reporting side rather than the accounting side. Now it's
        a direct sum over _open_legs, the real ledger of what was actually
        paid for what's actually still held: every BUY-opened leg IS a piece
        of the net long position, at its own real open_price, full stop.

        Must be called before stop() tears down level state — actually no
        longer true (legs live independently of level state now), kept
        callable at the same point regardless since callers still expect it.
        """
        with self._lock:
            long_legs = [leg for leg in self._open_legs.values() if leg.open_side == "BUY"]
            total_qty  = sum(leg.qty for leg in long_legs)
            total_cost = sum(leg.qty * leg.open_price for leg in long_legs)

        avg_price = (total_cost / total_qty) if total_qty > 0 else 0.0
        return total_qty, avg_price

    # ── Rebuild-time leg reconciliation ─────────────────────────────────────
    # Called by GridBot._rebuild_grid on the NEW engine, after _build_levels()
    # (so self._levels/self._open_legs already reflect the new grid and the
    # DB-seeded ledger) but before start() places any orders.

    def reconcile_open_legs(
        self, exclude_indices: Optional[set] = None,
        already_handled_leg_ids: Optional[set] = None,
        trend_risk: float = 0.0,
        effective_atr: float = 0.0,
        pending_since: Optional[Dict[int, float]] = None,
        zero_candidate_since: Optional[Dict[int, float]] = None,
    ) -> Tuple[Dict[int, int], List["OpenLeg"], set, List["OpenLeg"]]:
        """
        Decide, for every currently-open leg, whether it still fits the
        just-rebuilt grid.

        - Cleanly within [new lower, new upper]: pick the best-fit level to
          host its designated closer (nearest to one new-spacing away in the
          closing direction, among levels not already claimed by another
          leg or excluded by the caller) and reserve it — returned as
          {level_index: leg_id} for the caller to pass into
          start(skip_indices=...), then apply via apply_leg_reassignments()
          once start() has finished placing everything else.
        - Outside [lower,upper] but within a trend_risk-scaled tolerance
          buffer (see reconcile_buffer_atr_mult), AND a direction-correct
          closing level still exists: same tentative re-anchor as above, but
          its leg_id is also added to the third return value (still_pending)
          so the caller keeps a confirm-dwell timer running rather than
          treating it as a clean fit.
        - Outside the buffer too, but STILL flagged pending less than
          reconcile_confirm_s ago (per the caller-supplied pending_since
          map): same tentative re-anchor + still_pending, giving it more
          time to either recover or genuinely confirm before paying for a
          market close.
        - No direction-correct closing level exists at all (leg's open
          price is beyond every level in the required direction): this
          leg has no cell to rest a closer on at all, so it can't be
          assigned or held pending the way the cases above are. Unless
          trend_risk is already urgent (immediate to_liquidate, same as a
          confirmed-misfit clean/buffered leg), it's returned in the
          fourth list (zero_candidate_pending) — this method still does
          NOT act on it (no chase, no liquidation): GridBot._rebuild_grid
          owns kicking off the stray-leg chase the first time a leg
          appears here, and owns force-liquidating it once
          zero_candidate_since shows it's been stranded for
          >= reconcile_zero_candidate_max_dwell_s (that cap itself
          shrinking toward 0 as trend_risk climbs toward
          reconcile_urgent_trend_risk). See the "2026-08-03 16:35
          incident" GRID_CONFIG comment block.
        - Flagged pending (either kind) for too long and still not a
          clean fit: returned in the second list (to_liquidate). This
          method does NOT liquidate them itself — it only decides;
          GridBot._rebuild_grid executes the actual market close (real
          fill, real fee, real fill event in grid_fills) and only then
          removes the leg from the ledger. A leg that can't be reconciled
          stays fully tracked and safe until that happens.

        See the "reconcile_open_legs: trend-risk buffer + confirm-dwell"
        GRID_CONFIG comment block for the 2026-08-02 incident this
        replaces: an unconditional in_range check was force-liquidating
        legs at market/taker fees purely because a low-volatility retune
        had narrowed the range, with no relation to actual directional risk.

        exclude_indices: level indices already spoken for by something else
        (currently: a blue-green handoff's restore_plan) — never a candidate
        here. already_handled_leg_ids: legs already re-attached by that same
        mechanism — skipped entirely rather than double-assigned.
        trend_risk: [0,1] score from StopScoreCalculator.compute_trend_risk()
        — same score already used to gate the stop-raise system. 0 ("looks
        like noise") gives the full tolerance buffer; 1 ("genuine
        strengthening decline") collapses it to 0, matching pre-fix
        behaviour exactly.
        effective_atr: the volatility read the CURRENT rebuild's range was
        built from (GridParams.effective_atr) — buffer is sized in these
        units so it scales with the same regime the range itself reacted to.
        pending_since: {leg_id: first-flagged-ts}, owned and persisted by
        the caller (GridBot._leg_no_fit_since) across rebuilds, since a new
        GridEngine/leg set is reseeded from DB on every rebuild and can't
        hold dwell state itself.
        zero_candidate_since: {leg_id: first-flagged-ts} for the zero-candidate
        case specifically — owned and persisted by the caller
        (GridBot._leg_zero_candidate_since), same reasoning as pending_since.
        Kept as a separate map (not merged into pending_since) since the two
        cases mean different things operationally: pending_since always has
        a resting order in place while it waits; this one never does.
        """
        exclude_indices = exclude_indices or set()
        already_handled_leg_ids = already_handled_leg_ids or set()
        pending_since = pending_since or {}
        zero_candidate_since = zero_candidate_since or {}
        with self._lock:
            legs = [leg for leg in self._open_legs.values()
                    if leg.leg_id not in already_handled_leg_ids]
            all_levels = list(self._levels)
            levels = [lv for lv in all_levels if lv.index not in exclude_indices]

        if not legs:
            return {}, [], set(), []
        if not levels:
            return {}, list(legs), set(), []

        # lower/upper/spacing describe the REBUILT GRID'S true range, from
        # every level regardless of exclusion — not just the subset left
        # over after removing exclude_indices. Those two ranges diverge
        # whenever a handoff restore_plan happens to claim a boundary index
        # (e.g. the very lowest or highest level): using the filtered list
        # here would narrow the effective range and could mark a leg that's
        # genuinely still inside the grid as "no longer fits", sending it to
        # a needless market liquidation. `levels` (filtered) is still what
        # candidate-selection below draws from, since those ARE the only
        # slots actually available to host a new closer.
        # Structural boundaries: fixed lower/upper of the edge cells, not
        # their transient .price. len(all_levels) is now the CELL count
        # directly (one spacing per cell, not per-point), so no -1.
        lower   = all_levels[0].lower
        upper   = all_levels[-1].upper
        spacing = (upper - lower) / len(all_levels) if all_levels else 0.0

        max_buffer_mult = self._cfg.get("reconcile_buffer_atr_mult", 1.5)
        confirm_s       = self._cfg.get("reconcile_confirm_s", 1800.0)
        # Buffer shrinks to 0 as trend_risk -> 1, so a confirmed genuine
        # decline evicts exactly as fast as before this change.
        buffer = (max_buffer_mult * effective_atr * max(0.0, 1.0 - trend_risk)
                  if effective_atr > 0 else 0.0)

        # Urgent bypass — mirrors the stop-raise system's own urgent gate:
        # trend_risk at/above this is strong, real evidence of a genuine
        # move, so skip any dwell/wait and evict immediately. Computed once
        # up front since both the buffered-fit case below and the
        # zero-candidate case use the exact same threshold.
        urgent_threshold = self._cfg.get(
            "reconcile_urgent_trend_risk",
            self._cfg.get("stop_raise_urgent_trend_risk", 0.80),
        )
        # Zero-candidate dwell cap: full reconcile_zero_candidate_max_dwell_s
        # at trend_risk=0, linearly down to 0 at urgent_threshold — a leg
        # stranded during a strong, confirmed move gets barely any
        # unmanaged wait; one stranded during flat/noisy conditions gets
        # close to the full cap. See the "2026-08-03 16:35 incident"
        # GRID_CONFIG comment block.
        #
        # GRACE_DWELL_COORDINATION_2026_08_06: zero_candidate_since is the
        # SAME anchor zero_candidate_pre_chase_grace_s counts from (see
        # _rebuild_grid's pre-chase grace loop). Before this fix, this cap
        # was compared against that same anchor on its own — so with grace
        # set to 7200s and this cap at its 900s default, EVERY zero-
        # candidate leg hit this liquidation check ~900s in, while still
        # sitting inside its own 7200s grace window, and got a market
        # order with zero chase attempts ever dispatched. That's strictly
        # worse than both the pre-grace baseline (immediate chase, a real
        # shot at a maker fill) and the intended grace behavior (patient
        # wait, THEN chase) — confirmed against leg #816
        # (GEN00037_GREEN, 2026-08-06 19:03:12-19:19:59): 0 chase attempts,
        # liquidated at 1007s solely by this check. Adding the grace
        # period on top makes the total budget grace-time (no resting
        # order, not yet chasing) + dwell-time (no resting order, chase
        # already tried and exhausted) instead of the two silently
        # overlapping on the same clock.
        pre_chase_grace_s = self._cfg.get("zero_candidate_pre_chase_grace_s", 0.0)
        zc_max_dwell = self._cfg.get("reconcile_zero_candidate_max_dwell_s", 900.0)
        zc_dwell_cap = pre_chase_grace_s + (
            zc_max_dwell * max(0.0, 1.0 - trend_risk / urgent_threshold)
            if urgent_threshold > 0 else 0.0
        )

        assignments:   Dict[int, int] = {}
        claimed:       set            = set()
        to_liquidate:  List[OpenLeg]  = []
        still_pending: set            = set()
        zero_candidate_pending: List[OpenLeg] = []
        now = time.time()

        def _closer_price(lv: "GridLevel", leg: "OpenLeg") -> float:
            # The price THIS cell would actually place the leg's closer at
            # — its upper boundary for a BUY-opened leg (closes via SELL),
            # its lower boundary for a SELL-opened leg (closes via BUY).
            return lv.upper if leg.open_side == "BUY" else lv.lower

        def _candidates(leg: "OpenLeg") -> Tuple[float, List["GridLevel"]]:
            if leg.open_side == "BUY":
                # Long leg closes with a SELL above what it paid — never at
                # or below, that would be locking in a loss the grid itself
                # didn't ask for.
                tgt = leg.open_price + spacing if spacing > 0 else leg.open_price
                cands = [lv for lv in levels
                         if lv.index not in claimed and lv.upper > leg.open_price]
            else:
                tgt = leg.open_price - spacing if spacing > 0 else leg.open_price
                cands = [lv for lv in levels
                         if lv.index not in claimed and lv.lower < leg.open_price]
            return tgt, cands

        # Oldest-opened first within each pass: whichever position has been
        # waiting longest gets first pick if two legs' ideal target levels
        # collide. Clean fits are a SEPARATE, earlier pass — a buffered or
        # still-pending leg must never claim a level ahead of one that
        # genuinely belongs in the range, or an old out-of-range leg could
        # starve a newer clean-fit leg of its rightful level (caught by
        # simulating this exact function against the 2026-08-02 incident
        # numbers before shipping this change).
        ordered = sorted(legs, key=lambda l: l.opened_ts)
        clean_legs = [l for l in ordered if lower <= l.open_price <= upper]
        other_legs = [l for l in ordered if not (lower <= l.open_price <= upper)]

        for leg in clean_legs:
            target, candidates = _candidates(leg)
            if not candidates:
                # Every candidate level on the closing side is already
                # claimed by another clean-fit leg — unchanged from
                # pre-existing behaviour. Previously silent (2026-08-07:
                # leg #893's restart-triggered liquidation left no trace
                # anywhere in reconcile_open_legs' own logging, making the
                # actual branch that fired unrecoverable after the fact) —
                # log it like every other to_liquidate/zero_candidate_pending
                # branch below does.
                logger.info(
                    f"[GridEngine] Leg #{leg.leg_id} clean-fit (open="
                    f"{leg.open_price:.2f}, range=[{lower:.2f},{upper:.2f}]) "
                    f"but every closing-side level already claimed by "
                    f"another clean-fit leg — liquidating"
                )
                to_liquidate.append(leg)
                continue
            best = min(candidates, key=lambda lv: abs(_closer_price(lv, leg) - target))
            claimed.add(best.index)
            assignments[best.index] = leg.leg_id

        for leg in other_legs:
            target, candidates = _candidates(leg)
            if not candidates:
                # No level in the required direction exists at any price —
                # structurally stranded (e.g. a long opened well above a
                # grid that has since dropped entirely below it), or every
                # remaining level was already claimed by a clean fit above.
                # There's no cell to hold a resting closer on regardless of
                # trend_risk, so this can never be a clean/buffered
                # assignment — but unless trend_risk is already urgent, it
                # no longer means an instant market close either. See the
                # "2026-08-03 16:35 incident" GRID_CONFIG comment block.
                if trend_risk >= urgent_threshold:
                    logger.info(
                        f"[GridEngine] Leg #{leg.leg_id} zero-candidate + "
                        f"urgent trend_risk={trend_risk:.2f} >= "
                        f"{urgent_threshold:.2f} — liquidating now"
                    )
                    to_liquidate.append(leg)
                    continue

                first_zc = zero_candidate_since.get(leg.leg_id)
                if first_zc is not None and (now - first_zc) >= zc_dwell_cap:
                    logger.info(
                        f"[GridEngine] Leg #{leg.leg_id} zero-candidate for "
                        f"{now - first_zc:.0f}s >= dwell cap {zc_dwell_cap:.0f}s "
                        f"(trend_risk={trend_risk:.2f}) — liquidating"
                    )
                    to_liquidate.append(leg)
                    continue

                logger.info(
                    f"[GridEngine] Leg #{leg.leg_id} zero-candidate "
                    f"(open={leg.open_price:.2f}, range=[{lower:.2f},"
                    f"{upper:.2f}]) — no cell to hold a closer on, "
                    f"trend_risk={trend_risk:.2f}, dwell cap {zc_dwell_cap:.0f}s "
                    f"({'starting now' if first_zc is None else f'{now - first_zc:.0f}s elapsed'})"
                )
                zero_candidate_pending.append(leg)
                continue

            buffered_fit = buffer > 0 and (lower - buffer) <= leg.open_price <= (upper + buffer)
            if buffered_fit:
                logger.info(
                    f"[GridEngine] Leg #{leg.leg_id} outside [{lower:.2f},"
                    f"{upper:.2f}] (open={leg.open_price:.2f}) but within "
                    f"trend_risk-scaled buffer ({buffer:.2f}, trend_risk="
                    f"{trend_risk:.2f}) — tolerated, no dwell started"
                )
                best = min(candidates, key=lambda lv: abs(_closer_price(lv, leg) - target))
                claimed.add(best.index)
                assignments[best.index] = leg.leg_id
                continue

            if trend_risk >= urgent_threshold:
                logger.info(
                    f"[GridEngine] Leg #{leg.leg_id} misfit + urgent "
                    f"trend_risk={trend_risk:.2f} >= {urgent_threshold:.2f} "
                    f"— bypassing confirm-dwell, liquidating now"
                )
                to_liquidate.append(leg)
                continue

            # Outside the buffer too. Give it reconcile_confirm_s (tracked
            # by the caller across rebuilds) before forcing a market close.
            first_seen = pending_since.get(leg.leg_id)
            if first_seen is not None and (now - first_seen) >= confirm_s:
                logger.info(
                    f"[GridEngine] Leg #{leg.leg_id} confirmed misfit after "
                    f"{now - first_seen:.0f}s (open={leg.open_price:.2f}, "
                    f"range=[{lower:.2f},{upper:.2f}], buffer={buffer:.2f}) "
                    f"— liquidating"
                )
                to_liquidate.append(leg)
                continue

            logger.info(
                f"[GridEngine] Leg #{leg.leg_id} misfit "
                f"(open={leg.open_price:.2f}, range=[{lower:.2f},{upper:.2f}], "
                f"buffer={buffer:.2f}) — tentatively re-anchored, holding "
                f"{confirm_s:.0f}s before liquidating "
                f"({'new candidate' if first_seen is None else f'{now - first_seen:.0f}s elapsed'})"
            )
            best = min(candidates, key=lambda lv: abs(_closer_price(lv, leg) - target))
            claimed.add(best.index)
            assignments[best.index] = leg.leg_id
            still_pending.add(leg.leg_id)

        if assignments or to_liquidate or zero_candidate_pending:
            logger.info(
                f"[GridEngine] reconcile_open_legs: {len(assignments)} leg(s) "
                f"re-anchored to the rebuilt grid ({len(still_pending)} still "
                f"pending confirm), {len(to_liquidate)} liquidating, "
                f"{len(zero_candidate_pending)} zero-candidate pending "
                f"(new range=[{lower:.2f},{upper:.2f}])"
            )
        return assignments, to_liquidate, still_pending, zero_candidate_pending

    def apply_leg_reassignments(self, assignments: Dict[int, int]) -> None:
        """
        Place the designated-closer order for each (level_index -> leg_id)
        pair from reconcile_open_legs. Call AFTER start() so these specific
        indices (passed as skip_indices to start()) are still IDLE and
        weren't given a generic fresh-open order instead.
        """
        with self._lock:
            level_by_index = {lv.index: lv for lv in self._levels}
            legs_by_id = dict(self._open_legs)
        for idx, leg_id in assignments.items():
            lv  = level_by_index.get(idx)
            leg = legs_by_id.get(leg_id)
            if lv is None or leg is None:
                logger.error(
                    f"[GridEngine] apply_leg_reassignments: index={idx} "
                    f"leg_id={leg_id} not resolvable — leg stays tracked, just "
                    f"untargeted until the next rebuild's reconciliation."
                )
                continue
            # This cell is being repurposed to host leg's closer — its
            # future identity (what it re-opens as, once this leg's closer
            # fills) now follows the leg, not whatever it might have been
            # before reconciliation.
            lv.open_side = leg.open_side
            if leg.open_side == "BUY":
                self._place_sell(lv, qty_override=leg.qty, closes_leg_id=leg.leg_id)
            else:
                self._place_buy(lv, qty_override=leg.qty, closes_leg_id=leg.leg_id)

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
        # See config default "stop_loss_confirm_s" for why this exists: a
        # single tick below stop_price starts the clock rather than
        # triggering immediately, so a wick that reverts within a few
        # seconds doesn't cost a full liquidation + cooldown.
        self._confirm_s     = config.get("stop_loss_confirm_s", 3.0)
        self._below_since: Optional[float] = None

    def update_price(self, price: float):
        self._stop_price = price

    def check(self, mid: float) -> bool:
        if self._triggered or not self._enabled:
            return self._triggered

        if self._stop_price <= 0 or mid >= self._stop_price:
            # Recovered (or never breached) — cancel any candidate breach.
            if self._below_since is not None:
                logger.info(
                    f"[StopLoss] Candidate breach cancelled: mid={mid:.2f} "
                    f"recovered above stop={self._stop_price:.2f} after "
                    f"{time.time() - self._below_since:.1f}s below it"
                )
                self._below_since = None
            return False

        # mid < stop_price
        now = time.time()
        if self._below_since is None:
            self._below_since = now
            if self._confirm_s > 0:
                logger.info(
                    f"[StopLoss] Candidate breach: mid={mid:.2f} < "
                    f"stop={self._stop_price:.2f} — confirming for "
                    f"{self._confirm_s:.1f}s before triggering"
                )

        dwell = now - self._below_since
        if dwell >= self._confirm_s:
            logger.warning(
                f"[StopLoss] TRIGGERED: mid={mid:.2f} < stop={self._stop_price:.2f} "
                f"(confirmed {dwell:.1f}s continuously below stop)")
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

    @property
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

_GRID_DB_SCHEMA_VERSION = 10

_GRID_DB_DDL = """
-- Every grid fill: permanent, append-only audit log.
-- gross_pnl is meaningful only for closing fills (real realized PnL against
-- the specific leg closed — see open_legs below). 0 for opening fills.
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
    gross_pnl   REAL    NOT NULL DEFAULT 0.0,  -- real realized PnL; 0 for opening fills
    cycle_num   INTEGER NOT NULL DEFAULT 0,    -- monotonic cycle counter at time of fill
    leg_id        INTEGER,                  -- the open_legs.leg_id this fill opened or closed
    close_reason  TEXT                      -- NULL for a normal grid-cycle close;
                                             -- 'rebuild_reprice' for a leg the auto-tuner's
                                             -- new range no longer spans (see reconcile_open_legs)
);

-- Ledger of currently-open legs: one row per fill that opened a position with
-- nothing yet closing it. A leg is deleted from here the instant the fill
-- that closes it is processed (see GridEngine._on_fill) — this table is the
-- live "what do we actually hold and at what price" state, kept durable so
-- it survives both an in-process grid rebuild and a full process restart,
-- not just carried in memory. Added 2026-07-31 replacing the old
-- adjacent-level-price cost-basis assumption (see grid_fills.close_reason
-- comment and the 2026-07-30 wash-trade-PnL fix this generalizes).
CREATE TABLE IF NOT EXISTS open_legs (
    leg_id            INTEGER PRIMARY KEY AUTOINCREMENT,
    open_side         TEXT    NOT NULL,      -- 'BUY' | 'SELL' — the fill that opened this leg
    open_price        REAL    NOT NULL,
    qty               REAL    NOT NULL,
    opened_ts         REAL    NOT NULL,
    opened_level_idx  INTEGER NOT NULL,
    open_fee          REAL    NOT NULL DEFAULT 0.0  -- see OpenLeg.open_fee docstring
);

-- Pre-aggregated daily PnL (HKT date); updated atomically with each fill.
-- gross_pnl_usd: sum of real realized PnL for completed cycles
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
    sl_count      INTEGER NOT NULL DEFAULT 0,  -- number of stop-loss liquidation events
    -- Forced-close gross PnL/count: legs closed via _finalize_leg_close /
    -- _record_partial_leg_close — rebuild_reprice, rebuild_reprice_pending,
    -- trail_up, trail_down, and their *_chase_exhausted market-fallback
    -- variants (see record_fill's is_reprice_loss param). A different risk
    -- category from sl_*: these are individual legs cut loose by the grid
    -- range moving away from them, not a whole-grid stop-loss halt — kept
    -- as a separate bucket rather than folded into sl_* so /status can show
    -- each distinctly instead of one number conflating two different causes.
    reprice_gross_usd REAL NOT NULL DEFAULT 0.0,
    reprice_count     INTEGER NOT NULL DEFAULT 0
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

-- Single-instance lock: enforces that at most one grid_bot.py process is ever
-- the active trading instance for this DB at a time (standalone, blue, or
-- green — role doesn't matter). A single row (id=1). holder_pid/updated_at
-- form a heartbeat lease: the holder must refresh updated_at more often than
-- GridStateStore._BG_LOCK_STALE_AFTER_S or a later process is entitled to
-- treat the lock as abandoned and take it over via the same atomic
-- compare-and-swap used to originally acquire it. See GridStateStore.bg_lock_*.
-- log_gen: monotonically incremented by the SAME compare-and-swap that
-- grants the lock (see bg_lock_try_acquire), so every process that ever
-- becomes the live holder — cold start, handoff winner, or crash-recovery
-- resume, regardless of --role — gets a distinct number. Used to give each
-- live instance its own log file (grid_bot_gen{N}_*.log) instead of naming
-- log files off --role, which no longer 1:1-maps to "which process is
-- live" now that role only controls handoff-request behaviour (see the
-- blue-green deployment doc's "no role swap" section).
CREATE TABLE IF NOT EXISTS bg_lock (
    id         INTEGER PRIMARY KEY CHECK (id = 1),
    holder_pid INTEGER,
    updated_at REAL NOT NULL DEFAULT 0,
    log_gen    INTEGER NOT NULL DEFAULT 0
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
                self._conn.execute(
                    "INSERT OR IGNORE INTO bg_lock(id, holder_pid, updated_at) "
                    "VALUES (1, NULL, 0)"
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
                # v3->v4: blue-green deployment keys stored in existing meta table.
                # No DDL change required; version bump marks compatibility.
                if db_ver < 4:
                    logger.info("[GridStateStore] schema migrated -> v4 (blue-green deployment)")
                # v4->v5: collapsed the separate bg_blue_pid / bg_green_pid keys into
                # a single bg_live_pid key. Drop any stale v4 keys left over from a
                # process that didn't clear them.
                if db_ver < 5:
                    for stale_key in ("bg_blue_pid", "bg_green_pid"):
                        try:
                            self._conn.execute("DELETE FROM meta WHERE key=?", (stale_key,))
                        except Exception:
                            pass
                    logger.info(
                        "[GridStateStore] schema migrated -> v5 "
                        "(bg_blue_pid/bg_green_pid collapsed into bg_live_pid)"
                    )
                # v5->v6: replaced the plain bg_live_pid meta key (no staleness
                # detection — a hung or crashed holder looked identical to a
                # healthy one) and the short-lived bg_handoff_claimant_pid
                # experiment (PID-liveness checks are unreliable across
                # platforms — PIDs get reused) with a single CAS + heartbeat
                # lease in the dedicated bg_lock table (created by the DDL
                # above; CREATE TABLE IF NOT EXISTS handles existing DBs).
                # Drop the now-unused meta keys and seed the lock row.
                if db_ver < 6:
                    for stale_key in ("bg_live_pid", "bg_handoff_claimant_pid"):
                        try:
                            self._conn.execute("DELETE FROM meta WHERE key=?", (stale_key,))
                        except Exception:
                            pass
                    self._conn.execute(
                        "INSERT OR IGNORE INTO bg_lock(id, holder_pid, updated_at) "
                        "VALUES (1, NULL, 0)"
                    )
                    logger.info(
                        "[GridStateStore] schema migrated -> v6 "
                        "(bg_live_pid/bg_handoff_claimant_pid replaced by bg_lock "
                        "CAS + heartbeat lease)"
                    )
                # v6->v7: log_gen column added to bg_lock. CREATE TABLE IF NOT
                # EXISTS in the DDL above doesn't touch a table that already
                # exists, so an existing bg_lock row needs an explicit ALTER.
                if db_ver < 7:
                    try:
                        self._conn.execute(
                            "ALTER TABLE bg_lock ADD COLUMN log_gen "
                            "INTEGER NOT NULL DEFAULT 0"
                        )
                    except Exception:
                        pass  # column already exists (idempotent)
                    logger.info(
                        "[GridStateStore] schema migrated -> v7 "
                        "(bg_lock.log_gen for per-instance log file naming)"
                    )
                # v7->v8: open_legs table (CREATE IF NOT EXISTS in the DDL above
                # handles new DBs); grid_fills gets leg_id + close_reason columns
                # for existing DBs, added via ALTER since the table already exists.
                if db_ver < 8:
                    for col, typedef in [
                        ("leg_id",       "INTEGER"),
                        ("close_reason", "TEXT"),
                    ]:
                        try:
                            self._conn.execute(
                                f"ALTER TABLE grid_fills ADD COLUMN {col} {typedef}"
                            )
                        except Exception:
                            pass  # column already exists (idempotent)
                    logger.info(
                        "[GridStateStore] schema migrated -> v8 "
                        "(open_legs ledger + grid_fills.leg_id/close_reason — "
                        "real per-leg cost basis replacing the adjacent-level-"
                        "price assumption)"
                    )
                # v8->v9: reprice_gross_usd + reprice_count columns added to
                # daily_pnl — mirrors v2->v3's sl_gross_usd/sl_count, but for
                # forced-close legs (rebuild_reprice, rebuild_reprice_pending,
                # trail_up, trail_down) rather than stop-loss. Previously
                # these losses were already correctly included in
                # gross_pnl_usd/net_pnl_usd (record_fill's cycles_delta and
                # sl_* logic aside, every fill contributes to the plain
                # totals regardless of reason) but were invisible as a
                # distinct category in /status, and were miscounted into
                # cycle_count as if they were completed grid cycles rather
                # than forced early exits. See the 2026-08-04 status-report
                # request this fixes.
                if db_ver < 9:
                    for col, typedef in [
                        ("reprice_gross_usd", "REAL NOT NULL DEFAULT 0.0"),
                        ("reprice_count",     "INTEGER NOT NULL DEFAULT 0"),
                    ]:
                        try:
                            self._conn.execute(
                                f"ALTER TABLE daily_pnl ADD COLUMN {col} {typedef}"
                            )
                        except Exception:
                            pass  # column already exists (idempotent)
                    logger.info(
                        "[GridStateStore] schema migrated -> v9 "
                        "(daily_pnl reprice_gross_usd/reprice_count columns)"
                    )
                # v9->v10: open_fee column added to open_legs, so a leg's
                # opening fill's fee survives alongside it (see OpenLeg.
                # open_fee docstring) instead of only ever being visible at
                # the moment the opening fill itself was logged. Fixes the
                # closing-fill log line's net= (in GridEngine._on_fill and
                # GridBot._finalize_leg_close), which previously subtracted
                # only the closing fill's own fee — cosmetic only, since
                # record_fill's persisted fee_usd/gross_pnl (and therefore
                # daily_pnl/cumulative_net/status/alerts) already correctly
                # summed every fill's fee independently regardless of
                # open/close. Any leg already open at migration time has no
                # recorded opening fee to backfill (that fill already
                # happened and its fee is only preserved in grid_fills, not
                # linked back to this specific still-open leg row) — its
                # own open_fee defaults to 0.0, so its eventual closing log
                # line will still undercount the same way it did before
                # this fix, exactly once, until it closes. Every leg opened
                # after this migration is fully correct from the start.
                if db_ver < 10:
                    try:
                        self._conn.execute(
                            "ALTER TABLE open_legs ADD COLUMN open_fee "
                            "REAL NOT NULL DEFAULT 0.0"
                        )
                    except Exception:
                        pass  # column already exists (idempotent)
                    logger.info(
                        "[GridStateStore] schema migrated -> v10 "
                        "(open_legs.open_fee column)"
                    )
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
        gross_pnl:      float,     # 0.0 for an opening fill
        cycle_num:      int,
        is_liquidation: bool = False,  # True for stop-loss / shutdown liquidations
        is_reprice_loss: bool = False,  # True for rebuild_reprice(_pending)/trail_up/trail_down closes
        leg_id:         Optional[int] = None,   # open_legs.leg_id this fill opened/closed
        close_reason:   Optional[str] = None,   # e.g. 'rebuild_reprice'; NULL = normal cycle
        is_close:       bool = False,           # True if this fill closed a leg (real PnL)
    ) -> None:
        """
        Append one fill row and update the daily_pnl bucket atomically.
        Called from GridEngine._on_fill() — must be fast and non-blocking
        (the fill thread processes fills sequentially; a slow DB write here
        delays counter-order placement).  WAL + NORMAL sync keeps writes
        to ~1-2 ms on spinning rust; SSD is faster.

        is_close must be passed explicitly rather than inferred from side.
        Under the old adjacent-level-price model every real cycle ended in
        a SELL, so `side == "SELL"` was a correct proxy for "this completed
        a cycle." That's no longer true — a BUY can now close a short leg
        (real gross_pnl, a real completed round trip) just as validly as a
        SELL closes a long one. Inferring from side would silently
        undercount cycle_count/fill_count for daily_pnl whenever a short is
        involved, while gross_pnl_usd/net_pnl_usd stayed correct — a visible
        inconsistency between the dollar totals and the cycle tally.

        is_liquidation and is_reprice_loss are mutually exclusive in
        practice (the former only ever comes from _liquidate_position's own
        stop-loss caller, the latter only from _finalize_leg_close /
        _record_partial_leg_close) and both mean the same thing for
        gross_pnl_usd/net_pnl_usd/cycle_count purposes: this wasn't a leg
        reaching its own designed grid-cell price, so it's excluded from
        cycle_count the same way, just tallied into a different bucket
        (sl_* vs reprice_*) for /status to report separately — different
        risk categories (a whole-grid stop-loss halt vs. an individual leg
        cut loose by range drift), so kept visually distinct rather than
        combined into one "not a real cycle" number.
        """
        hkt_date = _db_hkt_date(ts_utc)
        # Liquidation SELLs (stop-loss) and forced repricing/trail closes are
        # not completed grid cycles.
        not_a_cycle  = is_liquidation or is_reprice_loss
        cycles_delta = 1 if is_close and not not_a_cycle else 0

        with self._lock:
            self._conn.execute(
                """INSERT INTO grid_fills
                   (ts_utc, hkt_date, side, level_idx, price_usd,
                    qty_btc, fee_usd, gross_pnl, cycle_num, leg_id, close_reason)
                   VALUES (?,?,?,?,?,?,?,?,?,?,?)""",
                (ts_utc, hkt_date, side, level_idx, price_usd,
                 qty_btc, fee_usd, gross_pnl, cycle_num, leg_id, close_reason),
            )
            # Update daily bucket — fees stored as negative (cost subtracted from net)
            sl_gross      = gross_pnl if is_liquidation  else 0.0
            sl_delta      = 1         if is_liquidation  else 0
            reprice_gross = gross_pnl if is_reprice_loss else 0.0
            reprice_delta = 1         if is_reprice_loss else 0
            self._conn.execute(
                """INSERT INTO daily_pnl
                   (hkt_date, gross_pnl_usd, fees_usd, net_pnl_usd, fill_count, cycle_count,
                    sl_gross_usd, sl_count, reprice_gross_usd, reprice_count)
                   VALUES (?, ?, ?, ?, 1, ?, ?, ?, ?, ?)
                   ON CONFLICT(hkt_date) DO UPDATE SET
                       gross_pnl_usd     = gross_pnl_usd     + excluded.gross_pnl_usd,
                       fees_usd          = fees_usd          + excluded.fees_usd,
                       net_pnl_usd       = net_pnl_usd       + excluded.gross_pnl_usd + excluded.fees_usd,
                       fill_count        = fill_count        + 1,
                       cycle_count       = cycle_count       + excluded.cycle_count,
                       sl_gross_usd      = sl_gross_usd      + excluded.sl_gross_usd,
                       sl_count          = sl_count          + excluded.sl_count,
                       reprice_gross_usd = reprice_gross_usd + excluded.reprice_gross_usd,
                       reprice_count     = reprice_count     + excluded.reprice_count""",
                (hkt_date, gross_pnl, -fee_usd, gross_pnl - fee_usd, cycles_delta,
                 sl_gross, sl_delta, reprice_gross, reprice_delta),
            )
            self._conn.commit()

    # ── Open-legs ledger ──────────────────────────────────────────────────────
    # The durable source of truth for GridEngine._open_legs — see the
    # open_legs table comment in _GRID_DB_DDL. Written through synchronously
    # on every leg open/close so a plain process restart (not just an
    # in-process grid rebuild) can pick up exactly where it left off, the
    # same way _realized_pnl/_total_fees/_cycle_count are seeded from
    # get_accumulated() today.

    def open_leg(self, open_side: str, open_price: float, qty: float,
                 opened_ts: float, opened_level_idx: int,
                 open_fee: float = 0.0) -> int:
        """Insert a new open leg row. Returns its leg_id."""
        with self._lock:
            cur = self._conn.execute(
                """INSERT INTO open_legs
                   (open_side, open_price, qty, opened_ts, opened_level_idx, open_fee)
                   VALUES (?,?,?,?,?,?)""",
                (open_side, open_price, qty, opened_ts, opened_level_idx, open_fee),
            )
            self._conn.commit()
            return cur.lastrowid

    def close_leg(self, leg_id: int) -> None:
        """Remove a leg once the fill that closes it has been processed."""
        with self._lock:
            self._conn.execute("DELETE FROM open_legs WHERE leg_id = ?", (leg_id,))
            self._conn.commit()

    def reduce_leg_qty(self, leg_id: int, new_qty: float,
                        new_open_fee: Optional[float] = None) -> None:
        """
        Persist a partial close in place: shrink an open leg's remaining
        qty rather than deleting it. Used when a leg is being closed
        across multiple partial fills (see
        GridBot._chase_close_leg_worker's chase-attempt partial-fill
        handling) — the leg is still open, just for less than it
        originally was, so a plain process restart re-seeding
        GridEngine._open_legs from this table must see the reduced
        amount, not the pre-partial original (which would overstate
        actual exposure and mis-size the next closing order).

        new_open_fee mirrors the same reasoning for open_fee (see OpenLeg.
        open_fee docstring): each partial chunk claims its proportional
        share of the leg's opening fee, so the DB row's remaining share
        must shrink in lockstep with qty — otherwise a restart mid-chase
        would re-seed the pre-partial (too large) open_fee, and the leg's
        eventual final close would double-count the share partial chunks
        already claimed. None (default) leaves open_fee untouched, for any
        future caller that only ever needs to adjust qty.
        """
        with self._lock:
            if new_open_fee is not None:
                self._conn.execute(
                    "UPDATE open_legs SET qty = ?, open_fee = ? WHERE leg_id = ?",
                    (new_qty, new_open_fee, leg_id))
            else:
                self._conn.execute(
                    "UPDATE open_legs SET qty = ? WHERE leg_id = ?", (new_qty, leg_id))
            self._conn.commit()

    def rollback(self) -> None:
        """
        Discard any uncommitted statements on the shared connection.
        Needed between retry attempts on a multi-statement write (e.g.
        record_fill's grid_fills insert + daily_pnl upsert): sqlite3's
        default deferred-transaction mode does NOT auto-rollback a failed
        execute() — a statement that already succeeded before a later one
        in the same call raised stays pending until the next commit(),
        including a commit() from a totally unrelated later write on this
        connection. Without an explicit rollback here, retrying the whole
        function re-runs the already-succeeded statement(s) again; if a
        later attempt then succeeds, everything from every attempt commits
        together — e.g. two grid_fills rows and a doubled daily_pnl delta
        for what was really one fill. Best-effort: a rollback failure here
        shouldn't mask the original write error the caller is already
        handling.
        """
        with self._lock:
            try:
                self._conn.rollback()
            except Exception:
                pass

    def execute_with_retry(self, fn, attempts: int = 3, base_delay: float = 0.05):
        """
        Retry a synchronous DB write a few times with small backoff before
        giving up, rolling back between attempts (see rollback()'s docstring
        for why that's required for any multi-statement write).

        Lives on the store rather than on GridEngine specifically because
        both GridEngine._on_fill and GridBot's liquidation paths
        (_liquidate_position, _liquidate_leg_at_market) write through this
        same store and need the identical protection — originally only
        GridEngine had it (as _retry_db_write, now a thin wrapper delegating
        here), which left the liquidation paths on bare try/except with no
        retry and no rollback at all.

        Local SQLite failures at this call rate are almost always transient
        (a WAL checkpoint, a momentary lock from another writer on the same
        file) rather than permanent — a couple of retries a few tens of
        milliseconds apart clears the great majority of them without
        meaningfully delaying whatever the caller does next (three attempts
        here is still well under 200ms worst case).

        Returns (True, result) on success, (False, last_exception) if every
        attempt failed — the caller decides how loudly to escalate a real,
        persistent failure.
        """
        last_exc = None
        for attempt in range(attempts):
            try:
                return True, fn()
            except Exception as e:
                last_exc = e
                self.rollback()
                if attempt < attempts - 1:
                    time.sleep(base_delay * (attempt + 1))
        return False, last_exc

    def get_open_legs(self) -> List[dict]:
        """All currently-open legs — used to seed GridEngine._open_legs."""
        with self._lock:
            rows = self._conn.execute(
                """SELECT leg_id, open_side, open_price, qty, opened_ts,
                          opened_level_idx, open_fee
                   FROM open_legs"""
            ).fetchall()
        return [dict(r) for r in rows]

    # ── Accumulated totals ────────────────────────────────────────────────────

    # Reason strings that _finalize_leg_close / _record_partial_leg_close
    # pass through as close_reason whenever is_reprice_loss=True (see
    # record_fill's docstring). Kept as one list so the read-time repair
    # below (REPRICE_UNDERCOUNT_2026_08_05) and any future caller checking
    # "is this row a reprice/trail loss" stay in sync with each other.
    REPRICE_CLOSE_REASONS = (
        "rebuild_reprice", "rebuild_reprice_pending", "trail_up", "trail_down",
    )

    def _reprice_totals_from_fills(self, hkt_date: Optional[str] = None) -> Tuple[float, int]:
        """
        REPRICE_UNDERCOUNT_2026_08_05: reprice_gross_usd/reprice_count in
        daily_pnl are maintained incrementally (record_fill's ON CONFLICT
        DO UPDATE, gated on is_reprice_loss) and were found to silently
        undercount — confirmed against grid_fills on 2026-08-06: 2026-08-04
        showed 0 of 4 real trail_up closes counted, 2026-08-05 showed 13 of
        27 rebuild_reprice(_pending)/trail_up closes counted (13 short,
        -$17.69 missing), while gross_pnl_usd/net_pnl_usd/fill_count — sourced
        from the exact same INSERT statement, same row, same call — reconciled
        against raw fills perfectly every time. is_reprice_loss=True is set
        unconditionally at both call sites that ever set close_reason (see
        record_fill's is_reprice_loss docstring), so this isn't a call-site
        logic bug; the leading hypothesis is a write race between concurrent
        chase-worker threads (each stray leg gets its own daemon thread —
        see _chase_close_leg — and several can close within milliseconds of
        each other, as happened at the 2026-08-05 04:19:07 UTC post-restart
        reconcile) that occasionally drops an UPDATE's contribution to just
        the sl_/reprice_ columns without affecting the row's other columns.
        Root cause not yet confirmed; this recomputes the reprice figure
        directly from grid_fills (an append-only INSERT-only log that HAS
        reconciled correctly in every check so far) instead of trusting the
        incremental counter, so /status is right regardless of whichever
        write path is losing updates. sl_gross_usd/sl_count are NOT touched
        here — no evidence of the same bug there yet (every single-SL day
        checked has matched exactly) — but see close_reason='stop_loss' now
        being tagged on liquidation fills (_liquidate_position) for the same
        kind of raw-fill fallback if that ever turns out to need it too.
        """
        placeholders = ",".join("?" for _ in self.REPRICE_CLOSE_REASONS)
        with self._lock:
            if hkt_date is None:
                row = self._conn.execute(
                    f"""SELECT COALESCE(SUM(gross_pnl), 0.0), COUNT(*)
                        FROM grid_fills WHERE close_reason IN ({placeholders})""",
                    self.REPRICE_CLOSE_REASONS,
                ).fetchone()
            else:
                row = self._conn.execute(
                    f"""SELECT COALESCE(SUM(gross_pnl), 0.0), COUNT(*)
                        FROM grid_fills
                        WHERE hkt_date=? AND close_reason IN ({placeholders})""",
                    (hkt_date, *self.REPRICE_CLOSE_REASONS),
                ).fetchone()
        return (row[0], row[1]) if row else (0.0, 0)

    def get_accumulated(self) -> dict:
        """
        Sum all rows in daily_pnl -> all-time totals. reprice_gross/
        reprice_count are recomputed from grid_fills rather than summed
        from the daily_pnl column — see _reprice_totals_from_fills'
        REPRICE_UNDERCOUNT_2026_08_05 docstring.
        """
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
        result = dict(row) if row else {
            "gross_pnl": 0.0, "fees": 0.0, "net_pnl": 0.0,
            "fill_count": 0,  "cycle_count": 0,
            "sl_gross": 0.0,  "sl_count": 0,
        }
        reprice_gross, reprice_count = self._reprice_totals_from_fills(hkt_date=None)
        result["reprice_gross"], result["reprice_count"] = reprice_gross, reprice_count
        return result

    # ── Daily PnL ─────────────────────────────────────────────────────────────

    def get_daily(self, hkt_date: Optional[str] = None) -> dict:
        """
        Return the daily_pnl row for hkt_date (today HKT if None).
        reprice_gross_usd/reprice_count are recomputed from grid_fills
        rather than read from the daily_pnl row — see
        _reprice_totals_from_fills' REPRICE_UNDERCOUNT_2026_08_05 docstring.
        """
        if hkt_date is None:
            hkt_date = _db_hkt_date(time.time())
        with self._lock:
            row = self._conn.execute(
                "SELECT * FROM daily_pnl WHERE hkt_date=?", (hkt_date,)
            ).fetchone()
        result = dict(row) if row else {
            "hkt_date": hkt_date, "gross_pnl_usd": 0.0, "fees_usd": 0.0,
            "net_pnl_usd": 0.0, "fill_count": 0, "cycle_count": 0,
            "sl_gross_usd": 0.0, "sl_count": 0,
        }
        reprice_gross, reprice_count = self._reprice_totals_from_fills(hkt_date=hkt_date)
        result["reprice_gross_usd"], result["reprice_count"] = reprice_gross, reprice_count
        return result

    def get_recent_daily(self, days: int = 7) -> list:
        """
        Return last N HKT-day rows, newest first. reprice_gross_usd/
        reprice_count per row are recomputed from grid_fills — see
        _reprice_totals_from_fills' REPRICE_UNDERCOUNT_2026_08_05 docstring.
        """
        with self._lock:
            rows = self._conn.execute(
                "SELECT * FROM daily_pnl ORDER BY hkt_date DESC LIMIT ?", (days,)
            ).fetchall()
        result = []
        for r in rows:
            d = dict(r)
            reprice_gross, reprice_count = self._reprice_totals_from_fills(hkt_date=d["hkt_date"])
            d["reprice_gross_usd"], d["reprice_count"] = reprice_gross, reprice_count
            result.append(d)
        return result

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

    def delete_meta(self, key: str) -> None:
        with self._lock:
            self._conn.execute("DELETE FROM meta WHERE key=?", (key,))
            self._conn.commit()

    # ── Blue-green deployment helpers ─────────────────────────────────────────
    # Keys/tables used:
    #   bg_lock (table)      → the single-instance lock. See bg_lock_* below —
    #                          this is what enforces "only one grid_bot.py
    #                          process is ever the active trading instance for
    #                          this DB at a time", and doubles as "who do I ask
    #                          for a handoff" (whoever currently holds it).
    #   "bg_handoff_request" → str(int)   written by the incoming process to trigger
    #                                     the live process's freeze+export
    #   "bg_handoff_json"    → JSON str   written by the live process after freeze;
    #                                     read (but NOT deleted yet — see
    #                                     GridBot._preregister_handoff_orders) by the
    #                                     incoming process; only deleted once
    #                                     GridBot._apply_handoff_restore() finishes
    #                                     applying it to a fully-built grid. Kept
    #                                     around that long so a process that crashes
    #                                     between reading it and finishing the restore
    #                                     can resume from where it left off on
    #                                     restart, instead of losing the snapshot and
    #                                     falling back to a destructive cold start
    #                                     (cancel-all-orders + liquidate).
    #
    # NOTE: there is deliberately no separate "blue" vs "green" PID slot. Whichever
    # process currently holds bg_lock — regardless of which --role it was launched
    # with — is the one a deploy hands off from. This means every deploy after the
    # very first one can use the exact same `--role green` invocation; no manual
    # "become blue" relabeling step is ever required.

    _BG_HANDOFF_REQUEST = "bg_handoff_request"
    _BG_HANDOFF_JSON    = "bg_handoff_json"

    # Heartbeat interval the lock holder refreshes on (GridBot._start_lock_heartbeat)
    # and how long a missed heartbeat is tolerated before a later process may treat
    # the lock as abandoned. Kept at roughly a 4x margin (rather than e.g. a single
    # heartbeat period) so one slow SQLite write, GC pause, or WAL checkpoint stall
    # doesn't look like a crash and trigger a false-positive takeover while the
    # actual holder is still very much alive — that failure mode (two processes
    # both believing they own the same orders) is worse than waiting a few extra
    # seconds to reclaim a genuinely-dead holder's lock.
    BG_LOCK_HEARTBEAT_S   = 5.0
    BG_LOCK_STALE_AFTER_S = 20.0

    def bg_lock_try_acquire(self, pid: int) -> Optional[int]:
        """
        Atomic compare-and-swap: acquires the single-instance lock iff it is
        currently unheld OR held-but-stale (its holder hasn't heartbeated
        within BG_LOCK_STALE_AFTER_S seconds — almost certainly crashed or
        hung, rather than just being a legitimately slow warmup, which
        refreshes the heartbeat throughout). This single UPDATE...WHERE is
        what actually enforces mutual exclusion: SQLite serialises writers
        against the same DB file across processes, so two processes racing
        to acquire at the same instant can never both succeed — exactly one
        UPDATE's WHERE clause matches, the other's rowcount is 0.

        The same UPDATE also atomically increments log_gen. Since this is
        the single choke point every process passes through to become the
        live holder — cold start, handoff winner, or crash-recovery resume,
        regardless of --role (see _request_and_await_handoff) — log_gen
        ends up a strictly-increasing, gap-free "instance number" that's
        safe to use for per-instance log file naming without any separate
        coordination.

        Returns the new log_gen iff THIS call acquired the lock, else None.
        (0 is never a valid return — log_gen starts at 0 and this always
        increments before returning, so `is None` is the correct check, not
        falsiness, though callers so far only ever check truthiness of a
        result that's always >= 1 on success.)
        """
        now = time.time()
        stale_cutoff = now - self.BG_LOCK_STALE_AFTER_S
        with self._lock:
            cur = self._conn.execute(
                "UPDATE bg_lock SET holder_pid=?, updated_at=?, log_gen=log_gen+1 "
                "WHERE id=1 AND (holder_pid IS NULL OR updated_at < ?)",
                (pid, now, stale_cutoff),
            )
            if cur.rowcount != 1:
                self._conn.commit()
                return None
            row = self._conn.execute(
                "SELECT log_gen FROM bg_lock WHERE id=1"
            ).fetchone()
            self._conn.commit()
            return int(row["log_gen"])

    def bg_lock_heartbeat(self, pid: int) -> bool:
        """
        Refresh the lock's lease. Called periodically by the current holder
        (GridBot._start_lock_heartbeat) for as long as it's running. Only
        succeeds if we're still the recorded holder — should always be true
        given bg_lock_try_acquire's CAS is the only way to become the holder,
        but cheap to verify rather than assume.
        """
        with self._lock:
            cur = self._conn.execute(
                "UPDATE bg_lock SET updated_at=? WHERE id=1 AND holder_pid=?",
                (time.time(), pid),
            )
            self._conn.commit()
            return cur.rowcount == 1

    def bg_lock_release(self, pid: int) -> None:
        """
        Clean release — called on a normal stop() and as part of
        export_handoff_snapshot() (that's the actual hand-off moment: the
        successor's pending bg_lock_try_acquire() only succeeds once this
        runs). Only clears the lock if we're still the recorded holder, so a
        delayed/duplicate release from a dying process can't clobber a lock
        a newer holder has since legitimately acquired.
        """
        with self._lock:
            self._conn.execute(
                "UPDATE bg_lock SET holder_pid=NULL, updated_at=0 "
                "WHERE id=1 AND holder_pid=?",
                (pid,),
            )
            self._conn.commit()

    def bg_lock_current_holder(self) -> Optional[int]:
        """
        Returns the current holder's PID if the lock is held AND fresh
        (heartbeated within BG_LOCK_STALE_AFTER_S), else None. A stale
        holder is treated identically to "unheld" — there's no live peer to
        request a handoff from — even though the row technically still
        names a PID; bg_lock_try_acquire applies the same staleness rule so
        the two stay consistent with each other.
        """
        stale_cutoff = time.time() - self.BG_LOCK_STALE_AFTER_S
        with self._lock:
            row = self._conn.execute(
                "SELECT holder_pid, updated_at FROM bg_lock WHERE id=1"
            ).fetchone()
        if row and row["holder_pid"] is not None and row["updated_at"] >= stale_cutoff:
            return row["holder_pid"]
        return None

    def bg_request_handoff(self, green_pid: int) -> None:
        """Green writes this to ask blue to freeze and export."""
        self.set_meta(self._BG_HANDOFF_REQUEST, str(green_pid))

    def bg_poll_handoff_request(self) -> Optional[int]:
        """Blue polls this; returns green_pid if a request is pending, else None."""
        val = self.get_meta(self._BG_HANDOFF_REQUEST)
        return int(val) if val else None

    def bg_clear_handoff_request(self) -> None:
        self.delete_meta(self._BG_HANDOFF_REQUEST)

    def bg_write_handoff_json(self, payload: str) -> None:
        self.set_meta(self._BG_HANDOFF_JSON, payload)

    def bg_read_handoff_json(self) -> Optional[str]:
        return self.get_meta(self._BG_HANDOFF_JSON)

    def bg_clear_handoff_json(self) -> None:
        self.delete_meta(self._BG_HANDOFF_JSON)

    # ── Lifecycle ─────────────────────────────────────────────────────────────

    # ── Reset (fresh-start) ───────────────────────────────────────────────────

    def reset_state(self, backup: bool = True) -> Optional[str]:
        """
        Wipe all persisted fills, daily PnL, meta rows, and open legs so the
        bot behaves as if this is the very first startup.

        Does NOT touch anything on the exchange — open orders/positions are
        always independently handled by OMS.reconcile_on_startup() on every
        launch, reset or not, so a stale live position is still detected and
        liquidated exactly as before.

        open_legs is included in the wipe (2026-08-03). Originally needed
        because _liquidate_position() left the individual rows that made up
        a liquidated net position behind (no close_leg() call), so this
        wipe was the only thing preventing them resurrecting as a phantom
        position on the next launch. 2026-08-05: _liquidate_position() now
        closes those rows itself right after its fill confirms (see that
        method), which also covers the auto-restart path this wipe never
        reached (auto-restart rebuilds in place / on a fresh process
        without going through reset_state() at all — see the 2026-08-05
        stop-loss incident that exposed exactly that gap). Keeping this
        wipe anyway as a belt-and-suspenders clean slate for an explicit
        reset, not because it's still the only protection.

        If backup=True (default), the WAL is checkpointed and the db file is
        copied to "<db_path>.bak-<timestamp>" before anything is wiped, so
        pre-reset history (including the pre-reset open_legs rows) is never
        silently lost.

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
            self._conn.execute("DELETE FROM open_legs")
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
            "[GridStateStore] STATE RESET — all fills, daily PnL, "
            "accumulated PnL, and open legs cleared. " +
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
    # STATUS_INTERVAL_S is now read from config at runtime
    # (see _run loop); the class constant below is the fallback.
    STATUS_INTERVAL_S     = 900.0   # 15 min default (was 60s)
    RETUNE_CHECK_INTERVAL = 300.0
    CANDLE_SAVE_INTERVAL_S = 300.0   # snapshot PriceCache history to DB every 5 min

    def __init__(self, config: dict, reset_state: bool = False, role: str = ""):
        self._cfg         = config
        self._role        = role             # "blue", "green", or "" (standalone)
        # Set by _try_acquire_bg_lock() once this process actually becomes
        # the live holder (via any of the three paths — cold start, handoff
        # winner, or crash-recovery resume). None until then. Used by
        # start() to re-init logging into a file unique to this instance.
        self._log_gen: Optional[int] = None
        self._stop_event  = threading.Event()
        self._engine:     Optional[GridEngine]    = None
        self._params:     Optional[GridParams]    = None
        self._sl_guard:   Optional[StopLossGuard] = None
        self._alerter:    AlertManager            = AlertManager(config)
        global _grid_bot_alerter
        _grid_bot_alerter = self._alerter
        self._last_tune:  float = 0.0
        self._last_status:float = 0.0
        self._last_funding_fetch: float = 0.0   # ts of last funding rate fetch
        self._last_funding_rate:  float = 0.0   # most recent rate (decimal, e.g. 0.0001)
        self._funding_accrued_usd: float = 0.0  # in-memory accumulator (lost on restart;
                                                 #  DB meta "funding_accrued_usd" persists)
        self._last_retune_check: float = 0.0
        self._last_candle_save:  float = 0.0
        self._halted:     bool  = False
        self._daily_loss_halted: bool = False  # True when daily loss circuit breaker fired
        self._halt_time:  float = 0.0       # timestamp of the last halt
        # Blue-green deployment state
        # _handoff_freeze lives on GridEngine (set there by export_handoff_snapshot)
        self._handoff_stop:   bool = False   # True → stop() skips position liquidation
        # Set by _start_handoff_watcher()'s background thread when a peer requests a
        # handoff; observed and acted on by _run() (main thread) — see that thread's
        # docstring for why the watcher itself never calls self.stop() directly.
        self._handoff_shutdown_requested = threading.Event()
        # Parsed handoff snapshot (dict) once a peer has handed off to us and its
        # orders have been pre-registered with the OMS, cleared once
        # _apply_handoff_restore() has applied it to the freshly built grid.
        self._pending_handoff_snapshot: Optional[dict] = None
        # Signals the bg_lock heartbeat thread (_start_lock_heartbeat) to stop;
        # set in stop().
        self._lock_heartbeat_stop_event = threading.Event()
        self._halt_stop_price: float = 0.0  # stop_price that triggered the halt
        self._restart_attempts: int = 0     # number of auto-restart attempts made
        self._last_restart_time: float = 0.0  # timestamp of the last successful auto-restart
                                               # (0.0 = no auto-restart has happened yet)
        self._recovery_floor_timeout_alerted = False  # one-shot alert flag for the hard
                                                       # halt timeout (auto_restart_max_halt_hours),
                                                       # reset on every new halt

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

        # ── reconcile_open_legs: confirm-dwell state ──────────────────────────
        # {leg_id: first-flagged-ts} for legs currently outside the rebuilt
        # grid's trend-risk buffer. Lives here (not on GridEngine/OpenLeg)
        # because a new GridEngine — and a fresh set of OpenLeg objects — is
        # reseeded from DB on every single rebuild; only GridBot survives
        # across rebuilds to accumulate real wall-clock dwell time. Mirrors
        # _pending_raise_candidate/_pending_raise_since above.
        self._leg_no_fit_since: Dict[int, float] = {}
        # Same idea, separate map, for the zero-candidate case (no cell to
        # rest a closer on at all — see reconcile_open_legs). Kept apart
        # from _leg_no_fit_since since the two dwell caps and the
        # first-time-seen bookkeeping (kick off a chase) differ.
        self._leg_zero_candidate_since: Dict[int, float] = {}

        # ── Confirmed-trend catch-up + SellGate state (2026-08-04) ────────────
        # Timestamps of top-sell-triggered up-shifts, pruned to
        # drift_shift_trend_lookback_s. Lives here (not on GridEngine) for
        # the same reason _leg_no_fit_since does: a full rebuild replaces
        # the engine instance entirely, and this evidence needs to survive
        # that. Written by _trend_confirm (called from GridEngine right
        # before a drift-shift), read by _uptrend_confirmed_now (used by
        # both _sell_gate and its release condition in _run()).
        self._recent_up_shifts: List[float] = []

        # ── Trail-flip downtrend evidence (2026-08-05) ─────────────────────────
        # Mirror of _recent_up_shifts above, for TRAIL DOWN. Written by
        # _record_down_shift (called from GridEngine._trail_down every time
        # it actually fires — there is no fill-triggered drift-shift path
        # on the bottom-buy side to hook this into instead, unlike the
        # top-sell side), read by _downtrend_confirmed_now (used by
        # GridEngine's trail-flip decision in _trail_down). Same
        # lookback/confirm-count knobs as the uptrend side
        # (drift_shift_trend_lookback_s / drift_shift_trend_confirm_count)
        # — this is the same underlying idea (repeated same-direction
        # shifts in a short window = a real, persistent move, not noise),
        # just tracked independently since it has no shared state with the
        # uptrend catch-up feature.
        self._recent_down_shifts: List[float] = []

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
        # Most recent regime that was NOT INSUFFICIENT_DATA. _buy_gate() falls
        # back to this while the live regime is INSUFFICIENT_DATA, instead of
        # treating "unknown" as "safe to buy" — see _buy_gate()'s trend-gate
        # block for why. Starts at NEUTRAL (no confirmed regime yet at a true
        # cold start, so no special protection is assumed until one commits).
        self._last_confirmed_trend_regime: str = TrendSignal.REGIME_NEUTRAL
        self._last_trend_log:    float = 0.0   # ts of last trend log line
        self._last_trend_slope_pct: float = 0.0  # cached for compute_trend_risk(),
                                                  # refreshed every _evaluate_trend() call

        # ── SQLite persistence ────────────────────────────────────────────────────
        # Opened once here and shared with every GridEngine instance so that
        # fills survive restarts, re-tunes, and stop-loss rebuilds.
        self._store = GridStateStore(config.get("db_path", "grid_bot.db"))
        self._spacing_tuner = SpacingAutoTuner(config, self._store, self._alerter)

        if reset_state:
            # Fresh-start requested via --reset-state: wipe fill history, daily
            # PnL, accumulated PnL, and open legs so /status reports as if this
            # is the very first launch and GridEngine.__init__ (below, via
            # _rebuild_grid()) seeds an empty _open_legs instead of resurrecting
            # whatever a previous process's net-position liquidation left
            # behind in the ledger. A pre-reset backup of the db is kept on
            # disk. Note: this only clears local bookkeeping — it does NOT
            # touch the exchange. Any real open orders/position are still
            # independently detected and liquidated by
            # OMS.reconcile_on_startup() below, same as on every normal launch.
            backup_path = self._store.reset_state()
            note = (f" (backup: {os.path.abspath(backup_path)})"
                    if backup_path else " (no backup — see log)")
            logger.warning(f"[GridBot] --reset-state: persisted PnL/fill history/open legs cleared{note}")
            self._alerter.send_sync(
                f"🧹 State reset requested — fill history, daily PnL, "
                f"accumulated PnL, and open legs cleared{note}.\nBot starting fresh."
            )

        # ── Telegram command poller ────────────────────────────────────────────
        self._cmd_poller = TelegramCommandPoller(
            token           = config.get("telegram_bot_token", ""),
            allowed_chat_id = config.get("telegram_chat_id",   ""),
        )
        self._cmd_poller.register("/status",  self._handle_status_command)
        self._cmd_poller.register("/handoff", self._handle_handoff_command)
        self._cmd_poller.register("/help",    self._handle_help_command)
        self._cmd_poller.register("/pnl",     self._handle_pnl_command)
        self._cmd_poller.register("/clear_halt", self._handle_clear_halt_command)

        # WS market feed
        self._ws_stop = threading.Event()
        # Rolling deque of reconnect timestamps for flood detection.
        # Kept on GridBot (not _ReconnectingWS) so alerting and config live in one place.
        self._ws_reconnect_times: collections.deque = collections.deque()
        # Consecutive-failure outage tracking (see _on_ws_error / _on_ws_reconnect).
        self._ws_consecutive_errors = 0
        self._ws_outage_alerted     = False
        self._ws_outage_since: Optional[float] = None

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
            on_error_fn      = self._on_ws_error,
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

        # A successful reconnect means the outage (if any) is over. If we'd
        # previously sent a "can't connect" alert (see _on_ws_error), send a
        # recovery message and reset the consecutive-failure streak.
        if self._ws_outage_alerted:
            outage_s = now - (self._ws_outage_since or now)
            logger.info(f"[MarketWS] Reconnected after ~{outage_s:.0f}s outage")
            self._alerter.send(
                f"🟢 MarketWS reconnected after ~{int(outage_s)}s outage "
                f"({self._ws_consecutive_errors} failed attempts)."
            )
        self._ws_consecutive_errors = 0
        self._ws_outage_alerted     = False
        self._ws_outage_since       = None

    def _on_ws_error(self, error: str) -> None:
        """
        Called by _ReconnectingWS on EVERY failed connection attempt (e.g. a
        handshake 403/other error before on_open ever fires) — unlike
        _on_ws_reconnect, which only fires on a successful reconnect.

        This exists specifically for the total-block case: if the WS never
        successfully reconnects at all (e.g. a Cloudflare/IP block on the
        handshake, as opposed to a flaky connection that keeps recovering),
        _on_ws_reconnect never fires, so the existing reconnect-flood alert
        would stay silent no matter how long the outage lasts. This tracks a
        rolling count of consecutive failures (reset on the next successful
        reconnect) and fires a single Telegram alert once that streak crosses
        ws_error_alert_count, then stays quiet until either a reconnect
        succeeds (see the recovery alert in _on_ws_reconnect) or the process
        restarts.
        """
        self._ws_consecutive_errors += 1
        threshold = self._cfg.get("ws_error_alert_count", 5)

        if self._ws_outage_since is None:
            self._ws_outage_since = time.time()

        logger.info(
            f"[MarketWS] Consecutive connect failures: "
            f"{self._ws_consecutive_errors} (alert threshold={threshold})"
        )

        if self._ws_consecutive_errors == threshold and not self._ws_outage_alerted:
            self._ws_outage_alerted = True
            logger.warning(
                f"[MarketWS] {self._ws_consecutive_errors} consecutive connect "
                f"failures — sending outage alert. Last error: {error}"
            )
            self._alerter.send(
                f"🔴 MarketWS can't connect: {self._ws_consecutive_errors} "
                f"failed attempts in a row.\n"
                f"Last error: {error}\n"
                f"No live price feed — grid is running blind until this clears."
            )

    # ── Blue-green: green-side handoff orchestration ──────────────────────────

    def _request_and_await_handoff(self) -> bool:
        """
        Called by green during start(), before reconcile_on_startup().

        Step 0 (crash recovery): check for a handoff snapshot already sitting
        in SQLite before looking for a live peer at all. Under normal
        operation there shouldn't be one — export_handoff_snapshot() and
        _preregister_handoff_orders() leave a snapshot present in exactly one
        situation: a previous process read it and started registering orders
        with its OMS, but crashed before _apply_handoff_restore() finished
        applying it to a built grid (see GridStateStore's bg_handoff_json
        docs for why the clear is deferred that long). If we find one, try
        to acquire the single-instance lock (bg_lock — see GridStateStore):
          - Acquired → whoever last touched this snapshot is gone (the lock
            was unheld, or held but stale past BG_LOCK_STALE_AFTER_S) — safe
            to resume. Register it directly via _preregister_handoff_orders,
            no peer needed, there isn't one.
          - Not acquired → another process holds a FRESH lock, i.e. is
            genuinely still working (most likely on this very snapshot,
            just deep in a slow warmup — not dead). reconcile_on_startup()
            cancels every open order for the instrument directly on the
            exchange, account-wide, so barging in here would rip out orders
            that still-live process owns. Refuse to start instead.

        Normal path (no snapshot already present):
        1. Find the current lock holder (bg_lock_current_holder — returns
           None if unheld or stale, treated the same as "no live peer").
           If None → no live peer, proceed with normal cold start (return False).
        2. Write bg_handoff_request to ask that peer to freeze + export.
        3. Poll bg_handoff_json every 1s until it appears or timeout fires.
        4. As soon as the snapshot appears, ATOMICALLY acquire bg_lock — this
           IS the hand-off moment (export_handoff_snapshot() released it
           right after writing the JSON, so this is a race exactly one
           incoming process can win; see the retry note below for why it
           isn't a hair-trigger single attempt). If we lose that race,
           another process already claimed it first — refuse to start
           rather than falling back to a cold start, which would cancel the
           winner's inherited orders out from under it.
        5. Register the snapshot's open orders with this process's OMS
           (_preregister_handoff_orders) — done here, not deferred to after
           grid rebuild/warmup, specifically to close the window during
           which this process's WS is live but doesn't yet know about
           inherited orders. See that method's docstring for why this matters.

        Returns True if a snapshot was found, the lock acquired, and orders
        registered (the grid rebuild step will later match it against this
        process's own grid via _match_handoff_levels()/_apply_handoff_restore()).
        Returns False if no peer/snapshot ever showed up at all (fall back to
        cold start) — as opposed to losing a race for one that did, which
        exits the process outright rather than returning False.
        """
        existing_payload = self._store.bg_read_handoff_json()
        if existing_payload:
            if not self._try_acquire_bg_lock():
                logger.error(
                    f"[GridBot] An un-applied handoff snapshot is present, "
                    f"but the instance lock is already held by another "
                    f"still-live process — refusing to start alongside it. "
                    f"Starting here would cancel every open order for "
                    f"{INSTRUMENT} on the exchange out from under it."
                )
                sys.exit(1)
            logger.warning(
                "[GridBot] Found an un-applied handoff snapshot left behind "
                "by a previous process (its lock had expired) — resuming it "
                "instead of starting a fresh handoff request."
            )
            return self._preregister_handoff_orders()

        live_pid = self._store.bg_lock_current_holder()
        if live_pid is None:
            logger.info("[GridBot] No live peer found — cold start")
            return False

        logger.info(f"[GridBot] Found live peer pid={live_pid} — requesting handoff")
        self._store.bg_request_handoff(os.getpid())

        timeout_s = self._cfg.get("bg_handoff_timeout_s", 10)
        deadline  = time.time() + timeout_s
        while time.time() < deadline:
            payload = self._store.bg_read_handoff_json()
            if payload:
                logger.info(f"[GridBot] Handoff JSON received ({len(payload)} bytes)")
                if not self._try_acquire_bg_lock():
                    holder = self._store.bg_lock_current_holder()
                    logger.error(
                        f"[GridBot] Handoff snapshot received, but pid={holder} "
                        f"already claimed the instance lock first — refusing "
                        f"to start (lost the race for this hand-off)."
                    )
                    sys.exit(1)
                return self._preregister_handoff_orders()
            time.sleep(1.0)

        logger.warning(
            f"[GridBot] Handoff timeout after {timeout_s}s "
            f"(peer pid={live_pid} may have already exited) — falling back to cold start"
        )
        self._store.bg_clear_handoff_request()
        return False

    def _try_acquire_bg_lock(self) -> bool:
        """
        Attempt to acquire the single-instance lock, with a short bounded
        retry (a few attempts a fraction of a second apart) rather than a
        single hair-trigger try. This isn't the primary correctness
        mechanism — bg_lock_try_acquire's CAS is — it just smooths over two
        benign sources of transient contention: (a) export_handoff_snapshot()
        writes the JSON and releases the lock as two separate SQLite writes
        a few microseconds apart, so a poll landing in that gap would
        otherwise see the JSON but find the lock still (momentarily) held by
        the process that's exiting; (b) ordinary SQLite write contention
        under WAL mode when multiple processes touch the DB at nearly the
        same instant. A real "someone else is genuinely running" case fails
        every attempt, since that holder's lease keeps refreshing.

        On success, stashes the acquisition's log_gen on self._log_gen —
        see GridBot.start() for where that's used to re-init logging into
        this instance's own file, once it's known which of the three
        acquisition paths (cold start / handoff winner / crash-recovery
        resume) got here.
        """
        for attempt in range(5):
            gen = self._store.bg_lock_try_acquire(os.getpid())
            if gen is not None:
                self._log_gen = gen
                return True
            if attempt < 4:
                time.sleep(0.1)
        return False

    def _preregister_handoff_orders(self) -> bool:
        """
        Read the handoff JSON from SQLite and immediately register every open
        order it contains with this process's OMS — BEFORE the grid is
        rebuilt or warmup even begins.

        By the time this runs, the instance lock has already been acquired
        by the caller (_request_and_await_handoff) — that's the actual
        hand-off/takeover moment; this method only handles OMS bookkeeping.

        This runs as early as possible (right after _request_and_await_handoff
        finds the snapshot, itself right after self._oms.start() brought the
        live order-update WS up) specifically to minimise the window during
        which this process's WS is listening for fills but doesn't yet know
        which exchange_ids belong to inherited orders. Before this fix, that
        registration only happened after full Phase 1/2 warmup + grid
        rebuild — a gap that's normally ~10-15s but can be tens of minutes if
        the ATR REST seed fails and Phase 2 falls back to live candle
        accumulation. Any fill on an inherited order during that gap was
        silently dropped. Registering here instead bounds the gap to the
        handoff round-trip itself (typically low single-digit seconds) — that
        residual window can't be eliminated entirely, since this process
        cannot know a peer's order IDs before the peer hands them off, but it
        is now bounded by handoff latency rather than by warmup duration. Any
        WS order-update that still slips through this smaller window is at
        least logged now (see OMS._handle_order_update) instead of being
        dropped silently.

        IMPORTANT: this does NOT clear bg_handoff_json — that's deferred to
        GridBot._apply_handoff_restore(), once the snapshot has actually been
        applied to a fully-built grid, not merely read. If this process
        crashes anywhere between here and then, the snapshot is still sitting
        in SQLite (and the lock will go stale) for _request_and_await_handoff()'s
        Step 0 to resume on restart, rather than being gone for good and
        forcing a destructive cold start.

        The actual GridLevel state (matching index/price against whatever
        grid this process ends up computing) is applied later, from
        _rebuild_grid(), via _match_handoff_levels()/_apply_handoff_restore()
        — that part necessarily has to wait until this process's own grid
        parameters exist.

        Returns True if a valid snapshot was found and registered, False
        otherwise (fall back to a normal cold start).
        """
        import json as _json

        payload = self._store.bg_read_handoff_json()
        if not payload:
            return False

        try:
            snap = _json.loads(payload)
        except Exception as e:
            logger.error(f"[GridBot] handoff snapshot: JSON parse error: {e}")
            # Corrupt beyond recovery — clear it (and release the lock we
            # just took to look at it) so it doesn't linger forever
            # re-failing this same check on every future restart.
            self._store.bg_clear_handoff_json()
            self._store.bg_lock_release(os.getpid())
            return False

        schema = snap.get("schema")
        if schema not in (2, 3):
            logger.warning(
                f"[GridBot] handoff snapshot: unknown schema {schema}"
            )
            self._store.bg_clear_handoff_json()
            self._store.bg_lock_release(os.getpid())
            return False

        # ── Schema 3: inject inherited price ticks and BuyGate scores early ──
        # Do this before OMS registration and before warmup begins, so that:
        #
        # (a) compute_stability(5) returns ok=True at the very first AutoTuner
        #     retune after handoff — the range guard is immediately available,
        #     preventing the cold-cache SL that occurred on 2026-07-16 14:37:
        #     the new process built a tight grid (stop=64588) only 214pts below
        #     mid because compute_stability(5) returned ok=False (< 10 ticks in
        #     the 5-min window) so the range guard was silently skipped.
        #
        # (b) _score_history has continuity across handoffs — if a SL occurs
        #     shortly after the handoff, _calibrate_threshold() sees the
        #     pre-handoff BuyGate scores and can auto-adjust the threshold.
        if schema == 3:
            price_ticks  = snap.get("price_ticks",  [])
            score_history = snap.get("score_history", [])

            if price_ticks:
                # Merge inherited ticks into _price_cache._history.  The deque
                # may already contain a few ticks from Phase 1 (the 10s warmup
                # wait).  Sort chronologically; the deque's maxlen cap applies.
                inherited = [(float(ts), float(mid)) for ts, mid in price_ticks]
                with _price_cache._lock:
                    existing = list(_price_cache._history)
                    merged   = inherited + existing
                    merged.sort(key=lambda x: x[0])
                    _price_cache._history.clear()
                    cap = _price_cache._history.maxlen or len(merged)
                    for item in merged[-cap:]:
                        _price_cache._history.append(item)
                logger.info(
                    f"[GridBot] Handoff: injected {len(inherited)} price ticks "
                    f"from peer — compute_stability(5) now has "
                    f"{len([t for t in merged if t[0] >= time.time()-300])} "
                    f"ticks in last 5min"
                )

            if score_history:
                # Prepend inherited scores to _score_history.  The pruning in
                # _buy_gate() will clean up stale entries on the next BuyGate call.
                inherited_scores = [(float(ts), float(sc)) for ts, sc in score_history]
                self._score_history = inherited_scores + self._score_history
                logger.info(
                    f"[GridBot] Handoff: inherited {len(inherited_scores)} "
                    f"BuyGate scores from peer"
                )

        n_registered = 0
        for snap_lv in snap.get("levels", []):
            state_str = snap_lv.get("state", "IDLE")
            if state_str not in ("BUY_OPEN", "SELL_OPEN"):
                continue
            exchange_id = snap_lv.get("exchange_id", "")
            client_oid  = snap_lv.get("client_oid", "")
            if not (exchange_id and client_oid):
                continue
            req = OrderRequest.limit_maker(
                side       = "BUY" if state_str == "BUY_OPEN" else "SELL",
                qty        = snap_lv["qty"],
                price      = snap_lv["price"],
                instrument = INSTRUMENT,
                purpose    = ("grid_buy" if state_str == "BUY_OPEN" else "grid_sell"),
            )
            req.client_oid = client_oid
            self._oms.restore_order(client_oid, exchange_id, req)
            n_registered += 1

        self._pending_handoff_snapshot = snap

        logger.info(
            f"[GridBot] Handoff snapshot registered: long_qty="
            f"{snap.get('long_qty', 0.0):.4f} open_orders_registered={n_registered} "
            f"(will be matched against this process's own grid once rebuilt; "
            f"snapshot stays in SQLite until then in case of a crash)"
        )
        return True

    # ── Blue-green: instance lock heartbeat ──────────────────────────────────

    def _start_lock_heartbeat(self) -> None:
        """
        Launched once this process holds bg_lock (either via a successful
        handoff hand-off in _request_and_await_handoff, or a fresh
        acquisition for a cold start in start()). Refreshes the lease every
        GridStateStore.BG_LOCK_HEARTBEAT_S seconds for as long as this
        process runs, so a later process's staleness check
        (bg_lock_try_acquire / bg_lock_current_holder) never mistakes a
        merely-slow-but-alive process for a crashed one.
        """
        interval = self._store.BG_LOCK_HEARTBEAT_S
        pid = os.getpid()

        def _heartbeat():
            while not self._lock_heartbeat_stop_event.wait(interval):
                if not self._store.bg_lock_heartbeat(pid):
                    # We're no longer the recorded holder — shouldn't happen
                    # given bg_lock_try_acquire's CAS is the only way to
                    # become holder, but if it ever does, another process
                    # may now believe it owns the same orders. Surface this
                    # loudly rather than silently continuing to trade.
                    logger.critical(
                        "[GridBot] Lost the instance lock while still "
                        "running — another process may now be handling the "
                        "same account. Stopping immediately."
                    )
                    self._handoff_shutdown_requested.set()
                    return

        t = threading.Thread(target=_heartbeat, name="BG-LockHeartbeat", daemon=True)
        t.start()

    # ── Blue-green: live-side handoff watcher ────────────────────────────────

    def _start_handoff_watcher(self) -> None:
        """
        Launched as a daemon thread once this process finishes startup
        (whether it was launched with --role blue or --role green — see
        GridStateStore's bg_lock docs for why both participate the same
        way). Polls SQLite every 1s for a handoff request from a successor.
        When found, calls export_handoff_snapshot() then asks the MAIN thread
        to perform the actual stop.

        IMPORTANT: this thread does NOT call self.stop() directly. GridBot.stop()
        mutates self._engine/self._oms state with no top-level lock, and the
        main thread's _run() loop could be mid-iteration touching that same
        state — calling stop() from a second OS thread concurrently would be
        a genuine race (unlike the SIGINT/SIGTERM path, which pre-empts the
        main thread itself rather than running on a separate thread). Instead
        this just sets a flag; _run() observes it on the main thread and
        calls self.stop() itself once it's safe to do so.
        """
        def _watch():
            while not self._stop_event.is_set():
                if self._handoff_shutdown_requested.is_set():
                    return  # already requested by an earlier iteration
                peer_pid = self._store.bg_poll_handoff_request()
                if peer_pid is not None:
                    logger.info(
                        f"[GridBot] Handoff request from pid={peer_pid} "
                        f"— exporting snapshot"
                    )
                    try:
                        ok = self.export_handoff_snapshot()
                    except Exception as e:
                        logger.error(f"[GridBot] Handoff export failed: {e}")
                        ok = False
                    if ok:
                        logger.info(
                            "[GridBot] Snapshot written — requesting main-thread shutdown"
                        )
                    else:
                        logger.warning(
                            "[GridBot] Snapshot failed — requesting main-thread shutdown anyway"
                        )
                    # _handoff_stop is already True if ok (set inside
                    # export_handoff_snapshot); _run() will see this flag,
                    # call self.stop() itself, and stop() will skip
                    # liquidation because _handoff_stop is True.
                    self._handoff_shutdown_requested.set()
                    return
                time.sleep(1.0)

        t = threading.Thread(target=_watch, name="BG-HandoffWatcher", daemon=True)
        t.start()
        logger.info("[GridBot] Handoff watcher started")

    # ── Lifecycle ─────────────────────────────────────────────────────────────

    def start(self):
        global logger
        logger.info(f"[GridBot] Starting (role={self._role or 'standalone'})")
        self._oms.start()
        self._cmd_poller.start()   # start Telegram command polling early so /status works during warmup

        # ── Blue-green: register PID ──────────────────────────────────────────
        # Green (or any --role process): request handoff from whoever is
        # currently registered as the live process, and wait for its snapshot.
        # Note this deliberately runs immediately after self._oms.start() (i.e.
        # as soon as our own order-update WS is live), and pre-registers the
        # snapshot's orders with the OMS the moment it arrives — see
        # _request_and_await_handoff()/_preregister_handoff_orders() for why
        # that timing matters.
        _handoff_loaded = False
        if self._role == "green":
            _handoff_loaded = self._request_and_await_handoff()

        # ── Startup reconciliation (skipped if handoff loaded) ────────────────
        # Normal path: cancel all open orders, then fetch position.
        # Handoff path: skip cancel-all — green inherits the peer's orders directly.
        if not _handoff_loaded:
            # Acquire the single-instance lock before doing anything that
            # touches the exchange. This is the same bg_lock a handoff
            # hand-off acquires — see _request_and_await_handoff — just
            # taken here instead because there's no peer to hand off from
            # (first-ever launch, or no live peer responded in time). Covers
            # the case of two cold starts racing against the same DB (e.g.
            # a mistaken double-launch), not just the blue-green path.
            if not self._try_acquire_bg_lock():
                holder = self._store.bg_lock_current_holder()
                logger.error(
                    f"[GridBot] Could not acquire the instance lock "
                    f"(already held by pid={holder}) — refusing to start. "
                    f"Another grid_bot.py process is already running "
                    f"against this DB."
                )
                sys.exit(1)

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

        # From here on, regardless of path, this process holds bg_lock —
        # start heartbeating it so a legitimately slow warmup (e.g. the ATR
        # REST-seed-failure fallback, which can take tens of minutes) doesn't
        # get mistaken for a crash by some future process's staleness check.
        self._start_lock_heartbeat()

        # Re-init logging now that log_gen is known, so this instance writes
        # to its own file (grid_bot_gen{N}_*.log) instead of whatever file
        # main()'s earlier role-only _init_logging call picked. _init_logging
        # is idempotent (see its docstring) — safe to call again here; the
        # previous listener/handler pair is torn down first. This is what
        # actually fixes blue/green log commingling: log_gen is unique per
        # live instance regardless of --role (see bg_lock_try_acquire), so
        # unlike naming purely off --role, no two instances — however many
        # deploys happen in a day — ever share a file.
        if self._log_gen is not None:
            logger = _init_logging(self._cfg, role=self._role, log_gen=self._log_gen)
            logger.info(
                f"[GridBot] Logging into generation {self._log_gen} "
                f"(role={self._role or 'standalone'}, pid={os.getpid()})"
            )

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

        # ── Phase 2c: seed recent range ticks for compute_stability warm-up ───
        # Guarantees compute_stability(5) returns ok=True at the first
        # _rebuild_grid so the ATR range guard fires immediately on cold start.
        # Root cause of 2026-07-17 13:40 SL: DB candles were ≥5 min old →
        # 0 ticks in the 5-min window → range guard skipped → tight stop →
        # SL within 12 minutes of boot. This 15-candle REST call fills the gap.
        self._seed_recent_range_ticks()

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
        # ── Live-mode config sanity log ───────────────────────────────────
        # Log all operationally critical config values on every startup so
        # the operator can verify them in the log before going live.
        _mode = self._cfg.get("trading_mode", TRADING_MODE)
        logger.info(
            f"[GridBot] Config summary "
            f"mode={_mode} "
            f"instrument={self._cfg.get('instrument')} "
            f"maker_fee={self._cfg.get('maker_fee_rate')} "
            f"notional/level={self._cfg.get('notional_per_level')} "
            f"stop_buf={self._cfg.get('stop_buffer_atr')}xATR "
            f"atr_floor={self._cfg.get('min_atr_floor_pts')}pts "
            f"daily_loss_limit={self._cfg.get('daily_loss_limit_usd')} USD "
            f"rest={self._cfg.get('rest_base_url')} "
            f"ws_market={self._cfg.get('ws_market_url')}"
        )
        # Restore persisted funding accrual from previous session
        self._funding_accrued_usd = self._get_funding_accrued()
        if self._funding_accrued_usd != 0.0:
            logger.info(
                f"[Funding] Restored accrued funding: "
                f"{self._funding_accrued_usd:+.4f} USD from previous sessions"
            )
        self._alerter.send(f"🟢 GridBot started — {TRADING_MODE.upper()} | {INSTRUMENT}")

        self._spacing_tuner.load_persisted()
        self._spacing_tuner.load_persisted_levels()

        # Restore the zero-candidate dwell map before anything reads it —
        # both _restore_halt_state() below (irrelevant if it returns True,
        # since no grid gets built this call) and, on the normal path,
        # _rebuild_grid()'s own bookkeeping, which needs this populated
        # BEFORE it runs its "already dwelling from a prior rebuild" check.
        # See _restore_leg_zero_candidate_since() docstring.
        self._restore_leg_zero_candidate_since()

        # Restore a halt (stop-loss cooldown/recovery-floor wait, or a
        # daily-loss circuit-breaker halt) from a previous session before
        # deciding whether to build a fresh grid. See _restore_halt_state()
        # docstring for why this matters — without it, a restart while
        # halted silently discarded the halt.
        if self._restore_halt_state():
            logger.info(
                "[GridBot] Startup: resuming in halted state — no grid "
                "built; the main loop's auto-restart checks take over "
                "from here."
            )
        else:
            self._rebuild_grid()

        # ── Any --role process: watch for the next handoff request ───────────
        # Whether we started as --role blue, or as --role green and just took
        # over via handoff (or fell back to a cold start), we already hold
        # bg_lock (acquired either in _request_and_await_handoff's hand-off
        # or just above for a cold start) and are heartbeating it — so we're
        # already the process a FUTURE `--role green` deploy will find and
        # hand off from. All that's left is to actually watch for that
        # request. Standalone (no --role) processes still take the lock for
        # safety but don't participate in handoff orchestration.
        if self._role:
            self._start_handoff_watcher()

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
        sorted in chronological order, and capped to the deque's maxlen
        (PriceCache.MAX_TICKS). Only ticks within PriceCache.HISTORY_WINDOW_S
        (27h) are loaded to match the deque's retention window.
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
            # BUGFIX (2026-07-14): this used to hardcode [-30000:], silently
            # re-imposing the old undersized cap on every restart even after
            # PriceCache.MAX_TICKS was raised. Use the deque's actual maxlen
            # so the two stay in sync by construction.
            cap = _price_cache._history.maxlen or len(merged)
            for item in merged[-cap:]:
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

    def _seed_recent_range_ticks(self) -> None:
        """
        Fetch the last 15 minutes of 1-min candles from CDC REST and inject
        them into PriceCache._history with their real wall-clock timestamps.

        Purpose
        ───────
        compute_stability(5) requires ≥10 ticks within the last 5 minutes of
        wall-clock time.  The DB candle cache is snapshotted every 5 minutes,
        so on a cold start the newest DB tick is ≥5 minutes old — outside the
        5-min window.  _seed_atr_from_rest() skips the currently-open bucket
        and typically injects ticks that are 1–2 minutes old, which may still
        fall short.

        Root cause of 2026-07-17 13:40 SL (cold-start stop-loss):
          Bot started at 13:28; _seed_atr_from_rest fetched historical candles
          but the most recent had ts ~13:27 (1 min old).  At the 13:28 rebuild
          compute_stability(5) saw 0–2 ticks in [13:23,13:28] → ok=False →
          range guard skipped → effective_atr=raw_atr → tight stop → SL.

        This method fills that gap: 15 candles of 1-min OHLC cover the last
        15 minutes, producing 60 synthetic ticks of which at least 20 fall
        within the 5-min window (4 ticks × 5 candles).  After injection,
        compute_stability(5) reliably returns ok=True at the very first build.

        Implementation
        ──────────────
        Uses the same REST endpoint and injection strategy as _seed_atr_from_rest.
        The 15-candle fetch is a separate, lightweight call (not merged into the
        main seed) because:
          1. It must run AFTER _seed_atr_from_rest so we don't overwrite the
             carefully-merged historical data.
          2. It targets a different goal (stability window freshness vs ATR
             depth) with a different candle count and timestamp handling.
          3. Keeping it separate makes the two phases independently testable.
        """
        rest_base = self._cfg.get("rest_base_url",
                                   "https://api.crypto.com/exchange/v1")
        url    = f"{rest_base}/public/get-candlestick"
        params = {"instrument_name": INSTRUMENT, "timeframe": "1m", "count": 15}

        logger.info(
            "[GridBot] Phase 2c: seeding recent 5-min range ticks from REST "
            "(15 × 1-min candles for compute_stability warm-up)..."
        )
        try:
            resp = requests.get(url, params=params, timeout=10.0)
            resp.raise_for_status()
            body = resp.json()
        except Exception as exc:
            logger.warning(
                f"[GridBot] Phase 2c: REST request failed ({exc}) — "
                f"compute_stability(5) may return ok=False on first build; "
                f"range guard will skip until live ticks accumulate (~5 min)"
            )
            return

        if body.get("code", -1) != 0:
            logger.warning(
                f"[GridBot] Phase 2c: API returned code={body.get('code')} — "
                f"range guard warm-up skipped"
            )
            return

        candles = []
        if isinstance(body.get("result"), dict):
            candles = body["result"].get("data", [])

        if not candles:
            logger.warning("[GridBot] Phase 2c: empty candle list — range guard "
                           "warm-up skipped")
            return

        current_bucket = int(time.time() // 60)
        synthetic_ticks: list = []
        injected = 0

        for c in candles:
            try:
                ts_ms = int(c.get("t", 0))
                o_px  = float(c.get("o", 0))
                h_px  = float(c.get("h", 0))
                l_px  = float(c.get("l", 0))
                cl_px = float(c.get("c", 0))
            except (TypeError, ValueError):
                continue
            if ts_ms <= 0 or any(p <= 0 for p in (o_px, h_px, l_px, cl_px)):
                continue
            ts_s   = ts_ms / 1000.0
            bucket = int(ts_s // 60)
            if bucket >= current_bucket:
                continue   # skip the live open candle
            synthetic_ticks.extend([
                (ts_s +  0.0, o_px),
                (ts_s + 15.0, h_px),
                (ts_s + 45.0, l_px),
                (ts_s + 59.0, cl_px),
            ])
            injected += 1

        if injected == 0:
            logger.warning("[GridBot] Phase 2c: no usable candles after filtering")
            return

        # Merge into _history (same strategy as _load_candles_from_db):
        # sort chronologically, deduplicate by keeping the last value per
        # timestamp (live WS ticks win over synthetic ones for the same ts).
        synthetic_ticks.sort(key=lambda x: x[0])
        with _price_cache._lock:
            existing = list(_price_cache._history)
            # Build a ts->mid dict so newer entries (live ticks) override older
            # synthetic ones for the same timestamp.
            merged_dict: dict = {}
            for ts, mid in synthetic_ticks:
                merged_dict[ts] = mid
            for ts, mid in existing:
                merged_dict[ts] = mid   # live ticks override synthetic
            merged = sorted(merged_dict.items())
            _price_cache._history.clear()
            cap = _price_cache._history.maxlen or len(merged)
            for item in merged[-cap:]:
                _price_cache._history.append(item)

        # Verify the warm-up worked
        stab = _price_cache.compute_stability(5)
        logger.info(
            f"[GridBot] Phase 2c: injected {injected} recent candles "
            f"({injected * 4} ticks) — compute_stability(5): "
            f"ok={stab['ok']} n_ticks={stab['n_ticks']} "
            f"hi_lo={stab['hi_lo']:.2f}"
        )
        if not stab["ok"]:
            logger.warning(
                "[GridBot] Phase 2c: compute_stability(5) still ok=False after "
                f"seeding ({stab['n_ticks']} ticks in 5-min window) — range "
                "guard will skip on first build; live ticks will warm it up "
                "within ~5 minutes"
            )

    # ── Funding rate ──────────────────────────────────────────────────────────

    def _fetch_and_accrue_funding(self) -> None:
        """
        Fetch the current funding rate from CDC REST public/get-valuations and
        accrue an estimated funding charge/income against the current net long.

        BTCUSD-PERP settles funding every 8 hours. This method is called from
        the main _run() loop every funding_rate_fetch_interval_s (default 8h).

        Accrual formula:
            funding_usd = long_qty_btc * mid_price * funding_rate
        Positive rate → long pays short (cost for us).
        Negative rate → short pays long (income for us).

        The accrued total is persisted in the meta table under key
        "funding_accrued_usd" so it survives restarts.
        """
        if not self._cfg.get("funding_rate_enabled", True):
            return

        rest_base  = self._cfg.get("rest_base_url",
                                   "https://api.crypto.com/exchange/v1")
        instrument = self._cfg.get("funding_rate_instrument", "BTCUSD-PERP")
        url = f"{rest_base}/public/get-valuations"
        params = {"instrument_name": instrument, "valuation_type": "funding_rate"}

        try:
            resp = requests.get(url, params=params, timeout=10.0)
            resp.raise_for_status()
            body = resp.json()
        except Exception as e:
            logger.warning(f"[Funding] REST request failed: {e}")
            return

        if body.get("code", -1) != 0:
            logger.warning(f"[Funding] API returned code={body.get('code')}")
            return

        data = body.get("result", {}).get("data", [])
        if not data:
            logger.warning("[Funding] Empty data in funding rate response")
            return

        try:
            latest   = data[-1]
            rate     = float(latest.get("v", 0.0))
        except (IndexError, TypeError, ValueError) as e:
            logger.warning(f"[Funding] Failed to parse funding rate: {e}")
            return

        self._last_funding_rate = rate

        # Accrue against current long position
        mid      = _price_cache.get_mid()
        long_qty = self._engine.get_stats().get("long_qty", 0.0) if self._engine else 0.0
        if mid and long_qty > 0:
            charge_usd = long_qty * mid * rate
            self._funding_accrued_usd += charge_usd
            # Persist to DB
            try:
                persisted = float(
                    self._store.get_meta("funding_accrued_usd") or "0.0"
                )
                self._store.set_meta(
                    "funding_accrued_usd",
                    f"{persisted + charge_usd:.6f}"
                )
            except Exception as e:
                logger.warning(f"[Funding] Failed to persist accrued funding: {e}")

            sign = "+" if charge_usd >= 0 else ""
            logger.info(
                f"[Funding] rate={rate*100:.4f}%  long={long_qty:.4f} BTC  "
                f"mid={mid:.2f}  charge={sign}{charge_usd:.4f} USD  "
                f"accrued_total={self._funding_accrued_usd:+.4f} USD"
            )
        else:
            logger.info(
                f"[Funding] rate={rate*100:.4f}%  "
                f"(no accrual — long_qty={long_qty:.4f})"
            )

    def _get_funding_accrued(self) -> float:
        """Return total accrued funding from DB (survives restarts)."""
        try:
            return float(self._store.get_meta("funding_accrued_usd") or "0.0")
        except Exception:
            return 0.0

    def stop(self):
        logger.info("[GridBot] Stopping")
        self._stop_event.set()
        self._ws_stop.set()
        self._market_ws.stop()

        # Liquidate any accumulated long before tearing down the OMS.
        # This mirrors what _emergency_halt does for stop-loss events so that
        # a clean SIGINT/SIGTERM also closes the position rather than leaving
        # it orphaned on the exchange.
        #
        # Exception: handoff-stop mode (set during a blue-green handoff export).
        # In this mode the successor process has already pre-registered and
        # will inherit the position via _apply_handoff_restore(), so this
        # process must NOT liquidate.
        long_qty = 0.0
        cost_basis_price = None
        if self._engine:
            long_qty = self._engine.get_stats().get("long_qty", 0.0)
            if long_qty > 0:
                _, cost_basis_price = self._engine.get_cost_basis()
            self._engine.stop()
            self._engine = None
        if self._handoff_stop:
            logger.warning(
                f"[GridBot] Handoff-stop: leaving {long_qty:.4f} BTC position "
                f"for successor process — NOT liquidating"
            )
        elif long_qty > 0:
            self._liquidate_position(long_qty, reason="GridBot stop",
                                      cost_basis_price=cost_basis_price)

        self._cmd_poller.stop()
        self._oms.stop()
        # Release unconditionally — every role (including standalone) now
        # acquires bg_lock before it's allowed to start trading (see
        # start()), so every role must release it here too. A no-op if we
        # never actually held it (e.g. SIGINT arrived before we got that
        # far) or already handed it off via export_handoff_snapshot(), since
        # bg_lock_release only clears the row if we're still the recorded
        # holder.
        self._lock_heartbeat_stop_event.set()
        self._store.bg_lock_release(os.getpid())
        self._store.close()
        self._alerter.send_sync(f"🔴 GridBot stopped")
        self._alerter.stop()
        logger.info("[GridBot] Stopped")

    # ── Blue-green: handoff snapshot export (called by blue) ─────────────────

    def export_handoff_snapshot(self) -> bool:
        """
        Freeze order activity, atomically snapshot GridEngine state, write the
        JSON to SQLite, then arm _handoff_stop so stop() skips liquidation.

        Sequence (designed to be race-safe):
          1. Set _handoff_freeze — GridEngine checks this before placing any
             new orders, so no new REST calls fire after this point.
          2. Wait 300ms for any in-flight OMS worker submissions to drain.
          3. Acquire GridEngine lock and read long_qty + level states atomically.
          4. Build JSON, write to SQLite meta['bg_handoff_json'].
          5. Clear bg_handoff_request, then release bg_lock — this is the
             actual hand-off moment a waiting successor's acquire attempt is
             racing to catch.
          6. Arm _handoff_stop so stop() won't liquidate.

        Returns True on success, False if no engine is running (nothing to hand off).
        """
        if self._engine is None:
            logger.warning("[GridBot] export_handoff_snapshot: no engine running")
            return False

        logger.info("[GridBot] Handoff export: freezing order activity")
        self._engine._handoff_freeze = True

        # Stop TgPoller immediately so the incoming process's TgPoller doesn't
        # get a 409 Conflict from Telegram (two simultaneous getUpdates calls
        # from the same bot token).  We signal stop but do NOT join() — the
        # thread will drain on its own when the process exits ~1-2s later.
        # Blocking here for up to HTTP_TIMEOUT+5=45s would break the <2s
        # handoff window.
        if self._cmd_poller is not None:
            self._cmd_poller.stop_nowait()

        # Let any in-flight REST submissions complete before we snapshot.
        time.sleep(0.3)

        with self._engine._lock:
            long_qty = self._engine._long_qty   # logging/alert display only now — see below
            params   = self._engine._params
            levels_data = []
            for lv in self._engine._levels:
                exchange_id = (self._oms.get_exchange_id(lv.client_oid)
                               if lv.client_oid else "")
                levels_data.append({
                    "index":            lv.index,
                    "price":            lv.price,
                    "state":            lv.state.value,
                    "client_oid":       lv.client_oid,
                    "exchange_id":      exchange_id,
                    "qty":              lv.qty,
                    "placed_at":        lv.placed_at,
                    # This cell's fixed identity (2026-08-03 cell restructure)
                    # — see GridLevel.open_side. Older snapshots won't have
                    # this key; _apply_handoff_restore derives it from
                    # state+closes_leg_id when absent, so it's included here
                    # for explicitness/debuggability, not because restore
                    # strictly depends on it.
                    "open_side":        lv.open_side,
                    # Needed so a restored order's eventual fill still closes
                    # the SAME OpenLeg it was the designated closer for — see
                    # GridEngine._on_fill and GridBot._apply_handoff_restore.
                    # None means "fresh open", same as any other untagged level.
                    "closes_leg_id":    lv.closes_leg_id,
                })

        # ── FIX: range guard warm-up + BuyGate calibration continuity ──────────
        # Collect recent price ticks and BuyGate scores to hand off to the
        # incoming process so it starts with a warm _price_cache and score history.
        #
        # Range guard (compute_stability(5)): needs ≥10 ticks within the last
        # 5 minutes. We pass 10 minutes of ticks to give a comfortable margin
        # even if warmup takes a few seconds.
        #
        # BuyGate calibration (_score_history): _calibrate_threshold() scans
        # the last calib_lookback_s (default 120s) before a halt. If a SL
        # occurs shortly after a handoff the score history in the new process
        # would otherwise be empty. Pass the last lookback+60s of scores.
        tick_window_s   = 10 * 60      # 10-minute tick window for range guard
        score_window_s  = self._cfg.get("stop_score_calib_lookback_s", 120) + 60
        now_export      = time.time()
        with _price_cache._lock:
            all_ticks = list(_price_cache._history)
        tick_cutoff   = now_export - tick_window_s
        recent_ticks  = [[ts, mid] for ts, mid in all_ticks if ts >= tick_cutoff]
        score_cutoff  = now_export - score_window_s
        recent_scores = [[ts, sc] for ts, sc in self._score_history
                         if ts >= score_cutoff]
        logger.info(
            f"[GridBot] Handoff export: including {len(recent_ticks)} price ticks "
            f"(last 10min) and {len(recent_scores)} BuyGate scores (last {score_window_s:.0f}s)"
        )

        snapshot = {
            "schema":      3,
            "exported_at": now_export,
            "role":        self._role,
            "long_qty":    long_qty,
            "params": {
                "lower":              params.lower,
                "upper":              params.upper,
                "levels":             params.levels,
                "spacing":            params.spacing,
                "stop_price":         params.stop_price,
                "notional_per_level": params.notional_per_level,
                "computed_at":        params.computed_at,
            },
            "levels":       levels_data,
            # Schema 3 additions:
            "price_ticks":  recent_ticks,   # [[ts, mid], …] last 10 min
            "score_history": recent_scores, # [[ts, score], …] last 120+60s
        }

        import json as _json
        payload = _json.dumps(snapshot)
        self._store.bg_write_handoff_json(payload)
        self._store.bg_clear_handoff_request()
        # Stop the lock-heartbeat thread BEFORE releasing the lock below.
        # Without this, there's a window (up to BG_LOCK_HEARTBEAT_S seconds)
        # between the intentional release here and stop()'s own
        # _lock_heartbeat_stop_event.set() during which the heartbeat thread
        # can wake up, find it's no longer the recorded lock holder, and log
        # its "Lost the instance lock while still running" CRITICAL — even
        # though this is a completely normal, successful hand-off. Setting
        # the stop event first closes that race so the CRITICAL is reserved
        # for a genuine, unexpected loss of the lock.
        self._lock_heartbeat_stop_event.set()
        # This is the actual hand-off moment: releasing the lock here is what
        # a waiting successor's bg_lock_try_acquire (polling since it saw the
        # JSON appear) is racing to catch. See GridStateStore.bg_lock_release
        # and GridBot._try_acquire_bg_lock's retry note for why a poll that
        # lands in the few-microsecond gap between the JSON write above and
        # this release doesn't cause a spurious refusal.
        self._store.bg_lock_release(os.getpid())

        self._handoff_stop = True

        n_open = sum(1 for lv in levels_data
                     if lv["state"] in ("BUY_OPEN", "SELL_OPEN"))
        logger.info(
            f"[GridBot] Handoff export complete: long_qty={long_qty:.4f} "
            f"open_orders={n_open} levels={len(levels_data)}"
        )
        self._alerter.send_sync(
            f"🔄 Handoff exported: {long_qty:.4f} BTC, {n_open} open orders "
            f"→ successor process"
        )
        return True

    # ── Blue-green: handoff snapshot matching/restore (called by the incoming
    # process during _rebuild_grid, using data pre-registered by
    # _preregister_handoff_orders) ────────────────────────────────────────────

    def _match_handoff_levels(
        self, new_params: "GridParams"
    ) -> Tuple[Dict[int, dict], List[dict]]:
        """
        Decide which of a pending handoff snapshot's open orders can be kept
        in place on the grid we're about to build, vs which no longer
        correspond to any cell in it and must be treated as orphans
        (cancelled, replaced by a fresh order instead).

        2026-08-03: matches by (price, SIDE), not price alone. Under the
        cell restructure a BUY always rests at a cell's lower boundary and
        a SELL always at its upper (see GridLevel docstring) — so a BUY
        snapshot entry is matched against {cell.lower: cell.index} and a
        SELL entry against {cell.upper: cell.index}, using the cell
        boundaries new_params would build (mirrors GridEngine._build_levels'
        own pairing of consecutive level_prices, without needing a live
        engine yet — this runs before the new GridEngine is constructed).
        Side disambiguation matters because adjacent cells legitimately
        share a boundary price with opposite-side orders resting there at
        once (cell k-1's SELL-closer and cell k's own BUY-opener, say) —
        collapsing both onto one flat price->index map, as the pre-cell-
        restructure version of this method did, would let one silently
        clobber the other's slot in `claimed`.

        Matching is done by price (not the snapshot's old point-based
        index), for the same reason as before: if the auto-tuner's freshly
        computed lower/upper/spacing shift the grid at either edge, price
        matching keeps as much of the previous grid — and as many of the
        peer's still-live orders — as possible, while index matching would
        misalign everything even though most actual price points are
        unchanged.

        Called from _rebuild_grid() BEFORE the new GridEngine/its orders are
        created, so the result can tell GridEngine.start() which indices to
        skip placing a fresh order for.
        """
        snap = self._pending_handoff_snapshot
        assert snap is not None

        new_prices = new_params.level_prices   # N+1 boundary points
        n_cells    = len(new_prices) - 1
        # A BUY always rests at a cell's LOWER boundary (new_prices[i]); a
        # SELL always at a cell's UPPER boundary (new_prices[i+1]).
        buy_price_to_cell:  Dict[str, int] = {
            f"{new_prices[i]:.2f}": i for i in range(n_cells)
        }
        sell_price_to_cell: Dict[str, int] = {
            f"{new_prices[i + 1]:.2f}": i for i in range(n_cells)
        }

        restore_plan: Dict[int, dict] = {}
        orphans:      List[dict]      = []
        claimed: set = set()

        for snap_lv in snap.get("levels", []):
            state_str = snap_lv.get("state", "IDLE")
            if state_str not in ("BUY_OPEN", "SELL_OPEN"):
                continue  # nothing to restore or orphan for an idle snapshot level

            lookup  = buy_price_to_cell if state_str == "BUY_OPEN" else sell_price_to_cell
            new_idx = lookup.get(f"{snap_lv['price']:.2f}")
            if new_idx is None or new_idx in claimed:
                orphans.append(snap_lv)
                continue

            claimed.add(new_idx)
            restore_plan[new_idx] = snap_lv

        return restore_plan, orphans

    def _apply_handoff_restore(
        self, restore_plan: Dict[int, dict], orphans: List[dict]
    ) -> None:
        """
        Apply a previously-computed restore plan onto the just-built engine.

        OMS-side registration (exchange_id/client_oid mapping + fill queue)
        already happened in _preregister_handoff_orders(), as early in
        startup as possible — see that method's docstring for why. This step
        only has to: set the matched GridLevel objects' state/client_oid/qty/
        placed_at/closes_leg_id (so a restored level's eventual fill closes
        the correct OpenLeg — see GridEngine._on_fill), and cancel + forget
        any orphaned orders that no longer correspond to any level in the
        grid we just built (GridEngine.start() already placed a fresh order
        at whatever new level took that price's place, if any).

        long_qty is NOT restored here — it's a derived property computed
        from _open_legs, which GridEngine.__init__ already seeded straight
        from the shared DB (the predecessor wrote through to the same
        open_legs table on every leg open/close, so this process sees the
        same ledger without any snapshot involved). What IS worth doing
        here is a sanity check: if our freshly-derived long_qty disagrees
        with what the predecessor itself reported at export time, something
        is inconsistent between the two processes' view of the ledger —
        worth a loud log rather than silent trust either way.
        """
        snap = self._pending_handoff_snapshot
        assert snap is not None and self._engine is not None

        with self._engine._lock:
            level_by_index = {lv.index: lv for lv in self._engine._levels}
            for new_idx, snap_lv in restore_plan.items():
                lv = level_by_index.get(new_idx)
                if lv is None:
                    continue  # shouldn't happen — restore_plan keys came from this same grid
                state_str = snap_lv["state"]
                lv.state         = LevelState(state_str)
                lv.client_oid    = snap_lv["client_oid"]
                lv.qty           = snap_lv["qty"]
                lv.price         = snap_lv["price"]
                lv.placed_at     = snap_lv.get("placed_at", time.time())
                lv.closes_leg_id = snap_lv.get("closes_leg_id")
                # This cell's fixed identity (see GridLevel.open_side).
                # Explicit in snapshots from this version onward; for an
                # older snapshot without the key, derive it: a cell only
                # ever closes on the side OPPOSITE how it opens, so an
                # untagged restored order IS its own open_side, and a
                # tagged one is the opposite of whatever side it's
                # currently resting.
                open_side = snap_lv.get("open_side")
                if open_side is None:
                    resting_side = "BUY" if state_str == "BUY_OPEN" else "SELL"
                    open_side = (resting_side if lv.closes_leg_id is None
                                 else ("SELL" if resting_side == "BUY" else "BUY"))
                lv.open_side = open_side

            restored_long_qty = self._engine._long_qty

        expected_long_qty = float(snap.get("long_qty", restored_long_qty))
        if abs(restored_long_qty - expected_long_qty) > 1e-6:
            logger.warning(
                f"[GridBot] Handoff: long_qty mismatch after restore — "
                f"predecessor reported {expected_long_qty:.4f}, this process's "
                f"open_legs ledger derives {restored_long_qty:.4f}. Both "
                f"processes read the same DB, so this points at a write that "
                f"didn't land (or landed between export and this check) — "
                f"worth a manual look at the open_legs table."
            )

        for olv in orphans:
            exid = olv.get("exchange_id", "")
            coid = olv.get("client_oid", "")
            if exid:
                logger.warning(
                    f"[GridBot] Handoff: orphaned order at price {olv['price']:.2f} "
                    f"(exid={exid}) has no matching level in the rebuilt grid "
                    f"— cancelling"
                )
                try:
                    self._oms._rest_cancel_order(exid)
                except Exception as e:
                    logger.error(f"[GridBot] Handoff: cancel orphan failed: {e}")
            if coid:
                # Drop the early OMS registration too, or its fill_queue entry
                # leaks for the life of the process — nothing will ever call
                # wait_fill() for a client_oid no GridLevel references.
                self._oms.forget_order(coid)

        # Only NOW is the restore fully durable in this process's own state —
        # clear the SQLite record so a future crash-recovery check doesn't
        # find (and needlessly re-apply) a snapshot that's already been
        # folded into a live, running grid. Deferred this long (rather than
        # clearing as soon as the snapshot was read, back in
        # _preregister_handoff_orders) specifically so a crash between those
        # two points still leaves something for the next restart to resume.
        # NOTE: bg_lock itself is NOT released here — we're continuing to run
        # as the now-live process and are already heartbeating it (see
        # start()); it's only released on a clean stop() or when we
        # ourselves later hand off to a successor.
        self._store.bg_clear_handoff_json()

        snap_params  = snap.get("params", {})
        new_params   = self._engine._params
        exported_at  = snap.get("exported_at", 0)
        age_ms       = int((time.time() - exported_at) * 1000) if exported_at else -1
        logger.info(
            f"[GridBot] Handoff applied: long_qty={self._engine._long_qty:.4f} "
            f"restored={len(restore_plan)} orphaned={len(orphans)} "
            f"snapshot_age={age_ms}ms | "
            f"peer params range=[{snap_params.get('lower', 0):.2f},"
            f"{snap_params.get('upper', 0):.2f}] spacing={snap_params.get('spacing', 0):.2f} "
            f"-> this grid's range=[{new_params.lower:.2f},{new_params.upper:.2f}] "
            f"spacing={new_params.spacing:.2f}"
        )
        self._alerter.send(
            f"✅ Handoff applied: {self._engine._long_qty:.4f} BTC, "
            f"{len(restore_plan)} orders restored in place, "
            f"{len(orphans)} orphans cancelled & recreated fresh. "
            f"Snapshot age {age_ms}ms."
        )

    # ── Main loop ─────────────────────────────────────────────────────────────

    def _run(self):
        logger.info("[GridBot] Main loop running")
        while not self._stop_event.is_set():
            if self._handoff_shutdown_requested.is_set():
                # A peer has requested (and, if export_handoff_snapshot()
                # succeeded, already received) our handoff snapshot. Perform
                # the actual stop() here, on the main thread, rather than
                # from the watcher's background thread — see
                # _start_handoff_watcher()'s docstring for why that
                # distinction matters. stop() itself sets _stop_event, so
                # this loop exits right after.
                logger.info("[GridBot] Handoff shutdown requested — stopping")
                self.stop()
                break

            if self._halted:
                self._check_auto_restart()
                time.sleep(10)   # poll every 10s while halted
                continue

            mid = _price_cache.get_mid()
            if mid is None:
                time.sleep(0.2)
                continue

            # Pull the engine's live params before anything below reads
            # self._params. _trail_up/_trail_down (grid drift-shift) rebind
            # the ENGINE's own params to a new object whenever the top/bottom
            # level fills — GridBot._params otherwise only gets refreshed on
            # a full _rebuild_grid(), so every check below (stop-score,
            # should_retune's outside-range check, the dead-band bypass,
            # the Telegram /status handler) would keep evaluating against a
            # stale pre-shift range until the next full rebuild — which, if
            # should_retune() itself is one of the things reading stale data,
            # may never come. See GridEngine.get_params()'s docstring; this
            # mirrors the same pull _log_status() already does, just at the
            # top of the tick instead of only at status-log time.
            if self._engine is not None:
                self._params = self._engine.get_params()

            # Stop-loss
            if self._sl_guard and self._sl_guard.check(mid):
                self._emergency_halt(mid)
                continue

            # Stop-score tick — update velocity EMA on every price tick so the
            # score stays fresh even between fills.  Also drives gradual release
            # of BuyGate-SUPPRESSED levels when the score recovers.
            if self._stop_scorer is not None and self._params is not None:
                score = self._stop_scorer.compute(mid, self._params.stop_price)
                resume_thr = self._cfg.get("stop_score_resume_threshold", 0.35)
                buy_release_ok = score <= resume_thr
                # 2026-08-05 (trail-flip): hold back release while
                # confirmed-downtrend shift evidence (_downtrend_confirmed_now,
                # written by GridEngine._trail_down via down_shift_record_fn)
                # is still fresh, even if the trend-risk score above has
                # already dipped back under resume_thr — a level suppressed
                # for THAT reason (see _place_initial_orders) should wait for
                # the same shift-count evidence that suppressed it to age out
                # of drift_shift_trend_lookback_s, same hysteresis SellGate's
                # own release condition below already gives the up side.
                # Additive only — never releases MORE eagerly than before.
                if (buy_release_ok
                        and self._cfg.get("trail_flip_to_sell_on_confirmed_downtrend", True)
                        and self._downtrend_confirmed_now()):
                    buy_release_ok = False
                if (buy_release_ok
                        and self._engine is not None
                        and self._engine.count_suppressed("BUY") > 0):
                    released = self._engine.release_one_suppressed_level("BUY")
                    if released:
                        logger.info(
                            f"[GridBot] Released one BUY-suppressed level "
                            f"(score={score:.4f} ≤ resume={resume_thr})"
                        )

            # SellGate release (2026-08-04) — independent of the stop-score
            # above (that score is explicitly downtrend-only, see
            # compute_trend_risk's docstring, so it says nothing about
            # whether the uptrend that caused a SellGate suppression has
            # actually eased). Mirrors _sell_gate's own two conditions
            # directly rather than reusing a resume-threshold gap the way
            # BuyGate does, since neither SellGate condition is a
            # continuous score — OUTSIDE_RANGE-above clears the instant mid
            # is back at/below the upper bound, and the confirmed-uptrend
            # condition already has its own built-in hysteresis: it only
            # eases as old shifts age out of drift_shift_trend_lookback_s,
            # not on every tick.
            if self._engine is not None and self._engine.count_suppressed("SELL") > 0:
                sell_gate_still_blocking = False
                if (self._cfg.get("sell_gate_enabled", True)
                        and self._cfg.get("trend_gate_enabled", True)):
                    if self._effective_trend_regime() == TrendSignal.REGIME_UP:
                        if (self._params is not None
                                and self._cfg.get("trend_gate_outside_range_block_on_up", True)
                                and mid > self._params.upper):
                            sell_gate_still_blocking = True
                        if (not sell_gate_still_blocking
                                and self._cfg.get("sell_gate_block_on_confirmed_uptrend", True)
                                and self._uptrend_confirmed_now()):
                            sell_gate_still_blocking = True
                # 2026-08-05 (trail-flip): also hold back release while the
                # regime-INDEPENDENT shift-count evidence
                # (_uptrend_confirmed_now) that _place_initial_orders / 
                # _trail_up may have suppressed or flipped against is still
                # fresh — same evidence sell_gate_block_on_confirmed_uptrend
                # reads above, just not gated behind TrendSignal's hourly
                # regime the way sell_gate_still_blocking's own checks are.
                # Without this, a level suppressed purely on shift evidence
                # while regime reads NEUTRAL would get released on the very
                # next tick, undoing the suppression before the evidence
                # itself has aged out of drift_shift_trend_lookback_s.
                if (not sell_gate_still_blocking
                        and self._cfg.get("trail_flip_to_buy_on_confirmed_uptrend", True)
                        and self._uptrend_confirmed_now()):
                    sell_gate_still_blocking = True
                if not sell_gate_still_blocking:
                    released = self._engine.release_one_suppressed_level("SELL")
                    if released:
                        logger.info(
                            "[GridBot] Released one SELL-suppressed level "
                            "(sell-gate cleared)"
                        )

            # Fill detection
            if self._engine:
                self._engine.check_price_fills(mid)

            # Engine-requested rebuild. Two setters share this one flag:
            # drift-shift's "far OOR" cascade guard, and the one-sided-grid
            # detector (see GridEngine.check_price_fills()) — both funnel
            # through pop_needs_rebuild() here. Neither setter has access to
            # trend_risk (it's a GridBot/_stop_scorer concept, not something
            # GridEngine can see), so it's logged here at the one place both
            # paths pass through instead. No behavior change — visibility
            # only, so if one of these ever shows up as a real loss source
            # the way the regime-change trigger did, the evidence is already
            # in the log instead of needing to be reconstructed after the
            # fact.
            if self._engine and self._engine.pop_needs_rebuild():
                engine_rebuild_trend_risk = 0.0
                if self._stop_scorer is not None:
                    engine_rebuild_trend_risk = self._stop_scorer.compute_trend_risk(
                        mid, self._effective_trend_regime(), self._last_trend_slope_pct
                    )
                logger.info(
                    "[GridBot] Engine requested full rebuild "
                    "(drift far OOR / one-sided grid) "
                    f"trend_risk={engine_rebuild_trend_risk:.2f}"
                )
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
            _status_interval = self._cfg.get(
                "status_interval_s", self.STATUS_INTERVAL_S)
            if now - self._last_status > _status_interval:
                self._last_status = now
                self._log_status(mid)
                self._evaluate_trend()
                # Daily loss circuit breaker — same cadence as status
                if self._check_daily_loss_limit():
                    continue

            # Periodic candle snapshot — persists PriceCache history to DB so
            # TrendSignal warm-up survives service restarts.
            if now - self._last_candle_save > self.CANDLE_SAVE_INTERVAL_S:
                self._last_candle_save = now
                self._save_candles_to_db()

            # Periodic funding rate fetch (every 8h)
            if (self._cfg.get("funding_rate_enabled", True)
                    and now - self._last_funding_fetch
                    > self._cfg.get("funding_rate_fetch_interval_s", 28800)):
                self._last_funding_fetch = now
                self._fetch_and_accrue_funding()

            # Periodic spacing auto-tune — no-ops internally unless
            # spacing_autotune_enabled and its own interval has elapsed.
            self._spacing_tuner.maybe_evaluate()
            if self._spacing_tuner.pop_rebuild_requested():
                # DEFERRED ADOPTION (2026-08-02 fix): min_grid_levels /
                # min_grid_pct were already updated in config the moment
                # update_levels_from_trend()/_evaluate() decided on a new
                # value — that's live for AutoTuner.compute() immediately,
                # regardless of when the grid itself next rebuilds. What we
                # NO LONGER do is force an out-of-band _rebuild_grid() call
                # here just to adopt it early.
                #
                # 2026-08-02 11:34 incident: mid was comfortably inside the
                # existing [62453.20,64207.30] range (no boundary breach, no
                # price-driven urgency at all) when a regime flip alone
                # forced this rebuild. AutoTuner recomputed against CURRENT
                # (low) volatility and collapsed a 13-level/~1754pt grid to
                # 1 level/180pt in one step, stranding 3 legs with no
                # candidate closing level at all — -26.52 USD, ~84% of that
                # session's entire rebuild-driven loss, none of it justified
                # by any actual price movement.
                #
                # Safe to defer: the reason a regime change matters at all —
                # suppressing buys in a confirmed downtrend — is read via
                # _effective_trend_regime() directly by the buy gate, and
                # was never gated on whether the GRID has physically
                # rebuilt. Deferring the level-count/spacing adoption to the
                # next real trigger (a genuine should_retune() boundary
                # breach, or the 24h periodic interval) costs nothing there.
                logger.info(
                    "[GridBot] Spacing auto-tune changed min_grid_pct/"
                    "min_grid_levels — deferred to next naturally-triggered "
                    "rebuild (boundary breach or periodic interval), not "
                    "forcing one now"
                )

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
                # level_idx=-1 / cycle_num=-1: sentinel marking this as a
                # liquidation fill rather than a normal numbered grid level/cycle.
                # is_close=True preserves this method's pre-existing behavior
                # exactly (it was always inferred from side=="SELL" before
                # record_fill's is_close param existed — this function only
                # ever closes/reduces a position, never opens one).
                # close_reason="stop_loss" (2026-08-06, REPRICE_UNDERCOUNT_2026_08_05):
                # previously left as the default None, indistinguishable from an
                # ordinary cycle fill in grid_fills. sl_gross_usd/sl_count haven't
                # shown the same incremental-counter undercount reprice_gross_usd
                # did, but there'd been no way to check directly from raw fills
                # either — this tag makes that possible going forward without
                # changing anything about how is_liquidation itself is tallied.
                ok, err = self._store.execute_with_retry(lambda: self._store.record_fill(
                    ts_utc=time.time(), side="SELL", level_idx=-1,
                    price_usd=fill.avg_price, qty_btc=fill.filled_qty,
                    fee_usd=fill.fee, gross_pnl=gross_pnl, cycle_num=-1,
                    is_liquidation=is_liquidation, is_close=True,
                    close_reason=("stop_loss" if is_liquidation else None),
                ))
                if not ok:
                    # The market SELL already happened for real — this is a
                    # real, already-executed liquidation. What's at risk is
                    # purely the audit trail: this fill's grid_fills row and
                    # its contribution to daily_pnl (and, if is_liquidation,
                    # the sl_gross_usd/sl_count stop-loss bucket specifically)
                    # never lands. Loud and distinct rather than the generic
                    # error this used to be, since a stop-loss silently
                    # missing from the loss ledger is exactly the kind of
                    # thing you'd want to know about before trusting the
                    # accumulated PnL number after an incident.
                    logger.critical(
                        f"[GridBot]{tag} PERSISTENT record_fill FAILURE for "
                        f"liquidation SELL {fill.filled_qty:.4f} @ "
                        f"{fill.avg_price:.2f} gross={gross_pnl:+.4f} "
                        f"is_liquidation={is_liquidation} after retries: {err}. "
                        f"This liquidation ALREADY EXECUTED for real — only its "
                        f"grid_fills row and daily_pnl/"
                        f"{'sl_gross_usd' if is_liquidation else 'accumulated'} "
                        f"contribution are missing.",
                        exc_info=err,
                    )
                    self._alerter.send(
                        f"⚠️ record_fill DB write failed for liquidation "
                        f"({_md_escape(reason)}) after retries — see log. The trade already "
                        f"happened; only its PnL record is missing."
                    )
            pnl_note = f" | realized {gross_pnl:+.4f} USD" if cost_basis_price else ""
            self._alerter.send(
                f"🔴 Position closed ({_md_escape(reason)})\n"
                f"Sold {fill.filled_qty:.4f} BTC @ {fill.avg_price:.2f}{pnl_note}"
            )

            # Close out the underlying per-leg open_legs rows this aggregate
            # fill just settled. This method always liquidates the WHOLE
            # position (every call site above passes the full long_qty /
            # stale_qty, never a partial amount), so every leg still marked
            # open in the DB at this point is, by definition, now closed —
            # there is no partial-liquidation case here that would make
            # closing ALL of them wrong.
            #
            # 2026-08-05 fix: this was a known, previously-worked-around gap
            # (see reset_state()'s open_legs-wipe comment, 2026-08-03) —
            # _liquidate_position recorded ONE aggregate record_fill row but
            # never called close_leg() on the individual rows that made up
            # that net qty, so they silently survived. reset_state() wiping
            # open_legs only helped a manual --reset-state cold start; it
            # never touched the auto-restart path, which rebuilds the grid
            # in-process/on-restart without going through reset_state() at
            # all. Confirmed in the 2026-08-05 11:19 stop-loss incident: the
            # 1-hour-later auto-restart re-seeded all 3 already-liquidated
            # legs from these stale rows (net_qty showing +0.0240 against an
            # already-flat position), and immediately chase-closed all 3
            # AGAIN — a second, phantom liquidation of BTC that was already
            # sold, roughly doubling the reported loss (-4.1969 real +
            # -4.4453 phantom) on top of corrupting net_qty tracking. In
            # live mode this would be worse: a real duplicate SELL against
            # an already-flat exchange position, not just a paper-mode
            # accounting artifact.
            if self._store is not None:
                try:
                    stale_legs = self._store.get_open_legs()
                except Exception as e:
                    stale_legs = []
                    logger.error(
                        f"[GridBot]{tag} Failed to read open_legs for "
                        f"post-liquidation cleanup: {e}"
                    )
                for row in stale_legs:
                    try:
                        self._store.close_leg(row["leg_id"])
                    except Exception as e:
                        logger.error(
                            f"[GridBot]{tag} Failed to close_leg(#{row['leg_id']}) "
                            f"during post-liquidation cleanup: {e}"
                        )
                if stale_legs:
                    logger.warning(
                        f"[GridBot]{tag} Post-liquidation cleanup: closed "
                        f"{len(stale_legs)} open_legs row(s) "
                        f"({sorted(r['leg_id'] for r in stale_legs)}) that this "
                        f"aggregate fill settled — prevents them resurfacing "
                        f"as a phantom position on the next rebuild/restart."
                    )
        else:
            logger.error(
                f"[GridBot]{tag} Liquidation fill TIMED OUT — "
                f"{qty:.4f} BTC may still be open. MANUAL INTERVENTION REQUIRED."
            )
            self._alerter.send(
                f"🚨 Liquidation TIMED OUT ({_md_escape(reason)})\n"
                f"{qty:.4f} BTC position may still be open.\n"
                f"MANUAL INTERVENTION REQUIRED"
            )

    def _liquidate_leg_at_market(self, leg: "OpenLeg", reason: str = "rebuild_reprice") -> bool:
        """
        Close one specific OpenLeg via a market order, for a leg that
        reconcile_open_legs decided no longer fits the just-rebuilt grid
        (its open_price fell outside the new range, or every level on its
        closing side was already claimed by another leg).

        Symmetric counterpart to _liquidate_position: that method always
        market-SELLs to reduce a long; this closes whichever direction the
        leg actually needs — a market BUY to cover a short leg is just as
        valid here as a market SELL to close a long one, since a leg can be
        opened by either side (see OpenLeg docstring).

        Returns True if the leg was confirmed closed (and has been removed
        from _open_legs / the DB ledger) — False on a timeout, in which
        case the leg is deliberately left tracked rather than assumed
        closed, so it's picked up again by the next rebuild's reconciliation
        instead of silently vanishing from the books.
        """
        close_side = "SELL" if leg.open_side == "BUY" else "BUY"
        tag = f"[{reason}]"
        logger.warning(
            f"[GridBot]{tag} Leg #{leg.leg_id} (opened {leg.open_side} "
            f"{leg.qty:.4f} @ {leg.open_price:.2f}) no longer fits the "
            f"rebuilt grid — closing via market {close_side}"
        )
        req = OrderRequest.market(side=close_side, qty=leg.qty,
                                   instrument=INSTRUMENT, purpose="liquidate")
        self._oms.submit(req)
        fill = self._oms.wait_fill(req.client_oid, timeout=15.0)
        if not (fill and fill.is_filled):
            logger.error(
                f"[GridBot]{tag} Leg #{leg.leg_id} liquidation TIMED OUT — "
                f"{leg.qty:.4f} BTC ({leg.open_side} @ {leg.open_price:.2f}) may "
                f"still be open. Leaving it tracked; MANUAL CHECK RECOMMENDED."
            )
            self._alerter.send(
                f"🚨 Leg #{leg.leg_id} liquidation TIMED OUT ({_md_escape(reason)})\n"
                f"{leg.qty:.4f} BTC ({leg.open_side} @ {leg.open_price:.2f}) "
                f"may still be open. MANUAL CHECK RECOMMENDED."
            )
            # Release from _chasing_leg_ids (a harmless no-op if this leg
            # never went through the chase path — e.g. reconcile's own
            # rebuild-reprice caller). If it DID come via
            # _chase_close_leg_worker's exhausted-attempts fallback, this
            # is the one path where a fully-failed close would otherwise
            # sit invisible to both the chase (which already gave up) and
            # the orphan-leg rebuild safety net (suppressed while
            # "chasing") forever. Let the safety net see it again.
            if self._engine is not None:
                with self._engine._lock:
                    self._engine._chasing_leg_ids.discard(leg.leg_id)
            return False

        self._finalize_leg_close(leg, fill, close_side, reason,
                                  log_verb="liquidated",
                                  alert_label="Leg closed at rebuild")
        return True

    def _finalize_leg_close(self, leg: "OpenLeg", fill: "FillEvent",
                             close_side: str, reason: str,
                             log_verb: str = "closed",
                             alert_label: str = "Leg closed") -> None:
        """
        Shared bookkeeping for a leg-closing fill, regardless of how the
        closing order was worked (market liquidation vs a chased
        POST_ONLY fill): PnL calc, record_fill, close_leg, pop from
        _open_legs, alert. Caller has already confirmed fill.is_filled.
        """
        tag = f"[{reason}]"
        if leg.open_side == "BUY":
            gross_pnl = (fill.avg_price - leg.open_price) * fill.filled_qty
        else:
            gross_pnl = (leg.open_price - fill.avg_price) * fill.filled_qty
        # Proportional share of the opening fill's fee attributable to the
        # qty closed by THIS fill — see OpenLeg.open_fee docstring and
        # GridEngine._on_fill's identical formula. leg.qty here is still
        # its pre-this-close value (this function doesn't mutate it) and
        # this is always the FINAL close for whatever's left of the leg
        # (a prior partial chunk, if any, already claimed its own share
        # via _record_partial_leg_close and shrunk leg.open_fee/leg.qty in
        # lockstep) — so fill.filled_qty == leg.qty here and this reduces
        # to attributing whatever open_fee remains, in full. The
        # proportional form is used anyway rather than assuming, so a
        # rounding-off qty mismatch degrades gracefully instead of over-
        # or under-attributing.
        open_fee_share = (
            leg.open_fee * (fill.filled_qty / leg.qty)
            if leg.qty > 0 else leg.open_fee
        )
        net_pnl = gross_pnl - fill.fee - open_fee_share

        logger.warning(
            f"[GridBot]{tag} Leg #{leg.leg_id} {log_verb}: {close_side} "
            f"{fill.filled_qty:.4f} @ {fill.avg_price:.2f} | "
            f"gross={gross_pnl:+.4f} net={net_pnl:+.4f} USD"
        )
        if self._store is not None:
            # level_idx=-1/cycle_num=-1: same sentinel _liquidate_position
            # uses — this isn't a normal numbered grid level fill.
            ok_rf, err_rf = self._store.execute_with_retry(lambda: self._store.record_fill(
                ts_utc=time.time(), side=close_side, level_idx=-1,
                price_usd=fill.avg_price, qty_btc=fill.filled_qty,
                fee_usd=fill.fee, gross_pnl=gross_pnl, cycle_num=-1,
                is_liquidation=False, is_reprice_loss=True, leg_id=leg.leg_id,
                close_reason=reason, is_close=True,
            ))
            if not ok_rf:
                # Same category as _on_fill's record_fill escalation: the
                # order already executed for real, so in-memory accounting
                # (and the close_leg removal below) is what matters and
                # isn't affected — only this fill's grid_fills row and
                # daily_pnl contribution are missing.
                logger.critical(
                    f"[GridBot]{tag} PERSISTENT record_fill FAILURE for leg "
                    f"#{leg.leg_id} {log_verb} {close_side} "
                    f"{fill.filled_qty:.4f} @ {fill.avg_price:.2f} "
                    f"gross={gross_pnl:+.4f} after retries: {err_rf}. "
                    f"AUDIT-TRAIL GAP: leg is correctly closed in memory/DB, "
                    f"but this fill's grid_fills row and daily_pnl "
                    f"contribution never landed.",
                    exc_info=err_rf,
                )
                self._alerter.send(
                    f"⚠️ record_fill DB write failed for leg #{leg.leg_id} "
                    f"{log_verb} ({_md_escape(reason)}) after retries — see log. Leg is "
                    f"correctly closed; only its PnL record is missing."
                )

            ok_cl, err_cl = self._store.execute_with_retry(
                lambda: self._store.close_leg(leg.leg_id))
            if not ok_cl:
                # Mirrors _on_fill's close_leg escalation exactly — this leg
                # is about to be popped from _engine._open_legs below
                # regardless (it's genuinely closed, real order, real
                # fill), so the risk is purely that the DB still shows it
                # open and a future rebuild/restart re-seeds and
                # double-counts it.
                logger.critical(
                    f"[GridBot]{tag} PERSISTENT close_leg FAILURE for leg "
                    f"#{leg.leg_id} (opened {leg.open_side} {leg.qty:.4f} @ "
                    f"{leg.open_price:.2f}) after retries: {err_cl}. "
                    f"GHOST-LEG RISK: closed in memory/grid_fills but the "
                    f"open_legs row may still exist — if so it will be "
                    f"re-seeded and its PnL double-counted at the next "
                    f"rebuild/restart. Manual fix: "
                    f"DELETE FROM open_legs WHERE leg_id={leg.leg_id};",
                    exc_info=err_cl,
                )
                self._alerter.send(
                    f"🚨 GHOST-LEG RISK: close_leg DB write failed for leg "
                    f"#{leg.leg_id} ({_md_escape(reason)}) after retries — see log. Manual "
                    f"DB cleanup recommended before the next rebuild/restart."
                )
        if self._engine is not None:
            with self._engine._lock:
                self._engine._open_legs.pop(leg.leg_id, None)
                self._engine._chasing_leg_ids.discard(leg.leg_id)
                # Mirror GridEngine._on_fill's closing-fill bookkeeping
                # (gross_pnl into _realized_pnl, the raw fill fee — not
                # open_fee_share — into _total_fees, one _cycle_count per
                # finished leg). Without this, chase-closes and reconcile-
                # forced market liquidations (this function's two callers)
                # land correctly in the DB via record_fill/close_leg above,
                # but the *running* engine's own net_pnl/cycles — what the
                # periodic Status line and Telegram alerts actually show —
                # never reflect them until the next full rebuild reseeds
                # from DB and the number silently jumps.
                self._engine._realized_pnl += gross_pnl
                self._engine._total_fees += fill.fee
                self._engine._cycle_count += 1

        self._alerter.send(
            f"🔁 {_md_escape(alert_label)} ({_md_escape(reason)})\n"
            f"{close_side} {fill.filled_qty:.4f} BTC @ {fill.avg_price:.2f} "
            f"(opened {leg.open_side} @ {leg.open_price:.2f}) | "
            f"realized {gross_pnl:+.4f} USD"
        )

    def _record_partial_leg_close(self, leg: "OpenLeg", fill: "FillEvent",
                                   close_side: str, reason: str) -> None:
        """
        Persist one partial-fill chunk of an in-progress chase close (an
        attempt that filled some but not all of its qty before its
        maker-timeout cancelled the rest — see _handle_order_update's
        CANCELED branch, which reports the real filled_qty/avg_price/fee
        for that chunk).

        This chunk's PnL is real and permanent regardless of what
        happens to the rest, so it's recorded the same way a full close
        is — via record_fill — but leg is NOT closed/popped here: it
        stays in _open_legs/DB, just for less qty. leg.qty is shrunk in
        place (both the in-memory OpenLeg and the DB row via
        reduce_leg_qty) so every downstream consumer of "how much of
        this leg is still open" — the next chase attempt's order size,
        the eventual market-fallback qty, _long_qty/stats, and a
        mid-chase process restart re-seeding from DB — sees only what's
        actually left, not the pre-partial original.

        leg.open_fee is shrunk the same way, in lockstep: this chunk
        claims its proportional share of whatever open_fee is still
        unclaimed (fill.filled_qty / leg.qty, using leg.qty's value
        BEFORE this chunk's reduction), so that share isn't available to
        be claimed again by a later chunk or the eventual final close —
        see OpenLeg.open_fee docstring. This function doesn't compute or
        log a net= figure itself (no caller currently surfaces one for a
        partial chunk), but gets this right anyway so whichever call
        eventually finishes the leg (_finalize_leg_close) sees a
        correctly-reduced open_fee rather than double-attributing a share
        this chunk already took.
        """
        tag = f"[{reason}]"
        if leg.open_side == "BUY":
            gross_pnl = (fill.avg_price - leg.open_price) * fill.filled_qty
        else:
            gross_pnl = (leg.open_price - fill.avg_price) * fill.filled_qty
        open_fee_share = (
            leg.open_fee * (fill.filled_qty / leg.qty)
            if leg.qty > 0 else leg.open_fee
        )

        if self._store is not None:
            ok, err = self._store.execute_with_retry(lambda: self._store.record_fill(
                ts_utc=time.time(), side=close_side, level_idx=-1,
                price_usd=fill.avg_price, qty_btc=fill.filled_qty,
                fee_usd=fill.fee, gross_pnl=gross_pnl, cycle_num=-1,
                is_liquidation=False, is_reprice_loss=True, leg_id=leg.leg_id,
                close_reason=reason,
                # is_close=False: this chunk does NOT close the leg — it
                # stays in _open_legs/DB with qty shrunk (see below), so it
                # must not add to daily_pnl.cycle_count. Whichever call
                # actually finishes the leg (_finalize_leg_close, reached
                # via a full chase fill or the market fallback) is the one
                # is_close=True call for this leg, so the leg contributes
                # exactly one cycle in total no matter how many partial
                # chunks it took to close it. gross_pnl_usd/fees_usd/
                # fill_count aren't gated on is_close, so this chunk's real
                # PnL and fee are still counted here as before.
                is_close=False,
            ))
            if not ok:
                logger.critical(
                    f"[GridBot]{tag} PERSISTENT record_fill FAILURE for leg "
                    f"#{leg.leg_id} PARTIAL chase-close {fill.filled_qty:.4f} "
                    f"@ {fill.avg_price:.2f} gross={gross_pnl:+.4f} after "
                    f"retries: {err}. This chunk already executed for real "
                    f"— only its grid_fills row and daily_pnl contribution "
                    f"are missing.",
                    exc_info=err,
                )
                self._alerter.send(
                    f"⚠️ record_fill DB write failed for leg #{leg.leg_id} "
                    f"partial chase-close ({_md_escape(reason)}) after retries — see log."
                )

        leg.qty = max(0.0, leg.qty - fill.filled_qty)
        leg.open_fee = max(0.0, leg.open_fee - open_fee_share)
        if self._store is not None:
            ok_r, err_r = self._store.execute_with_retry(
                lambda: self._store.reduce_leg_qty(leg.leg_id, leg.qty, leg.open_fee))
            if not ok_r:
                logger.critical(
                    f"[GridBot]{tag} PERSISTENT reduce_leg_qty FAILURE for "
                    f"leg #{leg.leg_id} after retries: {err_r}. In-memory "
                    f"qty is correct ({leg.qty:.4f} BTC remaining) but the "
                    f"open_legs DB row still shows the pre-partial amount — "
                    f"a restart before this is fixed would re-seed the "
                    f"stale, larger qty.",
                    exc_info=err_r,
                )
                self._alerter.send(
                    f"🚨 reduce_leg_qty DB write failed for leg #{leg.leg_id} "
                    f"({_md_escape(reason)}) after retries — see log. Restart risk: a "
                    f"stale, larger qty would be re-seeded."
                )

        self._alerter.send(
            f"🔁 Leg partially closed via chase ({_md_escape(reason)})\n"
            f"{close_side} {fill.filled_qty:.4f} BTC @ {fill.avg_price:.2f} "
            f"(opened {leg.open_side} @ {leg.open_price:.2f}) | "
            f"realized {gross_pnl:+.4f} USD | {leg.qty:.4f} BTC remaining"
        )

    def _chase_close_leg(self, leg: Optional["OpenLeg"], reason: str,
                          dropped_client_oid: Optional[str] = None,
                          dropped_order_side: Optional[str] = None) -> None:
        """
        stray_leg_fn — called by GridEngine (from _trail_up/_trail_down,
        inside its tick path) when trailing drops a cell that was
        holding a leg's designated closer (leg not None) and/or a still-
        live resting order of its own (dropped_client_oid not None).
        Must return immediately: it only spawns the worker thread and
        does no I/O itself, since the caller's tick loop (and every
        other level's fill processing behind it) is waiting on this
        call to return.
        """
        threading.Thread(
            target=self._reconcile_dropped_cell_worker,
            args=(leg, reason, dropped_client_oid, dropped_order_side),
            name=f"TrailDrop-{leg.leg_id if leg is not None else dropped_client_oid[:8]}",
            daemon=True).start()

    def _reconcile_dropped_cell_worker(
            self, leg: Optional["OpenLeg"], reason: str,
            dropped_client_oid: Optional[str],
            dropped_order_side: Optional[str]) -> None:
        """
        Runs off the tick loop. If the dropped cell had a live resting
        order (dropped_client_oid), cancel it and wait for its REAL
        resolution BEFORE deciding what, if anything, still needs
        chasing — see OMS.request_cancel_and_await's docstring for why
        guessing from the cancel response alone isn't safe, and the
        TRAIL UP/DOWN comments in GridEngine for why this has to be
        sequenced (cancel-confirm, then chase) rather than fired
        alongside the chase in parallel: if the original order and the
        chase's fresh order were both live at once for the same leg,
        both could fill and close it twice.

        Three cases fall out of dropped_client_oid's resolution:
          - Cleanly cancelled, nothing filled: proceed to chase leg for
            its full qty, same as if there'd been no order to cancel.
          - Filled (fully or partially) before the cancel could land:
            that's real, permanent PnL against leg's close side — apply
            it via the exact same accounting a normal chase fill would
            use (_finalize_leg_close / _record_partial_leg_close), THEN
            chase whatever qty (if any) is still left. If it was a
            plain fresh-open order with no leg attached at all, this is
            a brand new, entirely unexpected position — hand off to
            _register_and_close_orphan_leg instead.
          - Timed out with no resolution either way: order state is
            genuinely unknown. Don't guess — a wrong guess here risks
            either an untracked position (assumed cancelled, wasn't) or
            a double-close (assumed filled and chased anyway, but it
            was actually still resting and the chase's fill was the
            second execution). Escalate loudly and leave leg alone
            rather than auto-chase it.
        """
        close_side = None
        if leg is not None:
            close_side = "SELL" if leg.open_side == "BUY" else "BUY"

        fill = None
        if dropped_client_oid is not None:
            fill = self._oms.request_cancel_and_await(dropped_client_oid, timeout=15.0)

            if fill is None:
                logger.critical(
                    f"[GridBot][{reason}] Cancel-and-await UNKNOWN resolution "
                    f"for order [{dropped_client_oid[:8]}] (dropped by trail) "
                    f"after 15s — order state is genuinely unknown, not "
                    f"assumed cancelled. "
                    + (f"Leg #{leg.leg_id} is NOT being auto-chased to avoid "
                       f"a possible double-close — check manually." if leg
                       is not None else
                       "No leg was attached to this cell (it was a fresh, "
                       "still-unfilled open order), so there's no double-"
                       "close risk, but its true fate is still unknown — "
                       "check manually for a possible untracked position.")
                )
                self._alerter.send(
                    f"🚨 Trail-drop cancel: UNKNOWN resolution for order "
                    f"[{dropped_client_oid[:8]}] after 15s — manual check "
                    f"needed."
                    + (f" Leg #{leg.leg_id} withheld from auto-chase."
                       if leg is not None else "")
                )
                return

        if fill is not None and (fill.is_filled or fill.filled_qty > 0):
            if leg is not None and close_side is not None:
                if fill.is_filled:
                    self._finalize_leg_close(
                        leg, fill, close_side, reason,
                        log_verb="filled during trail-cancel",
                        alert_label="Leg closed (raced trail-cancel)")
                    return   # leg fully closed by its own original order
                self._record_partial_leg_close(leg, fill, close_side, reason)
                # falls through below to chase whatever's left of leg.qty
            elif leg is None:
                # Fresh open order, no leg ever attached — filled anyway
                # during what was meant to be a clean cancel. A real,
                # entirely new position now exists that nothing expected.
                if dropped_order_side is None:
                    # Shouldn't happen — GridEngine always sets this
                    # alongside dropped_client_oid (see _trail_up/
                    # _trail_down) — but fail loudly rather than guess a
                    # side for a real position.
                    logger.critical(
                        f"[GridBot][{reason}] TRAIL-DROP CANCEL RACE: order "
                        f"[{dropped_client_oid[:8]}] filled "
                        f"{fill.filled_qty:.4f} @ {fill.avg_price:.2f} with "
                        f"no known side — cannot safely register as a leg. "
                        f"Manual check required immediately."
                    )
                    self._alerter.send(
                        f"🚨 Trail-drop cancel race: order "
                        f"[{dropped_client_oid[:8]}] filled with unknown "
                        f"side — manual check required immediately."
                    )
                    return
                self._register_and_close_orphan_leg(
                    fill, dropped_order_side, reason)
                return

        if leg is not None and leg.qty > 0:
            self._chase_close_leg_worker(leg, reason)

    def _register_and_close_orphan_leg(self, fill: "FillEvent",
                                        open_side: str, reason: str) -> None:
        """
        A cell trail-up/trail-down dropped was a plain fresh-open order
        (no closes_leg_id — nothing was tracking it as any leg's
        closer) that filled for real in the narrow race window between
        deciding to cancel it and that cancel actually landing. That
        fill opened a brand new, real position nothing anywhere was
        expecting. Register it as a genuine OpenLeg — same shape
        GridEngine._on_fill's own opening path produces — and hand it
        straight to the chase worker to flatten back out, rather than
        leave a stray untracked position sitting on the book. Loud by
        design (CRITICAL + alert): this path firing at all means a
        trail-drop's cancel lost its race, which should be rare enough
        that every occurrence deserves a human's attention regardless
        of how cleanly it's handled automatically from here.
        """
        leg_id = None
        db_write_failed = False
        if self._store is not None:
            ok, result = self._store.execute_with_retry(lambda: self._store.open_leg(
                open_side=open_side, open_price=fill.avg_price,
                qty=fill.filled_qty, opened_ts=time.time(),
                opened_level_idx=-1, open_fee=fill.fee,
            ))
            if ok:
                leg_id = result
            else:
                db_write_failed = True
                logger.error(
                    f"[GridBot][{reason}] DB open_leg error after retries "
                    f"for orphaned trail-drop fill: {result}", exc_info=result,
                )
        if leg_id is None:
            with self._engine._lock:
                self._engine._local_leg_seq -= 1
                leg_id = self._engine._local_leg_seq
            if db_write_failed:
                logger.critical(
                    f"[GridBot][{reason}] UNTRACKED LEG (trail-drop race): "
                    f"open_leg DB write failed after retries for {open_side} "
                    f"{fill.filled_qty:.4f} @ {fill.avg_price:.2f} (local leg "
                    f"#{leg_id}). Tracked in-memory for now; will be silently "
                    f"lost from the ledger at the next rebuild/restart unless "
                    f"fixed manually first."
                )
        new_leg = OpenLeg(leg_id=leg_id, open_side=open_side,
                           open_price=fill.avg_price, qty=fill.filled_qty,
                           opened_ts=time.time(), opened_level_idx=-1,
                           open_fee=fill.fee)
        with self._engine._lock:
            self._engine._open_legs[leg_id] = new_leg
            self._engine._chasing_leg_ids.add(leg_id)
        logger.critical(
            f"[GridBot][{reason}] TRAIL-DROP CANCEL RACE: a dropped cell's "
            f"order filled for real during cancellation — {open_side} "
            f"{fill.filled_qty:.4f} @ {fill.avg_price:.2f}. Registered as "
            f"leg #{leg_id}; handing to chase now to flatten it back out."
        )
        self._alerter.send(
            f"⚠️ Trail-drop cancel race: order filled anyway — {open_side} "
            f"{fill.filled_qty:.4f} @ {fill.avg_price:.2f}. Registered as "
            f"leg #{leg_id}, closing via chase now."
        )
        self._chase_close_leg_worker(new_leg, reason)

    def _chase_close_leg_worker(self, leg: "OpenLeg", reason: str) -> None:
        """
        Close a leg that trailing dropped from the cell grid, without
        paying the market/taker cost immediately — trailing is routine
        drift, not an emergency, so it doesn't warrant one (unlike
        _emergency_halt's stop-loss liquidation).

        Repeats up to leg_chase_max_attempts times: POST_ONLY at the
        current best bid/ask on the closing side (joining the touch,
        not resting back at the old cell boundary — price has already
        trailed away from that boundary, that's WHY this leg is here),
        left resting for leg_chase_wait_s via OrderRequest.maker_timeout_s
        (overriding OMS's default 30s grid-cell maker timeout — see
        OrderRequest.maker_timeout_s; currently the two happen to match,
        but this stays explicit since they're conceptually independent
        knobs). Each unfilled attempt reprices against a fresh quote
        before retrying. Falls back to a market close via
        _liquidate_leg_at_market once the attempt budget is exhausted —
        EXCEPT for reason="rebuild_reprice_pending" (a zero-candidate leg
        from reconcile_open_legs, see GridBot._rebuild_grid and the
        "2026-08-03 16:35 incident" GRID_CONFIG comment block): that case
        re-checks trend_risk and the zero-candidate dwell cap first, and
        only liquidates if either has actually run out — otherwise it's
        released back into the unmanaged wait for the next rebuild to
        re-evaluate.

        leg.qty IS the remaining-to-close amount throughout — a partial
        fill on any attempt (delivered as a CANCELLED FillEvent carrying
        the real filled_qty when that attempt's maker timeout cancels
        the unfilled rest — see _handle_order_update) shrinks it in
        place via _record_partial_leg_close, durably, before the next
        attempt or the market fallback ever reads it. Nothing here
        reposts more than what's actually still open.
        """
        close_side    = "SELL" if leg.open_side == "BUY" else "BUY"
        max_attempts  = int(self._cfg.get("leg_chase_max_attempts", 3))
        wait_s        = float(self._cfg.get("leg_chase_wait_s", 30.0))
        tag           = f"[{reason}]"

        for attempt in range(1, max_attempts + 1):
            if leg.qty <= 0:
                # A prior attempt's partial fill already closed it in full.
                logger.info(
                    f"[GridBot]{tag} Leg #{leg.leg_id} fully closed via "
                    f"accumulated chase partials — nothing left to chase"
                )
                return

            bid, ask, _mid = _price_cache.get_l1()
            if bid is None or ask is None:
                logger.warning(
                    f"[GridBot]{tag} Leg #{leg.leg_id} chase attempt "
                    f"{attempt}/{max_attempts}: no live L1 quote — retrying shortly"
                )
                time.sleep(min(wait_s, 5.0))
                continue

            # Join the touch on the closing side — never cross (POST_ONLY
            # would reject/requote if it did).
            price = bid if close_side == "BUY" else ask
            qty   = leg.qty   # snapshot: what's still open right now
            req = OrderRequest.limit_maker(
                side=close_side, qty=qty, price=price,
                instrument=INSTRUMENT, purpose="leg_chase",
                maker_timeout_s=wait_s)
            logger.info(
                f"[GridBot]{tag} Leg #{leg.leg_id} chase attempt "
                f"{attempt}/{max_attempts}: POST_ONLY {close_side} "
                f"{qty:.4f} @ {price:.2f}, resting up to {wait_s:.0f}s"
            )
            self._oms.submit(req)
            fill = self._oms.wait_fill(req.client_oid, timeout=wait_s + 10.0)

            if fill and fill.is_filled:
                self._finalize_leg_close(leg, fill, close_side, reason,
                                          log_verb="chase-filled",
                                          alert_label="Leg closed via chase")
                return

            if fill and fill.filled_qty > 0:
                # Cancelled at the maker timeout, but part of it filled
                # first — shrink leg.qty by exactly that much before the
                # next attempt sizes its order.
                self._record_partial_leg_close(leg, fill, close_side, reason)
                logger.info(
                    f"[GridBot]{tag} Leg #{leg.leg_id} chase attempt "
                    f"{attempt} partially filled {fill.filled_qty:.4f} @ "
                    f"{fill.avg_price:.2f} — {leg.qty:.4f} BTC remaining, "
                    f"repricing and retrying"
                )
                continue

            logger.info(
                f"[GridBot]{tag} Leg #{leg.leg_id} chase attempt "
                f"{attempt}/{max_attempts} unfilled — repricing and retrying"
            )

        if leg.qty <= 0:
            return  # closed in full across partials during the loop above

        if reason == "rebuild_reprice_pending":
            urgent_threshold = self._cfg.get(
                "reconcile_urgent_trend_risk",
                self._cfg.get("stop_raise_urgent_trend_risk", 0.80),
            )
            zc_max_dwell = self._cfg.get("reconcile_zero_candidate_max_dwell_s", 900.0)
            trend_risk = 0.0
            if self._stop_scorer is not None:
                _bid, _ask, mid = _price_cache.get_l1()
                if mid is not None:
                    trend_risk = self._stop_scorer.compute_trend_risk(
                        mid, self._effective_trend_regime(), self._last_trend_slope_pct
                    )
            # GRACE_DWELL_COORDINATION_2026_08_06: same fix as the
            # reconcile_open_legs liquidation check above and for the same
            # reason — this elapsed-time clock started at the SAME
            # zero_candidate_since anchor, which already includes however
            # long the leg spent waiting out its pre-chase grace before
            # this chase was ever dispatched. Without adding the grace
            # period back in here too, a leg that used up most of its
            # grace time before finally getting chased would arrive here
            # with almost no dwell budget left, defeating the point of a
            # separate post-chase dwell. In practice this branch is only
            # reached once a leg's own grace has already elapsed (see
            # _rebuild_grid), so this mainly matters if grace and dwell
            # are ever retuned to overlapping magnitudes.
            pre_chase_grace_s = self._cfg.get("zero_candidate_pre_chase_grace_s", 0.0)
            zc_dwell_cap = pre_chase_grace_s + (
                zc_max_dwell * max(0.0, 1.0 - trend_risk / urgent_threshold)
                if urgent_threshold > 0 else 0.0
            )
            started = self._leg_zero_candidate_since.get(leg.leg_id, time.time())
            elapsed = time.time() - started

            if trend_risk < urgent_threshold and elapsed < zc_dwell_cap:
                # Neither the trend nor the dwell cap has actually run out
                # — release it back into the unmanaged wait rather than
                # paying for a market close. It stays fully tracked in
                # _open_legs with no resting order; the next rebuild's
                # reconcile_open_legs re-checks it (a real candidate cell
                # may simply reappear by then) and re-applies the same
                # cap check against the current trend_risk.
                with self._engine._lock:
                    self._engine._chasing_leg_ids.discard(leg.leg_id)
                logger.info(
                    f"[GridBot]{tag} Leg #{leg.leg_id} chase exhausted "
                    f"({max_attempts} attempts, {wait_s:.0f}s each) but "
                    f"trend_risk={trend_risk:.2f} < {urgent_threshold:.2f} "
                    f"and only {elapsed:.0f}s/{zc_dwell_cap:.0f}s of the "
                    f"dwell cap used — holding with no resting order, "
                    f"re-evaluated at the next rebuild"
                )
                return

            logger.warning(
                f"[GridBot]{tag} Leg #{leg.leg_id} chase exhausted AND "
                f"(trend_risk={trend_risk:.2f} >= {urgent_threshold:.2f} "
                f"or dwell cap {zc_dwell_cap:.0f}s used up after "
                f"{elapsed:.0f}s) — falling back to a market close for "
                f"the {leg.qty:.4f} BTC remaining"
            )
            self._liquidate_leg_at_market(leg, reason=f"{reason}_chase_exhausted")
            return

        logger.warning(
            f"[GridBot]{tag} Leg #{leg.leg_id} not filled after "
            f"{max_attempts} POST_ONLY attempt(s) ({wait_s:.0f}s each) — "
            f"falling back to a market close for the {leg.qty:.4f} BTC "
            f"remaining"
        )
        self._liquidate_leg_at_market(leg, reason=f"{reason}_chase_exhausted")

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

        new_params = self._auto_tuner.compute(trend_regime=self._effective_trend_regime())
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
                            mid, self._effective_trend_regime(), self._last_trend_slope_pct
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

            # ── TrendSignal gate integration ──────────────────────────────────
            # When TrendSignal is DOWN two protections activate (if enabled):
            #
            # 1. Threshold multiplier: effective threshold is lowered by
            #    trend_gate_down_threshold_mult, making it easier to suppress
            #    buys when the broader trend is bearish.
            #
            # 2. OUTSIDE_RANGE block: when regime is DOWN *and* price has
            #    fallen below the grid lower bound (the bot is fully long and
            #    most vulnerable to a continued decline), ALL new counter-buys
            #    are blocked regardless of score.  This directly addresses
            #    the SL2 pattern (2026-07-22 22:05–22:17): drift-shift moved
            #    the grid up, BTC reversed hard, price went OUTSIDE_RANGE
            #    below the grid into a DOWN regime, 5 buys accumulated
            #    (0.0300 BTC) → stop triggered.
            regime     = _bot_ref._last_trend_regime
            trend_note = ""
            threshold  = _bot_ref._get_threshold()

            if _bot_ref._cfg.get("trend_gate_enabled", True):
                # See _effective_trend_regime() for why this isn't just
                # `regime` — INSUFFICIENT_DATA shouldn't silently read as
                # "not DOWN".
                via_fallback = (regime == TrendSignal.REGIME_NODATA)
                effective_regime = _bot_ref._effective_trend_regime()

                is_down = (effective_regime == TrendSignal.REGIME_DOWN)
                if is_down:
                    # Protection 2: OUTSIDE_RANGE block
                    params = _bot_ref._params
                    if (params is not None
                            and _bot_ref._cfg.get(
                                "trend_gate_outside_range_block_on_down", True)
                            and mid_now < params.lower):
                        logger.info(
                            f"[BuyGate] SUPPRESS (TrendSignal DOWN"
                            f"{' [carried through INSUFFICIENT_DATA]' if via_fallback else ''}"
                            f" + OUTSIDE_RANGE: "
                            f"mid={mid_now:.2f} < lower={params.lower:.2f}) "
                            f"score={score:.4f}"
                        )
                        return False

                    # Protection 1: threshold multiplier
                    mult = _bot_ref._cfg.get("trend_gate_down_threshold_mult", 0.60)
                    old_thr = threshold
                    threshold = threshold * mult
                    trend_note = (
                        f" [DOWN"
                        f"{' (carried through INSUFFICIENT_DATA)' if via_fallback else ''}"
                        f": threshold {old_thr:.4f}×{mult:.2f}={threshold:.4f}]"
                    )

            allow = score < threshold
            # Always log at INFO so gate decisions are visible in the daily
            # log and calibration history is auditable.
            logger.info(
                f"[BuyGate] score={score:.4f} threshold={threshold:.4f}"
                f"{trend_note} → {'ALLOW' if allow else 'SUPPRESS'}"
            )
            return allow

        def _sell_gate() -> bool:
            """
            Return True (allow sell) or False (suppress sell). Mirror of
            _buy_gate above for the opposite side (2026-08-04) — see the
            SellGate GRID_CONFIG comment block for why this deliberately
            does NOT carry a score/threshold component the way _buy_gate
            does (no symmetric upside score exists, and reusing the
            downtrend-only one would silently under-react to a rally).
            """
            if not _bot_ref._cfg.get("sell_gate_enabled", True):
                return True
            if not _bot_ref._cfg.get("trend_gate_enabled", True):
                return True

            mid_now = _price_cache.get_mid()
            if mid_now is None:
                return True

            effective_regime = _bot_ref._effective_trend_regime()
            is_up = (effective_regime == TrendSignal.REGIME_UP)
            if not is_up:
                return True

            # Protection 1: OUTSIDE_RANGE-above block — mirrors _buy_gate's
            # DOWN + below-lower block: UP regime + mid already above the
            # grid's whole upper bound (fully short, most exposed to a
            # continued rise) blocks ALL new sell-side orders regardless
            # of anything else.
            params = _bot_ref._params
            if (params is not None
                    and _bot_ref._cfg.get("trend_gate_outside_range_block_on_up", True)
                    and mid_now > params.upper):
                logger.info(
                    f"[SellGate] SUPPRESS (TrendSignal UP + OUTSIDE_RANGE: "
                    f"mid={mid_now:.2f} > upper={params.upper:.2f})"
                )
                return False

            # Protection 2: confirmed-uptrend block — reuses the SAME
            # empirical drift_shift_trend_* evidence the catch-up feature
            # uses, rather than a second new asymmetric score. See
            # GridBot._uptrend_confirmed_now.
            if (_bot_ref._cfg.get("sell_gate_block_on_confirmed_uptrend", True)
                    and _bot_ref._uptrend_confirmed_now()):
                logger.info(
                    "[SellGate] SUPPRESS (TrendSignal UP + confirmed-uptrend "
                    "shift pattern)"
                )
                return False

            logger.info(f"[SellGate] mid={mid_now:.2f} → ALLOW")
            return True

        # ── Blue-green: match any pending handoff snapshot against the grid
        # we're about to build, BEFORE placing a single order ─────────────────
        # This has to happen here (using new_params, not self._pending_handoff_snapshot's
        # own old params) because the auto-tuner may have recomputed slightly
        # different levels than the peer we're inheriting from had — matching
        # is done by price against THIS grid's actual level prices, not by
        # blindly trusting the snapshot's own range. See _match_handoff_levels().
        #
        # skip_indices tells GridEngine.start() which levels already have a
        # live order (inherited) and must NOT get a fresh order placed on top
        # of it — that duplicate-order bug (placing a brand new order, then
        # immediately overwriting the in-memory reference to it with the old
        # order's identity when the snapshot was applied) was the original,
        # most severe bug in this feature.
        #
        # HANDOFF PARAM CONTINUITY: if the snapshot's params are "close enough"
        # to what the auto-tuner just computed, prefer the snapshot's own
        # lower/upper/spacing over the freshly-computed ones. This is the key
        # to avoiding a 0%-match on a minor mid-price drift between blue's
        # freeze and green's rebuild. A small price movement (e.g. $17 on BTC)
        # produces a new `lower` that's offset by that same amount, shifting
        # every level price — even though the grid structure is identical.
        # Reusing the snapshot's params keeps every level price identical to
        # blue's, matching 100% of open orders.
        #
        # "Close enough" means:
        #   - same level count (same number of orders to keep track of)
        #   - lower/upper shift ≤ handoff_anchor_max_spacing_drift × spacing
        #     (a pure mid-price drift, not a genuine structural retune)
        #   - spacing within handoff_anchor_max_spacing_pct of the snapshot's
        #     (auto-tuner has not meaningfully changed the grid structure)
        # If any condition fails, the fresh params are used and mismatching
        # orders are treated as orphans (cancel + recreate), as before.
        restore_plan: Dict[int, dict] = {}
        orphans:      List[dict]      = []
        if self._pending_handoff_snapshot is not None:
            snap_p = self._pending_handoff_snapshot.get("params", {})
            snap_levels   = int(snap_p.get("levels",  0))
            snap_lower    = float(snap_p.get("lower",  0.0))
            snap_upper    = float(snap_p.get("upper",  0.0))
            snap_spacing  = float(snap_p.get("spacing", 0.0))

            anchor_drift  = self._cfg.get("handoff_anchor_max_spacing_drift", 2.0)
            anchor_sp_pct = self._cfg.get("handoff_anchor_max_spacing_pct", 0.20)

            can_anchor = (
                snap_levels > 0
                and snap_spacing > 0
                and new_params.levels == snap_levels
                and abs(new_params.lower - snap_lower) <= anchor_drift * snap_spacing
                and abs(new_params.spacing - snap_spacing) / snap_spacing <= anchor_sp_pct
            )

            if can_anchor:
                # Reuse the snapshot's level prices verbatim.  Still recompute
                # stop_price and notional_per_level from the fresh auto-tuner
                # result so safety and sizing are always current.
                anchored_params = GridParams(
                    lower             = snap_lower,
                    upper             = snap_upper,
                    levels            = snap_levels,
                    spacing           = snap_spacing,
                    stop_price        = new_params.stop_price,
                    notional_per_level= new_params.notional_per_level,
                )
                logger.info(
                    f"[GridBot] Handoff anchor: reusing peer's level prices "
                    f"range=[{snap_lower:.2f},{snap_upper:.2f}] spacing={snap_spacing:.2f} "
                    f"(auto-tuner had [{new_params.lower:.2f},{new_params.upper:.2f}] "
                    f"spacing={new_params.spacing:.2f} — within drift tolerance). "
                    f"stop={new_params.stop_price:.2f} notional={new_params.notional_per_level:.2f} "
                    f"kept fresh from auto-tuner."
                )
                new_params = anchored_params

            restore_plan, orphans = self._match_handoff_levels(new_params)

        # Carry chasing-leg protection across the engine-instance swap.
        # A stray-leg chase (_chase_close_leg_worker) runs in its own
        # background thread and does NOT pause for a rebuild — it can still
        # be in flight against the OLD engine when THIS rebuild fires for a
        # completely unrelated reason (boundary breach, regime change,
        # one-sided detector, periodic interval; none of those wait on a
        # chase either). Without this, the new GridEngine() below starts
        # with an empty _chasing_leg_ids and has no way to know a leg it's
        # about to reconcile is already being independently worked by that
        # live thread — reconcile_open_legs would assign it a fresh closer
        # cell (or liquidate it) right out from under the chase, and
        # whichever one fills second finds the leg already gone: a genuine
        # double-close / untracked-position race. Read under the OLD
        # engine's own lock since _chasing_leg_ids is mutated from other
        # threads too (chase completion in _finalize_leg_close /
        # _liquidate_leg_at_market, which always act on whatever
        # self._engine currently is — safe on their own, since the
        # underlying ledger is the shared DB, not this specific instance;
        # it's only reconcile's fresh-assignment side that needed this fix).
        carried_chasing_leg_ids: set = set()
        # ORPHAN_COOLDOWN_CARRY_2026_08_07: _last_orphan_rebuild_request
        # (orphan_leg_rebuild_cooldown_s's cooldown clock) lives on the
        # GridEngine instance, but a real (non-dead-band-skipped) rebuild
        # replaces that instance right here — a plain GridEngine(...) call
        # with no carry-over resets the clock to 0.0 on the new object.
        # Confirmed in GEN00040_GREEN 2026-08-07: leg #875 zero-candidate
        # 07:22:09-08:05:19, warnings otherwise correctly spaced ~15s apart
        # (the cooldown holding), except immediately after every completed
        # rebuild (07:51:15, 07:52:16, 07:53:31, ...) where a second
        # warning fired in the same second — the fresh engine had no memory
        # of the one that had just fired moments earlier. Only matters right
        # at a rebuild boundary (one extra dead-band-skipped attempt, not a
        # busy loop), but it's the same class of bug as the original
        # incident, so carry it across the same way carried_chasing_leg_ids
        # already does below.
        carried_last_orphan_rebuild_request: float = 0.0
        if self._engine is not None:
            with self._engine._lock:
                carried_chasing_leg_ids = set(self._engine._chasing_leg_ids)
                carried_last_orphan_rebuild_request = \
                    self._engine._last_orphan_rebuild_request

        self._engine = GridEngine(
            params=new_params, oms=self._oms,
            instrument=INSTRUMENT, config=self._cfg,
            store=self._store,
            buy_gate_fn=_buy_gate,
            sell_gate_fn=_sell_gate,
            trend_confirm_fn=self._trend_confirm,
            stray_leg_fn=self._chase_close_leg,
            uptrend_confirmed_fn=self._uptrend_confirmed_now,
            downtrend_confirmed_fn=self._downtrend_confirmed_now,
            down_shift_record_fn=self._record_down_shift)
        self._engine._last_orphan_rebuild_request = \
            carried_last_orphan_rebuild_request
        if carried_chasing_leg_ids:
            # update(), not assignment — defensive against a concurrent
            # chase-completion discard landing in this same window; never
            # clobber, only add. Under the lock for the same reason as the
            # capture above.
            with self._engine._lock:
                self._engine._chasing_leg_ids.update(carried_chasing_leg_ids)
            logger.info(
                f"[GridBot] Carried {len(carried_chasing_leg_ids)} in-flight "
                f"chase(s) across rebuild: leg(s) "
                f"{sorted(carried_chasing_leg_ids)} — excluded from this "
                f"rebuild's reconciliation and the new engine's one-sided "
                f"detector until their chase finishes."
            )

        # ── Reconcile every still-open leg against the grid we just built ────
        # _open_legs was just seeded from DB inside GridEngine.__init__ above,
        # so this covers BOTH a same-process rebuild (the legs the OLD engine
        # was tracking a moment ago) and, incidentally, a plain process
        # restart (since the ledger lives in the DB, not memory). Legs a
        # handoff snapshot already re-attached (restore_plan) are excluded —
        # see _apply_handoff_restore's closes_leg_id restoration below. Legs
        # under an in-flight stray-leg chase are excluded too (see
        # carried_chasing_leg_ids above) — reconcile is not the right tool
        # for something _chase_close_leg_worker is already actively closing.
        already_handled_leg_ids = {
            snap_lv["closes_leg_id"] for snap_lv in restore_plan.values()
            if snap_lv.get("closes_leg_id") is not None
        } | carried_chasing_leg_ids
        # Same trend_risk score already used to gate the dead-band stop
        # raise above — reused here (not recomputed) so a leg's misfit
        # tolerance and the stop's raise aggressiveness always agree on
        # "does this look like noise or a genuine move" within one rebuild.
        reconcile_trend_risk = 0.0
        if self._stop_scorer is not None:
            reconcile_trend_risk = self._stop_scorer.compute_trend_risk(
                mid, self._effective_trend_regime(), self._last_trend_slope_pct
            )
        leg_assignments, legs_to_liquidate, still_pending_leg_ids, zero_candidate_legs = (
            self._engine.reconcile_open_legs(
                exclude_indices=set(restore_plan.keys()),
                already_handled_leg_ids=already_handled_leg_ids,
                trend_risk=reconcile_trend_risk,
                effective_atr=new_params.effective_atr,
                pending_since=self._leg_no_fit_since,
                zero_candidate_since=self._leg_zero_candidate_since,
            )
        )
        # Maintain the dwell dict across rebuilds: start the clock the
        # first time a leg is flagged (never reset it while it stays
        # pending — that's what confirm_s measures against), and drop any
        # leg that's no longer pending (either it recovered to a clean fit,
        # or reconcile_open_legs just confirmed-liquidated it).
        now_ts = time.time()
        for leg_id in still_pending_leg_ids:
            self._leg_no_fit_since.setdefault(leg_id, now_ts)
        for leg_id in list(self._leg_no_fit_since.keys()):
            if leg_id not in still_pending_leg_ids:
                del self._leg_no_fit_since[leg_id]

        # Same bookkeeping for the zero-candidate dwell map, plus: once a
        # leg has been zero-candidate for at least
        # zero_candidate_pre_chase_grace_s, kick off the stray-leg chase
        # for it (a shot at a decent price before it settles into the
        # unmanaged post-chase wait) — mirrors exactly how
        # _trail_up/_trail_down hand a dropped leg to the same chase
        # mechanism. Guarded on "not already chasing" so a leg that
        # recovers, goes stranded again, and is still mid-chase from
        # earlier doesn't get a second chase spawned on top of the first.
        #
        # zero_candidate_pre_chase_grace_s (2026-08-06, sized against the
        # REPRICE_UNDERCOUNT_2026_08_05 recovery-time backtest — see
        # GRID_CONFIG comment block): default 0.0 preserves the exact
        # legacy behavior (chase immediately, first rebuild a leg is seen
        # zero-candidate). Set > 0 to hold a newly-stranded leg with NO
        # resting order for up to that many seconds first — same
        # "unmanaged, re-checked at the next rebuild" mechanics the
        # POST-chase-exhaustion dwell cap already uses below, just applied
        # BEFORE the chase starts instead of only after it fails. If price
        # re-enters a real cell before the grace period elapses, the leg
        # is picked up by the ordinary (non-zero-candidate) reconcile path
        # next rebuild and never needs the chase at all. Same urgent-
        # trend_risk bypass as the post-chase dwell cap: a genuinely
        # urgent move skips the grace and chases immediately regardless.
        zero_candidate_ids = {leg.leg_id for leg in zero_candidate_legs}
        pre_chase_grace_s = self._cfg.get("zero_candidate_pre_chase_grace_s", 0.0)
        urgent_threshold = self._cfg.get(
            "reconcile_urgent_trend_risk",
            self._cfg.get("stop_raise_urgent_trend_risk", 0.80),
        )
        for leg in zero_candidate_legs:
            first_seen = self._leg_zero_candidate_since.setdefault(leg.leg_id, now_ts)
            with self._engine._lock:
                already_chasing = leg.leg_id in self._engine._chasing_leg_ids
            if already_chasing:
                continue  # chase already in flight from an earlier rebuild
            grace_elapsed = now_ts - first_seen
            within_grace = (
                pre_chase_grace_s > 0.0
                and grace_elapsed < pre_chase_grace_s
                and reconcile_trend_risk < urgent_threshold
            )
            if within_grace:
                logger.info(
                    f"[GridBot] Leg #{leg.leg_id} zero-candidate on this "
                    f"rebuild (open={leg.open_price:.2f}) — within pre-chase "
                    f"grace ({grace_elapsed:.0f}s/{pre_chase_grace_s:.0f}s, "
                    f"trend_risk={reconcile_trend_risk:.2f}<"
                    f"{urgent_threshold:.2f}) — holding with no resting "
                    f"order, re-evaluated at the next rebuild."
                )
                continue
            with self._engine._lock:
                self._engine._chasing_leg_ids.add(leg.leg_id)
            logger.info(
                f"[GridBot] Leg #{leg.leg_id} zero-candidate on this "
                f"rebuild (open={leg.open_price:.2f}) — handing off to "
                f"stray-leg chase for a shot at a decent price before "
                f"settling into the unmanaged wait."
            )
            self._chase_close_leg(leg, reason="rebuild_reprice_pending")
        for leg_id in list(self._leg_zero_candidate_since.keys()):
            if leg_id in zero_candidate_ids:
                continue  # still dwelling, reported again this rebuild
            with self._engine._lock:
                still_chasing = leg_id in self._engine._chasing_leg_ids
            if still_chasing:
                # Excluded from THIS reconcile call via already_handled_leg_ids
                # (or carried_chasing_leg_ids on the next one) purely because
                # its chase is in flight — not because it resolved. Keep the
                # dwell-start time so the chase worker's own exhaustion check
                # (and reconcile, once the chase ends) measure from when it
                # first went stranded, not from whenever the chase happens
                # to finish.
                continue
            # Neither zero-candidate this rebuild nor mid-chase: it
            # genuinely resolved — recovered to a clean/buffered fit, or
            # was already confirmed-liquidated (by reconcile's cap-expiry
            # branch, or by the chase worker's own exhaustion fallback).
            del self._leg_zero_candidate_since[leg_id]

        # Persist right after this block finishes mutating the dict (both
        # the additions above and the deletions just above) — see
        # _persist_leg_zero_candidate_since() docstring.
        self._persist_leg_zero_candidate_since()

        self._engine.start(
            mid, skip_indices=set(restore_plan.keys()) | set(leg_assignments.keys())
        )

        if self._pending_handoff_snapshot is not None:
            self._apply_handoff_restore(restore_plan, orphans)
            self._pending_handoff_snapshot = None

        if leg_assignments:
            self._engine.apply_leg_reassignments(leg_assignments)
            logger.info(
                f"[GridBot] Re-anchored {len(leg_assignments)} open leg(s) to "
                f"the rebuilt grid"
            )

        if legs_to_liquidate:
            logger.info(
                f"[GridBot] rebuild_reprice: reconcile_open_legs flagged "
                f"{len(legs_to_liquidate)} leg(s) for market liquidation this "
                f"rebuild: {[(leg.leg_id, leg.open_side, round(leg.open_price, 2)) for leg in legs_to_liquidate]}"
            )
        for leg in legs_to_liquidate:
            self._liquidate_leg_at_market(leg, reason="rebuild_reprice")

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
        self._recovery_floor_timeout_alerted = False

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

        # Persist halt state — see _persist_halt_state()/_restore_halt_state()
        # docstrings. Without this, a restart or blue-green handoff while
        # halted (cooldown/recovery-floor wait, or a daily-loss circuit-
        # breaker halt) silently dropped it, and the new process resumed
        # trading immediately regardless of why we were halted.
        self._persist_halt_state()

    # ── Halt-state persistence (survives restart / blue-green handoff) ───────

    def _persist_halt_state(self) -> None:
        """
        Persist halt/circuit-breaker state to SQLite so a process restart or
        blue-green handoff that happens WHILE the bot is halted doesn't
        silently drop the halt and let the new process resume trading
        immediately.

        Previously _halted / _daily_loss_halted / _halt_time /
        _halt_stop_price / _restart_attempts / _last_restart_time lived only
        in memory. export_handoff_snapshot() also bails out with "no engine
        running" while halted (self._engine is None during a halt), writing
        no snapshot at all — so ANY restart during a halt (a redeploy, a
        crash + NSSM auto-restart, or even the documented manual /restart,
        which just launches a fresh process) silently reset it. A daily-loss
        halt is now cleared deliberately via /clear_halt instead of just by
        restarting — see _handle_clear_halt_command().

        Called from _emergency_halt() (new halt), _check_auto_restart() (on
        a successful auto-restart and whenever the attempt counter changes),
        and _handle_clear_halt_command() (manual clear).
        """
        if self._store is None:
            return
        self._store.set_meta("halt_active",           "1" if self._halted else "0")
        self._store.set_meta("halt_daily_loss",        "1" if self._daily_loss_halted else "0")
        self._store.set_meta("halt_time",              str(self._halt_time))
        self._store.set_meta("halt_stop_price",        str(self._halt_stop_price))
        self._store.set_meta("halt_restart_attempts",  str(self._restart_attempts))
        self._store.set_meta("halt_last_restart_time", str(self._last_restart_time))

    def _restore_halt_state(self) -> bool:
        """
        Restore halt state persisted by _persist_halt_state(). Called once
        from start(), before _rebuild_grid(). Returns True if the bot should
        stay halted — caller must skip _rebuild_grid() and let the main
        loop's normal `if self._halted: self._check_auto_restart()` path
        (see _run()) take over from here, exactly as it would have if this
        were the same process that halted rather than a fresh one.

        A restored daily-loss halt is intentionally NOT auto-resumed by
        _check_auto_restart() (that function already returns immediately
        when _daily_loss_halted is True) — it requires /clear_halt.
        """
        if self._store is None:
            return False
        if self._store.get_meta("halt_active") != "1":
            return False

        self._halted            = True
        self._daily_loss_halted = self._store.get_meta("halt_daily_loss") == "1"
        self._halt_time         = float(self._store.get_meta("halt_time") or time.time())
        self._halt_stop_price   = float(self._store.get_meta("halt_stop_price") or 0.0)
        self._restart_attempts  = int(self._store.get_meta("halt_restart_attempts") or 0)
        self._last_restart_time = float(self._store.get_meta("halt_last_restart_time") or 0.0)

        hours_halted = (time.time() - self._halt_time) / 3600.0
        logger.warning(
            f"[GridBot] Restored halt state from previous session: "
            f"halted {hours_halted:.1f}h ago at stop={self._halt_stop_price:.2f} "
            f"daily_loss_halted={self._daily_loss_halted} "
            f"restart_attempts={self._restart_attempts} — staying halted."
        )
        self._alerter.send(
            f"⚠️ Restart occurred while halted — halt state restored "
            f"({hours_halted:.1f}h so far, stop={self._halt_stop_price:.2f}).\n"
            + ("Daily-loss circuit breaker active — send /clear_halt to "
               "resume manually.\n" if self._daily_loss_halted else
               "Auto-restart checks resuming from where they left off.\n")
        )
        return True

    # ── Zero-candidate dwell persistence (survives restart) ──────────────────

    def _persist_leg_zero_candidate_since(self) -> None:
        """
        Persist `_leg_zero_candidate_since` to SQLite so the zero-candidate
        dwell clock survives a process restart instead of restarting from
        "just discovered" for every currently-stranded leg.

        Previously this lived only in memory, populated fresh on every
        process start (see __init__). _rebuild_grid()'s zero-candidate
        bookkeeping treats "not yet in this dict" as "first time seen —
        kick off a chase attempt", so a cold restart with N stranded legs
        made ALL N look brand new simultaneously and fire one chase
        attempt each, all within the same rebuild — regardless of how
        much dwell budget any of them had already used up before the
        restart. The restart itself was the trigger, not any market
        event. See the 2026-08-04 "8 legs closed via chase" incident.

        Called once per _rebuild_grid() call, right after that method's
        zero-candidate bookkeeping block finishes mutating the dict (both
        the additions and the deletions) — mirrors _persist_halt_state()'s
        "persist right after the state changes" placement.
        """
        if self._store is None:
            return
        self._store.set_meta(
            "leg_zero_candidate_since",
            json.dumps({str(k): v for k, v in self._leg_zero_candidate_since.items()}),
        )

    def _restore_leg_zero_candidate_since(self) -> None:
        """
        Restore `_leg_zero_candidate_since` persisted by
        _persist_leg_zero_candidate_since(). Called once from start(),
        before the first _rebuild_grid() call (same timing as
        _restore_halt_state(), and for the same reason — this dict has to
        be populated before _rebuild_grid()'s bookkeeping runs its
        "already dwelling from a prior rebuild" check, or the restore is
        a no-op).

        A leg_id persisted here that no longer exists in the DB ledger
        (e.g. it was force-liquidated by a previous session, or
        reset_state wiped the open-leg ledger but happened to leave this
        key behind) is harmless — reconcile_open_legs only ever looks up
        zero_candidate_since for legs it currently has open; a stale key
        with no matching leg is simply never read and gets pruned the
        next time _rebuild_grid()'s bookkeeping loop runs (any leg_id in
        the dict but not in zero_candidate_ids that rebuild is deleted).
        """
        if self._store is None:
            return
        raw = self._store.get_meta("leg_zero_candidate_since")
        if not raw:
            return
        try:
            restored = json.loads(raw)
        except Exception as e:
            logger.error(
                f"[GridBot] Failed to parse persisted leg_zero_candidate_since "
                f"— starting with an empty dwell map: {e}"
            )
            return
        self._leg_zero_candidate_since = {int(k): float(v) for k, v in restored.items()}
        if self._leg_zero_candidate_since:
            logger.info(
                f"[GridBot] Restored zero-candidate dwell state for "
                f"{len(self._leg_zero_candidate_since)} leg(s) from previous "
                f"session: {sorted(self._leg_zero_candidate_since.keys())}"
            )

    # ── Auto-restart ──────────────────────────────────────────────────────────

    def _check_daily_loss_limit(self) -> bool:
        """
        Check today's realized net loss (HKT day) against daily_loss_limit_usd.
        Called once per STATUS_INTERVAL_S from the main _run() loop.
        If limit exceeded: fires _emergency_halt(), sets _daily_loss_halted=True
        to block auto-restart (requires manual /restart), sends Telegram alert.
        Returns True if the circuit breaker just fired.
        """
        if not self._cfg.get("daily_loss_limit_enabled", True):
            return False
        limit = self._cfg.get("daily_loss_limit_usd", 50.0)
        if limit <= 0:
            return False
        today     = self._store.get_daily()
        daily_net = today.get("net_pnl_usd", 0.0)
        if daily_net >= 0 or abs(daily_net) < limit:
            return False
        mid = _price_cache.get_mid() or 0.0
        logger.warning(
            f"[GridBot] Daily loss circuit breaker: today net={daily_net:+.4f} USD "
            f"exceeds limit -{limit:.2f} USD — halting"
        )
        self._daily_loss_halted = True
        self._emergency_halt(mid)
        self._alerter.send(
            f"🛑 Daily loss limit hit: {daily_net:+.2f} USD today "
            f"(limit: -{limit:.2f} USD)\n"
            f"Bot halted — manual /restart required to resume."
        )
        return True

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
        # Daily loss circuit breaker blocks auto-restart — requires manual /restart
        if self._daily_loss_halted:
            logger.info("[AutoRestart] Blocked: daily loss circuit breaker active. "
                        "Send /restart via launcher to resume manually.")
            return

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

        max_halt_hours = self._cfg.get("auto_restart_max_halt_hours", 8.0)
        timeout_bypass = max_halt_hours > 0 and hours_halted >= max_halt_hours

        if mid <= recovery_floor and not timeout_bypass:
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

        if mid <= recovery_floor and timeout_bypass:
            # Condition 2 (price-recovery-floor) is a hard-timeout bypass here:
            # once the floor decay has been capped (auto_restart_recovery_floor_min_atr)
            # for long enough, it can never ease further on its own — see the
            # 2026-07-28 GRID_CONFIG comment. Conditions 3/4 below are NOT
            # bypassed: the market must still show a genuinely tight, flat-or-
            # rising band before we restart, we just stop requiring that band
            # be within reach of the OLD stop level.
            if not self._recovery_floor_timeout_alerted:
                self._recovery_floor_timeout_alerted = True
                logger.warning(
                    f"[AutoRestart] auto_restart_max_halt_hours ({max_halt_hours:.1f}h) "
                    f"reached at mid={mid:.2f} (still below recovery_floor="
                    f"{recovery_floor:.2f}, halt_stop={self._halt_stop_price:.2f}). "
                    f"Skipping the price-recovery gate — still requires stability "
                    f"conditions (tight range + flat/rising) to actually restart."
                )
                self._alerter.send(
                    f"⏱️ Halted {hours_halted:.1f}h — price recovery floor gate "
                    f"timed out (>{max_halt_hours:.0f}h) and is now bypassed.\n"
                    f"mid={mid:.2f} vs halt_stop={self._halt_stop_price:.2f} "
                    f"(recovery_floor={recovery_floor:.2f})\n"
                    f"Still waiting on range-stability before auto-restart — "
                    f"or send /restart to resume manually now."
                )

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
        self._persist_halt_state()
        # NOTE: condition 2 above only requires mid > recovery_floor (halt_stop_price
        # minus a configurable ATR buffer) — NOT mid > halt_stop_price itself. The
        # previous log line here read "above stop={halt_stop_price}", which was
        # misleading: it implied mid had recovered above the old halt stop when it
        # may still be below it (by design, within recovery_buffer_mult × ATR).
        # Make that explicit so log readers aren't misled about what was checked.
        # (The subsequent _rebuild_grid() stop-proximity guard is what actually
        # protects against arming a new stop too close to current mid.)
        below_halt_stop  = mid < self._halt_stop_price
        below_recovery_floor = mid <= recovery_floor   # only possible via timeout_bypass here
        if below_recovery_floor:
            recovery_note = (
                f"mid={mid:.2f} < recovery_floor={recovery_floor:.2f} but "
                f"auto_restart_max_halt_hours timeout reached "
                f"(halted {hours_halted:.1f}h) — price-recovery gate bypassed"
            )
        elif below_halt_stop:
            recovery_note = (
                f"mid={mid:.2f} < halt_stop={self._halt_stop_price:.2f} but > "
                f"recovery_floor={recovery_floor:.2f} "
                f"(halted {hours_halted:.1f}h, decayed floor)"
            )
        else:
            recovery_note = f"mid={mid:.2f} >= halt_stop={self._halt_stop_price:.2f}"
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
            + (f"⏱️ price-recovery gate timed out after {hours_halted:.1f}h — "
               f"restarting below recovery floor {recovery_floor:.0f} "
               f"(halt_stop {self._halt_stop_price:.0f})\n"
               if below_recovery_floor else
               f"⚠️ still below halt stop {self._halt_stop_price:.0f} (buffered recovery)\n"
               if below_halt_stop else "")
            + f"Rebuilding grid..."
        )

        self._halted = False
        self._last_restart_time = now
        # Reset the stop-loss guard so it can fire again on the new grid
        self._sl_guard = None
        self._persist_halt_state()

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

    # ── /handoff Telegram command ─────────────────────────────────────────────

    def _handle_handoff_command(self) -> str:
        """
        Telegram /handoff command: initiate a blue-green handoff from within
        the running process.

        This is the safe way to deploy a new version without position
        liquidation.  The sequence after this command is received:

          1. This handler calls export_handoff_snapshot() on the TgPoller
             thread — it freezes order activity, snapshots GridEngine state,
             writes the JSON to SQLite, stops TgPoller (409-prevention), and
             arms _handoff_stop so stop() won't liquidate.
          2. This handler then sets _handoff_shutdown_requested so the main
             _run() loop will call stop() on the main thread on its next
             iteration (same pattern as _start_handoff_watcher — stop() must
             run on the main thread, not the TgPoller thread).
          3. The operator starts the new (green) process:
               python grid_bot.py --role green
             It finds the snapshot, acquires the lock, and resumes trading
             with the inherited position and orders.

        Returns a confirmation string — note TgPoller may not deliver it
        because stop_nowait() is called inside export_handoff_snapshot() and
        the underlying HTTP request may already be in flight when the poller
        stops.  That is expected and harmless.
        """
        if self._engine is None:
            return "⚠️ /handoff: no grid engine running — nothing to hand off."

        logger.info("[GridBot] /handoff command received — initiating handoff")

        try:
            ok = self.export_handoff_snapshot()
        except Exception as e:
            logger.error(f"[GridBot] /handoff: export_handoff_snapshot failed: {e}")
            return f"❌ /handoff: snapshot export failed: {e}"

        if not ok:
            return "⚠️ /handoff: snapshot export returned False — no engine running?"

        # Signal the main thread to shut down (stop() must run on main thread).
        # _handoff_stop is already True (set inside export_handoff_snapshot),
        # so stop() will skip position liquidation.
        self._handoff_shutdown_requested.set()

        # Note: TgPoller was stopped by stop_nowait() inside
        # export_handoff_snapshot(), so this reply string may not be delivered
        # before the process exits.  That is expected — the /deploy Telegram
        # alert will confirm the outgoing process stopped cleanly, and the
        # incoming process's startup alert confirms the handoff succeeded.
        return "✅ Handoff snapshot written — shutting down. Start green process now."

    # ── /clear_halt Telegram command ─────────────────────────────────────────

    def _handle_clear_halt_command(self) -> str:
        """
        Telegram /clear_halt: explicitly clear a halt (including a
        daily-loss circuit-breaker halt) and resume trading immediately.

        Now that halt state is persisted across restarts (see
        _persist_halt_state()/_restore_halt_state()), simply starting a new
        process via /restart correctly PRESERVES a halt instead of
        accidentally dropping it — so this command is the deliberate,
        auditable way to actually clear one. In particular a daily-loss
        halt, which is designed to require manual intervention, now
        requires this rather than just a bare restart.
        """
        if not self._halted:
            return "ℹ️ /clear_halt: bot is not currently halted."

        was_daily_loss = self._daily_loss_halted
        logger.warning(
            f"[GridBot] /clear_halt: manually clearing halt "
            f"(daily_loss_halted was {was_daily_loss})"
        )
        self._halted            = False
        self._daily_loss_halted = False
        self._restart_attempts  = 0
        self._last_restart_time = time.time()
        self._sl_guard = None
        self._persist_halt_state()
        self._rebuild_grid()
        return (
            "✅ Halt cleared manually"
            + (" (was a daily-loss circuit-breaker halt)" if was_daily_loss else "")
            + " — grid rebuilding now."
        )

    # ── /pnl Telegram command ────────────────────────────────────────────────

    def _handle_pnl_command(self) -> str:
        """
        Return a concise PnL summary:
          - Cumulative all-time net
          - Today's net (HKT day)
          - SL losses today
          - Reprice/trail losses today (rebuild_reprice(_pending), trail_up,
            trail_down — legs cut loose by grid range drift, a different
            risk category from a stop-loss halt; see record_fill's
            is_reprice_loss param)
          - Estimated funding accrued (all-time, from DB)
          - Last 7 daily rows
        """
        acc   = self._store.get_accumulated()
        today = self._store.get_daily()
        week  = self._store.get_recent_daily(7)

        cum_net      = acc.get("net_pnl",   0.0)
        cum_gross    = acc.get("gross_pnl", 0.0)
        cum_fees     = acc.get("fees",      0.0)
        cum_cycles   = acc.get("cycle_count", 0)
        cum_sl       = acc.get("sl_gross",  0.0)
        cum_reprice  = acc.get("reprice_gross", 0.0)
        cum_reprice_n = acc.get("reprice_count", 0)

        today_net    = today.get("net_pnl_usd",   0.0)
        today_gross  = today.get("gross_pnl_usd", 0.0)
        today_fees   = today.get("fees_usd",      0.0)
        today_cycles = today.get("cycle_count",   0)
        today_sl     = today.get("sl_gross_usd",  0.0)
        today_reprice = today.get("reprice_gross_usd", 0.0)
        today_reprice_n = today.get("reprice_count", 0)

        funding_usd  = self._get_funding_accrued()
        net_after_funding = cum_net + funding_usd   # funding already negative if cost

        def _s(v: float) -> str:
            return f"{v:+.2f}"

        lines = [
            "💰 PnL Summary",
            "",
            "All-time",
            f"  Net:      {_s(cum_net)} USD",
            f"  Gross:    {_s(cum_gross)} USD",
            f"  Fees:     {_s(-abs(cum_fees))} USD",
            f"  SL loss:  {_s(cum_sl)} USD",
            f"  Reprice/trail loss: {_s(cum_reprice)} USD (×{cum_reprice_n})",
            f"  Funding:  {_s(funding_usd)} USD (est.)",
            f"  Net+fund: {_s(net_after_funding)} USD",
            f"  Cycles:   {cum_cycles}",
            "",
            f"Today ({today.get('hkt_date','—')} HKT)",
            f"  Net:      {_s(today_net)} USD",
            f"  Gross:    {_s(today_gross)} USD",
            f"  Fees:     {_s(-abs(today_fees))} USD",
            f"  SL loss:  {_s(today_sl)} USD",
            f"  Reprice/trail loss: {_s(today_reprice)} USD (×{today_reprice_n})",
            f"  Cycles:   {today_cycles}",
        ]

        if week:
            lines += ["", "Last 7 days (HKT date | net | cycles)"]
            for row in week:
                lines.append(
                    f"  {row['hkt_date']}  "
                    f"{_s(row['net_pnl_usd']):>8}  "
                    f"{row['cycle_count']:>4} cyc"
                )

        return "\n".join(lines)

    # ── /help Telegram command ───────────────────────────────────────────────

    def _handle_help_command(self) -> str:
        """Return a summary of all available Telegram commands."""
        return (
            "📖 Available commands\n"
            "\n"
            "Bot commands (grid_bot.py)\n"
            "  /status    — Grid position, PnL, stop-score, TrendSignal\n"
            "  /pnl       — Cumulative PnL, today's PnL, funding, 7-day history\n"
            "  /handoff   — Hibernate: save state and exit without liquidating\n"
            "               (start new process with /restart to resume)\n"
            "  /clear_halt — Manually clear a stop-loss/daily-loss halt and\n"
            "               resume trading now. Halt state now survives a\n"
            "               restart (see 2026-07-29 fix) — /restart alone no\n"
            "               longer clears a halt, including a daily-loss\n"
            "               circuit-breaker halt. Use this instead.\n"
            "\n"
            "Launcher commands (grid_bot_launcher.py)\n"
            "  /restart   — Start new bot process (picks up /handoff snapshot,\n"
            "               or resumes a persisted halt if one is active)\n"
            "  /pstatus   — Process status: PID, uptime, last 10 log lines\n"
            "  /kill      — Emergency stop: SIGTERM → clean shutdown + liquidation\n"
            "\n"
            "Typical deployment flow\n"
            "  1\u20e3  /handoff  → bot saves state, exits\n"
            "  2\u20e3  /restart  → new bot resumes with same position\n"
            "\n"
            "Recovering from a halt\n"
            "  If halted (stop-loss cooldown, or daily-loss circuit breaker),\n"
            "  restarting the process no longer clears it — send /clear_halt\n"
            "  instead once you've confirmed it's safe to resume."
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
        daily_reprice   = today.get("reprice_gross_usd", 0.0)
        daily_reprice_n = today.get("reprice_count", 0)
        acc_net     = acc["net_pnl"]
        acc_gross   = acc["gross_pnl"]
        acc_fees    = acc["fees"]          # stored as negative in DB
        acc_cycles  = acc["cycle_count"]
        acc_sl      = acc.get("sl_gross", 0.0)
        acc_sl_n    = acc.get("sl_count", 0)
        acc_reprice   = acc.get("reprice_gross", 0.0)
        acc_reprice_n = acc.get("reprice_count", 0)

        # ── TradeProfit: pure grid-cycle capture, stripped of the one-off
        # SL / chase-reprice events, so it's visible on its own whether
        # ordinary buy-low/sell-high cycling is working even on a day where
        # SL/Reprice swamp the headline Gross/Net numbers.
        # IMPORTANT: per the daily_pnl schema (net_pnl_usd = gross_pnl_usd +
        # fees_usd), fees_usd is NOT a component of gross_pnl_usd — it's
        # applied once, separately, to get net. sl_gross_usd/reprice_gross_usd
        # ARE pre-fee subsets of gross_pnl_usd (see their "_gross_usd" naming
        # and the daily_pnl table comment). So TradeProfit here is still
        # gross/pre-fee — do NOT subtract fees_usd again on top, that would
        # double-count fees that are already excluded from gross_pnl_usd.
        #   Gross = TradeProfit + SL + Reprice   (fees not part of this)
        #   Net   = Gross + Fees                 (fees applied once, here)
        daily_trade_profit = today["gross_pnl_usd"] - daily_sl - daily_reprice
        acc_trade_profit   = acc_gross - acc_sl - acc_reprice

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
            reprice_tag = (f"  🔁Reprice={row.get('reprice_gross_usd', 0.0):+.4f}×{row.get('reprice_count', 0)}"
                           if row.get("reprice_count", 0) > 0 else "")
            hist_lines.append(
                f"  {sign} {row['hkt_date']}  "
                f"net={row['net_pnl_usd']:+.4f}  "
                f"cycles={row['cycle_count']}"
                f"{sl_tag}"
                f"{reprice_tag}"
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
            f"  • Gross: `{today['gross_pnl_usd']:+.4f}`"
            + (f" _(incl. 🚨SL `{daily_sl:+.4f}` ×{daily_sl_n})_" if daily_sl_n > 0 else "")
            + (f" _(incl. 🔁Reprice `{daily_reprice:+.4f}` ×{daily_reprice_n})_" if daily_reprice_n > 0 else "")
            + f"  Fees: `{today['fees_usd']:+.4f}`"
            + (f" _({abs(today['fees_usd']) / today['gross_pnl_usd'] * 100:.1f}% of gross)_"
               if today['gross_pnl_usd'] != 0 else ""),
            f"  {_e(daily_trade_profit)} Trade P/L: `{daily_trade_profit:+.4f}`"
            + " _(ex SL/Reprice, gross/pre-fee — pure grid-cycle capture)_",
            f"  • Cycles today: `{today['cycle_count']}`",
            "",
            "━━━━━━━━━━━━━━━━━━━━━",
            "*3️⃣  Accumulated PnL* (all-time from DB)",
            f"  {_e(acc_net)}  Net:   `{acc_net:+.4f} USD` (`{_pct(acc_net)}`)",
            f"  • Gross realised: `{acc_gross:+.4f} USD`"
            + (f" _(incl. 🚨SL `{acc_sl:+.4f}` ×{acc_sl_n})_" if acc_sl_n > 0 else "")
            + (f" _(incl. 🔁Reprice `{acc_reprice:+.4f}` ×{acc_reprice_n})_" if acc_reprice_n > 0 else ""),
            f"  • Total fees:     `{acc_fees:+.4f} USD`"
            + (f" _({abs(acc_fees) / acc_gross * 100:.1f}% of gross)_"
               if acc_gross != 0 else ""),
            f"  {_e(acc_trade_profit)} Trade P/L: `{acc_trade_profit:+.4f} USD`"
            + " _(ex SL/Reprice, gross/pre-fee — pure grid-cycle capture)_",
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
                f"_Affects: BuyGate threshold and min grid levels_"
            )
            logger.info(
                f"[TrendSignal] ⚠️  Regime change: {prev} → {regime} "
                f"(Telegram alert sent)"
            )

        self._last_trend_regime = regime
        if regime != TrendSignal.REGIME_NODATA:
            self._last_confirmed_trend_regime = regime
        self._last_trend_slope_pct = result.get("slope_pct", 0.0)

        # Adjust min_grid_levels based on the new regime.  This is the
        # mechanism that breaks the SpacingAutoTuner deadlock: a regime
        # change immediately sets a regime-appropriate level count and
        # requests a grid rebuild, so the next retune uses the right levels
        # without waiting for the 24h SpacingAutoTuner evaluation cycle.
        self._spacing_tuner.update_levels_from_trend(regime)

        return result

    def _effective_trend_regime(self) -> str:
        """
        The regime every DOWN-gated protection should actually read, instead
        of the raw live TrendSignal output.

        _last_trend_regime can be INSUFFICIENT_DATA for reasons that have
        nothing to do with the market having calmed down — a cold start, or
        rebuilding trust after a feed gap (which can take ~26h even after a
        gap of just a couple of hours, since trend_signal_min_history_h needs
        that much fresh contiguous data regardless of how short the gap that
        triggered the reset was). Every call site that gates on "== DOWN" was
        found reading the raw value directly, which meant a routine WS gap
        would silently and fully lift every one of these protections for as
        long as the rebuild took — exactly when the bot might still be in the
        downtrend that caused the gap in the first place:

          - _buy_gate()'s OUTSIDE_RANGE block and threshold multiplier
          - _sell_gate()'s OUTSIDE_RANGE-above block (2026-08-04)
          - GridAutoTuner.compute()'s ATR-widen skip-on-downtrend
          - StopScoreCalculator.compute_trend_risk()'s regime-risk component

        This carries the last CONFIRMED (non-INSUFFICIENT_DATA) regime
        forward instead, matching how SpacingAutoTuner already holds
        min_grid_levels steady through INSUFFICIENT_DATA rather than
        resetting it — the same principle, applied consistently everywhere
        the regime feeds a protective decision.
        """
        if self._last_trend_regime == TrendSignal.REGIME_NODATA:
            return self._last_confirmed_trend_regime
        return self._last_trend_regime

    def _trend_confirm(self) -> int:
        """
        trend_confirm_fn passed to GridEngine (see its __init__ docstring).
        Called once per top-sell-triggered drift-shift decision, right
        before GridEngine would otherwise perform exactly one _trail_up.

        Records this event (a timestamp, pruned to
        drift_shift_trend_lookback_s) as evidence, then returns how many
        EXTRA shifts (beyond the usual 1) this event should perform, based
        on how many same-direction shifts already preceded it within that
        window. See the drift_shift_trend_* GRID_CONFIG comment block and
        the 2026-08-03 17:46-17:56 incident it documents: by the time of
        the FIRST forced chase-close eviction that day, FIVE consecutive
        top-sell-triggered shifts had already fired over the prior 58
        minutes — direct, already-logged evidence of a sustained move, not
        a one-off blip.

        This is the ONLY place _recent_up_shifts gains a new entry —
        _uptrend_confirmed_now() below reads the same list but never adds
        to it, so ticks that merely check gate status don't get counted as
        shifts themselves.
        """
        now = time.time()
        lookback = self._cfg.get("drift_shift_trend_lookback_s", 1800.0)
        self._recent_up_shifts = [t for t in self._recent_up_shifts if now - t <= lookback]
        prior_count = len(self._recent_up_shifts)
        self._recent_up_shifts.append(now)

        confirm_count = self._cfg.get("drift_shift_trend_confirm_count", 2)
        if prior_count >= confirm_count:
            extra = self._cfg.get("drift_shift_trend_catchup_extra", 1)
            logger.info(
                f"[GridBot] Confirmed uptrend: {prior_count} prior "
                f"same-direction shift(s) within {lookback:.0f}s (>= "
                f"{confirm_count}) — catching up {extra} extra shift(s) "
                f"this event"
            )
            return extra
        return 0

    def _uptrend_confirmed_now(self) -> bool:
        """
        Read-only check: is the drift_shift_trend_* confirmed-uptrend
        condition currently active, based purely on shifts _trend_confirm
        has already recorded? Never adds to _recent_up_shifts itself —
        safe to call every tick. Used by both _sell_gate (to decide whether
        to suppress) and its release condition in _run() (to decide when
        to stop suppressing) — the SAME check both places, so a suppressed
        level is never released just because a different signal happened
        to look fine.

        Note this uses a slightly different count than _trend_confirm's
        own "prior_count >= confirm_count" test (that one deliberately
        excludes the shift currently being decided, since it's asking
        "should THIS shift accelerate"; this one includes everything
        currently on record, since it's asking "is the trend confirmed
        right now") — both intentionally share the same lookback/
        confirm_count knobs, just applied for slightly different
        purposes.
        """
        now = time.time()
        lookback = self._cfg.get("drift_shift_trend_lookback_s", 1800.0)
        recent = [t for t in self._recent_up_shifts if now - t <= lookback]
        confirm_count = self._cfg.get("drift_shift_trend_confirm_count", 2)
        return len(recent) >= confirm_count

    def _record_down_shift(self) -> None:
        """
        down_shift_record_fn passed to GridEngine (see its __init__
        docstring). Called once per actual TRAIL DOWN, right as
        GridEngine._trail_down fires — the down-side mirror of
        _trend_confirm's recording half. Unlike _trend_confirm, this has
        no "extra shifts" catch-up decision to make (that feature only
        exists on the up side today) — it just records the evidence for
        _downtrend_confirmed_now to read back.
        """
        now = time.time()
        lookback = self._cfg.get("drift_shift_trend_lookback_s", 1800.0)
        self._recent_down_shifts = [
            t for t in self._recent_down_shifts if now - t <= lookback
        ]
        self._recent_down_shifts.append(now)

    def _downtrend_confirmed_now(self) -> bool:
        """
        Read-only check: mirror of _uptrend_confirmed_now for the down
        side — is the SAME repeated-shift evidence _record_down_shift has
        been collecting currently at/above drift_shift_trend_confirm_count
        within drift_shift_trend_lookback_s? Never adds to
        _recent_down_shifts itself — safe to call every tick.
        """
        now = time.time()
        lookback = self._cfg.get("drift_shift_trend_lookback_s", 1800.0)
        recent = [t for t in self._recent_down_shifts if now - t <= lookback]
        confirm_count = self._cfg.get("drift_shift_trend_confirm_count", 2)
        return len(recent) >= confirm_count


# ─────────────────────────────────────────────────────────────────────────────
# Entry point
# ─────────────────────────────────────────────────────────────────────────────

def _parse_args():
    parser = argparse.ArgumentParser(description="Grid trading bot")
    parser.add_argument(
        "--reset-state", action="store_true",
        help=(
            "Wipe persisted fill history, daily PnL, accumulated PnL, and "
            "open legs (grid_fills/daily_pnl/meta/open_legs tables) so the "
            "bot starts fresh, as if this were the very first launch. A "
            "timestamped backup of the db file is taken automatically "
            "before wiping. This does NOT close or affect any real "
            "position/orders on the exchange — those are independently "
            "reconciled on every startup regardless of this flag."
        ),
    )
    parser.add_argument(
        "--yes", "-y", action="store_true",
        help="Skip the interactive confirmation prompt for --reset-state "
             "(required when running non-interactively, e.g. under NSSM).",
    )
    parser.add_argument(
        "--role", choices=["blue", "green"], default="",
        help=(
            "Blue-green deployment role. Use 'blue' for the very first launch "
            "(nothing to hand off from yet), then use 'green' for every deploy "
            "after that — including the one after this one, and the one after "
            "that. There's no need to relaunch a successor as 'blue' once it's "
            "live: whichever process is currently running (regardless of which "
            "role it was started with) registers itself as the live process and "
            "watches for the next handoff request, so 'green' is always the "
            "right choice for a deploy. "
            "'green' requests a handoff from whoever is currently live on "
            "startup; both roles freeze+export their orders on request from a "
            "successor, then take over without cancelling or re-placing any "
            "inherited order. "
            "Omit entirely for standalone operation (no blue-green, normal "
            "start/stop, cancels all orders on startup like before)."
        ),
    )
    return parser.parse_args()


def main():
    args = _parse_args()

    role = args.role  # "blue", "green", or ""

    # Re-initialise logging with role suffix so blue/green write to separate files.
    global logger
    if role:
        logger = _init_logging(GRID_CONFIG, role=role)
        logger.info(f"[Main] Blue-green mode: role={role} pid={os.getpid()}")

    if args.reset_state:
        warning = (
            "\n" + "=" * 70 +
            "\n⚠️  --reset-state: this will PERMANENTLY clear all persisted\n"
            "   fill history, daily PnL, accumulated PnL, and open legs\n"
            "   for this bot.\n"
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

    bot = GridBot(GRID_CONFIG, reset_state=args.reset_state, role=role)

    def _shutdown(sig, frame):
        logger.info(f"[Main] Signal {sig} — shutting down")
        bot.stop()

    signal.signal(signal.SIGINT,  _shutdown)
    signal.signal(signal.SIGTERM, _shutdown)

    bot.start()

    # bot.start() is blocking — it only returns once stop() has fully run
    # (either from a signal handler or the handoff watcher). At that point
    # all trading activity has ceased and the DB connection is closed.
    # Explicitly stop the async log QueueListener so its non-daemon thread
    # doesn't keep the process alive indefinitely after main() returns.
    # (The atexit handler does the same thing, but atexit only fires once
    # Python's exit machinery can run — which it can't while a non-daemon
    # thread is still blocking, creating the deadlock we see in practice.)
    if _listener is not None:
        _listener.stop()


if __name__ == "__main__":
    main()
