# Bot Strategies Plan

**Capital:** 6 BTC (~USD 360k at current ~$60k price)  
**Target:** HKD 1,000/day (~USD 128/day, ~13% annual return on capital)  
**Constraint:** Minimal human intervention, automated 24/7  
**Exchange:** Crypto.com (maker fee 0.01%, taker fee 0.03% — derivatives tier)  
**Market:** Bear / low-volatility as of mid-2026

---

## Current Bot Status

| Bot | Strategy | Status | Problem |
|-----|----------|--------|---------|
| Bot 1 (`trading_bot.py`) | Swing trading on BTC/USD spot via signal detection | 🟡 In development | Win rate unstable — BTC spent ~53% of 2026 in consolidation, not clean trends. Signal edge is hard to sustain. |
| Bot 2 (`funding_arb_bot.py`) | Short BTCUSD-PERP to harvest funding payments, hedged by 6 BTC spot | 🔴 Paused | Bear market funding rates collapsed to ~4.46% annualised; over 93% of trading days fall below the 5% fee-breakeven threshold. Only profitable in bull markets. |
| Bot 3 (`grid_bot.py`) | Neutral futures grid on BTCUSD-PERP | 🟢 Built, paper testing | Captures oscillation in either direction; does not require price prediction. |

---

## Strategy Roadmap

### Phase 1 — Grid Bot (now) ✅

**Status:** Built and paper-testing (`grid_bot.py`)

**Why first:**
- Reuses existing WebSocket + OMS infrastructure, lowest incremental build cost
- Bear market with sideways consolidation is good grid territory — BTC oscillates rather than trends
- Profits from volatility in either direction, no directional bet needed
- Fast feedback loop: paper-trade for 1–2 weeks to validate cycle frequency vs fee cost

**Expected income:** Depends on number of completed cycles/day. With `notional_per_level=$500`, `spacing≈$50–100`, `maker_fee=0.01%`, each cycle nets ~$4–8 after fees. Need ~20–30 cycles/day across all levels to approach target. Achievable in active oscillating markets.

**Key risks:**
- Sustained directional breakout below grid lower bound → accumulating underwater long
- Mitigated by: hard stop-loss + auto-tuner that rebuilds grid when price exits range

**Next actions:**
- [ ] Paper trade ≥ 2 weeks, track actual cycles/day vs target
- [ ] Verify fee cost matches model (check `grid_trades.csv`)
- [ ] Go live with small `notional_per_level` (e.g. $200) to validate live OMS/fill path
- [ ] Scale up `notional_per_level` once live fills are confirmed correct

---

### Phase 2 — Statistical Arbitrage / Pairs Trading (next)

**Status:** Not started

**What it is:**  
Trade the spread between two cointegrated assets — BTC and ETH are historically one of the most cointegrated crypto pairs. The bot monitors the BTC/ETH price ratio, computes a z-score of the current spread vs its rolling mean, and enters a market-neutral trade when the spread deviates beyond a threshold:

- Spread too wide (BTC expensive vs ETH) → short BTC, long ETH
- Spread too narrow (ETH expensive vs BTC) → long BTC, short ETH
- Exit when spread reverts to mean

**Why this complements the grid:**
- Genuinely market-neutral: bear, bull, or sideways — does not matter
- Edge is structural (cointegration), not predictive — more durable than signal-based swing trading
- Adds a second uncorrelated income stream alongside the grid

**Key risks:**
- Cointegration breaking — can happen during major narrative shifts (regulatory crackdown on one asset, protocol hack, etc.)
- Requires trading ETH perps on Crypto.com alongside BTC perps
- Spread can gap on news events before the bot can exit

**Implementation notes:**
- Need to verify ETHUSD-PERP is available and liquid on Crypto.com
- Cointegration test (Engle-Granger or Johansen) should be run on at least 6–12 months of historical data before going live
- Open Python repos using CCXT for exchange access provide a starting framework
- Can reuse existing WebSocket + OMS infrastructure

**Next actions:**
- [ ] Pull 6–12 months of BTC/ETH perp OHLCV data from Crypto.com
- [ ] Run cointegration test; confirm pair is stationary (p < 0.05)
- [ ] Build `stat_arb_bot.py` with z-score entry/exit logic
- [ ] Backtest on historical spread data; target Sharpe > 1.5
- [ ] Paper trade ≥ 2 weeks alongside grid bot before going live

---

### Phase 3 — Delta-Neutral Market Making (later)

**Status:** Not started

**What it is:**  
An evolution of Bot 2. Instead of just holding a naked funding-rate short, overlay a grid of limit orders on top of the hedged position:

- **Spot leg:** Hold 6 BTC (already held) — long delta
- **Perp leg:** Short ~equivalent notional on BTCUSD-PERP — cancels delta → net-zero directional exposure
- **Grid overlay:** Place limit buy/sell orders around the mid-price on the perp; collect the bid-ask spread on every oscillation
- **Funding income:** Collect positive funding payments when rates are favourable (bull market)
- **Net income:** Grid spread income + funding income, near-zero directional risk

**Why strongest but last:**
- Combines three income sources (grid cycles + funding + potential basis)
- Historical data shows delta-neutral strategies returned +0.43% to +1.42%/month in 2025 with max drawdown of just 0.80%
- However, most complex to implement and manage: requires coordinating spot balance, perp position, and grid orders simultaneously
- Only worth building once Phase 1 (grid) and Phase 2 (stat arb) are stable and generating income

**Key risks:**
- Funding rate turns negative (bear market) — perp short costs money rather than earning it
- Grid breakout on the perp side while spot leg is illiquid
- Requires careful collateral management across spot + perp

**Next actions:**
- [ ] Complete Phase 1 + Phase 2 first
- [ ] When funding rates recover (next bull leg), re-activate Bot 2 manually to capture rates
- [ ] Design delta-neutral layer as extension of `grid_bot.py` + `funding_arb_bot.py`

---

### Bot 1 — Swing Trading (parallel, lower priority)

**Status:** 🟡 In development with separate AI collaboration

**Decision:** Do not abandon, but do not let it block Phase 1–2. The signal-quality problem is real but solvable over time. Swing trading has the highest upside of all strategies when signals are good — it should be kept alive in paper mode and iterated on in parallel.

**Trigger to re-prioritise:** If win rate in paper mode sustainably exceeds 55% over a 2-week window, re-evaluate allocating live capital.

---

## Income Model Summary

| Phase | Strategy | Est. monthly return | Market condition needed | Risk level |
|-------|----------|--------------------|-----------------------|------------|
| 1 | Grid bot | 3–8% on deployed capital | Oscillating / sideways | Medium |
| 2 | Stat arb | 2–5% on deployed capital | Any (market-neutral) | Low–Medium |
| 3 | Delta-neutral MM | 5–15% on deployed capital | Grid: any; Funding: bull | Low |
| Parallel | Swing trading | Highly variable | Clear trend | High |

**Combined Phase 1+2 target:** ~5–13%/month on deployed capital. At $10k deployed (20 grid levels × $500), that's $500–$1,300/month ≈ HKD 3,900–10,000/month — above the HKD 1,000/day target if upper range is achieved. Scale `notional_per_level` accordingly once validated.

---

## Key Shared Infrastructure

All bots reuse the same core components (currently copied inline per bot; to be refactored into shared modules once all bots are stable):

| Component | Used by | Notes |
|-----------|---------|-------|
| WebSocket (`_ReconnectingWS`) | All bots | Generation-tagged, DOA detection, stale watchdog |
| OMS (`oms.py` / inline) | Bot 1, Bot 3 | Paper + live REST+WS fill |
| Logging (HKT daily rotation) | All bots | Async queue, crash hook |
| AlertManager (Telegram) | All bots | Async queue with retry |
| `TRADING_MODE` pattern | Bot 2, Bot 3 | paper / uat / live — one line to change |

---

## Decision Log

| Date | Decision | Rationale |
|------|----------|-----------|
| 2026-06 | Pivot to grid bot as primary income strategy | Bot 1 win rate too unstable; Bot 2 funding rates too low in bear market |
| 2026-06 | Build `grid_bot.py` standalone (no shared imports) | Easier to deploy and debug independently; refactor later when all bots stable |
| 2026-06 | Add trailing up/down to grid bot | Complements auto-tuner: auto-tuner handles regime shifts (full rebuild), trailing handles smooth trend-following (1-level shift) |
| 2026-06 | Keep Bot 1 alive in paper mode | Signal-based swing trading has highest upside; not worth discarding, but not worth blocking grid bot development |
