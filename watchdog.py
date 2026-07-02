"""
watchdog.py — Process supervisor for grid_bot.py

Launches grid_bot.py as a subprocess and automatically restarts it if it
crashes, with exponential backoff to avoid rapid crash loops.

Features:
  - Exponential backoff: 5s → 10s → 20s → 40s → 60s (capped) between restarts
  - Fast-crash detection: halts if the bot dies within FAST_CRASH_WINDOW_SEC
    more than MAX_FAST_CRASHES times in a row (prevents thrashing on bad config)
  - Telegram crash alert: sends via the same keyring credentials as grid_bot.py
  - Clean shutdown: Ctrl+C / SIGTERM / SIGBREAK stop both watchdog and bot
  - Logs restart history to logs_grid/watchdog.log (same dir as grid_bot logs)

Credentials (same keyring entries used by grid_bot.py — no extra setup needed):
    cmdkey /generic:cdc_grid_tg_token  /user:token  /pass:YOUR_TG_TOKEN
    cmdkey /generic:cdc_grid_tg_chatid /user:chatid /pass:YOUR_CHAT_ID
  Or set env vars CDC_GRID_TG_BOT_TOKEN / CDC_GRID_TG_CHAT_ID as fallback.

Usage:
    python watchdog.py          # normal start; launches grid_bot.py

Windows Service (via NSSM) — point NSSM at watchdog.py, not grid_bot.py:
    nssm install GridBot "C:\\Python311\\python.exe" "E:\\path\\to\\watchdog.py"
    nssm set GridBot AppDirectory "E:\\path\\to\\"
"""

import json
import logging
import os
import signal
import subprocess
import sys
import time
from datetime import datetime, timezone, timedelta
from pathlib import Path
from typing import Optional

# ── Configuration ─────────────────────────────────────────────────────────────

# Seconds to wait before first restart after a crash
INITIAL_BACKOFF_SEC   = 5
# Maximum backoff cap (seconds)
MAX_BACKOFF_SEC       = 60
# If the bot dies within this many seconds of starting, it counts as a "fast crash"
FAST_CRASH_WINDOW_SEC = 30
# Halt the watchdog after this many consecutive fast crashes
MAX_FAST_CRASHES      = 5
# Exit codes that should NOT trigger a restart
# 0 = clean shutdown (SIGTERM from NSSM stop, Ctrl+C, etc.)
NO_RESTART_EXIT_CODES = {0}

# ── Paths ─────────────────────────────────────────────────────────────────────

HERE         = Path(__file__).parent.resolve()
BOT_SCRIPT   = HERE / "grid_bot.py"
LOG_DIR      = HERE / "logs_grid"
WATCHDOG_LOG = LOG_DIR / "watchdog.log"

# ── Logging ───────────────────────────────────────────────────────────────────

LOG_DIR.mkdir(parents=True, exist_ok=True)

logging.basicConfig(
    level   = logging.INFO,
    format  = "%(asctime)s [%(levelname)s] %(message)s",
    datefmt = "%Y-%m-%d %H:%M:%S",
    handlers= [
        logging.StreamHandler(sys.stdout),
        logging.FileHandler(str(WATCHDOG_LOG), encoding="utf-8"),
    ],
)
log = logging.getLogger("watchdog")

# ── Telegram helper (standalone — no imports from grid_bot.py) ────────────────

def _tg_send(token: str, chat_id: str, text: str) -> bool:
    """Send a plain Telegram message using HTML parse_mode. Returns True on success."""
    if not token or not chat_id:
        return False
    try:
        import urllib.request
        payload = json.dumps({
            "chat_id":                  chat_id,
            "text":                     text,
            "parse_mode":               "HTML",
            "disable_web_page_preview": True,
        }).encode()
        req = urllib.request.Request(
            f"https://api.telegram.org/bot{token}/sendMessage",
            data    = payload,
            headers = {"Content-Type": "application/json"},
            method  = "POST",
        )
        with urllib.request.urlopen(req, timeout=8) as resp:
            return json.loads(resp.read()).get("ok", False)
    except Exception as e:
        log.debug(f"[Watchdog] Telegram send failed: {e}")
        return False


def _load_tg_credentials() -> tuple:
    """
    Load Telegram token and chat_id from Windows Credential Manager (keyring),
    falling back to environment variables CDC_GRID_TG_BOT_TOKEN / CDC_GRID_TG_CHAT_ID.

    Uses the same keyring entries as grid_bot.py — no extra cmdkey setup needed:
        cdc_grid_tg_token  / user=token
        cdc_grid_tg_chatid / user=chatid

    Returns (token, chat_id) — either may be empty string if not configured.
    """
    token   = os.environ.get("CDC_GRID_TG_BOT_TOKEN", "")
    chat_id = os.environ.get("CDC_GRID_TG_CHAT_ID",   "")
    try:
        import keyring as _kr
        token   = _kr.get_password("cdc_grid_tg_token",  "token")  or token
        chat_id = _kr.get_password("cdc_grid_tg_chatid", "chatid") or chat_id
    except Exception:
        pass   # keyring not installed or no backend — fall through to env vars
    return (token or ""), (chat_id or "")


# ── Signal handling ───────────────────────────────────────────────────────────

_stop_requested = False
_child_proc: Optional[subprocess.Popen] = None
_shutdown_event = __import__("threading").Event()


def _handle_signal(signum, frame):
    global _stop_requested
    log.info(f"[Watchdog] Signal {signum} received — requesting clean shutdown")
    _stop_requested = True
    _shutdown_event.set()   # wake any _shutdown_event.wait() immediately
    if _child_proc and _child_proc.poll() is None:
        log.info("[Watchdog] Sending SIGTERM to grid_bot process")
        try:
            _child_proc.terminate()
        except Exception:
            pass


signal.signal(signal.SIGTERM, _handle_signal)
signal.signal(signal.SIGINT,  _handle_signal)
if hasattr(signal, "SIGBREAK"):     # Windows Ctrl+Break
    signal.signal(signal.SIGBREAK, _handle_signal)


# ── Main watchdog loop ────────────────────────────────────────────────────────

def run():
    global _child_proc, _stop_requested

    token, chat_id = _load_tg_credentials()

    cmd = [sys.executable, str(BOT_SCRIPT)]

    backoff          = INITIAL_BACKOFF_SEC
    restart_count    = 0
    fast_crash_count = 0

    log.info("=" * 60)
    log.info(f"[Watchdog] Starting — bot: {BOT_SCRIPT}")
    log.info(f"[Watchdog] Max fast crashes: {MAX_FAST_CRASHES}  "
             f"(window: {FAST_CRASH_WINDOW_SEC}s)")
    log.info(f"[Watchdog] Telegram alerts: "
             f"{'enabled' if token else 'disabled (no credentials)'}")
    log.info("=" * 60)

    while not _stop_requested:
        start_time = time.time()
        ts_hkt = datetime.now(timezone(timedelta(hours=8))).strftime("%Y-%m-%d %H:%M:%S HKT")

        log.info(f"[Watchdog] Launching grid_bot (attempt #{restart_count + 1})  {ts_hkt}")

        try:
            _child_proc = subprocess.Popen(
                cmd,
                cwd    = str(HERE),
                stdout = None,   # let output flow to console / NSSM log unchanged
                stderr = None,
            )
        except Exception as e:
            log.error(f"[Watchdog] Failed to launch grid_bot: {e}")
            _tg_send(token, chat_id,
                     f"💥 <b>GridBot watchdog: LAUNCH FAILED</b>\n<code>{e}</code>")
            time.sleep(backoff)
            backoff = min(backoff * 2, MAX_BACKOFF_SEC)
            continue

        # Block until the bot process exits
        try:
            exit_code = _child_proc.wait()
        except KeyboardInterrupt:
            _stop_requested = True
            break

        elapsed = time.time() - start_time
        restart_count += 1

        if _stop_requested:
            log.info(f"[Watchdog] grid_bot exited (code={exit_code}), "
                     f"stop requested — not restarting")
            break

        if exit_code in NO_RESTART_EXIT_CODES:
            log.info(f"[Watchdog] grid_bot exited cleanly (code={exit_code}) — not restarting")
            break

        # ── Crash detected ────────────────────────────────────────────────────
        log.warning(
            f"[Watchdog] grid_bot crashed!  exit_code={exit_code}  "
            f"uptime={elapsed:.0f}s  restart_count={restart_count}"
        )

        # Fast-crash detection
        if elapsed < FAST_CRASH_WINDOW_SEC:
            fast_crash_count += 1
            log.warning(
                f"[Watchdog] Fast crash #{fast_crash_count}/{MAX_FAST_CRASHES} "
                f"(died within {elapsed:.0f}s)"
            )
        else:
            fast_crash_count = 0         # healthy run — reset counter
            backoff = INITIAL_BACKOFF_SEC  # and reset backoff

        if fast_crash_count >= MAX_FAST_CRASHES:
            msg = (
                f"🛑 <b>GridBot watchdog: HALTED</b>\n"
                f"{fast_crash_count} consecutive fast crashes "
                f"(each &lt;{FAST_CRASH_WINDOW_SEC}s).\n"
                f"Manual intervention required."
            )
            log.error(f"[Watchdog] {MAX_FAST_CRASHES} fast crashes in a row — halting")
            _tg_send(token, chat_id, msg)
            break

        # Send Telegram crash alert before sleeping
        _tg_send(
            token, chat_id,
            f"⚠️ <b>GridBot watchdog: crashed</b>\n"
            f"exit_code={exit_code}  uptime={elapsed:.0f}s\n"
            f"Restarting in <b>{backoff}s</b> "
            f"(restart #{restart_count})\n"
            f"<i>Phase 1+2 warmup will run on next start.</i>"
        )

        log.info(f"[Watchdog] Waiting {backoff}s before restart...")
        # Event.wait() is reliably interruptible by Ctrl+C on Windows;
        # time.sleep() in a bare loop is not.
        _shutdown_event.wait(timeout=backoff)

        if not _stop_requested:
            backoff = min(backoff * 2, MAX_BACKOFF_SEC)
            log.info(f"[Watchdog] Next backoff will be {backoff}s")

    log.info("[Watchdog] Supervisor exiting.")
    _tg_send(token, chat_id,
             f"🔴 <b>GridBot watchdog stopped</b>\n"
             f"Total restarts: {restart_count}")


if __name__ == "__main__":
    run()
