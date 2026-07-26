"""
grid_bot_launcher.py — Remote deployment launcher for grid_bot.py
==================================================================

A lightweight, always-running process that manages the grid_bot.py lifecycle
and allows remote deployment via Telegram without physical access to the machine.

Commands
--------
  /restart   — Start a new grid_bot.py --role green process.
               If a previous process exported a handoff snapshot (via /handoff),
               the new process will inherit its position and orders seamlessly.
               If no snapshot exists, a normal cold start is performed.
  /pstatus   — Show process status: whether a bot process is running, its PID,
               uptime, and the last few lines of its log.
  /kill      — Emergency stop: send SIGTERM to the running bot process
               (triggers clean shutdown + position liquidation).

Typical deployment flow
-----------------------
  1. Send /handoff to the running bot     → bot exports snapshot and exits
  2. Send /restart to the launcher       → launcher starts new --role green
  3. New bot inherits position seamlessly

The launcher itself never stops unless you kill it manually (NSSM/systemd
handles keeping the launcher alive). It is intentionally separate from the
bot so that bot crashes, restarts, and upgrades do not affect the launcher.

Setup
-----
  1. Place this file alongside grid_bot.py
  2. Configure LAUNCHER_CONFIG below (or use the same keyring/env vars)
  3. Start: python grid_bot_launcher.py
  4. Keep it running via NSSM:
       nssm install GridBotLauncher python grid_bot_launcher.py
       nssm set GridBotLauncher AppDirectory E:\\code\\python\\grid-bot\\cdc
       nssm set GridBotLauncher AppRestartDelay 3000
       nssm start GridBotLauncher
"""

import logging
import os
import subprocess
import sys
import threading
import time
from pathlib import Path
from typing import Optional

import requests

# ── Configuration ──────────────────────────────────────────────────────────────
# Reads the same keyring / env vars as grid_bot.py so you don't need to
# store credentials in two places.

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
            pass
    return os.environ.get(env_var, "")

LAUNCHER_CONFIG = {
    # Telegram credentials — shared with grid_bot.py
    "telegram_bot_token": _secret("cdc_grid_tg_token",  "token",  "CDC_GRID_TG_BOT_TOKEN"),
    "telegram_chat_id":   _secret("cdc_grid_tg_chatid", "chatid", "CDC_GRID_TG_CHAT_ID"),

    # Path to grid_bot.py (defaults to same directory as this file)
    "grid_bot_script": str(Path(__file__).parent / "grid_bot.py"),

    # Python interpreter to use (defaults to the one running this script)
    "python_executable": sys.executable,

    # How long to wait after /handoff before allowing /restart (seconds).
    # Gives the outgoing process time to flush its DB writes before the
    # incoming process starts reading the snapshot.
    "restart_delay_s": 3,

    # Number of log lines to show in /status
    "status_log_lines": 10,

    # Polling interval for Telegram getUpdates (seconds, Telegram long-poll)
    "poll_timeout_s": 30,

    # How long between launcher heartbeat log lines (seconds)
    "heartbeat_interval_s": 3600,
}

# ── Logging ────────────────────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
    handlers=[
        logging.StreamHandler(sys.stdout),
        logging.FileHandler("grid_bot_launcher.log", encoding="utf-8"),
    ],
)
log = logging.getLogger("Launcher")


# ── Telegram helpers ────────────────────────────────────────────────────────────

class TelegramClient:
    """Minimal Telegram Bot API client for the launcher."""

    _API = "https://api.telegram.org"

    def __init__(self, token: str, chat_id: str, poll_timeout: int = 30):
        self._token    = token
        self._chat_id  = chat_id
        self._timeout  = poll_timeout
        self._offset   = 0            # getUpdates long-poll offset
        self._http_timeout = poll_timeout + 10

    def send(self, text: str) -> None:
        """Send a message to the configured chat."""
        if not self._token or not self._chat_id:
            log.warning("[TG] Token/chat_id not configured — message not sent")
            return
        try:
            resp = requests.post(
                f"{self._API}/bot{self._token}/sendMessage",
                json={"chat_id": self._chat_id, "text": text},
                timeout=10,
            )
            if not resp.ok:
                log.warning(f"[TG] sendMessage failed: {resp.text[:200]}")
        except Exception as e:
            log.warning(f"[TG] sendMessage error: {e}")

    def poll(self) -> list:
        """
        Long-poll for updates. Returns a list of (command, message_text) tuples
        for messages from the allowed chat that start with '/'.
        Blocks for up to poll_timeout seconds if no updates arrive.
        """
        if not self._token:
            time.sleep(self._timeout)
            return []
        try:
            resp = requests.get(
                f"{self._API}/bot{self._token}/getUpdates",
                params={
                    "offset":          self._offset,
                    "timeout":         self._timeout,
                    "allowed_updates": ["message"],
                },
                timeout=self._http_timeout,
            )
            if not resp.ok:
                log.warning(f"[TG] getUpdates failed: {resp.text[:200]}")
                time.sleep(5)
                return []
            data = resp.json()
        except Exception as e:
            log.warning(f"[TG] getUpdates error: {e}")
            time.sleep(5)
            return []

        commands = []
        for update in data.get("result", []):
            self._offset = update["update_id"] + 1
            msg = update.get("message", {})
            # Only accept messages from the configured chat
            if str(msg.get("chat", {}).get("id", "")) != str(self._chat_id):
                continue
            text = msg.get("text", "").strip()
            if text.startswith("/"):
                # Strip bot username suffix (e.g. /restart@MyBot → /restart)
                cmd = text.split()[0].split("@")[0].lower()
                commands.append(cmd)
        return commands


# ── Bot process management ─────────────────────────────────────────────────────

class BotProcessManager:
    """Manages the grid_bot.py child process."""

    def __init__(self, script: str, python: str):
        self._script  = script
        self._python  = python
        self._process: Optional[subprocess.Popen] = None
        self._started_at: Optional[float] = None
        self._lock = threading.Lock()

    @property
    def is_running(self) -> bool:
        with self._lock:
            return self._process is not None and self._process.poll() is None

    @property
    def pid(self) -> Optional[int]:
        with self._lock:
            if self._process is not None:
                return self._process.pid
        return None

    @property
    def uptime_s(self) -> Optional[float]:
        if self._started_at is None or not self.is_running:
            return None
        return time.time() - self._started_at

    def start(self) -> str:
        """
        Start grid_bot.py --role green.
        Returns a status string describing what happened.
        """
        with self._lock:
            if self._process is not None and self._process.poll() is None:
                return f"⚠️ Bot is already running (pid={self._process.pid}). Send /kill first."

            if not Path(self._script).exists():
                return f"❌ Script not found: {self._script}"

            try:
                self._process = subprocess.Popen(
                    [self._python, self._script, "--role", "green"],
                    cwd=str(Path(self._script).parent),
                )
                self._started_at = time.time()
                log.info(f"[BotManager] Started grid_bot.py (pid={self._process.pid})")
                return f"✅ Bot started (pid={self._process.pid})"
            except Exception as e:
                log.error(f"[BotManager] Failed to start bot: {e}")
                return f"❌ Failed to start bot: {e}"

    def kill(self) -> str:
        """Send SIGTERM to the running bot process (triggers clean shutdown)."""
        with self._lock:
            if self._process is None or self._process.poll() is not None:
                return "⚠️ No bot process is running."
            pid = self._process.pid
            try:
                self._process.terminate()
                log.info(f"[BotManager] Sent SIGTERM to pid={pid}")
                return f"✅ SIGTERM sent to pid={pid} — bot will shut down cleanly."
            except Exception as e:
                log.error(f"[BotManager] Failed to terminate pid={pid}: {e}")
                return f"❌ Failed to terminate pid={pid}: {e}"

    def status_lines(self) -> str:
        """Return a human-readable status string."""
        if self.is_running:
            uptime = self.uptime_s or 0
            h, rem = divmod(int(uptime), 3600)
            m, s   = divmod(rem, 60)
            return f"🟢 Running (pid={self.pid}, uptime={h}h{m:02d}m{s:02d}s)"
        else:
            rc = self._process.poll() if self._process else None
            if rc is None:
                return "⚫ Not started"
            return f"🔴 Stopped (last exit code={rc})"


# ── Launcher main loop ─────────────────────────────────────────────────────────

class Launcher:

    def __init__(self, cfg: dict):
        self._cfg  = cfg
        self._tg   = TelegramClient(
            token        = cfg["telegram_bot_token"],
            chat_id      = cfg["telegram_chat_id"],
            poll_timeout = cfg["poll_timeout_s"],
        )
        self._bot  = BotProcessManager(
            script = cfg["grid_bot_script"],
            python = cfg["python_executable"],
        )
        self._stop = threading.Event()

    def run(self) -> None:
        log.info("[Launcher] Starting — listening for Telegram commands")
        self._tg.send("🚀 Grid bot launcher started. Commands: /restart /pstatus /kill")
        last_heartbeat = time.time()

        while not self._stop.is_set():
            # Heartbeat log
            if time.time() - last_heartbeat > self._cfg["heartbeat_interval_s"]:
                log.info(f"[Launcher] Heartbeat — bot status: {self._bot.status_lines()}")
                last_heartbeat = time.time()

            # Poll Telegram for commands
            try:
                commands = self._tg.poll()
            except Exception as e:
                log.warning(f"[Launcher] Poll error: {e}")
                time.sleep(5)
                continue

            for cmd in commands:
                log.info(f"[Launcher] Received command: {cmd}")
                self._dispatch(cmd)

    def _dispatch(self, cmd: str) -> None:
        if cmd == "/restart":
            self._cmd_restart()
        elif cmd == "/pstatus":
            self._cmd_status()
        elif cmd == "/kill":
            self._cmd_kill()
        else:
            # Ignore unknown commands (may be intended for the bot's own poller)
            log.debug(f"[Launcher] Ignoring unknown command: {cmd}")

    def _cmd_restart(self) -> None:
        delay = self._cfg["restart_delay_s"]
        if self._bot.is_running:
            self._tg.send(
                f"⚠️ Bot is already running (pid={self._bot.pid}).\n"
                f"Send /handoff to the bot first, wait for it to stop, then /restart."
            )
            return

        self._tg.send(f"⏳ Starting grid_bot.py in {delay}s...")
        time.sleep(delay)
        result = self._bot.start()
        self._tg.send(result)

    def _cmd_status(self) -> None:
        bot_status = self._bot.status_lines()

        # Show tail of the most recent log file
        log_lines = ""
        try:
            log_dir = Path(self._cfg["grid_bot_script"]).parent / "logs_grid"
            logs = sorted(log_dir.glob("grid_bot_gen*.log"))
            if logs:
                latest = logs[-1]
                with open(latest, "r", encoding="utf-8", errors="replace") as f:
                    all_lines = f.readlines()
                tail = "".join(all_lines[-self._cfg["status_log_lines"]:])
                log_lines = f"\n\nLast {self._cfg['status_log_lines']} lines of {latest.name}:\n```\n{tail}```"
        except Exception as e:
            log_lines = f"\n(Could not read log: {e})"

        self._tg.send(f"📋 Process status\nBot: {bot_status}{log_lines}")

    def _cmd_kill(self) -> None:
        result = self._bot.kill()
        self._tg.send(result)


# ── Entry point ────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    if not LAUNCHER_CONFIG["telegram_bot_token"]:
        log.error(
            "No Telegram bot token found. Set CDC_GRID_TG_BOT_TOKEN env var "
            "or configure keyring entry 'cdc_grid_tg_token'."
        )
        sys.exit(1)

    launcher = Launcher(LAUNCHER_CONFIG)
    try:
        launcher.run()
    except KeyboardInterrupt:
        log.info("[Launcher] Interrupted — stopping")
