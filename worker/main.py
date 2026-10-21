# worker/main.py
"""
Orchestration supervisor engine managing thread execution,
health assertions, signal traps, and Prometheus telemetry metrics exposition.
INTEGRATED WITH: Flat Stake Engine (5,000 MMK) + Persistent Bankroll (100,000 MMK)
"""

import time
import signal
import sys
import os
import logging
from prometheus_client import start_http_server

# Ensure worker module is importable
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

# Import core loop routines and internal application memory states
from bot import (
    run_bot_cycle,
    SLEEP_TIME,
    initialize_bot_services,
    shutdown_bot,
    send_telegram,
    install_log_redaction,
    LOCAL_TRACKED_MATCHES,
    # Staking engine imports
    set_staking_engine,
    FLAT_STAKE_MMK,
    INITIAL_BANKROLL_MMK,
)

# Import the Flat Stake Engine
from staking_engine import StakingEngine, CURRENCY

# Import shared metric counters to expose and manipulate state updates
from metrics import CYCLE_COUNTER, MATCHES_TRACKED_GAUGE, API_FAILURES

# Setup isolated logger namespace for supervisor context tracking
LOG_DIR = os.getenv("LOG_DIR", "logs")
os.makedirs(LOG_DIR, exist_ok=True)
LOG_FILE = os.path.join(LOG_DIR, "bot_activity.log")

_log_handlers = [logging.StreamHandler()]
try:
    _log_handlers.append(logging.FileHandler(LOG_FILE, encoding="utf-8"))
except OSError as exc:  # pragma: no cover - filesystem dependent
    print(f"⚠️ Could not open log file {LOG_FILE!r} ({exc}); logging to stdout only.")

logging.basicConfig(
    level=os.getenv("LOG_LEVEL", "INFO").upper(),
    format='%(asctime)s | %(name)s | %(levelname)s | %(message)s',
    handlers=_log_handlers,
)
logger = logging.getLogger("BetBot.Supervisor")

# Strip bot credentials from every log line (covers requests errors that embed
# the Telegram token in the request URL).
install_log_redaction()

# --- CONSTRAINTS ---
WATCHDOG_LIMIT = int(os.getenv("WATCHDOG_LIMIT", "300"))
REBOOT_LIMIT = int(os.getenv("REBOOT_LIMIT", "86400"))
# Periodic status check message: exactly 2 times per day by default (every 12h).
STATUS_INTERVAL_SECONDS = int(os.getenv("STATUS_INTERVAL_SECONDS", str(12 * 3600)))
METRICS_PORT = int(os.getenv("PORT", 8000))
# Minimum gap between watchdog-triggered recovery handshakes, so a burst of
# slow cycles cannot trigger recovery storms.
RECOVERY_COOLDOWN = int(os.getenv("RECOVERY_COOLDOWN", "600"))

RUNNING = True
LAST_REBOOT = time.time()
LAST_STATUS = time.time()
LAST_RECOVERY = 0.0

# ============================================================
# GLOBAL STAKING ENGINE
# ============================================================
staking_engine = None

# --------------------------------------------------
# SIGNAL TRAP HANDLERS
# --------------------------------------------------
def handle_shutdown_signal(signum, frame):
    global RUNNING
    logger.warning(f"⚠️ OS Termination interrupt captured ({signum}). Setting loop exit parameters...")
    RUNNING = False

# --------------------------------------------------
# STAKING ENGINE STATUS REPORTING
# --------------------------------------------------
def send_staking_status():
    """Send a status update about the staking engine via Telegram."""
    if staking_engine:
        status_msg = staking_engine.get_status_message()
        send_telegram(status_msg)
    else:
        send_telegram("⚠️ Staking engine not initialized.")

# --------------------------------------------------
# SYSTEM RESTORATION ROUTINES
# --------------------------------------------------
def execute_safe_recovery_handshake() -> bool:
    logger.warning("🔄 Initiating service recovery loop execution...")
    send_telegram("⚠️ Supervisor Core: Initiating automatic service layer recovery sequence...")
    API_FAILURES.inc()

    try:
        shutdown_bot()
    except Exception as e:
        logger.error(f"Error dropping active services during recovery context: {e}")

    retry_count = 0
    while RUNNING:
        retry_count += 1
        logger.info(f"Attempting full platform re-mount sequence step #{retry_count}...")
        if initialize_bot_services():
            logger.info("✅ Platform modules successfully recovered.")
            send_telegram("✅ Supervisor Core: Service recovery completed. Bot is back online.")

            # Re-attach staking engine after recovery (state reloaded in initialize_bot_services)
            if staking_engine:
                set_staking_engine(staking_engine)
                logger.info(f"📊 Staking engine re-attached: {staking_engine.get_current_step_display()}")
                logger.info(f"💰 Bankroll: {staking_engine.current_bankroll:,.0f} {CURRENCY}")

            return True
        time.sleep(30)
    return False

def can_safely_reboot() -> bool:
    if not LOCAL_TRACKED_MATCHES:
        return True
    active_matches = [fid for fid, s in LOCAL_TRACKED_MATCHES.items() if s.get('active', False)]
    return len(active_matches) == 0

# --------------------------------------------------
# APPLICATION RUNTIME ORCHESTRATION ENTRY
# --------------------------------------------------
def main():
    global RUNNING, LAST_REBOOT, LAST_STATUS, LAST_RECOVERY, staking_engine

    logger.info(f"🚀 INITIALIZING SUPERVISOR MANAGER ASSET PROCESS. PID={os.getpid()}")

    # ============================================================
    # INITIALIZE FLAT STAKE ENGINE (persistence attached in initialize_bot_services)
    # ============================================================
    initial_bankroll = float(os.getenv("INITIAL_BANKROLL", str(INITIAL_BANKROLL_MMK)))
    staking_engine = StakingEngine(initial_bankroll)  # persistence attached later
    set_staking_engine(staking_engine)

    logger.info("📊 Flat Stake Engine loaded (MMK).")
    logger.info(f"🎯 Flat Stake: {FLAT_STAKE_MMK:,} {CURRENCY}")
    logger.info(f"💰 Initial Bankroll: {initial_bankroll:,.0f} {CURRENCY}")
    logger.info("🔒 Bankroll is persistent (Firestore: bot_state/primary).")

    # Attach kernel operational interrupt triggers
    signal.signal(signal.SIGINT, handle_shutdown_signal)
    signal.signal(signal.SIGTERM, handle_shutdown_signal)

    # 📊 START THE PROMETHEUS METRICS ENDPOINT SERVER
    try:
        start_http_server(METRICS_PORT)
        logger.info(f"📊 Prometheus telemetry instrumentation pipeline online at port :{METRICS_PORT}/metrics")
    except Exception as metrics_ex:
        logger.critical(f"💥 Failed to lock Prometheus HTTP socket port allocation: {metrics_ex}")

    # Core Startup Routine (also attaches Firebase to staking engine & reloads state)
    if not initialize_bot_services():
        logger.critical("💥 Initial core component initialization failure! Terminating Supervisor process.")
        sys.exit(1)

    # Send startup messages
    send_telegram(
        "🚀 **BetBot Daemon Supervisor Online**\n"
        "━━━━━━━━━━━━━━━━━━━━━\n"
        "📊 **Flat Stake Engine Active**\n"
        f"🎯 Stake: {FLAT_STAKE_MMK:,} {CURRENCY} per bet\n"
        f"💰 Bankroll: {staking_engine.current_bankroll:,.0f} {CURRENCY}\n"
        f"🔒 Persistent across restarts\n"
        "━━━━━━━━━━━━━━━━━━━━━"
    )

    # Send initial staking status
    time.sleep(2)
    send_staking_status()

    # Main loop
    while RUNNING:
        cycle_start_time = time.time()
        CYCLE_COUNTER.inc()

        try:
            # Execute business logic queries
            run_bot_cycle()

            # Push live cache data structure updates to Prometheus Gauge
            MATCHES_TRACKED_GAUGE.set(len(LOCAL_TRACKED_MATCHES))

            # Watchdog Execution Time Limit Assertion
            elapsed_cycle_time = time.time() - cycle_start_time
            if elapsed_cycle_time > WATCHDOG_LIMIT:
                if time.time() - LAST_RECOVERY < RECOVERY_COOLDOWN:
                    logger.warning(
                        "⚠️ Cycle overran watchdog (%.2fs > %ss) but recovery is "
                        "cooling down; skipping handshake.",
                        elapsed_cycle_time, WATCHDOG_LIMIT,
                    )
                else:
                    logger.error(
                        "🚨 Watchdog breached: cycle took %.2fs (limit %ss). "
                        "Initiating recovery handshake.",
                        elapsed_cycle_time, WATCHDOG_LIMIT,
                    )
                    LAST_RECOVERY = time.time()
                    if not execute_safe_recovery_handshake():
                        break

            # Smart Maintenance Window Assertion
            if time.time() - LAST_REBOOT > REBOOT_LIMIT:
                if can_safely_reboot():
                    logger.warning("Scheduled maintenance window verified open. Executing restart execution pass...")
                    if execute_safe_recovery_handshake():
                        LAST_REBOOT = time.time()
                else:
                    LAST_REBOOT += 300  # Shift evaluation window forward 5 minutes

            # Status check message: sent exactly twice per day (every 12h by default)
            if time.time() - LAST_STATUS >= STATUS_INTERVAL_SECONDS:
                send_staking_status()
                LAST_STATUS = time.time()

        except KeyboardInterrupt:
            logger.info("Keyboard interrupt received. Shutting down...")
            RUNNING = False
            break

        except Exception as e:
            logger.error(f"💥 Top level supervisor loop runtime exception failure: {e}", exc_info=True)
            API_FAILURES.inc()
            time.sleep(15)

        finally:
            if RUNNING:
                # Calculate processing execution latency to keep loop sleep iterations perfectly consistent
                execution_latency = time.time() - cycle_start_time
                dynamic_sleep = max(1.0, SLEEP_TIME - execution_latency)
                time.sleep(dynamic_sleep)

    # ============================================================
    # CLEAN EXIT PATH
    # ============================================================
    logger.warning("🛑 Closing application loops. Freeing structural process parameters...")

    # Send final staking summary
    if staking_engine:
        final_msg = staking_engine.get_status_message()
        final_msg = f"🛑 **Bot Shutdown - Final Summary**\n{final_msg}"
        send_telegram(final_msg)

    try:
        shutdown_bot()
    except Exception as e:
        logger.error(f"Error handling system breakdown operations: {e}")

    send_telegram("🛑 **BetBot Daemon Supervisor Offline**")
    logger.info("🛑 Bot shutdown complete.")


if __name__ == "__main__":
    main()
