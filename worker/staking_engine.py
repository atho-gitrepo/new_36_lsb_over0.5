# worker/staking_engine.py
"""
Flat Stake Engine with Persistent Bankroll
==========================================
Stake: 5000 MMK (flat, every bet)
Initial Bankroll: 100,000 MMK
Persistence: Firebase-backed state survives restarts/redeploys.
"""

import time
import logging
from typing import Dict, Optional, List
from datetime import datetime

logger = logging.getLogger("BetBot.Staking")

# ============================================================
# CONSTANTS
# ============================================================
FLAT_STAKE = 5000          # MMK per bet
CURRENCY = "MMK"
ODDS = 1.65

# ============================================================
# STAKING ENGINE
# ============================================================

class StakingEngine:
    """
    Flat-stake engine. Every bet uses FLAT_STAKE (5000 MMK).
    Bankroll + stats persist via a persistence backend (Firebase).
    """

    def __init__(self, initial_bankroll: float = 100000.0,
                 persistence=None, state_id: str = "primary"):
        """
        Args:
            initial_bankroll: Fallback if no persisted state exists.
            persistence: FirebaseManager instance (must expose
                         load_staking_state()/save_staking_state()).
            state_id: Document ID for the persisted state.
        """
        self.initial_bankroll = initial_bankroll
        self.persistence = persistence
        self.state_id = state_id

        self.current_bankroll = initial_bankroll
        self.start_time = time.time()
        self.last_activity_time = self.start_time

        # Stats
        self.total_bets = 0
        self.total_wins = 0
        self.total_losses = 0
        self.total_staked = 0.0
        self.total_profit = 0.0
        self.max_drawdown = 0.0
        self.peak_bankroll = initial_bankroll
        self.current_streak = 0
        self.max_win_streak = 0
        self.max_loss_streak = 0

        self.bet_history: List[Dict] = []
        self.max_history_size = 100
        self.last_10_results: List[bool] = []

        # Try loading persisted state
        self._load_state()

        logger.info("=" * 50)
        logger.info("🔄 Flat Stake Engine Initialized")
        logger.info(f"💰 Bankroll: {self.current_bankroll:,.0f} {CURRENCY}")
        logger.info(f"🎯 Flat Stake: {FLAT_STAKE:,} {CURRENCY}")
        logger.info("=" * 50)

    # ------------------------------------------------------------
    # PERSISTENCE
    # ------------------------------------------------------------
    def _load_state(self):
        """Load state from persistence backend if available."""
        if not self.persistence:
            logger.warning("⚠️ No persistence backend. Using initial bankroll.")
            return
        try:
            state = self.persistence.load_staking_state(self.state_id)
            if not state:
                logger.info("📂 No persisted state found. Starting fresh.")
                return

            self.current_bankroll = float(state.get("current_bankroll", self.initial_bankroll))
            self.peak_bankroll = float(state.get("peak_bankroll", self.current_bankroll))
            self.total_bets = int(state.get("total_bets", 0))
            self.total_wins = int(state.get("total_wins", 0))
            self.total_losses = int(state.get("total_losses", 0))
            self.total_staked = float(state.get("total_staked", 0.0))
            self.total_profit = float(state.get("total_profit", 0.0))
            self.max_drawdown = float(state.get("max_drawdown", 0.0))
            self.current_streak = int(state.get("current_streak", 0))
            self.max_win_streak = int(state.get("max_win_streak", 0))
            self.max_loss_streak = int(state.get("max_loss_streak", 0))
            self.bet_history = state.get("bet_history", [])[-self.max_history_size:]
            self.last_10_results = state.get("last_10_results", [])[-10:]

            logger.info(
                f"✅ Restored state | Bankroll: {self.current_bankroll:,.0f} {CURRENCY} | "
                f"Bets: {self.total_bets} | W/L: {self.total_wins}/{self.total_losses}"
            )
        except Exception as e:
            logger.exception(f"❌ Failed to load persisted state: {e}")

    def _save_state(self):
        """Persist current state to backend."""
        if not self.persistence:
            return
        try:
            self.persistence.save_staking_state(self.state_id, {
                "current_bankroll": self.current_bankroll,
                "peak_bankroll": self.peak_bankroll,
                "total_bets": self.total_bets,
                "total_wins": self.total_wins,
                "total_losses": self.total_losses,
                "total_staked": self.total_staked,
                "total_profit": self.total_profit,
                "max_drawdown": self.max_drawdown,
                "current_streak": self.current_streak,
                "max_win_streak": self.max_win_streak,
                "max_loss_streak": self.max_loss_streak,
                "bet_history": self.bet_history[-self.max_history_size:],
                "last_10_results": self.last_10_results[-10:],
                "updated_at": datetime.utcnow().isoformat(),
            })
        except Exception as e:
            logger.exception(f"❌ Failed to persist state: {e}")

    # ------------------------------------------------------------
    # CORE
    # ------------------------------------------------------------
    def reset(self):
        """Reset to initial bankroll (also persists)."""
        self.current_bankroll = self.initial_bankroll
        self.peak_bankroll = self.initial_bankroll
        self.total_bets = 0
        self.total_wins = 0
        self.total_losses = 0
        self.total_staked = 0.0
        self.total_profit = 0.0
        self.max_drawdown = 0.0
        self.current_streak = 0
        self.max_win_streak = 0
        self.max_loss_streak = 0
        self.bet_history = []
        self.last_10_results = []
        self.start_time = time.time()
        self._save_state()
        logger.info(f"🔄 Reset. Bankroll: {self.current_bankroll:,.0f} {CURRENCY}")

    def get_current_stake(self) -> float:
        """Always returns flat stake."""
        return FLAT_STAKE

    def get_current_step_display(self) -> str:
        return f"Flat Stake: {FLAT_STAKE:,} {CURRENCY}"

    def record_result(self, is_win: bool, match_info: Optional[Dict] = None,
                      odds: float = ODDS) -> Dict:
        """Record a settled bet.

        Args:
            is_win: outcome of the bet.
            match_info: optional context dict (match, score, trigger, etc.).
            odds: decimal odds used for this specific bet. Defaults to the
                  engine-wide ODDS so plain callers behave exactly as before.
        """
        stake = FLAT_STAKE
        self.total_bets += 1
        self.total_staked += stake
        self.last_activity_time = time.time()

        if is_win:
            profit = stake * (odds - 1)
            self.total_profit += profit
            self.current_bankroll += profit
            self.total_wins += 1
            self.current_streak = self.current_streak + 1 if self.current_streak >= 0 else 1
            if self.current_streak > self.max_win_streak:
                self.max_win_streak = self.current_streak
            if self.current_bankroll > self.peak_bankroll:
                self.peak_bankroll = self.current_bankroll
        else:
            loss = stake
            self.total_profit -= loss
            self.current_bankroll -= loss
            self.total_losses += 1
            self.current_streak = self.current_streak - 1 if self.current_streak <= 0 else -1
            if abs(self.current_streak) > self.max_loss_streak:
                self.max_loss_streak = abs(self.current_streak)
            drawdown = self.peak_bankroll - self.current_bankroll
            if drawdown > self.max_drawdown:
                self.max_drawdown = drawdown

        profit_out = (stake * (odds - 1)) if is_win else -stake
        self.bet_history.append({
            "timestamp": time.time(),
            "datetime": datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S"),
            "stake": stake,
            "odds": odds,
            "is_win": is_win,
            "profit": profit_out,
            "bankroll": self.current_bankroll,
            "match_info": match_info or {},
        })
        if len(self.bet_history) > self.max_history_size:
            self.bet_history.pop(0)

        self.last_10_results.append(is_win)
        if len(self.last_10_results) > 10:
            self.last_10_results.pop(0)

        # Persist after every result
        self._save_state()

        return {
            "is_win": is_win,
            "stake": stake,
            "odds": odds,
            "profit": profit_out,
            "bankroll": self.current_bankroll,
            "total_bets": self.total_bets,
            "total_wins": self.total_wins,
            "total_losses": self.total_losses,
            "total_profit": self.total_profit,
        }

    # ------------------------------------------------------------
    # STATS
    # ------------------------------------------------------------
    def get_stats(self) -> Dict:
        win_rate = (self.total_wins / self.total_bets * 100) if self.total_bets > 0 else 0
        roi = (self.total_profit / self.total_staked * 100) if self.total_staked > 0 else 0
        bankroll_change = (
            (self.current_bankroll - self.initial_bankroll) / self.initial_bankroll * 100
            if self.initial_bankroll > 0 else 0
        )
        last_10_wins = sum(1 for r in self.last_10_results if r)
        last_10_rate = (last_10_wins / len(self.last_10_results) * 100) if self.last_10_results else 0

        running_seconds = int(time.time() - self.start_time)
        running_hours = running_seconds // 3600
        running_minutes = (running_seconds % 3600) // 60

        return {
            "total_bets": self.total_bets,
            "total_wins": self.total_wins,
            "total_losses": self.total_losses,
            "win_rate": f"{win_rate:.1f}%",
            "last_10_win_rate": f"{last_10_rate:.1f}%",
            "total_staked": f"{self.total_staked:,.0f} {CURRENCY}",
            "total_profit": f"{self.total_profit:,.0f} {CURRENCY}",
            "roi": f"{roi:.1f}%",
            "current_bankroll": f"{self.current_bankroll:,.0f} {CURRENCY}",
            "initial_bankroll": f"{self.initial_bankroll:,.0f} {CURRENCY}",
            "bankroll_change": f"{bankroll_change:+.1f}%",
            "peak_bankroll": f"{self.peak_bankroll:,.0f} {CURRENCY}",
            "max_drawdown": f"{self.max_drawdown:,.0f} {CURRENCY}",
            "current_step": 0,
            "current_stake": f"{FLAT_STAKE:,} {CURRENCY}",
            "is_paused": False,
            "pause_until": "N/A",
            "pause_reason": "N/A",
            "consecutive_losses": 0,
            "current_streak": self.current_streak,
            "max_win_streak": self.max_win_streak,
            "max_loss_streak": self.max_loss_streak,
            "running_time": f"{running_hours}h {running_minutes}m",
            "last_activity": time.ctime(self.last_activity_time),
        }

    def get_status_message(self) -> str:
        s = self.get_stats()
        streak_display = f"+{s['current_streak']}" if s['current_streak'] > 0 else str(s['current_streak'])
        return (
            f"📊 **Staking Engine Status (Flat Stake)**\n"
            f"━━━━━━━━━━━━━━━━━━━━━\n"
            f"🎯 Stake: **{s['current_stake']}**\n"
            f"💰 Bankroll: **{s['current_bankroll']}**\n"
            f"📊 Change: **{s['bankroll_change']}**\n"
            f"✅ Wins: **{s['total_wins']}**\n"
            f"❌ Losses: **{s['total_losses']}**\n"
            f"📊 Win Rate: **{s['win_rate']}**\n"
            f"📊 Last 10: **{s['last_10_win_rate']}**\n"
            f"💵 Total Staked: **{s['total_staked']}**\n"
            f"📈 Total Profit: **{s['total_profit']}**\n"
            f"📈 ROI: **{s['roi']}**\n"
            f"📉 Max Drawdown: **{s['max_drawdown']}**\n"
            f"🔥 Streak: **{streak_display}**\n"
            f"━━━━━━━━━━━━━━━━━━━━━\n"
            f"⏱️ Running: **{s['running_time']}**"
        )

