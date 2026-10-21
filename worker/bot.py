"""
Core business strategy processing engine for LSB BetBot.

A single Firestore-backed strategy is evaluated against the live match feed on
every cycle:

  OVER +0.5 -- "one more goal" strategy
    Trigger : minute OVER_BET_MINUTE (default 36) when the scoreline is
              present in OVER_TRIGGER_SCORES (default 2-0,1-0,0-1).
    Bet     : Over +0.5 on the running total, i.e. at least one additional
              goal is expected before the half-time whistle.
    Settle  : at HALF TIME. WIN when the HT score differs from the trigger
              score (an extra goal arrived).
    Storage : unresolved_overbets -> resolved_overbets.

Every threshold, trigger scoreline, trigger minute, decimal odds and Firestore
collection name is environment-configurable (see .env.example). OVER_ODDS
defaults to 2.75.

LOCKING: a PER-SCORE lock applies and is held until the pending bet resolves
at HALF TIME. Each scoreline owns its own lock, so a `1-0` bet locks `1-0` and
a `2-0` bet locks `2-0` independently - different scores never block each
other. A duplicate trigger of the SAME score is blocked; once the half-time
result is written, the score unlocks again (LOCK_UNTIL_HT_RESOLUTION).
"""

import json
import logging
import os
import time
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict, List, Optional, Tuple

import requests

# ---------------------------------------------------------------------------
# .env loading. Platform/container env always wins (load_dotenv does NOT
# override already-set variables), which keeps Railway/Docker behaviour.
# ---------------------------------------------------------------------------
try:
    from dotenv import load_dotenv
except ImportError:  # pragma: no cover - optional dependency

    def load_dotenv(*_args, **_kwargs):  # type: ignore
        return False


_PROJECT_ROOT = Path(__file__).resolve().parent.parent
load_dotenv(_PROJECT_ROOT / ".env")
load_dotenv()

import firebase_admin  # noqa: E402
from firebase_admin import credentials, firestore  # noqa: E402

# Live data feed (Sofascore primary, LiveScore automatic fallback)
from esd.sofascore import SofascoreClient  # noqa: E402

# Shared telemetry registry (isolated to prevent circular imports)
from metrics import STATE_LOCKS, BET_TRIGGERS, API_FAILURES  # noqa: E402

# Flat stake engine
from staking_engine import StakingEngine, FLAT_STAKE, CURRENCY  # noqa: E402

logger = logging.getLogger("BetBot.ExecutionEngine")


# ===========================================================================
# ENVIRONMENT HELPERS
# ===========================================================================

def _env(name: str, default: str) -> str:
    """Return a string env var, falling back to ``default`` when unset/empty."""
    value = os.getenv(name)
    return value if value not in (None, "") else default


def _env_int(name: str, default: int) -> int:
    try:
        return int(float(_env(name, str(default))))
    except (TypeError, ValueError):
        logger.warning("⚠️ Invalid integer for %s; using default %s", name, default)
        return default


def _env_float(name: str, default: float) -> float:
    try:
        return float(_env(name, str(default)))
    except (TypeError, ValueError):
        logger.warning("⚠️ Invalid float for %s; using default %s", name, default)
        return default


def _env_bool(name: str, default: bool) -> bool:
    raw = _env(name, "true" if default else "false").strip().lower()
    return raw in ("1", "true", "yes", "y", "on")


def _env_list(name: str, default: List[str]) -> List[str]:
    raw = os.getenv(name)
    if raw is None or raw.strip() == "":
        return [str(item) for item in default]
    raw = raw.replace(";", ",")
    return [token.strip() for token in raw.split(",") if token.strip()]


def _env_int_list(name: str, default: List[int]) -> List[int]:
    minutes: List[int] = []
    for token in _env_list(name, [str(item) for item in default]):
        try:
            minutes.append(int(float(token)))
        except (TypeError, ValueError):
            logger.warning("⚠️ Ignoring non-numeric value %r in %s", token, name)
    return minutes


def _normalize_score(token: str) -> str:
    """Normalise common score spellings ("2:0", "2 - 0") to "2-0"."""
    return str(token).strip().replace(":", "-").replace(" ", "")


def _env_score_set(name: str, default: List[str]) -> frozenset:
    return frozenset(
        normalized
        for normalized in (_normalize_score(token) for token in _env_list(name, default))
        if normalized
    )


# ===========================================================================
# STATIC CONFIGURATION
# ===========================================================================

TELEGRAM_TOKEN = _env("TELEGRAM_TOKEN", "YOUR_TOKEN_HERE")
TELEGRAM_CHAT_ID = _env("TELEGRAM_CHAT_ID", "YOUR_CHAT_ID_HERE")
FIREBASE_CREDENTIALS = _env("FIREBASE_CREDENTIALS_JSON", "")

# --- Staking ---
FLAT_STAKE_MMK = float(FLAT_STAKE)
INITIAL_BANKROLL_MMK = _env_int("INITIAL_BANKROLL", 100000)
CURRENCY_CODE = CURRENCY
ORIGINAL_STAKE = _env_float("ORIGINAL_STAKE", FLAT_STAKE_MMK)

# --- Per-strategy decimal odds ---
# Over +0.5: quoted odds for this market (default 2.75).
OVER_ODDS = max(1.0, _env_float("OVER_ODDS", 3))

# --- Timing ---
SLEEP_TIME = _env_int("SLEEP_TIME", 55)
PREDICT_START_MIN = _env_int("PREDICT_START_MIN", 30)
PRE_WARM_WINDOW = (
    _env_int("PRE_WARM_WINDOW_MIN", 30),
    _env_int("PRE_WARM_WINDOW_MAX", 38),
)
MEMORY_PRUNE_TIMEOUT = _env_int("MEMORY_PRUNE_TIMEOUT", 5400)

# --- Placement locking -----------------------------------------------------
# Per-score lock held until the pending bet resolves at HALF TIME. Each
# scoreline owns its own lock, so 1-0 and 2-0 are placed independently, but a
# duplicate trigger of the SAME score is blocked until the pending bet is
# settled. Once the half-time result arrives, the score unlocks again.
LOCK_UNTIL_HT_RESOLUTION = _env_bool("LOCK_UNTIL_HT_RESOLUTION", True)

AMATEUR_KEYWORDS = [
    keyword.lower()
    for keyword in _env_list(
        "AMATEUR_KEYWORDS",
        [
            "amateur", "youth", "reserves", "u18", "u17", "u16", "u19",
            "u22", "u23", "u21", "u20", "college",
        ],
    )
]

# Status aliases coming from either provider.
HALFTIME_STATUSES = frozenset({"HT", "HALFTIME", "HALF", "HALF-TIME", "HALF TIME"})
FULLTIME_STATUSES = frozenset({
    "FT", "FULLTIME", "FULL-TIME", "FULL TIME",
    "ENDED", "FINISHED", "AFTER FT", "AFTER ET", "PEN",
})

# --- Firestore collection names ---
COLLECTION_UNRESOLVED_OVER = _env("COLLECTION_UNRESOLVED_OVER", "unresolved_overbets")
COLLECTION_RESOLVED_OVER = _env("COLLECTION_RESOLVED_OVER", "resolved_overbets")

# --- Firestore score field -------------------------------------------------
# Firestore field paths must match ^[_a-zA-Z][_a-zA-Z0-9]*$. The historical
# name "36_score" begins with a digit, which makes
# ``collection.where("36_score", ...)`` raise
# ``Path 36_score not consumed, residue: 36_score`` inside the SDK. Because the
# lock check fails safe (returns True/locked) on error, that exception blocked
# EVERY placement. New writes therefore use the valid ``trigger_score`` field;
# reads fall back to the legacy ``36_score`` key so bets written by older
# builds still settle.
SCORE_FIELD = "trigger_score"
LEGACY_SCORE_FIELD = "36_score"

# --- Volatile in-memory cache ---
LOCAL_TRACKED_MATCHES: Dict[str, Dict] = {}

# --- Global staking engine handle ---
_staking_engine: Optional[StakingEngine] = None
_staking_enabled: bool = True
firebase_manager: Optional["FirebaseManager"] = None
LIVESCORE_CLIENT: Optional[SofascoreClient] = None

# --- Geographical flag map ---
COUNTRY_FLAGS = {
    "iceland": "🇮🇸", "argentina": "🇦🇷", "england": "🏴󠁧󠁢󠁥󠁮󠁧󠁿", "germany": "🇩🇪",
    "spain": "🇪🇸", "italy": "🇮🇹", "france": "🇫🇷", "brazil": "🇧🇷",
    "malaysia": "🇲🇾", "belarus": "🇧🇾", "faroe islands": "🇫🇴",
    "netherlands": "🇳🇱", "portugal": "🇵🇹", "belgium": "🇧🇪", "turkey": "🇹🇷",
    "russia": "🇷🇺", "ukraine": "🇺🇦", "scotland": "🏴󠁧󠁢󠁳󠁣󠁴󠁿", "switzerland": "🇨🇭",
    "austria": "🇦🇹", "denmark": "🇩🇰", "sweden": "🇸🇪", "norway": "🇳🇴",
    "greece": "🇬🇷", "croatia": "🇭🇷", "poland": "🇵🇱", "united states": "🇺🇸",
    "mexico": "🇲🇽", "australia": "🇦🇺", "japan": "🇯🇵", "south korea": "🇰🇷",
    "saudi arabia": "🇸🇦", "qatar": "🇶🇦", "uae": "🇦🇪", "china": "🇨🇳",
    "egypt": "🇪🇬", "nigeria": "🇳🇬", "south africa": "🇿🇦", "chile": "🇨🇱",
    "colombia": "🇨🇴", "peru": "🇵🇪", "uruguay": "🇺🇾", "paraguay": "🇵🇾",
    "ecuador": "🇪🇨", "venezuela": "🇻🇪", "bolivia": "🇧🇴", "costarica": "🇨🇷",
    "finland": "🇫🇮", "world": "🌍",
}


# ===========================================================================
# STRATEGY DEFINITIONS
# ===========================================================================

@dataclass(frozen=True)
class Strategy:
    """Declarative description of a single betting engine."""

    key: str
    label: str
    bet_type: str
    enabled: bool
    trigger_scores: frozenset
    trigger_minutes: frozenset
    unresolved_collection: str
    resolved_collection: str
    # True  -> WIN when HT score == trigger score (draw persistence)
    # False -> WIN when HT score != trigger score (over +0.5, extra goal)
    win_if_same_score: bool
    emoji: str
    odds: float


STRATEGIES: Tuple[Strategy, ...] = (
    Strategy(
        key="over_0_5",
        label="Over +0.5",
        bet_type="over_0.5",
        enabled=_env_bool("ENABLE_OVER_BET", True),
        trigger_scores=_env_score_set("OVER_TRIGGER_SCORES", ["2-0", "1-1", "1-0"]),
        trigger_minutes=frozenset(_env_int_list("OVER_BET_MINUTE", [36])),
        unresolved_collection=COLLECTION_UNRESOLVED_OVER,
        resolved_collection=COLLECTION_RESOLVED_OVER,
        win_if_same_score=False,
        emoji="⚽",
        odds=OVER_ODDS,
    ),
)


# ===========================================================================
# STAKING ENGINE GLUE
# ===========================================================================

def set_staking_engine(engine: StakingEngine) -> None:
    """Attach the staking engine instance (called by the supervisor)."""
    global _staking_engine
    _staking_engine = engine
    logger.info("✅ Staking Engine attached to bot.")
    logger.info("📊 Flat Stake: %s %s", f"{FLAT_STAKE_MMK:,.0f}", CURRENCY_CODE)


def calculate_stake() -> Tuple[float, int]:
    """Return the flat stake to use for a placement and its sequence number."""
    if _staking_enabled and _staking_engine:
        return float(_staking_engine.get_current_stake()), 1
    return float(ORIGINAL_STAKE), 1


# ===========================================================================
# FIREBASE PERSISTENCE LAYER
# ===========================================================================

class FirebaseManager:
    """Thin, fail-safe wrapper around Firestore for bets and bankroll state."""

    def __init__(self, creds_json: str):
        self.creds_json = creds_json
        self.db = None
        self._connect()

    # -------------------------------------------------- connection
    def _connect(self) -> bool:
        if not self.creds_json:
            logger.error("❌ Firebase credentials missing from environment variables!")
            return False
        try:
            cred = credentials.Certificate(json.loads(self.creds_json))
            if not firebase_admin._apps:
                firebase_admin.initialize_app(cred)
            self.db = firestore.client()
            logger.info("✅ Firebase connection successfully established.")
            return True
        except Exception as exc:  # noqa: BLE001 - defensive by design
            logger.exception("❌ Firebase initialization error: %s", exc)
            self.db = None
            return False

    def _ensure_connection(self) -> bool:
        return True if self.db is not None else self._connect()

    # -------------------------------------------------- score-scoped locks
    def is_score_locked(self, collection: str, score: str) -> bool:
        """
        True when an unresolved bet with the SAME trigger score already exists
        in ``collection``. Fails safe (returns True / locked) on any error so
        that duplicate stakes can never be placed.
        """
        if not self._ensure_connection():
            return True
        try:
            query = (
                self.db.collection(collection)
                .where(SCORE_FIELD, "==", str(score))
                .limit(1)
                .get()
            )
            return len(query) > 0
        except Exception as exc:  # noqa: BLE001
            logger.error("❌ Error checking score lock for '%s' in %s: %s", score, collection, exc)
            return True

    # -------------------------------------------------- generic CRUD
    def add_unresolved(self, collection: str, match_id: str, data: dict) -> bool:
        data["placed_at"] = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")
        if not self._ensure_connection():
            logger.critical("❌ Transmit blocked: database offline. Drop ID %s!", match_id)
            return False
        try:
            self.db.collection(collection).document(str(match_id)).set(data)
            logger.info("✅ Wrote unresolved bet to '%s' for ID %s", collection, match_id)
            return True
        except Exception as exc:  # noqa: BLE001
            logger.exception("❌ Failed to save unresolved bet %s: %s", match_id, exc)
            return False

    def get_unresolved(self, collection: str, match_id: str) -> Optional[dict]:
        if not self._ensure_connection():
            return None
        try:
            doc = self.db.collection(collection).document(str(match_id)).get()
            return doc.to_dict() if doc.exists else None
        except Exception as exc:  # noqa: BLE001
            logger.error("❌ Error reading unresolved document %s from %s: %s", match_id, collection, exc)
            return None

    def move_to_resolved(
        self,
        unresolved_collection: str,
        resolved_collection: str,
        match_id: str,
        data: dict,
        outcome: str,
    ) -> bool:
        data = dict(data)
        data.update({
            "outcome": outcome,
            "resolved_at": datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S"),
            "resolution_timestamp": firestore.SERVER_TIMESTAMP,
        })
        if not self._ensure_connection():
            return False
        try:
            self.db.collection(resolved_collection).document(str(match_id)).set(data)
            self.db.collection(unresolved_collection).document(str(match_id)).delete()
            return True
        except Exception as exc:  # noqa: BLE001
            logger.exception("❌ Failed to resolve match %s: %s", match_id, exc)
            return False

    # -------------------------------------------------- staking state
    def load_staking_state(self, state_id: str = "primary") -> Optional[dict]:
        if not self._ensure_connection():
            return None
        try:
            doc = self.db.collection("bot_state").document(state_id).get()
            return doc.to_dict() if doc.exists else None
        except Exception as exc:  # noqa: BLE001
            logger.error("❌ Error loading staking state: %s", exc)
            return None

    def save_staking_state(self, state_id: str, data: dict) -> bool:
        if not self._ensure_connection():
            return False
        try:
            self.db.collection("bot_state").document(state_id).set(data, merge=True)
            return True
        except Exception as exc:  # noqa: BLE001
            logger.error("❌ Error saving staking state: %s", exc)
            return False


# ===========================================================================
# UTILITIES
# ===========================================================================

def redact_secrets(text) -> str:
    """Replace configured credentials with a placeholder.

    Used both when logging network errors and (via the log filter) on every
    emitted record, so a bot token embedded in a ``requests`` exception URL
    can never leak into the logs.
    """
    if text is None:
        return text
    result = str(text)
    for secret in (TELEGRAM_TOKEN, TELEGRAM_CHAT_ID):
        if secret and secret not in ("YOUR_TOKEN_HERE", "YOUR_CHAT_ID_HERE"):
            result = result.replace(secret, "«redacted»")
    return result


class _SecretRedactingFilter(logging.Filter):
    """Strip configured secrets from every log record that reaches a handler."""

    def filter(self, record: logging.LogRecord) -> bool:  # noqa: D102
        try:
            message = record.getMessage()
        except Exception:  # noqa: BLE001 - never break logging
            return True
        redacted = redact_secrets(message)
        if redacted != message:
            record.msg = redacted
            record.args = ()
        if record.exc_text:
            record.exc_text = redact_secrets(record.exc_text)
        return True


def install_log_redaction() -> None:
    """Attach the redacting filter to every active root handler.

    Filters must live on handlers (not just the root logger) because records
    from child loggers propagate straight to the handlers.
    """
    redactor = _SecretRedactingFilter()
    for handler in logging.getLogger().handlers:
        handler.addFilter(redactor)
    logger.debug("🔐 Log redaction filter installed on %d handler(s).",
                 len(logging.getLogger().handlers))


def send_telegram(message: str) -> bool:
    """Push a Markdown-formatted alert to the configured Telegram chat.

    Falls back to a plain (unformatted) send when Telegram rejects the Markdown
    payload, so alerts are never lost to a scoreline or team name that happens
    to contain a Markdown control character. Any network error is logged with
    the bot token redacted.
    """
    if not TELEGRAM_TOKEN or TELEGRAM_TOKEN == "YOUR_TOKEN_HERE":
        logger.warning("⚠️ Telegram token not configured; skipping alert.")
        return False

    url = f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage"
    payload = {"chat_id": TELEGRAM_CHAT_ID, "text": message}

    for parse_mode in ("Markdown", None):
        body = dict(payload)
        if parse_mode:
            body["parse_mode"] = parse_mode
        try:
            response = requests.post(url, data=body, timeout=15)
        except Exception as exc:  # noqa: BLE001
            logger.error("❌ Network error sending Telegram alert: %s", redact_secrets(exc))
            return False

        if response.status_code == 200:
            return True

        logger.warning(
            "⚠️ Telegram send failed (status=%s, parse_mode=%s): %s",
            response.status_code,
            parse_mode,
            redact_secrets(response.text[:300]),
        )
        if parse_mode is None:
            break

    return False


def prune_volatile_cache_leaks() -> None:
    """Evict stale match contexts from the in-memory cache."""
    now = time.time()
    stale = [
        fid
        for fid, state in LOCAL_TRACKED_MATCHES.items()
        if now - state.get("last_seen", 0.0) > MEMORY_PRUNE_TIMEOUT
    ]
    for fid in stale:
        LOCAL_TRACKED_MATCHES.pop(fid, None)
    if stale:
        logger.info("🧹 Evicted %d stale match contexts from memory.", len(stale))


def extract_hybrid_geography(match) -> Tuple[str, str, str]:
    """
    Resolve (league, country, country_slug) from either a parsed Event object
    or a raw LiveScore dictionary payload.
    """
    if hasattr(match, "tournament"):
        tournament = match.tournament
        league_name = getattr(tournament, "name", None) or "Unknown League"
        country_name, country_slug = "World", "world"
        category = getattr(tournament, "category", None)
        if category is not None:
            country_name = getattr(category, "name", None) or "World"
            country_slug = country_name.lower() if country_name else "world"
        return league_name, country_name, country_slug

    if isinstance(match, dict):
        stage = match.get("Stg") if isinstance(match.get("Stg"), dict) else {}
        league_name = (
            stage.get("Snm")
            or stage.get("CompN")
            or stage.get("Nm")
            or match.get("Snm")
            or match.get("CompN")
            or match.get("league")
            or match.get("tournament")
            or "Unknown League"
        )
        country_name = (
            stage.get("Cnm")
            or stage.get("Rgn")
            or stage.get("Country")
            or match.get("Cnm")
            or match.get("country")
            or "World"
        )
        return league_name, country_name, country_name.lower()

    return "Unknown League", "World", "world"


def _flag_for(country_slug: str, country: str) -> str:
    return COUNTRY_FLAGS.get(
        (country_slug or "").lower(),
        COUNTRY_FLAGS.get((country or "").lower(), "🌍"),
    )


# ---------------------------------------------------------------------------
# PER-SCORE LOCK, HELD UNTIL HALF-TIME RESOLUTION
#
# Each scoreline owns its OWN lock:
#   * match A @ 36' score 1-0 -> locks "1-0"
#   * match B @ 36' score 2-0 -> locks "2-0" (independent, still allowed)
#   * a duplicate trigger of the SAME score is blocked while a bet with that
#     score is pending in the unresolved collection.
#
# The lock lives in Firestore (the unresolved bet itself) and is therefore
# restart-safe. When the half-time result arrives the bet is moved to the
# resolved collection, the unresolved document is deleted, and the score is
# automatically unlocked again.
# ---------------------------------------------------------------------------

def _is_score_pending(score: str) -> bool:
    """True when a bet with this score is still pending.

    Any unresolved document carrying the same trigger score keeps the score
    locked until its half-time settlement removes it. ``is_score_locked``
    fails safe (returns True / locked) on Firestore errors so duplicates are
    never placed. Callers guard the ``firebase_manager is None`` case first.
    """
    if firebase_manager is None:
        return False
    return any(
        firebase_manager.is_score_locked(strategy.unresolved_collection, score)
        for strategy in STRATEGIES
    )


# ===========================================================================
# MATCH CONTEXT EXTRACTION
# ===========================================================================

@dataclass
class MatchContext:
    fid: str
    name: str
    league: str
    country: str
    country_slug: str
    score: str
    status: str
    minute: Optional[int]
    is_first_half: bool


def _resolve_phase(match, status: str) -> Tuple[Optional[int], bool]:
    """Return (current_pitch_minute, is_first_half) from a normalised status."""
    minute: Optional[int] = None
    is_first_half = False

    if status.isdigit():
        minute = int(status)
        is_first_half = minute <= 45
    elif status in HALFTIME_STATUSES:
        minute, is_first_half = 45, True
    elif "1ST" in status:
        is_first_half = True
        if isinstance(match, dict):
            minute = match.get("total_elapsed_minutes", 35)
        else:
            minute = getattr(match, "total_elapsed_minutes", 35) or 35
        try:
            minute = int(minute)
        except (TypeError, ValueError):
            minute = 35

    return minute, is_first_half


def extract_match_context(match) -> Optional[MatchContext]:
    """
    Build a normalised MatchContext from an Event object or raw dict.
    Returns None when the match should be ignored (amateur filter or too early).
    """
    if isinstance(match, dict):
        fid = str(match.get("match_id") or match.get("id") or match.get("Eid", ""))
        home = match.get("home_name", "Home")
        away = match.get("away_name", "Away")
        match_name = match.get("match_name") or f"{home} vs {away}"
    else:
        fid = str(getattr(match, "id", ""))
        home_team = getattr(match, "home_team", None)
        away_team = getattr(match, "away_team", None)
        match_name = (
            f"{getattr(home_team, 'name', 'Home')} vs {getattr(away_team, 'name', 'Away')}"
        )

    if not fid:
        return None

    league, country, country_slug = extract_hybrid_geography(match)
    if any(keyword in f"{league} {country}".lower() for keyword in AMATEUR_KEYWORDS):
        logger.debug("⏭️ Skipping amateur match: %s (%s %s)", match_name, league, country)
        return None

    if isinstance(match, dict):
        status = str(
            match.get("status_string") or match.get("status") or match.get("Eps", "")
        ).upper()
        score = f"{match.get('home_score', 0)}-{match.get('away_score', 0)}"
    else:
        status = str(getattr(getattr(match, "status", None), "description", "")).upper()
        score = f"{match.home_score.current}-{match.away_score.current}"

    minute, is_first_half = _resolve_phase(match, status)
    if minute is None or minute < PREDICT_START_MIN:
        return None

    return MatchContext(
        fid=fid,
        name=match_name,
        league=league,
        country=country,
        country_slug=country_slug,
        score=score,
        status=status,
        minute=minute,
        is_first_half=is_first_half,
    )


# ===========================================================================
# STRATEGY EXECUTION
# ===========================================================================

def _place_bet(ctx: MatchContext, strategy: Strategy) -> bool:
    """
    Attempt to place a bet for the given strategy.

    Returns True only when the bet was actually persisted, so the caller can
    decide whether the match is "done" for this cycle. Every guard fails safe
    (returns False) so duplicate/overlapping stakes are never placed.
    """
    if firebase_manager is None:
        STATE_LOCKS.inc()
        logger.warning(
            "🚫 Firebase unavailable — skipping %s placement for '%s'.",
            strategy.label, ctx.name,
        )
        return False

    # --- Per-score lock, held until the pending bet resolves at HT ----------
    if LOCK_UNTIL_HT_RESOLUTION and _is_score_pending(ctx.score):
        STATE_LOCKS.inc()
        logger.warning(
            "🚫 [%s] score '%s' already locked by a pending bet. "
            "Skipping '%s' — unlocks when that bet settles at HT.",
            strategy.label, ctx.score, ctx.name,
        )
        return False

    stake, seq = calculate_stake()
    flag = _flag_for(ctx.country_slug, ctx.country)

    logger.warning(
        "⚡ QUALIFIED (%s): firing placement for %s at score %s",
        strategy.label, ctx.name, ctx.score,
    )

    data = {
        "match_name": ctx.name,
        "league": ctx.league,
        "country": ctx.country,
        "country_slug": ctx.country_slug,
        SCORE_FIELD: ctx.score,
        LEGACY_SCORE_FIELD: ctx.score,
        "odds": strategy.odds,
        "stake": stake,
        "currency": CURRENCY_CODE,
        "match_sequence": seq,
        "bet_type": strategy.bet_type,
        "strategy": strategy.key,
        "staking_step": 0,
        "staking_sequence": [FLAT_STAKE_MMK],
    }
    if not firebase_manager.add_unresolved(strategy.unresolved_collection, ctx.fid, data):
        return False

    BET_TRIGGERS.inc()
    logger.info(
        "🔒 Locked score '%s' until half-time settlement (pending in %s).",
        ctx.score, strategy.unresolved_collection,
    )

    extra = " | one more goal expected before HT" if strategy.key == "over_0_5" else ""
    odds_display = f"{strategy.odds:.2f}"
    send_telegram(
        f"{strategy.emoji} **{strategy.label.upper()} BET PLACED (Flat {CURRENCY_CODE})**\n"
        f"⏱ Min: {ctx.minute}' | {ctx.name}\n"
        f"{flag} {ctx.country} | 🏆 {ctx.league}\n"
        f"🔢 Score: {ctx.score}{extra}\n"
        f"🎯 Odds: {odds_display}\n"
        f"💰 Stake: {stake:,.0f} {CURRENCY_CODE} | Flat Stake"
    )
    return True


def _settle_bet(ctx: MatchContext, strategy: Strategy, state: Dict) -> None:
    """Resolve a pending bet at half time using the strategy's win condition."""
    if firebase_manager is None:
        return

    unresolved = firebase_manager.get_unresolved(strategy.unresolved_collection, ctx.fid)
    if not unresolved:
        return

    trigger_score = str(
        unresolved.get(SCORE_FIELD) or unresolved.get(LEGACY_SCORE_FIELD)
    )
    same_score = ctx.score == trigger_score
    is_win = same_score if strategy.win_if_same_score else (not same_score)
    outcome = "win" if is_win else "loss"

    db_league = unresolved.get("league", ctx.league)
    db_country = unresolved.get("country", ctx.country)
    db_slug = unresolved.get("country_slug", ctx.country_slug)
    flag = _flag_for(db_slug, db_country)
    stake = unresolved.get("stake", 0)
    bets_odds = float(unresolved.get("odds", strategy.odds))

    if _staking_engine:
        _staking_engine.record_result(
            is_win,
            {
                "match_name": ctx.name,
                "league": db_league,
                "country": db_country,
                "score": ctx.score,
                "trigger_score": trigger_score,
                "stake": stake,
                "odds": bets_odds,
                "bet_type": strategy.bet_type,
            },
            odds=bets_odds,
        )
        step_display = f" | {_staking_engine.get_current_step_display()}"
        bankroll_display = f" | 💰 Bankroll: {_staking_engine.current_bankroll:,.0f} {CURRENCY_CODE}"
        profit_display = f" | 📈 Profit: {_staking_engine.total_profit:,.0f} {CURRENCY_CODE}"
    else:
        step_display = bankroll_display = profit_display = ""

    payout = (stake * (bets_odds - 1)) if is_win else (-stake)
    payout_display = f" | 💵 Payout: {payout:+,.0f} {CURRENCY_CODE} (x{bets_odds:.2f})"

    resolved_ok = firebase_manager.move_to_resolved(
        strategy.unresolved_collection,
        strategy.resolved_collection,
        ctx.fid,
        unresolved,
        outcome,
    )
    if resolved_ok:
        logger.info(
            "🔓 Unlocked score '%s' (%s -> %s) — available again for new bets.",
            trigger_score, strategy.unresolved_collection, strategy.resolved_collection,
        )

    result_icon = "✅ WIN" if is_win else "❌ LOSS"
    detail = "extra goal scored" if is_win else "no extra goal before HT"

    send_telegram(
        f"{result_icon} **HT Settlement ({strategy.label})**\n"
        f"⏱ 45' | {ctx.name}\n"
        f"{flag} {db_country} | 🏆 {db_league}\n"
        f"🔢 HT Score: {ctx.score} (Trigger: {trigger_score} — {detail})\n"
        f"🎯 Odds: {bets_odds:.2f} | 💰 Stake: {stake:,.0f} {CURRENCY_CODE}"
        f"{payout_display}"
        f"{step_display}{bankroll_display}{profit_display}"
    )

    state["active"] = False


def process_strategy(ctx: MatchContext, strategy: Strategy, state: Dict) -> None:
    """Evaluate one strategy for one match context."""
    if not strategy.enabled:
        return

    placed_key = f"placed_{strategy.key}"

    # Resolve first: at half time a pending bet is always settled.
    if ctx.status in HALFTIME_STATUSES:
        _settle_bet(ctx, strategy, state)
        return

    # Otherwise look for a fresh placement inside the trigger window.
    if (
        ctx.is_first_half
        and ctx.minute in strategy.trigger_minutes
        and not state.get(placed_key)
    ):
        if ctx.score in strategy.trigger_scores:
            # Only mark as done when the bet was actually persisted. If the
            # per-score lock blocked it, the match stays eligible and will
            # retry once the pending bet resolves at half time.
            if _place_bet(ctx, strategy):
                state[placed_key] = True
        else:
            logger.debug(
                "🔍 [%s] %s @ %s' | score %s not in %s",
                strategy.label, ctx.name, ctx.minute, ctx.score,
                sorted(strategy.trigger_scores),
            )


# ===========================================================================
# DRIVER LAYER
# ===========================================================================

def initialize_bot_services() -> bool:
    """Instantiate Firebase + the live data engine and reload persisted state."""
    global firebase_manager, LIVESCORE_CLIENT

    try:
        firebase_manager = FirebaseManager(FIREBASE_CREDENTIALS)
    except Exception as exc:  # noqa: BLE001
        logger.warning("⚠️ Firebase initialization failed (running without persistence): %s", exc)
        firebase_manager = None

    try:
        LIVESCORE_CLIENT = SofascoreClient()
        LIVESCORE_CLIENT.initialize()

        if _staking_engine is not None:
            if firebase_manager and firebase_manager.db is not None:
                _staking_engine.persistence = firebase_manager
                try:
                    _staking_engine._load_state()
                except Exception as exc:  # noqa: BLE001
                    logger.warning("⚠️ Could not load persisted staking state: %s", exc)
            logger.info("📊 Flat Stake Engine active: %s", _staking_engine.get_current_step_display())
            logger.info("💰 Bankroll: %s %s", f"{_staking_engine.current_bankroll:,.0f}", CURRENCY_CODE)
        else:
            logger.info("📊 Staking Engine not attached. Using flat fallback stake.")

        _log_strategy_summary()
        return True
    except Exception as exc:  # noqa: BLE001
        logger.exception("❌ Failed to instantiate data engine driver context: %s", exc)
        API_FAILURES.inc()
        return False


def _log_strategy_summary() -> None:
    for strategy in STRATEGIES:
        state = "ON" if strategy.enabled else "OFF"
        logger.info(
            "🎛️ Strategy [%s] %s | odds=%s | scores=%s | minutes=%s | %s -> %s",
            state,
            strategy.label,
            f"{strategy.odds:.2f}",
            sorted(strategy.trigger_scores),
            sorted(strategy.trigger_minutes),
            strategy.unresolved_collection,
            strategy.resolved_collection,
        )
    logger.info(
        "🔒 Locks | per-score held until HT resolution=%s",
        "ON" if LOCK_UNTIL_HT_RESOLUTION else "OFF",
    )


def shutdown_bot() -> None:
    global LIVESCORE_CLIENT
    if LIVESCORE_CLIENT:
        try:
            LIVESCORE_CLIENT.close()
        except Exception as exc:  # noqa: BLE001
            logger.error("Error shutting down client: %s", exc)
        finally:
            LIVESCORE_CLIENT = None


def run_bot_cycle() -> None:
    """Fetch live events once and evaluate every enabled strategy per match."""
    if not LIVESCORE_CLIENT:
        return

    try:
        events = LIVESCORE_CLIENT.get_events(live=True)
    except Exception as exc:  # noqa: BLE001
        logger.error("Ingestion lifecycle exception: %s", exc)
        API_FAILURES.inc()
        return

    if not events:
        logger.debug("No live events found in current cycle.")
        return

    for match in events:
        try:
            ctx = extract_match_context(match)
            if ctx is None:
                continue

            state = LOCAL_TRACKED_MATCHES.setdefault(
                ctx.fid, {"last_seen": time.time(), "active": False}
            )
            state["last_seen"] = time.time()
            if PRE_WARM_WINDOW[0] <= ctx.minute <= PRE_WARM_WINDOW[1]:
                state["active"] = True

            logger.info(
                "🔍 Match: %s | %s' | %s | %s | %s",
                ctx.name, ctx.minute, ctx.score, ctx.league, ctx.country,
            )

            if ctx.status in FULLTIME_STATUSES:
                state["active"] = False
                continue

            for strategy in STRATEGIES:
                process_strategy(ctx, strategy, state)
        except Exception as inner:  # noqa: BLE001
            logger.exception("Error evaluating single match node: %s", inner)

    prune_volatile_cache_leaks()
