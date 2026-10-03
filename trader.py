"""
trader.py — CS2 Arbitrage Decision Support System (DSS)
Production-grade inference pipeline with XGBoost model and multi-stage risk filters.

WORKFLOW:
1. Extract latest market snapshot from cs2_market.db
2. Engineer features with 100% parity to train_ml.py
3. XGBoost inference → filter by confidence threshold (≥80%)
4. Apply hard safety filters (price floor, regime, relative valuation, absolute profit)
5. Portfolio management (position tracking, diversification)
6. Telegram alerts for approved signals
"""

import json
import logging
import os
import sqlite3
import sys
from datetime import datetime, timedelta
from pathlib import Path
from typing import Dict, List, Optional

import numpy as np
import pandas as pd
import requests
from xgboost import XGBClassifier

# ══════════════════════════════════════════════════════════════════════════════
# CONFIGURATION
# ══════════════════════════════════════════════════════════════════════════════

DB_PATH = Path("cs2_market.db")
MODEL_PATH = Path("cs2_arbitrage_xgb.json")
POSITIONS_PATH = Path("active_positions.json")
LOG_PATH = Path("trader.log")

# Model threshold
CONFIDENCE_THRESHOLD = 0.80  # 80% probability for Class 1

# Safety filters
MIN_ENTRY_PRICE = 1.00  # PLN (Valve minimum fee protection)
MIN_ABSOLUTE_PROFIT = 0.20  # PLN
MIN_VOLUME = 10  # Minimum Steam volume
RELATIVE_VALUATION_MAX_RATIO = 3.0  # 300% above basket median

# Portfolio management
MAX_POSITIONS_PER_CATEGORY = 3
POSITION_COOLDOWN_HOURS = 12

# Telegram
TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN")
TELEGRAM_CHAT_ID = os.getenv("TELEGRAM_CHAT_ID")

# ══════════════════════════════════════════════════════════════════════════════
# LOGGING SETUP
# ══════════════════════════════════════════════════════════════════════════════

logging.basicConfig(
    level=logging.INFO,
    format="[%(asctime)s] [%(levelname)s] [%(module)s] - %(message)s",
    handlers=[
        logging.FileHandler(LOG_PATH, mode="a", encoding="utf-8"),
        logging.StreamHandler(sys.stdout),
    ],
)
logger = logging.getLogger(__name__)


# ══════════════════════════════════════════════════════════════════════════════
# STAGE 1: DATA EXTRACTION & FEATURE ENGINEERING
# ══════════════════════════════════════════════════════════════════════════════

def connect_db() -> sqlite3.Connection:
    """Open database connection with safety timeout."""
    if not DB_PATH.exists():
        logger.error("Database not found: %s", DB_PATH.resolve())
        sys.exit(1)
    
    conn = sqlite3.connect(DB_PATH, timeout=30.0)
    conn.execute("PRAGMA busy_timeout=30000;")
    return conn


def extract_latest_snapshot(conn: sqlite3.Connection) -> pd.DataFrame:
    """
    Extract the latest reading for each item from price_history.
    Returns a DataFrame with one row per item (most recent timestamp).
    """
    logger.info("Extracting latest market snapshot...")
    
    query = """
        WITH ranked AS (
            SELECT
                timestamp,
                item_name,
                steam_price,
                volume AS steam_volume,
                external_price AS csfloat_price,
                ROW_NUMBER() OVER (PARTITION BY item_name ORDER BY timestamp DESC) AS rn
            FROM price_history
            WHERE steam_price IS NOT NULL
              AND external_price IS NOT NULL
              AND steam_price > 0
              AND external_price > 0
        )
        SELECT
            timestamp,
            item_name,
            steam_price,
            steam_volume,
            csfloat_price
        FROM ranked
        WHERE rn = 1
    """
    
    df = pd.read_sql_query(query, conn)
    logger.info("Extracted %d items from latest snapshot", len(df))
    
    if len(df) == 0:
        logger.warning("No valid data in latest snapshot")
        return df
    
    # Convert timestamp to datetime
    df["timestamp_dt"] = pd.to_datetime(df["timestamp"])
    df["ts_epoch"] = df["timestamp_dt"].astype(np.int64) // 10**9
    
    return df


def load_historical_for_features(conn: sqlite3.Connection, item_names: List[str]) -> pd.DataFrame:
    """
    Load historical data for feature engineering (rolling windows).
    Need at least 7 days of history per item for volatility_7d and momentum_7d.
    """
    logger.info("Loading historical data for %d items...", len(item_names))
    
    if len(item_names) == 0:
        return pd.DataFrame()
    
    # Get data from last 14 days to ensure sufficient history
    cutoff_date = (datetime.utcnow() - timedelta(days=14)).isoformat()
    
    placeholders = ",".join(["?" for _ in item_names])
    query = f"""
        SELECT
            timestamp,
            item_name,
            steam_price,
            volume AS steam_volume,
            external_price AS csfloat_price
        FROM price_history
        WHERE item_name IN ({placeholders})
          AND timestamp >= ?
          AND steam_price IS NOT NULL
          AND external_price IS NOT NULL
          AND steam_price > 0
          AND csfloat_price > 0
        ORDER BY timestamp ASC
    """
    
    df = pd.read_sql_query(query, conn, params=item_names + [cutoff_date])
    logger.info("Loaded %d historical rows for feature engineering", len(df))
    
    # Convert timestamp
    df["timestamp_dt"] = pd.to_datetime(df["timestamp"])
    df["ts_epoch"] = df["timestamp_dt"].astype(np.int64) // 10**9
    
    return df


def winsorize_prices(df: pd.DataFrame) -> pd.DataFrame:
    """
    Winsorize prices per item (1% / 99% percentiles).
    EXACT REPLICATION of train_ml.py Track A.
    """
    logger.info("Winsorizing prices per item...")
    
    def winsorize_per_item(group: pd.DataFrame, col: str) -> pd.Series:
        """Cap values at 1st and 99th percentiles within each item group."""
        lower = group[col].quantile(0.01)
        upper = group[col].quantile(0.99)
        return group[col].clip(lower=lower, upper=upper)
    
    df["steam_price_winsorized"] = df.groupby("item_name", group_keys=False).apply(
        lambda g: winsorize_per_item(g, "steam_price")
    ).values
    
    df["csfloat_price_winsorized"] = df.groupby("item_name", group_keys=False).apply(
        lambda g: winsorize_per_item(g, "csfloat_price")
    ).values
    
    return df


def engineer_features(df: pd.DataFrame) -> pd.DataFrame:
    """
    Engineer features with EXACT PARITY to train_ml.py.
    All rolling windows use closed='left' to prevent future leak.
    """
    logger.info("Engineering features (per-item rolling windows)...")
    
    # Sort by item and timestamp
    df = df.sort_values(["item_name", "ts_epoch"]).reset_index(drop=True)
    
    # ── Volatility (7-day rolling std, closed='left') ──────────────────────────
    df["volatility_7d"] = df.groupby("item_name", group_keys=False)["steam_price_winsorized"].transform(
        lambda x: x.rolling(window=168, min_periods=1, closed="left").std()
    )
    
    # ── Price Momentum (% change over 3h, 24h, 7d) ─────────────────────────────
    def calc_momentum(group: pd.DataFrame, hours: int) -> pd.Series:
        """Calculate % price change over specified hours (per-item)."""
        shifted = group["steam_price_winsorized"].shift(hours)
        return ((group["steam_price_winsorized"] - shifted) / (shifted + 1e-9)) * 100
    
    for hours in [3, 24, 168]:  # 3h, 24h, 7d
        col_name = f"price_momentum_{hours}h" if hours < 168 else "price_momentum_7d"
        df[col_name] = df.groupby("item_name", group_keys=False).apply(
            lambda g: calc_momentum(g, hours)
        ).values
    
    # ── Market Stress Index ────────────────────────────────────────────────────
    df["steam_volume"] = df["steam_volume"].fillna(0).astype(float)
    df["market_stress_index"] = df["steam_volume"] / (df["volatility_7d"] + 1e-5)
    
    # ── Temporal Features ──────────────────────────────────────────────────────
    df["day_of_week"] = df["timestamp_dt"].dt.dayofweek
    df["hour"] = df["timestamp_dt"].dt.hour
    
    # ── Volume Reliability Flag ────────────────────────────────────────────────
    # For inference, assume all current data is reliable (post May 2026)
    df["is_reliable_volume"] = 1
    
    # Impute missing volume with 0 (conservative approach for inference)
    df["steam_volume"] = df["steam_volume"].fillna(0)
    
    # ── Dynamic Haircut ────────────────────────────────────────────────────────
    def calc_bid_haircut(volume: np.ndarray) -> np.ndarray:
        """
        H_t = 0.20 - clip(((log10(volume + 1) - 1) / 4) * 0.08, 0.0, 0.08)
        Result always in [0.12, 0.20].
        """
        log_vol = np.log10(volume + 1)
        reduction = np.clip(((log_vol - 1) / 4) * 0.08, 0.0, 0.08)
        return 0.20 - reduction
    
    df["bid_haircut"] = calc_bid_haircut(df["steam_volume"].values)
    
    logger.info("Feature engineering complete")
    return df


def prepare_inference_data(conn: sqlite3.Connection) -> pd.DataFrame:
    """
    Full pipeline: extract latest snapshot + historical data → engineer features.
    Returns DataFrame with latest row per item, ready for model inference.
    """
    # Step 1: Get latest snapshot
    latest_df = extract_latest_snapshot(conn)
    
    if len(latest_df) == 0:
        return pd.DataFrame()
    
    item_names = latest_df["item_name"].unique().tolist()
    
    # Step 2: Load historical data for rolling windows
    hist_df = load_historical_for_features(conn, item_names)
    
    if len(hist_df) == 0:
        logger.warning("No historical data available for feature engineering")
        return pd.DataFrame()
    
    # Step 3: Winsorize prices
    hist_df = winsorize_prices(hist_df)
    
    # Step 4: Engineer features
    hist_df = engineer_features(hist_df)
    
    # Step 5: Extract only the latest row per item (after feature calculation)
    hist_df = hist_df.sort_values(["item_name", "ts_epoch"])
    latest_with_features = hist_df.groupby("item_name").tail(1).reset_index(drop=True)
    
    # Step 6: Drop rows with NaN in critical features (insufficient history)
    critical_features = ["volatility_7d", "price_momentum_7d", "price_momentum_24h", "price_momentum_3h"]
    initial_count = len(latest_with_features)
    latest_with_features = latest_with_features.dropna(subset=critical_features)
    dropped = initial_count - len(latest_with_features)
    
    if dropped > 0:
        logger.warning("Dropped %d items due to insufficient history for rolling features", dropped)
    
    logger.info("Prepared %d items for inference", len(latest_with_features))
    return latest_with_features


# ══════════════════════════════════════════════════════════════════════════════
# STAGE 2: MODEL INFERENCE
# ══════════════════════════════════════════════════════════════════════════════

def load_model() -> XGBClassifier:
    """Load trained XGBoost model from JSON."""
    if not MODEL_PATH.exists():
        logger.error("Model file not found: %s", MODEL_PATH.resolve())
        sys.exit(1)
    
    model = XGBClassifier()
    model.load_model(str(MODEL_PATH))
    logger.info("Model loaded from: %s", MODEL_PATH.resolve())
    return model


def run_inference(model: XGBClassifier, df: pd.DataFrame) -> pd.DataFrame:
    """
    Run XGBoost inference and filter by confidence threshold.
    Returns only signals with P(Class 1) >= CONFIDENCE_THRESHOLD.
    """
    logger.info("Running model inference on %d items...", len(df))
    
    # Define feature columns (EXACT match to train_ml.py)
    exclude_cols = {
        "timestamp", "timestamp_dt", "ts_epoch", "item_name",
        "steam_price", "csfloat_price", "steam_volume",
        "steam_price_winsorized", "csfloat_price_winsorized",
        "bid_haircut"
    }
    
    feature_cols = [col for col in df.columns if col not in exclude_cols]
    
    # Verify all expected features are present
    expected_features = [
        "volatility_7d", "price_momentum_3h", "price_momentum_24h", "price_momentum_7d",
        "market_stress_index", "day_of_week", "hour", "is_reliable_volume"
    ]
    
    missing_features = set(expected_features) - set(feature_cols)
    if missing_features:
        logger.error("Missing features for inference: %s", missing_features)
        return pd.DataFrame()
    
    X = df[feature_cols].values
    
    # Predict probabilities
    y_proba = model.predict_proba(X)[:, 1]  # Probability of Class 1
    
    df["confidence"] = y_proba
    
    # Filter by confidence threshold
    signals = df[df["confidence"] >= CONFIDENCE_THRESHOLD].copy()
    
    logger.info("Model inference complete: %d / %d items passed confidence threshold (≥%.0f%%)",
                len(signals), len(df), CONFIDENCE_THRESHOLD * 100)
    
    return signals


# ══════════════════════════════════════════════════════════════════════════════
# STAGE 3: HARD SAFETY FILTERS (Risk Management)
# ══════════════════════════════════════════════════════════════════════════════

def apply_safety_filters(df: pd.DataFrame) -> pd.DataFrame:
    """
    Apply multi-stage safety filters to protect against edge cases.
    Each filter logs rejection count and reason.
    """
    if len(df) == 0:
        return df
    
    initial_count = len(df)
    logger.info("=" * 80)
    logger.info("STAGE 3: SAFETY FILTERS")
    logger.info("=" * 80)
    logger.info("Initial signals: %d", initial_count)
    
    # ── FILTER 1: Minimum Entry Price ──────────────────────────────────────────
    df = df[df["csfloat_price"] >= MIN_ENTRY_PRICE].copy()
    rejected_price = initial_count - len(df)
    logger.info("Filter 1 (Min Entry Price ≥%.2f PLN): Rejected %d, Remaining %d",
                MIN_ENTRY_PRICE, rejected_price, len(df))
    
    if len(df) == 0:
        return df
    
    # ── FILTER 2: Regime Filter (Falling Knife Protection) ─────────────────────
    falling_knife = (df["price_momentum_24h"] < 0) & (df["price_momentum_7d"] < 0)
    df = df[~falling_knife].copy()
    rejected_regime = initial_count - rejected_price - len(df)
    logger.info("Filter 2 (Regime: No Falling Knife): Rejected %d, Remaining %d",
                rejected_regime, len(df))
    
    if len(df) == 0:
        return df
    
    # ── FILTER 3: Relative Valuation (Basket Median Protection) ────────────────
    df = apply_relative_valuation_filter(df)
    rejected_valuation = initial_count - rejected_price - rejected_regime - len(df)
    logger.info("Filter 3 (Relative Valuation): Rejected %d, Remaining %d",
                rejected_valuation, len(df))
    
    if len(df) == 0:
        return df
    
    # ── FILTER 4: Absolute Profit Floor ────────────────────────────────────────
    # Calculate Steam Net: (steam_price * 0.88) / 1.15
    # 0.88 = 1 - 0.12 (minimum haircut from bid_haircut range)
    # 1.15 = VAT adjustment
    df["steam_net"] = (df["steam_price"] * 0.88) / 1.15
    df["profit"] = df["steam_net"] - df["csfloat_price"]
    df["roi"] = (df["profit"] / df["csfloat_price"]) * 100
    
    df = df[df["profit"] >= MIN_ABSOLUTE_PROFIT].copy()
    rejected_profit = initial_count - rejected_price - rejected_regime - rejected_valuation - len(df)
    logger.info("Filter 4 (Min Absolute Profit ≥%.2f PLN): Rejected %d, Remaining %d",
                MIN_ABSOLUTE_PROFIT, rejected_profit, len(df))
    
    # ── FILTER 5: Minimum Volume ───────────────────────────────────────────────
    df = df[df["steam_volume"] >= MIN_VOLUME].copy()
    rejected_volume = initial_count - rejected_price - rejected_regime - rejected_valuation - rejected_profit - len(df)
    logger.info("Filter 5 (Min Volume ≥%d): Rejected %d, Remaining %d",
                MIN_VOLUME, rejected_volume, len(df))
    
    logger.info("=" * 80)
    logger.info("SAFETY FILTERS COMPLETE: %d / %d signals approved", len(df), initial_count)
    logger.info("=" * 80)
    
    return df


def apply_relative_valuation_filter(df: pd.DataFrame) -> pd.DataFrame:
    """
    Relative Valuation: Group items into baskets and reject outliers.
    Reject if current Steam price > 300% of basket median.
    """
    if len(df) == 0:
        return df
    
    # Categorize items into baskets
    df["category"] = df["item_name"].apply(categorize_item)
    
    # Calculate median Steam price per category
    category_medians = df.groupby("category")["steam_price"].median().to_dict()
    
    # Filter out items priced > 300% above their basket median
    def is_valid_valuation(row):
        median = category_medians.get(row["category"], row["steam_price"])
        if median == 0:
            return True  # Skip check if median is 0
        ratio = row["steam_price"] / median
        return ratio <= RELATIVE_VALUATION_MAX_RATIO
    
    mask = df.apply(is_valid_valuation, axis=1)
    return df[mask].copy()


def categorize_item(item_name: str) -> str:
    """
    Categorize items into baskets for relative valuation.
    Simple heuristic based on item name keywords.
    """
    item_lower = item_name.lower()
    
    if "case" in item_lower or "skrzynka" in item_lower:
        return "Cases"
    elif "sticker" in item_lower or "naklejka" in item_lower:
        return "Stickers"
    elif "capsule" in item_lower or "kapsuła" in item_lower:
        return "Capsules"
    elif "package" in item_lower or "pakiet" in item_lower:
        return "Packages"
    elif "pin" in item_lower or "przypinka" in item_lower:
        return "Pins"
    elif "patch" in item_lower or "naszywka" in item_lower:
        return "Patches"
    elif "graffiti" in item_lower:
        return "Graffiti"
    elif "music kit" in item_lower or "zestaw muzyczny" in item_lower:
        return "Music Kits"
    else:
        return "Other"


# ══════════════════════════════════════════════════════════════════════════════
# STAGE 4: PORTFOLIO MANAGEMENT & TELEGRAM ALERTS
# ══════════════════════════════════════════════════════════════════════════════

def load_active_positions() -> List[Dict]:
    """Load active positions from JSON file."""
    if not POSITIONS_PATH.exists():
        logger.info("No active positions file found, creating new one")
        return []
    
    try:
        with open(POSITIONS_PATH, "r", encoding="utf-8") as f:
            positions = json.load(f)
        logger.info("Loaded %d active positions", len(positions))
        return positions
    except Exception as e:
        logger.error("Failed to load active positions: %s", e)
        return []


def save_active_positions(positions: List[Dict]) -> None:
    """Save active positions to JSON file."""
    try:
        with open(POSITIONS_PATH, "w", encoding="utf-8") as f:
            json.dump(positions, f, indent=2, ensure_ascii=False)
        logger.info("Saved %d active positions", len(positions))
    except Exception as e:
        logger.error("Failed to save active positions: %s", e)


def apply_portfolio_filters(df: pd.DataFrame, positions: List[Dict]) -> pd.DataFrame:
    """
    Apply portfolio management rules:
    1. Reject if item was signaled in last 12 hours (cooldown)
    2. Reject if category already has >= 3 open positions (diversification)
    """
    if len(df) == 0:
        return df
    
    logger.info("Applying portfolio filters...")
    initial_count = len(df)
    
    # Build cooldown map: item_name -> last signal timestamp
    cooldown_cutoff = datetime.utcnow() - timedelta(hours=POSITION_COOLDOWN_HOURS)
    cooldown_items = set()
    
    for pos in positions:
        pos_time = datetime.fromisoformat(pos["timestamp"])
        if pos_time >= cooldown_cutoff:
            cooldown_items.add(pos["item_name"])
    
    # Build category count map
    category_counts = {}
    for pos in positions:
        cat = pos.get("category", "Other")
        category_counts[cat] = category_counts.get(cat, 0) + 1
    
    # Filter signals
    def passes_portfolio_rules(row):
        # Check cooldown
        if row["item_name"] in cooldown_items:
            return False
        
        # Check category limit
        cat = row["category"]
        if category_counts.get(cat, 0) >= MAX_POSITIONS_PER_CATEGORY:
            return False
        
        return True
    
    df = df[df.apply(passes_portfolio_rules, axis=1)].copy()
    
    rejected = initial_count - len(df)
    logger.info("Portfolio filters: Rejected %d (cooldown or category limit), Remaining %d",
                rejected, len(df))
    
    return df


def send_telegram_alert(signal: Dict) -> bool:
    """
    Send Telegram alert for approved signal.
    Returns True if successful, False otherwise.
    """
    if not TELEGRAM_BOT_TOKEN or not TELEGRAM_CHAT_ID:
        logger.warning("Telegram credentials not configured, skipping alert")
        return False
    
    message = f"""🚨 SYGNAŁ ARBITRAŻOWY (Pewność: {signal['confidence']:.1f}%)

📦 Item: {signal['item_name']} ({signal['category']})

💰 Wejście (CSFloat): {signal['csfloat_price']:.2f} PLN
💵 Wyjście (Steam Netto): {signal['steam_net']:.2f} PLN
📈 Szacowany Zysk: +{signal['profit']:.2f} PLN ({signal['roi']:.1f}%)
📊 Wolumen: {signal['volume']:.0f}

⏰ {signal['timestamp']}"""
    
    url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage"
    payload = {
        "chat_id": TELEGRAM_CHAT_ID,
        "text": message,
        "parse_mode": "HTML"
    }
    
    try:
        response = requests.post(url, json=payload, timeout=10)
        response.raise_for_status()
        logger.info("Telegram alert sent for: %s", signal['item_name'])
        return True
    except Exception as e:
        logger.error("Failed to send Telegram alert: %s", e)
        return False


def process_signals(df: pd.DataFrame, positions: List[Dict]) -> List[Dict]:
    """
    Process approved signals: apply portfolio filters, send alerts, update positions.
    Returns updated positions list.
    """
    if len(df) == 0:
        logger.info("No signals to process")
        return positions
    
    # Apply portfolio filters
    df = apply_portfolio_filters(df, positions)
    
    if len(df) == 0:
        logger.info("All signals rejected by portfolio filters")
        return positions
    
    # Sort by ROI descending (prioritize best opportunities)
    df = df.sort_values("roi", ascending=False)
    
    logger.info("=" * 80)
    logger.info("PROCESSING %d APPROVED SIGNALS", len(df))
    logger.info("=" * 80)
    
    new_positions = []
    
    for idx, row in df.iterrows():
        signal = {
            "timestamp": datetime.utcnow().isoformat(),
            "item_name": row["item_name"],
            "category": row["category"],
            "csfloat_price": float(row["csfloat_price"]),
            "steam_net": float(row["steam_net"]),
            "profit": float(row["profit"]),
            "roi": float(row["roi"]),
            "volume": float(row["steam_volume"]),
            "confidence": float(row["confidence"])
        }
        
        # Send Telegram alert
        success = send_telegram_alert(signal)
        
        if success:
            new_positions.append(signal)
            logger.info("✓ Signal processed: %s (ROI: %.1f%%, Confidence: %.1f%%)",
                       signal["item_name"], signal["roi"], signal["confidence"] * 100)
        else:
            logger.warning("✗ Failed to send alert for: %s", signal["item_name"])
    
    # Merge with existing positions
    updated_positions = positions + new_positions
    
    # Clean up old positions (older than 24 hours)
    cutoff = datetime.utcnow() - timedelta(hours=24)
    updated_positions = [
        pos for pos in updated_positions
        if datetime.fromisoformat(pos["timestamp"]) >= cutoff
    ]
    
    logger.info("=" * 80)
    logger.info("SIGNAL PROCESSING COMPLETE")
    logger.info("New signals sent: %d", len(new_positions))
    logger.info("Total active positions: %d", len(updated_positions))
    logger.info("=" * 80)
    
    return updated_positions


# ══════════════════════════════════════════════════════════════════════════════
# MAIN PIPELINE
# ══════════════════════════════════════════════════════════════════════════════

def main():
    """Execute full trading decision pipeline."""
    logger.info("=" * 80)
    logger.info("CS2 ARBITRAGE DECISION SUPPORT SYSTEM (DSS)")
    logger.info("Timestamp: %s", datetime.utcnow().isoformat())
    logger.info("=" * 80)
    
    try:
        # Stage 0: Load model and positions
        model = load_model()
        positions = load_active_positions()
        
        # Stage 1: Data extraction and feature engineering
        conn = connect_db()
        df = prepare_inference_data(conn)
        conn.close()
        
        if len(df) == 0:
            logger.warning("No data available for inference. Exiting.")
            return
        
        # Stage 2: Model inference
        signals = run_inference(model, df)
        
        if len(signals) == 0:
            logger.info("No signals passed confidence threshold. Exiting.")
            return
        
        # Stage 3: Safety filters
        signals = apply_safety_filters(signals)
        
        if len(signals) == 0:
            logger.info("No signals passed safety filters. Exiting.")
            return
        
        # Stage 4: Portfolio management and alerts
        updated_positions = process_signals(signals, positions)
        save_active_positions(updated_positions)
        
        # Summary metrics
        if len(signals) > 0:
            avg_roi = signals["roi"].mean()
            avg_confidence = signals["confidence"].mean()
            logger.info("=" * 80)
            logger.info("EXECUTION SUMMARY")
            logger.info("Opportunities: %d", len(signals))
            logger.info("Avg ROI: %.2f%%", avg_roi)
            logger.info("Avg Confidence: %.2f%%", avg_confidence * 100)
            logger.info("=" * 80)
        
        logger.info("DSS execution complete")
        
    except Exception as e:
        logger.exception("Fatal error in DSS pipeline: %s", e)
        sys.exit(1)


if __name__ == "__main__":
    main()
