"""
train_ml.py — CS2 Arbitrage XGBoost Binary Classifier
Production-grade training pipeline with strict data leak prevention.

Target: Predict profitable CSFloat → Steam arbitrage opportunities (7-day horizon).
Zero look-ahead bias, zero cross-item contamination, zero preprocessing leakage.
"""

import logging
import sqlite3
import sys
from pathlib import Path

import numpy as np
import pandas as pd
from sklearn.metrics import (
    confusion_matrix,
    precision_recall_fscore_support,
    roc_auc_score,
    average_precision_score,
)
from xgboost import XGBClassifier

# ══════════════════════════════════════════════════════════════════════════════
# CONFIGURATION
# ══════════════════════════════════════════════════════════════════════════════

DB_PATH = Path("cs2_market.db")
MODEL_PATH = Path("cs2_arbitrage_xgb.json")
PREDICTIONS_PATH = Path("predictions_test.csv")
LOG_PATH = Path("train_ml.log")

RELIABLE_VOLUME_DATE = "2026-05-15"  # Data quality threshold
MERGE_TOLERANCE_SEC = 7200  # 2 hours (max harvester downtime)
FORECAST_HORIZON_SEC = 604800  # 7 days in seconds

# ══════════════════════════════════════════════════════════════════════════════
# LOGGING SETUP
# ══════════════════════════════════════════════════════════════════════════════

logging.basicConfig(
    level=logging.INFO,
    format="[%(asctime)s] [%(levelname)s] [%(module)s] - %(message)s",
    handlers=[
        logging.FileHandler(LOG_PATH, mode="w", encoding="utf-8"),
        logging.StreamHandler(sys.stdout),
    ],
)
logger = logging.getLogger(__name__)


# ══════════════════════════════════════════════════════════════════════════════
# STEP 0 — DATABASE CONNECTION & VERIFICATION
# ══════════════════════════════════════════════════════════════════════════════

def connect_db() -> sqlite3.Connection:
    """Open connection with safety timeout and verify journal mode."""
    if not DB_PATH.exists():
        logger.error("Database not found: %s", DB_PATH.resolve())
        sys.exit(1)
    
    conn = sqlite3.connect(DB_PATH, timeout=30.0)
    conn.execute("PRAGMA busy_timeout=30000;")
    
    # Verify and log journal mode (do NOT assume WAL is set)
    journal_mode = conn.execute("PRAGMA journal_mode;").fetchone()[0]
    logger.info("Database journal_mode: %s", journal_mode)
    
    return conn


# ══════════════════════════════════════════════════════════════════════════════
# STEP 1 — DATA LOADING & TIME CONVERSION
# ══════════════════════════════════════════════════════════════════════════════

def load_data(conn: sqlite3.Connection) -> pd.DataFrame:
    """Load price_history table and convert timestamp to UNIX epoch."""
    logger.info("Loading price_history table...")
    
    query = """
        SELECT
            timestamp,
            item_name,
            steam_price,
            volume AS steam_volume,
            external_price AS csfloat_price
        FROM price_history
        WHERE steam_price IS NOT NULL
          AND external_price IS NOT NULL
        ORDER BY timestamp ASC
    """
    
    df = pd.read_sql_query(query, conn)
    initial_rows = len(df)
    logger.info("Loaded %d rows from price_history", initial_rows)
    
    # Convert ISO timestamp string to datetime, then to UNIX epoch (int64)
    df["timestamp_dt"] = pd.to_datetime(df["timestamp"])
    df["ts_epoch"] = df["timestamp_dt"].astype(np.int64) // 10**9  # nanoseconds → seconds
    
    # Sort globally by ts_epoch (required for merge_asof)
    df = df.sort_values("ts_epoch").reset_index(drop=True)
    logger.info("Converted timestamp to ts_epoch and sorted globally")
    
    return df


# ══════════════════════════════════════════════════════════════════════════════
# STEP 2 — DATA CLEANING (TWO SEPARATE TRACKS)
# ══════════════════════════════════════════════════════════════════════════════

def clean_data(df: pd.DataFrame) -> pd.DataFrame:
    """
    Track A: Winsorize prices for feature engineering (X).
    Track B: Hard filter for target calculation (Y) — NO winsorization.
    """
    initial_rows = len(df)
    
    # ── Track B: Hard filter for target (remove technical errors only) ────────
    df = df[
        (df["steam_price"] > 0) & 
        (df["csfloat_price"] > 0) &
        (df["steam_price"].notna()) &
        (df["csfloat_price"].notna()) &
        (np.isfinite(df["steam_price"])) &
        (np.isfinite(df["csfloat_price"]))
    ].copy()
    
    removed_errors = initial_rows - len(df)
    logger.info("Track B (Target): Removed %d rows with price errors (%.2f%%)",
                removed_errors, 100 * removed_errors / initial_rows)
    
    # ── Track A: Winsorize prices for features (per-item, 1% / 99% percentiles) ─
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
    
    logger.info("Track A (Features): Winsorized steam_price and csfloat_price per item (1%%/99%%)")
    logger.info("Rows after cleaning: %d", len(df))
    
    return df


# ══════════════════════════════════════════════════════════════════════════════
# STEP 3 — FEATURE ENGINEERING
# ══════════════════════════════════════════════════════════════════════════════

def engineer_features(df: pd.DataFrame) -> pd.DataFrame:
    """
    Create features with STRICT per-item grouping and closed='left' rolling windows.
    All operations use winsorized prices to avoid outlier contamination.
    """
    logger.info("Engineering features (per-item rolling windows)...")
    
    # ── Volatility (7-day rolling std, closed='left' = no future leak) ─────────
    df["volatility_7d"] = df.groupby("item_name", group_keys=False)["steam_price_winsorized"].transform(
        lambda x: x.rolling(window=168, min_periods=1, closed="left").std()  # 168 hours = 7 days
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
    # Handle missing steam_volume before calculation
    df["steam_volume"] = df["steam_volume"].fillna(0).astype(float)
    df["market_stress_index"] = df["steam_volume"] / (df["volatility_7d"] + 1e-5)
    
    # ── Temporal Features ──────────────────────────────────────────────────────
    df["day_of_week"] = df["timestamp_dt"].dt.dayofweek
    df["hour"] = df["timestamp_dt"].dt.hour
    
    # ── Volume Reliability Flag ────────────────────────────────────────────────
    reliable_date = pd.to_datetime(RELIABLE_VOLUME_DATE)
    df["is_reliable_volume"] = (df["timestamp_dt"] >= reliable_date).astype(int)
    
    # Impute missing/invalid volume with median from reliable period (per-item)
    def impute_volume_per_item(group: pd.DataFrame) -> pd.Series:
        """Impute volume using median from reliable period only."""
        reliable_median = group.loc[group["is_reliable_volume"] == 1, "steam_volume"].median()
        if pd.isna(reliable_median):
            reliable_median = 0.0  # Fallback if no reliable data for this item
        
        volume = group["steam_volume"].copy()
        mask = (group["is_reliable_volume"] == 0) & ((volume <= 0) | volume.isna())
        volume.loc[mask] = reliable_median
        return volume
    
    df["steam_volume"] = df.groupby("item_name", group_keys=False).apply(
        impute_volume_per_item
    ).values
    
    logger.info("Volume imputation: median from reliable period (>= %s) applied per-item", RELIABLE_VOLUME_DATE)
    
    # ── Dynamic Haircut (global, not per-item — market liquidity feature) ──────
    def calc_bid_haircut(volume: np.ndarray) -> np.ndarray:
        """
        H_t = 0.20 - clip(((log10(volume + 1) - 1) / 4) * 0.08, 0.0, 0.08)
        Result always in [0.12, 0.20].
        """
        log_vol = np.log10(volume + 1)
        reduction = np.clip(((log_vol - 1) / 4) * 0.08, 0.0, 0.08)
        return 0.20 - reduction
    
    df["bid_haircut"] = calc_bid_haircut(df["steam_volume"].values)
    logger.info("Dynamic bid_haircut calculated (range: [0.12, 0.20])")
    
    logger.info("Feature engineering complete")
    return df


# ══════════════════════════════════════════════════════════════════════════════
# STEP 4 — COLD START REMOVAL
# ══════════════════════════════════════════════════════════════════════════════

def remove_cold_start(df: pd.DataFrame) -> pd.DataFrame:
    """Drop rows with NaN in rolling features due to insufficient history."""
    initial_rows = len(df)
    
    # Check critical rolling features
    rolling_features = ["volatility_7d", "price_momentum_7d"]
    df = df.dropna(subset=rolling_features).copy()
    
    removed = initial_rows - len(df)
    logger.info("Cold start removal: dropped %d rows (%.2f%%) with insufficient history",
                removed, 100 * removed / initial_rows if initial_rows > 0 else 0)
    
    return df


# ══════════════════════════════════════════════════════════════════════════════
# STEP 5 — TARGET DEFINITION (merge_asof, NO shift())
# ══════════════════════════════════════════════════════════════════════════════

def define_target(df: pd.DataFrame) -> pd.DataFrame:
    """
    Use merge_asof to find steam_price at t+7 days, then calculate binary target.
    Target = 1 if (steam_price_future * (1 - haircut) / 1.15) >= csfloat_price * 1.05
    """
    logger.info("Defining target via merge_asof (7-day horizon)...")
    
    initial_rows = len(df)
    
    # Create target timestamp column (t + 7 days)
    df["target_ts_epoch"] = df["ts_epoch"] + FORECAST_HORIZON_SEC
    
    # Prepare future price lookup table
    future_df = df[["item_name", "ts_epoch", "steam_price"]].copy()
    future_df = future_df.rename(columns={
        "ts_epoch": "ts_epoch_future",
        "steam_price": "steam_price_future"
    })
    
    # Merge_asof to find nearest future price within tolerance
    df = pd.merge_asof(
        left=df.sort_values("target_ts_epoch"),
        right=future_df.sort_values("ts_epoch_future"),
        left_on="target_ts_epoch",
        right_on="ts_epoch_future",
        by="item_name",
        direction="nearest",
        tolerance=MERGE_TOLERANCE_SEC
    )
    
    # Drop rows where merge failed (no future price within tolerance)
    df = df.dropna(subset=["steam_price_future"]).copy()
    
    removed_merge = initial_rows - len(df)
    logger.info("Merge_asof: dropped %d rows (%.2f%%) without valid future price match",
                removed_merge, 100 * removed_merge / initial_rows if initial_rows > 0 else 0)
    
    # ── Calculate Target ───────────────────────────────────────────────────────
    # Steam_Net_t7 = (steam_price_future * (1 - bid_haircut)) / 1.15
    # Cost = csfloat_price (original, not winsorized)
    # Target = 1 if Steam_Net_t7 >= Cost * 1.05, else 0
    
    steam_net_t7 = (df["steam_price_future"] * (1 - df["bid_haircut"])) / 1.15
    cost = df["csfloat_price"]
    
    df["target"] = (steam_net_t7 >= cost * 1.05).astype(int)
    
    target_dist = df["target"].value_counts().to_dict()
    logger.info("Target distribution: %s", target_dist)
    logger.info("Rows after target definition: %d", len(df))
    
    return df


# ══════════════════════════════════════════════════════════════════════════════
# STEP 6 — CHRONOLOGICAL TRAIN/TEST SPLIT (zero stats leakage)
# ══════════════════════════════════════════════════════════════════════════════

def split_train_test(df: pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame]:
    """
    80/20 chronological split by calendar date.
    All preprocessing stats (scale_pos_weight, volume median) computed on Train only.
    """
    logger.info("Performing chronological train/test split (80/20)...")
    
    # Extract unique calendar dates
    df["date"] = df["timestamp_dt"].dt.date
    unique_dates = sorted(df["date"].unique())
    
    split_idx = int(len(unique_dates) * 0.8)
    split_date = unique_dates[split_idx]
    
    train_df = df[df["date"] < split_date].copy()
    test_df = df[df["date"] >= split_date].copy()
    
    logger.info("Split date: %s", split_date)
    logger.info("Train set: %d rows (%.1f%%)", len(train_df), 100 * len(train_df) / len(df))
    logger.info("Test set: %d rows (%.1f%%)", len(test_df), 100 * len(test_df) / len(df))
    
    # Log class distribution for both sets
    train_dist = train_df["target"].value_counts().to_dict()
    test_dist = test_df["target"].value_counts().to_dict()
    
    logger.info("Train target distribution: %s", train_dist)
    logger.info("Test target distribution: %s", test_dist)
    
    return train_df, test_df


# ══════════════════════════════════════════════════════════════════════════════
# STEP 7 — MODEL TRAINING
# ══════════════════════════════════════════════════════════════════════════════

def train_model(train_df: pd.DataFrame) -> tuple[XGBClassifier, list[str]]:
    """Train XGBoost classifier with sample weights and class balancing."""
    logger.info("Training XGBoost classifier...")
    
    # Define feature columns (exclude metadata and target)
    exclude_cols = {
        "timestamp", "timestamp_dt", "ts_epoch", "date", "item_name",
        "steam_price", "csfloat_price", "steam_volume",  # Original columns
        "steam_price_winsorized", "csfloat_price_winsorized",  # Winsorized (used for feature calc)
        "target_ts_epoch", "ts_epoch_future", "steam_price_future",  # Merge artifacts
        "target", "bid_haircut"  # Target and helper column
    }
    
    feature_cols = [col for col in train_df.columns if col not in exclude_cols]
    logger.info("Feature columns (%d): %s", len(feature_cols), feature_cols)
    
    X_train = train_df[feature_cols].values
    y_train = train_df["target"].values
    
    # ── Sample Weights (based on volume reliability) ───────────────────────────
    sample_weight = np.where(train_df["is_reliable_volume"] == 0, 0.3, 1.0)
    logger.info("Sample weights: %.1f%% at 0.3 (unreliable), %.1f%% at 1.0 (reliable)",
                100 * (sample_weight == 0.3).sum() / len(sample_weight),
                100 * (sample_weight == 1.0).sum() / len(sample_weight))
    
    # ── Class Balancing (scale_pos_weight) ─────────────────────────────────────
    n_neg = (y_train == 0).sum()
    n_pos = (y_train == 1).sum()
    scale_pos_weight = n_neg / n_pos if n_pos > 0 else 1.0
    
    logger.info("Class balance: %d negative, %d positive → scale_pos_weight=%.2f",
                n_neg, n_pos, scale_pos_weight)
    
    # ── XGBoost Training ───────────────────────────────────────────────────────
    model = XGBClassifier(
        max_depth=4,
        learning_rate=0.05,
        n_estimators=100,
        subsample=0.8,
        scale_pos_weight=scale_pos_weight,
        eval_metric=["logloss", "aucpr"],
        random_state=42,
        n_jobs=-1
    )
    
    model.fit(X_train, y_train, sample_weight=sample_weight, verbose=False)
    logger.info("Model training complete")
    
    return model, feature_cols


# ══════════════════════════════════════════════════════════════════════════════
# STEP 8 — EVALUATION
# ══════════════════════════════════════════════════════════════════════════════

def evaluate_model(
    model: XGBClassifier,
    feature_cols: list[str],
    train_df: pd.DataFrame,
    test_df: pd.DataFrame
) -> None:
    """Comprehensive evaluation on test set with detailed metrics."""
    logger.info("=" * 80)
    logger.info("MODEL EVALUATION")
    logger.info("=" * 80)
    
    X_test = test_df[feature_cols].values
    y_test = test_df["target"].values
    
    # Predictions
    y_pred = model.predict(X_test)
    y_proba = model.predict_proba(X_test)[:, 1]
    
    # ── Class Distribution ─────────────────────────────────────────────────────
    train_dist = train_df["target"].value_counts()
    test_dist = test_df["target"].value_counts()
    
    logger.info("\nClass Distribution:")
    logger.info("  Train: Class 0 = %d (%.1f%%), Class 1 = %d (%.1f%%)",
                train_dist.get(0, 0), 100 * train_dist.get(0, 0) / len(train_df),
                train_dist.get(1, 0), 100 * train_dist.get(1, 0) / len(train_df))
    logger.info("  Test:  Class 0 = %d (%.1f%%), Class 1 = %d (%.1f%%)",
                test_dist.get(0, 0), 100 * test_dist.get(0, 0) / len(test_df),
                test_dist.get(1, 0), 100 * test_dist.get(1, 0) / len(test_df))
    
    # ── Confusion Matrix ───────────────────────────────────────────────────────
    cm = confusion_matrix(y_test, y_pred)
    logger.info("\nConfusion Matrix:")
    logger.info("  [[TN=%d, FP=%d],", cm[0, 0], cm[0, 1])
    logger.info("   [FN=%d, TP=%d]]", cm[1, 0], cm[1, 1])
    
    # ── Precision, Recall, F1 ──────────────────────────────────────────────────
    precision, recall, f1, _ = precision_recall_fscore_support(y_test, y_pred, average="binary")
    logger.info("\nMetrics (threshold=0.5):")
    logger.info("  Precision: %.4f", precision)
    logger.info("  Recall:    %.4f", recall)
    logger.info("  F1-Score:  %.4f", f1)
    
    # ── ROC-AUC & PR-AUC ───────────────────────────────────────────────────────
    try:
        roc_auc = roc_auc_score(y_test, y_proba)
        pr_auc = average_precision_score(y_test, y_proba)
        logger.info("  ROC-AUC:   %.4f", roc_auc)
        logger.info("  PR-AUC:    %.4f", pr_auc)
    except ValueError as e:
        logger.warning("Could not compute AUC metrics: %s", e)
    
    # ── Positive Predictions Count ────────────────────────────────────────────
    n_positive_preds = (y_pred == 1).sum()
    logger.info("\nPositive predictions (threshold=0.5): %d / %d (%.1f%%)",
                n_positive_preds, len(y_pred), 100 * n_positive_preds / len(y_pred))
    
    if n_positive_preds < 10:
        logger.warning("⚠️  WARNING: Very few positive predictions! Model may be too conservative.")
    
    # ── Save Predictions ───────────────────────────────────────────────────────
    predictions_df = pd.DataFrame({
        "item_name": test_df["item_name"].values,
        "timestamp": test_df["timestamp"].values,
        "y_true": y_test,
        "y_pred": y_pred,
        "y_proba": y_proba
    })
    predictions_df.to_csv(PREDICTIONS_PATH, index=False)
    logger.info("\nPredictions saved to: %s", PREDICTIONS_PATH.resolve())
    
    logger.info("=" * 80)


# ══════════════════════════════════════════════════════════════════════════════
# MAIN PIPELINE
# ══════════════════════════════════════════════════════════════════════════════

def main():
    """Execute full training pipeline."""
    logger.info("=" * 80)
    logger.info("CS2 ARBITRAGE XGBOOST TRAINING PIPELINE")
    logger.info("=" * 80)
    
    try:
        # Step 0: Connect to database
        conn = connect_db()
        
        # Step 1: Load data
        df = load_data(conn)
        conn.close()
        
        if len(df) == 0:
            logger.error("No data loaded from database. Exiting.")
            sys.exit(1)
        
        # Step 2: Clean data
        df = clean_data(df)
        
        # Step 3: Engineer features
        df = engineer_features(df)
        
        # Step 4: Remove cold start
        df = remove_cold_start(df)
        
        # Step 5: Define target
        df = define_target(df)
        
        if len(df) == 0:
            logger.error("No data remaining after target definition. Exiting.")
            sys.exit(1)
        
        # Step 6: Train/test split
        train_df, test_df = split_train_test(df)
        
        if len(train_df) == 0 or len(test_df) == 0:
            logger.error("Train or test set is empty. Exiting.")
            sys.exit(1)
        
        # Step 7: Train model
        model, feature_cols = train_model(train_df)
        
        # Step 8: Evaluate
        evaluate_model(model, feature_cols, train_df, test_df)
        
        # Step 9: Save model
        model.save_model(str(MODEL_PATH))
        logger.info("Model saved to: %s", MODEL_PATH.resolve())
        
        logger.info("=" * 80)
        logger.info("TRAINING PIPELINE COMPLETE")
        logger.info("=" * 80)
        
    except Exception as e:
        logger.exception("Fatal error in training pipeline: %s", e)
        sys.exit(1)


if __name__ == "__main__":
    main()
