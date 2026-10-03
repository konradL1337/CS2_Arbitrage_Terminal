#!/usr/bin/env python3
"""
CALIBRATE_THRESHOLD.PY
======================
Mathematical calibration of XGBoost decision threshold with rigorous holdout validation.
Implements chronological split to prevent data leakage and p-hacking protection.

Author: Principal Data Scientist / Quant Evaluator
Version: 2.5
"""

import pandas as pd
import numpy as np
from pathlib import Path
from datetime import datetime
import sys


def load_predictions(filepath: str = "predictions_test.csv") -> pd.DataFrame:
    """
    Load predictions CSV with proper timestamp handling.
    
    Args:
        filepath: Path to predictions CSV file
        
    Returns:
        DataFrame with columns: item_name, timestamp, y_true, y_proba
    """
    if not Path(filepath).exists():
        raise FileNotFoundError(f"Predictions file not found: {filepath}")
    
    df = pd.read_csv(filepath)
    
    # Validate required columns
    required_cols = ['y_true', 'y_proba']
    missing = [col for col in required_cols if col not in df.columns]
    if missing:
        raise ValueError(f"Missing required columns: {missing}")
    
    # Handle timestamp column (might be named differently or be index)
    if 'timestamp' not in df.columns:
        # Try to find timestamp-like column
        time_cols = [col for col in df.columns if 'time' in col.lower() or 'date' in col.lower()]
        if time_cols:
            df['timestamp'] = pd.to_datetime(df[time_cols[0]])
            print(f"[INFO] Using column '{time_cols[0]}' as timestamp")
        else:
            # Use index if it looks like datetime
            if pd.api.types.is_datetime64_any_dtype(df.index):
                df['timestamp'] = df.index
                print("[INFO] Using index as timestamp")
            else:
                # Create synthetic timestamp based on row order
                df['timestamp'] = pd.date_range(start='2024-01-01', periods=len(df), freq='H')
                print("[WARNING] No timestamp found, using synthetic chronological timestamps")
    else:
        df['timestamp'] = pd.to_datetime(df['timestamp'])
    
    # Sort chronologically to ensure proper temporal ordering
    df = df.sort_values('timestamp').reset_index(drop=True)
    
    print(f"[INFO] Loaded {len(df)} predictions from {filepath}")
    print(f"[INFO] Date range: {df['timestamp'].min()} to {df['timestamp'].max()}")
    
    return df


def chronological_split(df: pd.DataFrame, split_ratio: float = 0.5) -> tuple:
    """
    Split data chronologically by unique dates (50/50 by default).
    
    Args:
        df: DataFrame with timestamp column
        split_ratio: Fraction for calibration set (default 0.5)
        
    Returns:
        Tuple of (df_calib, df_holdout)
    """
    # Extract unique dates
    df['date'] = df['timestamp'].dt.date
    unique_dates = sorted(df['date'].unique())
    
    n_dates = len(unique_dates)
    split_idx = int(n_dates * split_ratio)
    
    if split_idx == 0 or split_idx == n_dates:
        raise ValueError(f"Insufficient dates for split: {n_dates} unique dates")
    
    calib_dates = unique_dates[:split_idx]
    holdout_dates = unique_dates[split_idx:]
    
    df_calib = df[df['date'].isin(calib_dates)].copy()
    df_holdout = df[df['date'].isin(holdout_dates)].copy()
    
    print("\n" + "="*70)
    print("CHRONOLOGICAL SPLIT (50/50 by unique dates)")
    print("="*70)
    print(f"Total unique dates: {n_dates}")
    print(f"Calibration dates: {len(calib_dates)} (from {calib_dates[0]} to {calib_dates[-1]})")
    print(f"Holdout dates: {len(holdout_dates)} (from {holdout_dates[0]} to {holdout_dates[-1]})")
    print(f"Calibration samples: {len(df_calib)}")
    print(f"Holdout samples: {len(df_holdout)}")
    print(f"Split date boundary: {calib_dates[-1]} | {holdout_dates[0]}")
    print("="*70 + "\n")
    
    return df_calib, df_holdout


def compute_metrics(df: pd.DataFrame, threshold: float) -> dict:
    """
    Compute precision, recall, and signal count for a given threshold.
    
    Args:
        df: DataFrame with y_true and y_proba columns
        threshold: Decision threshold
        
    Returns:
        Dictionary with metrics
    """
    y_true = df['y_true'].values
    y_proba = df['y_proba'].values
    
    # Predictions at this threshold
    y_pred = (y_proba >= threshold).astype(int)
    
    # Count signals (positive predictions)
    n_signals = y_pred.sum()
    
    # True positives
    tp = ((y_true == 1) & (y_pred == 1)).sum()
    
    # Total actual positives
    total_positives = (y_true == 1).sum()
    
    # Compute metrics
    precision = tp / n_signals if n_signals > 0 else np.nan
    recall = tp / total_positives if total_positives > 0 else np.nan
    
    return {
        'threshold': threshold,
        'precision': precision,
        'recall': recall,
        'n_signals': n_signals,
        'tp': tp,
        'total_positives': total_positives
    }


def calibration_table(df_calib: pd.DataFrame, thresholds: np.ndarray) -> pd.DataFrame:
    """
    Generate calibration table for all thresholds.
    
    Args:
        df_calib: Calibration dataset
        thresholds: Array of thresholds to evaluate
        
    Returns:
        DataFrame with calibration metrics
    """
    results = []
    
    for thresh in thresholds:
        metrics = compute_metrics(df_calib, thresh)
        results.append(metrics)
    
    calib_df = pd.DataFrame(results)
    
    return calib_df


def print_calibration_table(calib_df: pd.DataFrame):
    """
    Print calibration table in human-readable ASCII format.
    
    Args:
        calib_df: Calibration results DataFrame
    """
    print("\n" + "="*70)
    print("STEP 1: CALIBRATION TABLE (on df_calib)")
    print("="*70)
    print(f"{'Threshold':<12} {'Precision':<12} {'Recall':<12} {'N_Signals':<12}")
    print("-"*70)
    
    for _, row in calib_df.iterrows():
        thresh = row['threshold']
        prec = f"{row['precision']:.4f}" if not np.isnan(row['precision']) else "N/A"
        rec = f"{row['recall']:.4f}" if not np.isnan(row['recall']) else "N/A"
        n_sig = int(row['n_signals'])
        
        print(f"{thresh:<12.2f} {prec:<12} {rec:<12} {n_sig:<12}")
    
    print("="*70 + "\n")


def select_optimal_threshold(calib_df: pd.DataFrame, 
                            min_precision: float = 0.65,
                            min_signals_strict: int = 30,
                            min_signals_fallback: int = 10) -> float:
    """
    Select optimal threshold with p-hacking protection.
    
    Strategy:
    1. Find lowest threshold where Precision >= 0.65 AND N_signals >= 30
    2. If none found, find highest Precision where N_signals >= 10
    3. If still none, default to 0.50 with warning
    
    Args:
        calib_df: Calibration results DataFrame
        min_precision: Minimum acceptable precision (default 0.65)
        min_signals_strict: Minimum signals for strict selection (default 30)
        min_signals_fallback: Minimum signals for fallback (default 10)
        
    Returns:
        Selected threshold
    """
    print("\n" + "="*70)
    print("STEP 2: OPTIMAL THRESHOLD SELECTION (Auto-Tuning)")
    print("="*70)
    
    # Strategy 1: Strict criteria (Precision >= 0.65, N >= 30)
    strict_candidates = calib_df[
        (calib_df['precision'] >= min_precision) & 
        (calib_df['n_signals'] >= min_signals_strict)
    ]
    
    if len(strict_candidates) > 0:
        # Select lowest threshold (most permissive while meeting criteria)
        optimal_thresh = strict_candidates['threshold'].min()
        optimal_row = calib_df[calib_df['threshold'] == optimal_thresh].iloc[0]
        
        print(f"[SUCCESS] Strategy 1: Found threshold meeting strict criteria")
        print(f"  Threshold: {optimal_thresh:.2f}")
        print(f"  Precision: {optimal_row['precision']:.4f} (>= {min_precision})")
        print(f"  Recall: {optimal_row['recall']:.4f}")
        print(f"  N_Signals: {int(optimal_row['n_signals'])} (>= {min_signals_strict})")
        print("="*70 + "\n")
        
        return optimal_thresh
    
    print(f"[WARNING] No threshold meets strict criteria (Precision >= {min_precision}, N >= {min_signals_strict})")
    
    # Strategy 2: Fallback (Highest Precision with N >= 10)
    fallback_candidates = calib_df[
        (calib_df['n_signals'] >= min_signals_fallback) &
        (~calib_df['precision'].isna())
    ]
    
    if len(fallback_candidates) > 0:
        # Select highest precision
        optimal_row = fallback_candidates.loc[fallback_candidates['precision'].idxmax()]
        optimal_thresh = optimal_row['threshold']
        
        print(f"[FALLBACK] Strategy 2: Using highest precision with N >= {min_signals_fallback}")
        print(f"  Threshold: {optimal_thresh:.2f}")
        print(f"  Precision: {optimal_row['precision']:.4f}")
        print(f"  Recall: {optimal_row['recall']:.4f}")
        print(f"  N_Signals: {int(optimal_row['n_signals'])}")
        print("="*70 + "\n")
        
        return optimal_thresh
    
    # Strategy 3: Last resort
    print("[CRITICAL WARNING] No threshold meets even fallback criteria!")
    print("  Defaulting to threshold = 0.50")
    print("  RECOMMENDATION: Collect more data or retrain model")
    print("="*70 + "\n")
    
    return 0.50


def holdout_validation(df_holdout: pd.DataFrame, optimal_threshold: float, n_holdout_days: int):
    """
    Validate selected threshold on holdout set and print final report.
    
    Args:
        df_holdout: Holdout dataset
        optimal_threshold: Selected threshold from calibration
        n_holdout_days: Number of unique days in holdout set
    """
    metrics = compute_metrics(df_holdout, optimal_threshold)
    
    signals_per_day = metrics['n_signals'] / n_holdout_days if n_holdout_days > 0 else 0
    
    print("\n" + "="*70)
    print("STEP 3: HOLDOUT VALIDATION (Out-of-Sample Performance)")
    print("="*70)
    print(f"Selected Threshold: {optimal_threshold:.2f}")
    print("-"*70)
    print(f"True Precision (Holdout): {metrics['precision']:.4f}" if not np.isnan(metrics['precision']) else "True Precision (Holdout): N/A")
    print(f"True Recall (Holdout): {metrics['recall']:.4f}" if not np.isnan(metrics['recall']) else "True Recall (Holdout): N/A")
    print(f"Real Signals Generated: {int(metrics['n_signals'])}")
    print(f"Holdout Period: {n_holdout_days} days")
    print(f"Estimated Signals/Day: {signals_per_day:.2f}")
    print("-"*70)
    print(f"True Positives: {int(metrics['tp'])}")
    print(f"Total Actual Positives: {int(metrics['total_positives'])}")
    print("="*70 + "\n")
    
    # Additional insights
    if not np.isnan(metrics['precision']):
        if metrics['precision'] >= 0.70:
            print("[EXCELLENT] Holdout precision >= 70% - Model is production-ready")
        elif metrics['precision'] >= 0.60:
            print("[GOOD] Holdout precision >= 60% - Model is acceptable")
        elif metrics['precision'] >= 0.50:
            print("[ACCEPTABLE] Holdout precision >= 50% - Monitor closely")
        else:
            print("[WARNING] Holdout precision < 50% - Consider model retraining")
    
    if metrics['n_signals'] < 10:
        print("[WARNING] Very few signals on holdout - Results may have high variance")
    
    print()


def main():
    """
    Main execution flow for threshold calibration.
    """
    print("\n" + "="*70)
    print("XGBOOST THRESHOLD CALIBRATION - HOLDOUT VALIDATION")
    print("="*70)
    print(f"Execution Time: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("="*70 + "\n")
    
    try:
        # Step 0: Load predictions
        df = load_predictions("predictions_test.csv")
        
        # Chronological split (50/50)
        df_calib, df_holdout = chronological_split(df, split_ratio=0.5)
        
        # Count unique days in holdout for daily signal estimation
        n_holdout_days = df_holdout['date'].nunique()
        
        # Step 1: Generate calibration table
        thresholds = np.arange(0.50, 1.00, 0.05)
        calib_df = calibration_table(df_calib, thresholds)
        print_calibration_table(calib_df)
        
        # Step 2: Select optimal threshold
        optimal_threshold = select_optimal_threshold(
            calib_df,
            min_precision=0.65,
            min_signals_strict=30,
            min_signals_fallback=10
        )
        
        # Step 3: Holdout validation
        holdout_validation(df_holdout, optimal_threshold, n_holdout_days)
        
        print("[SUCCESS] Calibration completed successfully")
        print(f"[RECOMMENDATION] Use threshold = {optimal_threshold:.2f} in production\n")
        
    except FileNotFoundError as e:
        print(f"[ERROR] {e}", file=sys.stderr)
        print("[INFO] Please ensure 'predictions_test.csv' exists in the current directory")
        sys.exit(1)
    except Exception as e:
        print(f"[ERROR] Unexpected error: {e}", file=sys.stderr)
        import traceback
        traceback.print_exc()
        sys.exit(1)


if __name__ == "__main__":
    main()
