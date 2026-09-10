"""
Reusable statistical helpers for the thalamus-time literature project.

Functions
---------
mann_kendall  – Mann-Kendall trend test on year/value pairs
wilson_ci     – Wilson score confidence interval for proportions
rolling_mean  – Convenience wrapper around pandas rolling mean
trend_label   – Classify a Mann-Kendall result as emerging / declining / stable
"""

import numpy as np
import pandas as pd
from scipy.stats import kendalltau


# ── Mann-Kendall trend test ──────────────────────────────────────────────────

def mann_kendall(years, values, min_n=5):
    """Mann-Kendall trend test on paired year / value vectors.

    Parameters
    ----------
    years : array-like
        Numeric year values (e.g. 1990 .. 2025).
    values : array-like
        Corresponding metric values (proportions, counts, etc.).
    min_n : int, optional
        Minimum number of finite observations required. Default 5.

    Returns
    -------
    tau : float
        Kendall tau correlation coefficient (NaN if insufficient data).
    p : float
        Two-sided p-value (NaN if insufficient data).
    sig : str
        Significance string: "***" (p<0.001), "**" (p<0.01),
        "*" (p<0.05), "ns", or "n.d." if too few points.
    label : str
        Human-readable trend label via :func:`trend_label`.
    """
    years = np.asarray(years, dtype=float)
    values = np.asarray(values, dtype=float)
    mask = np.isfinite(values)
    if mask.sum() < min_n:
        return np.nan, np.nan, "n.d.", "insufficient data"
    tau, p = kendalltau(years[mask], values[mask])
    sig = (
        "***" if p < 0.001 else
        "**"  if p < 0.01  else
        "*"   if p < 0.05  else
        "ns"
    )
    return tau, p, sig, trend_label(tau, p)


# ── Wilson score confidence interval ─────────────────────────────────────────

def wilson_ci(count, total, z=1.96):
    """Wilson score confidence interval for a binomial proportion.

    Parameters
    ----------
    count : int
        Number of successes.
    total : int
        Number of trials.
    z : float, optional
        Z-score for the desired confidence level (default 1.96 = 95 %).

    Returns
    -------
    lower : float
        Lower bound of the confidence interval.
    upper : float
        Upper bound of the confidence interval.
    """
    if total == 0:
        return 0.0, 0.0
    p_hat = count / total
    denom = 1 + z ** 2 / total
    centre = p_hat + z ** 2 / (2 * total)
    spread = z * np.sqrt((p_hat * (1 - p_hat) + z ** 2 / (4 * total)) / total)
    lower = (centre - spread) / denom
    upper = (centre + spread) / denom
    return max(0.0, lower), min(1.0, upper)


# ── Rolling mean helper ──────────────────────────────────────────────────────

def rolling_mean(series, window=3):
    """Centred rolling mean with minimum 1 observation.

    Parameters
    ----------
    series : pd.Series or array-like
        Input time-series values.
    window : int, optional
        Window size (default 3).

    Returns
    -------
    pd.Series
        Smoothed series with the same index as the input.
    """
    s = pd.Series(series)
    return s.rolling(window, center=True, min_periods=1).mean()


# ── Trend label ──────────────────────────────────────────────────────────────

def trend_label(tau, p, alpha=0.05):
    """Classify a Mann-Kendall result into a human-readable trend label.

    Parameters
    ----------
    tau : float
        Kendall tau correlation coefficient.
    p : float
        Two-sided p-value.
    alpha : float, optional
        Significance threshold (default 0.05).

    Returns
    -------
    str
        ``"emerging"`` if tau > 0 and p < alpha,
        ``"declining"`` if tau < 0 and p < alpha,
        ``"stable"`` otherwise.
    """
    if np.isnan(tau) or np.isnan(p):
        return "insufficient data"
    if p < alpha and tau > 0:
        return "emerging"
    if p < alpha and tau < 0:
        return "declining"
    return "stable"
