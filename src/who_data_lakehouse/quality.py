"""
Data quality checks for WHO lakehouse DataFrames.

Provides completeness, temporal coverage, and outlier detection utilities
that work on any extractor output DataFrame.
"""
from __future__ import annotations

import numpy as np
import pandas as pd


def check_completeness(
    df: pd.DataFrame,
    country_col: str = "country_iso3",
    year_col: str = "year",
    value_col: str = "value",
) -> float:
    """
    Percentage of country-year cells with non-null values.

    Returns 0.0 for empty DataFrames.
    """
    if df.empty:
        return 0.0
    total = len(df)
    non_null = df[value_col].notna().sum()
    return float(non_null / total * 100) if total > 0 else 0.0


def check_temporal_coverage(
    df: pd.DataFrame,
    country_col: str = "country_iso3",
    year_col: str = "year",
) -> pd.DataFrame:
    """
    Min/max year per country, plus gap detection.

    Returns DataFrame with columns:
        country_iso3, min_year, max_year, n_years, expected_years, gaps
    """
    if df.empty:
        return pd.DataFrame(
            columns=[country_col, "min_year", "max_year", "n_years", "expected_years", "gaps"]
        )

    grouped = df.groupby(country_col)[year_col].agg(["min", "max", "nunique"])
    grouped = grouped.reset_index()
    grouped.columns = [country_col, "min_year", "max_year", "n_years"]
    grouped["expected_years"] = grouped["max_year"] - grouped["min_year"] + 1
    grouped["gaps"] = grouped["expected_years"] - grouped["n_years"]
    return grouped


def detect_outliers(
    df: pd.DataFrame,
    value_col: str = "value",
    country_col: str = "country_iso3",
    year_col: str = "year",
    sigma: float = 3.0,
) -> pd.DataFrame:
    """
    Flag values > sigma standard deviations from country-specific mean.

    Uses a robust approach: for each country, compute mean and std of the
    value column, and flag rows where the absolute z-score exceeds sigma.

    Returns a filtered DataFrame containing only the outlier rows, with an
    additional 'z_score' column.
    """
    if df.empty:
        return df.copy()

    # Need at least 2 observations per country to compute std
    result_rows: list[pd.DataFrame] = []
    for country, group in df.groupby(country_col):
        if len(group) < 2:
            continue
        values = group[value_col].dropna()
        if len(values) < 2:
            continue
        mean = values.mean()
        std = values.std()
        if std == 0 or np.isnan(std):
            continue
        z_scores = (group[value_col] - mean) / std
        outlier_mask = z_scores.abs() > sigma
        if outlier_mask.any():
            outlier_rows = group.loc[outlier_mask].copy()
            outlier_rows["z_score"] = z_scores[outlier_mask]
            result_rows.append(outlier_rows)

    if not result_rows:
        empty = df.iloc[:0].copy()
        empty["z_score"] = pd.Series(dtype=float)
        return empty

    return pd.concat(result_rows, ignore_index=True)
