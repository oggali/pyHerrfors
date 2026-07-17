"""Date-range helpers for consumption data assembly."""
import datetime

import pandas as pd

from .const import resolve_time_step


def expected_intervals(day):
    """Return required interval count for a day (96 at 15-min, 24 at 60-min)."""
    return 96 if resolve_time_step(day) == 15 else 24


def day_interval_count(consumption_df, day):
    """Count consumption intervals stored for a calendar day."""
    if consumption_df is None or consumption_df.empty:
        return 0
    if "timestamp_tz" not in consumption_df.columns:
        return 0
    return int((consumption_df["timestamp_tz"].dt.date == day).sum())


def has_complete_day_consumption(consumption_df, day):
    """Return True when a day has the full expected number of intervals."""
    return day_interval_count(consumption_df, day) >= expected_intervals(day)


def date_range(start_day, last_day):
    days = []
    current = start_day
    while current <= last_day:
        days.append(current)
        current += datetime.timedelta(days=1)
    return days


def cached_dates(consumption_df, *, complete_only=False):
    if consumption_df is None or consumption_df.empty:
        return set()
    if "timestamp_tz" not in consumption_df.columns:
        return set()
    dates = set(consumption_df["timestamp_tz"].dt.date)
    if not complete_only:
        return dates
    return {day for day in dates if has_complete_day_consumption(consumption_df, day)}


def filter_date_range(consumption_df, start_day, last_day):
    if consumption_df is None or consumption_df.empty:
        return pd.DataFrame(columns=["timestamp_tz", "consumption"])
    mask = (consumption_df["timestamp_tz"].dt.date >= start_day) & (
        consumption_df["timestamp_tz"].dt.date <= last_day
    )
    return consumption_df.loc[mask].copy()


def upsert_by_date(existing_df, new_dfs):
    frames = []
    if existing_df is not None and not existing_df.empty:
        frames.append(existing_df)
    for df in new_dfs:
        if df is not None and not df.empty:
            frames.append(df)
    if not frames:
        return pd.DataFrame(columns=["timestamp_tz", "consumption"])

    combined = pd.concat(frames, ignore_index=True)
    combined = combined.drop_duplicates(subset=["timestamp_tz"], keep="last")
    return combined.sort_values("timestamp_tz").reset_index(drop=True)
