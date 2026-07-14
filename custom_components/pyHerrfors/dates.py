"""Date-range helpers for consumption data assembly."""
import datetime

import pandas as pd


def date_range(start_day, last_day):
    days = []
    current = start_day
    while current <= last_day:
        days.append(current)
        current += datetime.timedelta(days=1)
    return days


def cached_dates(consumption_df):
    if consumption_df is None or consumption_df.empty:
        return set()
    if "timestamp_tz" not in consumption_df.columns:
        return set()
    return set(consumption_df["timestamp_tz"].dt.date)


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
