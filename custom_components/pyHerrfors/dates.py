"""Date-range helpers for consumption data assembly."""
import datetime

import pandas as pd

from .const import MIN_NONZERO_CONSUMPTION_INTERVALS, resolve_time_step

CONSUMPTION_COLUMN_ALIASES = ("consumption", "value", "kwh", "kWh", "energy")


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


def day_nonzero_consumption_count(consumption_df, day):
    """Count intervals for a day where consumption is non-zero (NaN treated as zero)."""
    if consumption_df is None or consumption_df.empty:
        return 0
    if "timestamp_tz" not in consumption_df.columns or "consumption" not in consumption_df.columns:
        return 0
    day_mask = consumption_df["timestamp_tz"].dt.date == day
    day_rows = consumption_df.loc[day_mask]
    nonzero = day_rows["consumption"].fillna(0) != 0
    return int(nonzero.sum())


def has_complete_day_consumption(consumption_df, day):
    """Return True when a day has enough intervals and non-zero consumption values."""
    return (
        day_interval_count(consumption_df, day) >= expected_intervals(day)
        and day_nonzero_consumption_count(consumption_df, day)
        >= MIN_NONZERO_CONSUMPTION_INTERVALS
    )


def ensure_consumption_column(consumption_df):
    """Rename a known API alias to `consumption` when that column is missing."""
    if consumption_df is None or consumption_df.empty:
        return consumption_df
    if "consumption" in consumption_df.columns:
        return consumption_df
    for alias in CONSUMPTION_COLUMN_ALIASES:
        if alias in consumption_df.columns:
            return consumption_df.rename(columns={alias: "consumption"})
    return consumption_df


def format_consumption_day_debug(consumption_df, day):
    """Compact diagnostic string for incomplete or unexpected consumption payloads."""
    if consumption_df is None or consumption_df.empty:
        return f"day={day} empty=True"
    columns = list(consumption_df.columns)
    total_rows = len(consumption_df)
    day_intervals = day_interval_count(consumption_df, day)
    nonzero = day_nonzero_consumption_count(consumption_df, day)
    dates = {}
    first_ts = last_ts = None
    if "timestamp_tz" in consumption_df.columns:
        dates = {
            str(key): int(count)
            for key, count in consumption_df["timestamp_tz"].dt.date.value_counts().items()
        }
        first_ts = str(consumption_df["timestamp_tz"].iloc[0])
        last_ts = str(consumption_df["timestamp_tz"].iloc[-1])
    cons = ""
    if "consumption" in consumption_df.columns:
        series = pd.to_numeric(consumption_df["consumption"], errors="coerce")
        cons = (
            f" min={series.min()} max={series.max()} sum={float(series.fillna(0).sum())}"
            f" nan={int(series.isna().sum())} zero_or_nan={int((series.fillna(0) == 0).sum())}"
        )
    return (
        f"day={day} rows={total_rows} day_intervals={day_intervals}/{expected_intervals(day)}"
        f" nonzero={nonzero}/{MIN_NONZERO_CONSUMPTION_INTERVALS} columns={columns}"
        f" dates={dates} first={first_ts} last={last_ts}{cons}"
    )


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
