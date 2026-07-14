"""Structured data models for pyHerrfors sensor state."""
from __future__ import annotations

from dataclasses import dataclass, field
from datetime import date
from typing import Any

import pandas as pd


@dataclass
class PeriodSummary:
    label: str | date | None = None
    consumption_kwh: float | None = None
    price_euro: float | None = None
    avg_kwh_price_vat: float | None = None
    optimization_savings_eur: float | None = None
    optimization_efficiency: float | None = None
    avg_price_with_vat: float | None = None
    avg_price_by_avg_spot: float | None = None
    avg_spot_price_with_vat: float | None = None


@dataclass
class HerrforsSnapshot:
    latest_day: date | None = None
    latest_month: str | None = None
    day: PeriodSummary | None = None
    month: PeriodSummary | None = None
    day_group_calculations: pd.DataFrame | None = None
    month_group_calculations: pd.DataFrame | None = None
    latest_day_detail: pd.DataFrame | None = None
    _legacy_values: dict[str, Any] = field(default_factory=dict, repr=False)

    def get_sensor_value(self, sensor_type: str) -> Any:
        if sensor_type in self._legacy_values:
            return self._legacy_values[sensor_type]

        if sensor_type == "latest_day":
            return self.latest_day
        if sensor_type == "latest_month":
            return self.latest_month

        day_map = {
            "latest_day_electricity_consumption_sum": ("day", "consumption_kwh"),
            "latest_day_electricity_price_euro": ("day", "price_euro"),
            "latest_day_avg_khw_price_with_vat": ("day", "avg_kwh_price_vat"),
            "latest_day_optimization_savings_eur": ("day", "optimization_savings_eur"),
            "latest_day_optimization_efficiency": ("day", "optimization_efficiency"),
            "latest_day_avg_price_with_vat": ("day", "avg_price_with_vat"),
            "latest_day_avg_price_by_avg_spot": ("day", "avg_price_by_avg_spot"),
            "latest_day_avg_spot_price_with_vat": ("day", "avg_spot_price_with_vat"),
        }
        month_map = {
            "latest_month_electricity_consumption": ("month", "consumption_kwh"),
            "latest_month_electricity_price_euro": ("month", "price_euro"),
            "latest_month_avg_khw_price_with_vat": ("month", "avg_kwh_price_vat"),
            "latest_month_optimization_savings_eur": ("month", "optimization_savings_eur"),
            "latest_month_optimization_efficiency": ("month", "optimization_efficiency"),
            "latest_month_avg_price_with_vat": ("month", "avg_price_with_vat"),
            "latest_month_avg_price_by_avg_spot": ("month", "avg_price_by_avg_spot"),
            "latest_month_avg_spot_price_with_vat": ("month", "avg_spot_price_with_vat"),
        }

        mapping = {**day_map, **month_map}
        if sensor_type not in mapping:
            return None

        period_name, attr = mapping[sensor_type]
        period = getattr(self, period_name)
        if period is None:
            return None
        return getattr(period, attr)
