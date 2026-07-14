"""Unit tests for pyHerrfors helpers."""
import datetime
import importlib
import os
import sys
import tempfile
import types
import unittest
from unittest.mock import patch

import pandas as pd

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "custom_components"))
_COMPONENTS = os.path.join(os.path.dirname(__file__), "..", "custom_components", "pyHerrfors")
if "pyHerrfors" not in sys.modules:
    _pkg = types.ModuleType("pyHerrfors")
    _pkg.__path__ = [_COMPONENTS]
    sys.modules["pyHerrfors"] = _pkg

from pyHerrfors.const import (  # noqa: E402
    build_readings_params,
    resolve_time_step,
)
from pyHerrfors.dates import (  # noqa: E402
    cached_dates,
    date_range,
    filter_date_range,
    upsert_by_date,
)
from pyHerrfors.db import db_empty, get_all_from_db_as_df, insert_to_db  # noqa: E402
from pyHerrfors.models import HerrforsSnapshot, PeriodSummary  # noqa: E402
from pyHerrfors.pricing import (  # noqa: E402
    add_vat_to_prices,
    apply_price_calculations,
    group_price_calculations,
)


class TestConstHelpers(unittest.TestCase):
    def test_resolve_time_step_before_cutoff(self):
        self.assertEqual(resolve_time_step(datetime.date(2025, 9, 30)), 60)

    def test_resolve_time_step_after_cutoff(self):
        self.assertEqual(resolve_time_step(datetime.date(2025, 10, 1)), 15)

    def test_build_readings_params(self):
        day = datetime.date(2025, 10, 15)
        params = build_readings_params(day)
        self.assertEqual(params["coId"], "60754370")
        self.assertEqual(params["timeStep"], "15")
        self.assertTrue(params["from"].startswith("2025-10-14"))
        self.assertTrue(params["to"].startswith("2025-10-15"))


class TestDatesHelpers(unittest.TestCase):
    def test_date_range(self):
        start = datetime.date(2025, 1, 1)
        end = datetime.date(2025, 1, 3)
        self.assertEqual(date_range(start, end), [start, datetime.date(2025, 1, 2), end])

    def test_upsert_by_date_deduplicates(self):
        ts = pd.Timestamp("2025-01-01 00:00:00", tz="EET")
        df1 = pd.DataFrame({"timestamp_tz": [ts], "consumption": [1.0]})
        df2 = pd.DataFrame({"timestamp_tz": [ts], "consumption": [2.0]})
        result = upsert_by_date(df1, [df2])
        self.assertEqual(len(result), 1)
        self.assertEqual(result["consumption"].iloc[0], 2.0)

    def test_filter_date_range(self):
        rows = pd.DataFrame(
            {
                "timestamp_tz": pd.to_datetime(
                    ["2025-01-01", "2025-01-02", "2025-01-03"], utc=True
                ).tz_convert("EET"),
                "consumption": [1, 2, 3],
            }
        )
        filtered = filter_date_range(
            rows, datetime.date(2025, 1, 1), datetime.date(2025, 1, 2)
        )
        self.assertEqual(len(filtered), 2)

    def test_cached_dates_empty(self):
        self.assertEqual(cached_dates(None), set())


class TestPricing(unittest.TestCase):
    def test_add_vat_discount_period(self):
        ts = pd.Timestamp("2023-01-01 00:00:00", tz="EET")
        prices = pd.Series([10.0], index=[ts], name="prices")
        result = add_vat_to_prices(prices)
        self.assertAlmostEqual(result["vat"].iloc[0], 0.1)

    def test_add_vat_earlier_normal(self):
        ts = pd.Timestamp("2022-01-01 00:00:00", tz="EET")
        prices = pd.Series([10.0], index=[ts], name="prices")
        result = add_vat_to_prices(prices)
        self.assertAlmostEqual(result["vat"].iloc[0], 0.24)

    def test_add_vat_current_normal(self):
        ts = pd.Timestamp("2024-10-01 00:00:00", tz="EET")
        prices = pd.Series([10.0], index=[ts], name="prices")
        result = add_vat_to_prices(prices)
        self.assertAlmostEqual(result["vat"].iloc[0], 0.255)

    def test_apply_price_calculations(self):
        df = pd.DataFrame(
            {
                "consumption": [2.0],
                "prices_cent": [5.0],
                "prices_cent_vat": [6.0],
            }
        )
        result = apply_price_calculations(df, marginal_price=0.5)
        self.assertAlmostEqual(result["price"].iloc[0], 10.0)
        self.assertAlmostEqual(result["price_marginal_alv"].iloc[0], 6.5)

    def test_group_price_calculations_daily(self):
        ts = pd.date_range("2025-01-01", periods=24, freq="h", tz="EET")
        df = pd.DataFrame(
            {
                "timestamp_tz": ts,
                "consumption": [1.0] * 24,
                "prices_cent": [5.0] * 24,
                "prices_cent_vat": [6.0] * 24,
                "price_marginal_alv": [6.5] * 24,
                "price": [5.0] * 24,
                "price_euro": [0.05] * 24,
                "price_vat": [6.0] * 24,
                "price_marg_alv": [6.5] * 24,
                "price_marg_alv_euro": [0.065] * 24,
            }
        )
        grouped, name = group_price_calculations(df, "D")
        self.assertEqual(name, "day")
        self.assertAlmostEqual(grouped["consumption_sum"].iloc[0], 24.0)


class TestModels(unittest.TestCase):
    def test_snapshot_sensor_mapping(self):
        snapshot = HerrforsSnapshot(
            latest_day=datetime.date(2025, 1, 1),
            day=PeriodSummary(consumption_kwh=12.5, price_euro=3.2),
        )
        self.assertEqual(
            snapshot.get_sensor_value("latest_day_electricity_consumption_sum"), 12.5
        )
        self.assertEqual(snapshot.get_sensor_value("latest_day"), datetime.date(2025, 1, 1))


class TestDb(unittest.TestCase):
    def test_db_roundtrip(self):
        with tempfile.TemporaryDirectory() as tmp:
            db_path = os.path.join(tmp, "test.db")
            with patch("pyHerrfors.db.DB_FILE", db_path):
                self.assertTrue(db_empty())
                df = pd.DataFrame({"timestamp_tz": ["2025-01-01"], "consumption": [1.0]})
                insert_to_db(df, "price_calculations_all", "timestamp_tz")
                loaded = get_all_from_db_as_df("price_calculations_all")
                self.assertEqual(len(loaded), 1)
                self.assertFalse(db_empty())


if __name__ == "__main__":
    unittest.main()
