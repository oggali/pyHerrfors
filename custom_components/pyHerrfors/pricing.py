"""Electricity price and VAT calculation helpers."""
import pandas as pd

from .const import (
    DISCOUNT_VAT,
    DISCOUNT_VAT_END,
    DISCOUNT_VAT_START,
    EARLIER_NORMAL_VAT,
    NEW_NORMAL_VAT_START,
    NORMAL_VAT,
)


def add_vat_to_prices(prices):
    if isinstance(prices, pd.DataFrame):
        prices_df = prices
        prices_df["datetime"] = prices["timestamp_tz"]
        prices_df["prices_cent"] = prices_df["prices"]
        prices_df["prices"] = prices_df["prices"] * 10
        prices_df = prices_df[["datetime", "prices", "prices_cent"]]
    else:
        prices_df = prices.to_frame(name="prices")
        prices_df = prices_df.reset_index().rename(columns={"index": "datetime"})
        prices_df["prices_cent"] = prices_df["prices"] / 10

    discount_start = pd.Timestamp(DISCOUNT_VAT_START, tz="EET")
    discount_end = pd.Timestamp(DISCOUNT_VAT_END, tz="EET")
    new_normal_start = pd.Timestamp(NEW_NORMAL_VAT_START, tz="EET")

    prices_df["vat"] = prices_df["datetime"].apply(
        lambda x: EARLIER_NORMAL_VAT
        if x < discount_start or (discount_end < x < new_normal_start)
        else DISCOUNT_VAT
        if x < new_normal_start
        else NORMAL_VAT
    )

    prices_df["prices_cent_vat"] = prices_df["prices_cent"] * (1 + prices_df["vat"])
    prices_df["timestamp_tz"] = prices_df["datetime"]
    prices_df["date"] = prices_df["timestamp_tz"].apply(
        lambda x: x.tz_convert("EET").date()
    )

    return prices_df


def apply_price_calculations(price_calculations, marginal_price=0):
    fixed_marginal_price = marginal_price or 0

    price_calculations["price"] = (
        price_calculations["consumption"] * price_calculations["prices_cent"]
    )
    price_calculations["price_euro"] = price_calculations["price"] / 100
    price_calculations["price_marginal_alv"] = (
        price_calculations["prices_cent_vat"] + fixed_marginal_price
    )
    price_calculations["price_vat"] = (
        price_calculations["consumption"] * price_calculations["prices_cent_vat"]
    )
    price_calculations["price_marg_alv"] = price_calculations["consumption"] * (
        price_calculations["prices_cent_vat"] + fixed_marginal_price
    )
    price_calculations["price_marg_alv_euro"] = price_calculations["price_marg_alv"] / 100

    return price_calculations
