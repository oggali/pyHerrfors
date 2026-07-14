# pyHerrfors

A [Home Assistant](https://www.home-assistant.io/) custom integration that pulls
your electricity **consumption** from the [Herrfors](https://www.herrfors.fi/)
customer portal, combines it with **Nord Pool / Entso-E day-ahead spot prices**,
and turns the two into ready-to-use cost and optimization sensors.

For every day and month it calculates how much energy you used, what it cost
(VAT and your fixed marginal price included), the average c/kWh you paid, and how
well your consumption was "optimized" against the average spot price of the
period. All calculations are persisted to a local [DuckDB](https://duckdb.org/)
database so history survives restarts.

> This is an unofficial, community-built integration and is not affiliated with
> or endorsed by Herrfors / Katterno.

## Features

- Daily and monthly electricity **consumption** (kWh) read from the Herrfors portal
- Day-ahead **spot prices** for Finland (`FI`) fetched from the Entso-E Transparency Platform
- **VAT-aware** pricing (handles the 24% / 10% / 25.5% Finnish VAT periods automatically)
- Optional **fixed marginal price** added on top of the spot price
- **Cost** in euro, **average c/kWh paid**, and **optimization savings / efficiency** vs. average spot
- Per-interval detail exposed as **state attributes** for charting (hourly or 15-minute resolution)
- Results stored in a **DuckDB** database under `/share` so data persists
- Configuration via the Home Assistant **UI config flow** (no YAML required)

## How it works

```
Herrfors portal  ──(consumption kWh)──┐
                                      ├──►  cost / avg price / savings  ──►  HA sensors
Entso-E API      ──(spot prices €)────┘                                      + DuckDB (/share)
```

The integration code is split into focused modules under `custom_components/pyHerrfors/`:

| Module | Role |
| --- | --- |
| `client.py` | Orchestrates fetching, caching, and updates |
| `session.py` | Portal token and aiohttp session lifecycle |
| `pricing.py` | VAT and electricity price calculations |
| `db.py` | DuckDB persistence helpers |
| `dates.py` | Date-range helpers for assembling consumption |
| `models.py` | `HerrforsSnapshot` dataclass exposed to sensors |

1. A session token for the Herrfors portal is read from `/share/herrfors_token.json`
   and decrypted locally using your e-mail + password (see [Authentication](#authentication)).
2. Consumption readings are fetched from `portal.herrfors.fi`.
3. Day-ahead spot prices are fetched from Entso-E for the same period.
4. The two are merged; VAT, your marginal price, costs and optimization metrics
   are computed, grouped by day / month / year, and written to DuckDB.
5. Sensors expose the latest day and latest month values via a structured `HerrforsSnapshot` (legacy flat attribute names on the client are still populated for compatibility).

## Requirements

- Home Assistant (recent release; the integration uses the modern
  `DataUpdateCoordinator` / config-entry APIs)
- An **Entso-E API key** — free, request one via the
  [Entso-E Transparency Platform](https://transparency.entsoe.eu/) (create an
  account, then ask support to enable API access)
- Herrfors portal credentials (e-mail + password)
- Python packages (installed automatically from `manifest.json`):
  `pandas >= 2.2.3`, `entsoe-py == 0.7.8`, `duckdb >= 1.4.2`

### A note about DuckDB

`duckdb >= 1.4.2` does not ship a prebuilt `musllinux` wheel, so on Home Assistant
OS (Alpine / musl) it has to be compiled on first install, which can take a while.
A helper script is provided to do this once:

```sh
sh /config/custom_components/pyHerrfors/scripts/install_duckdb_ha.sh
```

It installs a prebuilt wheel on Python < 3.14 and compiles from source on 3.14+.
Restart Home Assistant Core afterwards. If you build your own container image, the
included [`Dockerfile`](Dockerfile) compiles DuckDB at image-build time instead.

## Installation

### HACS (recommended)

1. In HACS, add this repository as a **custom repository** (category: *Integration*):
   `https://github.com/oggali/pyHerrfors`
2. Install **pyHerrfors**.
3. Restart Home Assistant.

### Manual

1. Copy `custom_components/pyHerrfors` into your Home Assistant `config/custom_components/` directory.
2. Restart Home Assistant.

## Configuration

Add the integration from **Settings → Devices & Services → Add Integration → Herrfors**.

| Field | Required | Description |
| --- | --- | --- |
| **Email** | yes | Herrfors portal e-mail. Also used to decrypt the session token. |
| **Password** | yes | Herrfors portal password. Also used to decrypt the session token. |
| **Electricity Marginal Price** | no | Your contract's fixed marginal (c/kWh) added on top of the spot price. Defaults to `0`. |
| **Entso-E api_key** | no (but needed for prices) | Your Entso-E Transparency Platform API key. Without it, spot prices and cost calculations are unavailable. |

### Authentication

The integration does **not** log in to the Herrfors portal itself. It expects an
encrypted session token at `/share/herrfors_token.json` (configurable via the
`TOKEN_FILE` environment variable). The token is decrypted on the fly with a key
derived from your e-mail and password (PBKDF2 + AES-GCM, see
[`decode_token.py`](custom_components/pyHerrfors/decode_token.py)).

The expected token file format is:

```json
{
  "token": "<ISO-timestamp>:<urlsafe-base64-ciphertext>",
  "expires": "2026-01-01T00:00:00Z",
  "token_timestamp": "..."
}
```

Producing/refreshing this token is handled by a companion login helper (e.g. a
small add-on or script that authenticates against `portal.herrfors.fi` and writes
the encrypted token). The DuckDB database is stored alongside it at
`/share/herrfors_data.db` (configurable via `DB_FILE`).

## Sensors

All entities are created under the `pyherrfors.` domain.

### Latest day

| Entity | Description | Unit |
| --- | --- | --- |
| `latest_day` | Date of the most recent fully available day | date |
| `latest_day_electricity_consumption_sum` | Total consumption for the day | kWh |
| `latest_day_electricity_price_euro` | Total cost for the day (marginal + VAT) | € |
| `latest_day_avg_khw_price_with_vat` | Average price actually paid | c/kWh |
| `latest_day_optimization_savings_eur` | Savings vs. paying the average spot price | € |
| `latest_day_optimization_efficiency` | Optimization efficiency vs. average spot | % |
| `latest_day_avg_price_with_vat` | Average daily price incl. VAT | c/kWh |
| `latest_day_avg_price_by_avg_spot` | Average price weighted by average spot | c/kWh |
| `latest_day_avg_spot_price_with_vat` | Average spot price incl. VAT | c/kWh |

### Latest month

| Entity | Description | Unit |
| --- | --- | --- |
| `latest_month` | The current month (`YYYY/M`) | — |
| `latest_month_electricity_consumption` | Total consumption for the month | kWh |
| `latest_month_electricity_price_euro` | Total cost for the month (marginal + VAT) | € |
| `latest_month_optimization_savings_eur` | Savings vs. average spot price | € |
| `latest_month_optimization_efficiency` | Optimization efficiency vs. average spot | % |
| `latest_month_avg_price_with_vat` | Average monthly price incl. VAT | c/kWh |
| `latest_month_avg_price_by_avg_spot` | Average price weighted by average spot | c/kWh |
| `latest_month_avg_spot_price_with_vat` | Average spot price incl. VAT | c/kWh |
| `latest_month_avg_khw_price_with_vat` | Average price actually paid | c/kWh |

The `latest_day` and `latest_month` sensors also expose per-interval arrays
(timestamps, consumption, prices) as **state attributes**, which are handy for
building [ApexCharts](https://github.com/RomRider/apexcharts-card) or template
visualizations.

The data is refreshed roughly every 15 minutes, with the integration polling the
portal more aggressively in the morning (when the previous day's data lands) and
backing off to about once per hour otherwise.

## Services

| Service | Description | Fields |
| --- | --- | --- |
| `pyherrfors.get_consumed_energy_and_prices` | Fetch consumption + prices for a specific day | `date` (optional, e.g. `2023-01-01`) |
| `pyherrfors.force_check_latest_month` | Force a refresh of the latest month from the API | — |
| `pyherrfors.force_update_current_year` | Recompute the whole current year | `day_level` (optional, also recompute each day) |

## Development & testing

Module layout: see [How it works](#how-it-works). Sensor values are read from `Herrfors.snapshot` with fallback to legacy client attributes.

Unit tests for pricing, date helpers, DuckDB, and snapshot models (no live API or Home Assistant required):

```sh
pip install -r requirements.txt
python -m unittest tests.test_pyherrfors_helpers -v
```

A [`compose.yml`](compose.yml) is provided to run Home Assistant locally with this
integration mounted live:

```sh
docker compose up --build
```

It mounts `custom_components/` into the container and exposes Home Assistant on
[http://localhost:8124](http://localhost:8124).

## Releasing

Versioning follows a `YYYY.MM.DD` (CalVer) scheme. `manifest.json` is the single
source of truth: bump the `version` field and push to `master`, and the
[release workflow](.github/workflows/publish_release.yaml) creates the matching
`vYYYY.MM.DD` git tag and publishes a GitHub Release automatically.

## Disclaimer

This project is provided as-is, for personal use. Spot prices come from Entso-E
and consumption from the Herrfors portal; the author is not responsible for any
discrepancies with your actual electricity bill.
