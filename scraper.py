"""Gabungkan HDB Carpark Information (statis) dengan Carpark Availability (realtime).

Sumber:
- HDB Carpark Information : d_23f946fa557947f93a8043bbef41dd09 (CSV)
- Carpark Availability    : d_ca933a644e55d34fe21f28b8052fac63 (API per menit)

Kunci join: car_park_no (info) <-> carpark_number (availability).
"""

from __future__ import annotations

import argparse
import io
import os
import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path

import pandas as pd
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

DATASET_INFO_ID = "d_23f946fa557947f93a8043bbef41dd09"
INITIATE_DOWNLOAD_URL = (
    "https://api-open.data.gov.sg/v1/public/api/datasets/"
    f"{DATASET_INFO_ID}/initiate-download"
)
AVAILABILITY_URL = "https://api.data.gov.sg/v1/transport/carpark-availability"

SGT = timezone(timedelta(hours=8))
LOT_TYPE_ORDER = ["C", "H", "S", "Y"]
NUMERIC_INFO_COLUMNS = [
    "x_coord",
    "y_coord",
    "car_park_decks",
    "gantry_height",
]


def build_session(api_key: str | None = None) -> requests.Session:
    retry = Retry(
        total=3,
        backoff_factor=1,
        status_forcelist=[429, 500, 502, 503, 504],
        allowed_methods=["GET"],
    )
    session = requests.Session()
    session.mount("https://", HTTPAdapter(max_retries=retry))
    session.headers.update(
        {
            "User-Agent": "carpark-merge/1.0 (+https://data.gov.sg)",
            "Accept": "application/json, text/csv, */*",
        }
    )
    if api_key:
        session.headers["x-api-key"] = api_key
    return session


def fetch_carpark_info(session: requests.Session) -> pd.DataFrame:
    response = session.get(INITIATE_DOWNLOAD_URL, timeout=30)
    response.raise_for_status()
    payload = response.json()
    download_url = (payload.get("data") or {}).get("url")
    if not download_url:
        raise RuntimeError(f"URL download tidak ditemukan: {payload}")

    download = session.get(download_url, timeout=60)
    download.raise_for_status()

    df = pd.read_csv(io.BytesIO(download.content), dtype=str, encoding="utf-8-sig")
    df.columns = [column.strip().lower() for column in df.columns]
    if "car_park_no" not in df.columns:
        raise RuntimeError(f"Kolom car_park_no tidak ada. Kolom: {list(df.columns)}")
    return df


def fetch_availability(session: requests.Session) -> tuple[pd.DataFrame, str]:
    response = session.get(AVAILABILITY_URL, timeout=30)
    response.raise_for_status()
    payload = response.json()

    items = payload.get("items") or []
    if not items:
        raise RuntimeError("Respons availability kosong (items tidak ada).")

    snapshot_timestamp = items[0].get("timestamp", "")
    records = items[0].get("carpark_data") or []
    if not records:
        raise RuntimeError("Respons availability tidak berisi carpark_data.")

    rows: list[dict] = []
    for record in records:
        carpark_number = str(record.get("carpark_number", "")).strip().upper()
        update_datetime = record.get("update_datetime", "")
        for info in record.get("carpark_info") or []:
            rows.append(
                {
                    "carpark_number": carpark_number,
                    "update_datetime": update_datetime,
                    "lot_type": str(info.get("lot_type", "")).strip().upper(),
                    "lots_available": pd.to_numeric(
                        info.get("lots_available"), errors="coerce"
                    ),
                    "total_lots": pd.to_numeric(
                        info.get("total_lots"), errors="coerce"
                    ),
                }
            )

    return pd.DataFrame(rows), snapshot_timestamp


def pivot_availability(availability: pd.DataFrame) -> pd.DataFrame:
    if availability.empty:
        return pd.DataFrame(columns=["carpark_number"])

    grouped = (
        availability.groupby(["carpark_number", "lot_type"], as_index=False)[
            ["lots_available", "total_lots"]
        ]
        .sum(min_count=1)
    )
    grouped["update_datetime"] = availability.groupby(
        ["carpark_number", "lot_type"]
    )["update_datetime"].max().to_numpy()

    lots = grouped.pivot(
        index="carpark_number", columns="lot_type", values="lots_available"
    )
    totals = grouped.pivot(
        index="carpark_number", columns="lot_type", values="total_lots"
    )

    found_types = [t for t in LOT_TYPE_ORDER if t in lots.columns]
    found_types += sorted(t for t in lots.columns if t not in LOT_TYPE_ORDER)
    lots = lots.reindex(columns=found_types).fillna(0)
    totals = totals.reindex(columns=found_types).fillna(0)

    lots.columns = [f"lots_available_{t}" for t in found_types]
    totals.columns = [f"total_lots_{t}" for t in found_types]

    result = lots.join(totals, how="outer")
    update = (
        availability.groupby("carpark_number")["update_datetime"]
        .max()
        .rename("update_datetime")
    )
    return result.join(update, how="outer").reset_index()


def merge_data(
    info: pd.DataFrame, availability: pd.DataFrame, how: str
) -> pd.DataFrame:
    info = info.copy()
    info["car_park_no"] = info["car_park_no"].astype(str).str.strip().str.upper()
    info = info.drop_duplicates(subset="car_park_no", keep="first")

    merged = info.merge(
        availability,
        left_on="car_park_no",
        right_on="carpark_number",
        how=how,
        indicator=True,
    )
    merged["matched"] = merged["_merge"].eq("both")
    merged["car_park_no"] = merged["car_park_no"].fillna(merged["carpark_number"])
    merged = merged.drop(columns=["_merge", "carpark_number"])

    lot_columns = [
        column
        for column in merged.columns
        if column.startswith("lots_available_")
        or column.startswith("total_lots_")
    ]
    merged[lot_columns] = merged[lot_columns].apply(
        pd.to_numeric, errors="coerce"
    )

    available_columns = [c for c in lot_columns if c.startswith("lots_available_")]
    total_columns = [c for c in lot_columns if c.startswith("total_lots_")]

    merged["total_lots_available"] = merged[available_columns].sum(
        axis=1, min_count=1
    )
    merged["total_lots"] = merged[total_columns].sum(axis=1, min_count=1)

    denominator = merged["total_lots"].where(merged["total_lots"] > 0)
    merged["occupancy_pct"] = (
        merged["total_lots_available"].div(denominator).mul(100).round(2)
    )

    for column in NUMERIC_INFO_COLUMNS:
        if column in merged.columns:
            merged[column] = pd.to_numeric(merged[column], errors="coerce")

    merged = merged.sort_values("car_park_no").reset_index(drop=True)
    return merged


def order_columns(df: pd.DataFrame) -> pd.DataFrame:
    base = [
        "car_park_no",
        "address",
        "x_coord",
        "y_coord",
        "car_park_type",
        "type_of_parking_system",
        "short_term_parking",
        "free_parking",
        "night_parking",
        "car_park_decks",
        "gantry_height",
        "car_park_basement",
    ]
    lot_types = set()
    for column in df.columns:
        if column.startswith("lots_available_"):
            lot_types.add(column[len("lots_available_"):])
        elif column.startswith("total_lots_") and column != "total_lots_available":
            lot_types.add(column[len("total_lots_"):])
    lot_types = sorted(
        lot_types,
        key=lambda t: (LOT_TYPE_ORDER.index(t) if t in LOT_TYPE_ORDER else 99, t),
    )
    lots = [f"lots_available_{t}" for t in lot_types]
    totals = [f"total_lots_{t}" for t in lot_types]
    tail = [
        "total_lots_available",
        "total_lots",
        "occupancy_pct",
        "update_datetime",
        "snapshot_timestamp",
        "matched",
    ]
    ordered = [c for c in base if c in df.columns]
    ordered += [c for c in lots + totals if c in df.columns]
    ordered += [c for c in tail if c in df.columns]
    ordered += [c for c in df.columns if c not in ordered]
    return df[ordered]


def save_output(df: pd.DataFrame, out_dir: Path) -> tuple[Path, Path]:
    out_dir.mkdir(parents=True, exist_ok=True)
    stamp = datetime.now(SGT).strftime("%Y%m%d_%H%M%S")
    timestamped = out_dir / f"carpark_merged_{stamp}.csv"
    latest = out_dir / "latest.csv"

    df.to_csv(timestamped, index=False, encoding="utf-8-sig")
    df.to_csv(latest, index=False, encoding="utf-8-sig")
    return timestamped, latest


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Gabungkan HDB Carpark Information dan Carpark Availability."
    )
    parser.add_argument(
        "--out", default="output", help="Direktori output (default: output)"
    )
    parser.add_argument(
        "--api-key",
        default=os.environ.get("DATA_GOV_API_KEY"),
        help="API key data.gov.sg (default: env DATA_GOV_API_KEY)",
    )
    parser.add_argument(
        "--join",
        choices=["left", "inner", "outer"],
        default="left",
        help="Jenis join (default: left)",
    )
    parser.add_argument(
        "--inner", action="store_const", dest="join", const="inner",
        help="Alias untuk --join inner",
    )
    parser.add_argument(
        "--outer", action="store_const", dest="join", const="outer",
        help="Alias untuk --join outer",
    )
    parser.add_argument("--quiet", action="store_true", help="Sembunyikan ringkasan")
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)
    session = build_session(args.api_key)

    try:
        info = fetch_carpark_info(session)
        availability_raw, snapshot_timestamp = fetch_availability(session)
    except (requests.RequestException, RuntimeError, ValueError) as error:
        print(f"Gagal mengambil data: {error}", file=sys.stderr)
        return 1

    availability = pivot_availability(availability_raw)
    merged = merge_data(info, availability, args.join)
    merged["snapshot_timestamp"] = snapshot_timestamp
    merged = order_columns(merged)

    timestamped, latest = save_output(merged, Path(args.out))

    if not args.quiet:
        total = len(merged)
        matched = int(merged["matched"].sum())
        unmatched = total - matched
        avg_occupancy = merged["occupancy_pct"].mean()
        print(f"Info carpark      : {len(info)} baris")
        print(f"Availability unik : {len(availability)} carpark")
        print(f"Hasil merge ({args.join:<5}): {total} baris")
        print(f"Matched           : {matched} ({matched / total * 100:.1f}%)")
        print(f"Unmatched         : {unmatched}")
        if pd.notna(avg_occupancy):
            print(f"Rata-rata okupansi: {avg_occupancy:.1f}%")
        print(f"Snapshot          : {snapshot_timestamp}")
        print(f"Output            : {timestamped}")
        print(f"Output terbaru    : {latest}")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
