from __future__ import annotations

from datetime import date, timedelta
from pathlib import Path

import pandas as pd


REQUIRED_COLUMNS = [
    "date",
    "spotify_streams",
    "spotify_listeners",
    "spotify_replay_rate",
    "spotify_saves",
    "save_rate",
    "spotify_skips",
    "apple_music_streams",
    "number_of_shazams",
]

NUMERIC_COLUMNS = [column for column in REQUIRED_COLUMNS if column != "date"]
WINDOW_DAYS = [1, 3, 7, 14, 21, 30]


def parse_release_csv(csv_path: Path) -> tuple[pd.DataFrame, list[str]]:
    dataframe = pd.read_csv(csv_path)
    warnings: list[str] = []

    missing_columns = [column for column in REQUIRED_COLUMNS if column not in dataframe.columns]
    for missing in missing_columns:
        dataframe[missing] = pd.NaT if missing == "date" else 0
    if missing_columns:
        warnings.append(
            "Missing columns auto-filled (date as empty, numeric fields as 0): "
            + ", ".join(missing_columns)
        )

    dataframe = dataframe[REQUIRED_COLUMNS].copy()

    dataframe["date"] = pd.to_datetime(dataframe["date"], errors="coerce").dt.date
    invalid_date_rows = int(dataframe["date"].isna().sum())
    if invalid_date_rows > 0:
        warnings.append(f"Dropped {invalid_date_rows} rows with invalid dates")
    dataframe = dataframe.dropna(subset=["date"])  # type: ignore[arg-type]

    for column in NUMERIC_COLUMNS:
        dataframe[column] = pd.to_numeric(dataframe[column], errors="coerce")
        invalid_values = int(dataframe[column].isna().sum())
        if invalid_values > 0:
            warnings.append(f"Filled {invalid_values} invalid values in '{column}' with 0")
        dataframe[column] = dataframe[column].fillna(0)

    dataframe = dataframe.sort_values("date").drop_duplicates(subset=["date"], keep="last")
    dataframe["combined_total"] = (
        dataframe["spotify_streams"]
        + dataframe["apple_music_streams"]
        + dataframe["number_of_shazams"]
    )
    dataframe = dataframe.reset_index(drop=True)
    return dataframe, warnings


def dataframe_to_records(dataframe: pd.DataFrame) -> list[dict]:
    records: list[dict] = []
    for _, row in dataframe.iterrows():
        records.append(
            {
                "date": row["date"].isoformat(),
                "spotify_streams": float(row["spotify_streams"]),
                "spotify_listeners": float(row["spotify_listeners"]),
                "spotify_replay_rate": float(row["spotify_replay_rate"]),
                "spotify_saves": float(row["spotify_saves"]),
                "save_rate": float(row["save_rate"]),
                "spotify_skips": float(row["spotify_skips"]),
                "apple_music_streams": float(row["apple_music_streams"]),
                "number_of_shazams": float(row["number_of_shazams"]),
                "combined_total": float(row["combined_total"]),
            }
        )
    return records


def _day_one_adjustments(dataframe: pd.DataFrame, release_date: date) -> tuple[dict, bool]:
    working_df = dataframe.copy()
    working_df["date"] = pd.to_datetime(working_df["date"]).dt.date
    indexed = working_df.set_index("date")
    previous_day = release_date - timedelta(days=1)

    if previous_day not in indexed.index:
        return {
            "combined_total": 0.0,
            "spotify_listeners": 0.0,
            "spotify_saves": 0.0,
            "spotify_skips": 0.0,
            "spotify_replay_rate": 0.0,
            "save_rate": 0.0,
        }, False

    previous_row = indexed.loc[previous_day]
    if isinstance(previous_row, pd.DataFrame):
        previous_row = previous_row.iloc[-1]

    return {
        "combined_total": float(previous_row["combined_total"]),
        "spotify_listeners": float(previous_row["spotify_listeners"]),
        "spotify_saves": float(previous_row["spotify_saves"]),
        "spotify_skips": float(previous_row["spotify_skips"]),
        "spotify_replay_rate": float(previous_row["spotify_replay_rate"]),
        "save_rate": float(previous_row["save_rate"]),
    }, True


def build_reporting_timeseries(dataframe: pd.DataFrame, release_date: date) -> pd.DataFrame:
    working_df = dataframe.copy()
    working_df["date"] = pd.to_datetime(working_df["date"]).dt.date
    working_df = working_df.sort_values("date")

    release_window = working_df[working_df["date"] >= release_date].copy()
    if release_window.empty:
        raise ValueError("No metric rows exist on or after the release date")

    adjustments, has_previous_day = _day_one_adjustments(working_df, release_date)

    release_window["cumulative_combined_total"] = (
        release_window["combined_total"].cumsum() + adjustments["combined_total"]
    )
    release_window["cumulative_spotify_listeners"] = (
        release_window["spotify_listeners"].cumsum() + adjustments["spotify_listeners"]
    )
    release_window["cumulative_spotify_saves"] = (
        release_window["spotify_saves"].cumsum() + adjustments["spotify_saves"]
    )
    release_window["cumulative_spotify_skips"] = (
        release_window["spotify_skips"].cumsum() + adjustments["spotify_skips"]
    )

    day_counts = pd.Series(range(1, len(release_window) + 1), index=release_window.index, dtype=float)
    replay_divisor = day_counts + (1.0 if has_previous_day else 0.0)

    release_window["avg_replay_rate"] = (
        release_window["spotify_replay_rate"].cumsum() + adjustments["spotify_replay_rate"]
    ) / replay_divisor

    release_window["avg_save_rate"] = (
        release_window["save_rate"].cumsum() + adjustments["save_rate"]
    ) / replay_divisor

    release_window["day_number"] = (
        (pd.to_datetime(release_window["date"]) - pd.Timestamp(release_date)).dt.days + 1
    )
    return release_window.reset_index(drop=True)


def compute_window_totals(dataframe: pd.DataFrame, release_date: date) -> dict[int, float]:
    working_df = dataframe.copy()
    working_df["date"] = pd.to_datetime(working_df["date"]).dt.date
    working_df = working_df.sort_values("date")
    series = working_df.set_index("date")["combined_total"]

    results: dict[int, float] = {}
    previous_day = release_date - timedelta(days=1)

    for window in WINDOW_DAYS:
        end_date = release_date + timedelta(days=window - 1)
        in_window = series[(series.index >= release_date) & (series.index <= end_date)]
        total = float(in_window.sum())

        if window == 1 and previous_day in series.index:
            total += float(series.loc[previous_day])

        results[window] = round(total, 3)

    return results


def build_summary(dataframe: pd.DataFrame, release_date: date) -> tuple[dict, pd.DataFrame, dict[int, float]]:
    timeline = build_reporting_timeseries(dataframe, release_date)
    windows = compute_window_totals(dataframe, release_date)
    latest = timeline.iloc[-1]

    summary = {
        "release_date": release_date.isoformat(),
        "rows_considered": int(len(timeline)),
        "first_data_date": timeline.iloc[0]["date"].isoformat(),
        "last_data_date": timeline.iloc[-1]["date"].isoformat(),
        "cumulative_windows": {
            "24h": windows[1],
            "3d": windows[3],
            "7d": windows[7],
            "14d": windows[14],
            "21d": windows[21],
            "30d": windows[30],
        },
        "latest_cumulative_streams": round(float(latest["cumulative_combined_total"]), 3),
        "latest_cumulative_listeners": round(float(latest["cumulative_spotify_listeners"]), 3),
        "avg_replay_rate": round(float(latest["avg_replay_rate"]), 5),
        "avg_save_rate": round(float(latest["avg_save_rate"]), 5),
        "latest_cumulative_saves": round(float(latest["cumulative_spotify_saves"]), 3),
        "latest_cumulative_skips": round(float(latest["cumulative_spotify_skips"]), 3),
    }
    return summary, timeline, windows
