from __future__ import annotations

from datetime import date, timedelta
from pathlib import Path
import re

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


def _normalize_header(value: str) -> str:
    return re.sub(r"[^a-z0-9]+", "", str(value).strip().lower())


COLUMN_ALIASES = {
    "date": {
        "date",
        "day",
        "reportdate",
        "streamdate",
        "releasedate",
    },
    "spotify_streams": {
        "spotifystreams",
        "spotifystreams",
        "spotifytotalstreams",
        "streamsspotify",
    },
    "spotify_listeners": {
        "spotifylisteners",
        "listenersspotify",
        "listeners",
    },
    "spotify_replay_rate": {
        "spotifyreplayrate",
        "replayrate",
        "spotifyreplay",
    },
    "spotify_saves": {
        "spotifysaves",
        "savesspotify",
        "saves",
    },
    "save_rate": {
        "saverate",
        "spotifysaverate",
    },
    "spotify_skips": {
        "spotifyskips",
        "skipsspotify",
        "skips",
    },
    "apple_music_streams": {
        "applemusicstreams",
        "applemusicstreams",
        "applemusic",
        "applemusicplays",
        "applemusictotalstreams",
        "applestreams",
    },
    "number_of_shazams": {
        "numberofshazams",
        "shazams",
        "shazamcount",
        "shazam",
    },
}


def _apply_column_aliases(dataframe: pd.DataFrame) -> tuple[pd.DataFrame, list[str]]:
    warnings: list[str] = []
    normalized_to_original: dict[str, list[str]] = {}
    for column in dataframe.columns:
        key = _normalize_header(str(column))
        normalized_to_original.setdefault(key, []).append(str(column))

    renamed: dict[str, str] = {}
    used_original_columns: set[str] = set()

    for canonical in REQUIRED_COLUMNS:
        if canonical in dataframe.columns:
            used_original_columns.add(canonical)
            continue

        candidate_aliases = {_normalize_header(canonical), *COLUMN_ALIASES.get(canonical, set())}
        matched_column = ""
        for alias in candidate_aliases:
            for original in normalized_to_original.get(alias, []):
                if original not in used_original_columns and original not in renamed:
                    matched_column = original
                    break
            if matched_column:
                break

        if matched_column:
            renamed[matched_column] = canonical
            used_original_columns.add(matched_column)
            warnings.append(f"Mapped column '{matched_column}' to '{canonical}'")

    if renamed:
        dataframe = dataframe.rename(columns=renamed)

    return dataframe, warnings


def parse_release_csv(csv_path: Path) -> tuple[pd.DataFrame, list[str]]:
    dataframe = _load_metrics_dataframe(csv_path)
    dataframe, alias_warnings = _apply_column_aliases(dataframe)
    warnings: list[str] = list(alias_warnings)

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


def _load_metrics_dataframe(file_path: Path) -> pd.DataFrame:
    suffix = file_path.suffix.lower()
    if suffix == ".csv":
        return pd.read_csv(file_path)

    if suffix == ".xlsx":
        try:
            return pd.read_excel(file_path)
        except ImportError as exc:
            raise ValueError(
                "XLSX support requires 'openpyxl'. Install dependencies and retry."
            ) from exc

    raise ValueError("Unsupported metrics file. Use .csv or .xlsx")


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


def build_reporting_timeseries(dataframe: pd.DataFrame, release_date: date) -> pd.DataFrame:
    working_df = dataframe.copy()
    working_df["date"] = pd.to_datetime(working_df["date"]).dt.date
    working_df = working_df.sort_values("date")

    release_window = working_df[working_df["date"] >= release_date].copy()
    if release_window.empty:
        raise ValueError("No metric rows exist on or after the release date")

    release_window["cumulative_combined_total"] = release_window["combined_total"].cumsum()
    release_window["cumulative_spotify_listeners"] = release_window["spotify_listeners"].cumsum()
    release_window["cumulative_spotify_saves"] = release_window["spotify_saves"].cumsum()
    release_window["cumulative_spotify_skips"] = release_window["spotify_skips"].cumsum()

    day_counts = pd.Series(range(1, len(release_window) + 1), index=release_window.index, dtype=float)

    release_window["avg_replay_rate"] = release_window["spotify_replay_rate"].cumsum() / day_counts

    release_window["avg_save_rate"] = release_window["save_rate"].cumsum() / day_counts

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
    for window in WINDOW_DAYS:
        in_window = _window_slice(series, release_date, window)
        total = float(in_window.sum())

        results[window] = round(total, 3)

    return results


def build_summary(dataframe: pd.DataFrame, release_date: date) -> tuple[dict, pd.DataFrame, dict[int, float]]:
    timeline = build_reporting_timeseries(dataframe, release_date)
    windows = compute_window_totals(dataframe, release_date)
    spotify_windows = compute_window_totals_for_column(dataframe, release_date, "spotify_streams")
    apple_windows = compute_window_totals_for_column(dataframe, release_date, "apple_music_streams")
    shazam_windows = compute_window_totals_for_column(dataframe, release_date, "number_of_shazams")

    day1_rows = _window_rows(dataframe, release_date, 1)
    day1_dates = [row.isoformat() for row in day1_rows["date"]] if not day1_rows.empty else []
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
        "day1_rule": "Day 1 = release day + day 2 totals. Pre-release rows are ignored.",
        "day1_window_dates": day1_dates,
        "day1_breakdown": {
            "rows_used": int(len(day1_rows)),
            "spotify_streams": round(float(day1_rows["spotify_streams"].sum()) if not day1_rows.empty else 0.0, 3),
            "apple_music_streams": round(float(day1_rows["apple_music_streams"].sum()) if not day1_rows.empty else 0.0, 3),
            "number_of_shazams": round(float(day1_rows["number_of_shazams"].sum()) if not day1_rows.empty else 0.0, 3),
            "combined_total": round(float(day1_rows["combined_total"].sum()) if not day1_rows.empty else 0.0, 3),
        },
        "platform_windows": {
            "spotify_streams": {
                "24h": spotify_windows[1],
                "3d": spotify_windows[3],
                "7d": spotify_windows[7],
                "14d": spotify_windows[14],
                "21d": spotify_windows[21],
                "30d": spotify_windows[30],
            },
            "apple_music_streams": {
                "24h": apple_windows[1],
                "3d": apple_windows[3],
                "7d": apple_windows[7],
                "14d": apple_windows[14],
                "21d": apple_windows[21],
                "30d": apple_windows[30],
            },
            "number_of_shazams": {
                "24h": shazam_windows[1],
                "3d": shazam_windows[3],
                "7d": shazam_windows[7],
                "14d": shazam_windows[14],
                "21d": shazam_windows[21],
                "30d": shazam_windows[30],
            },
        },
    }
    return summary, timeline, windows


def _window_slice(series: pd.Series, release_date: date, window: int) -> pd.Series:
    if window == 1:
        # Business rule: Day 1 checkpoint uses release day + day 2 totals.
        end_date = release_date + timedelta(days=1)
    else:
        end_date = release_date + timedelta(days=window - 1)
    return series[(series.index >= release_date) & (series.index <= end_date)]


def _window_rows(dataframe: pd.DataFrame, release_date: date, window: int) -> pd.DataFrame:
    working_df = dataframe.copy()
    working_df["date"] = pd.to_datetime(working_df["date"]).dt.date
    working_df = working_df.sort_values("date")
    series = working_df.set_index("date")["combined_total"]
    allowed_dates = set(_window_slice(series, release_date, window).index)
    return working_df[working_df["date"].isin(allowed_dates)].copy()


def compute_window_totals_for_column(dataframe: pd.DataFrame, release_date: date, column: str) -> dict[int, float]:
    working_df = dataframe.copy()
    working_df["date"] = pd.to_datetime(working_df["date"]).dt.date
    working_df = working_df.sort_values("date")
    series = working_df.set_index("date")[column]

    results: dict[int, float] = {}
    for window in WINDOW_DAYS:
        in_window = _window_slice(series, release_date, window)
        results[window] = round(float(in_window.sum()), 3)
    return results
