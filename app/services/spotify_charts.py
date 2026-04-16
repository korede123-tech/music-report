from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime, timedelta
import json
import re
from typing import Any
from urllib.parse import urlparse

import requests


CHARTS_API_BASE = "https://charts-spotify-com-service.spotify.com/auth/v0/charts/"
DEFAULT_CHART_REFERENCES = (
    "regional-global-weekly",
    "regional-global-daily",
)
TRACK_URI_PATTERN = re.compile(r"^spotify:track:([A-Za-z0-9]{22})$")


class ChartSyncError(RuntimeError):
    """Raised when Spotify Charts sync fails."""


@dataclass(frozen=True)
class ChartReference:
    alias: str
    date_value: str = "latest"


@dataclass
class ChartSyncResult:
    processed_references: list[str]
    processed_aliases: list[str]
    total_requests: int
    matches: list[dict[str, Any]]
    song_summaries: list[dict[str, Any]]
    warnings: list[str]

    def as_dict(self) -> dict[str, Any]:
        return {
            "processed_references": self.processed_references,
            "processed_aliases": self.processed_aliases,
            "total_requests": self.total_requests,
            "total_matches": len(self.matches),
            "matches": self.matches,
            "song_summaries": self.song_summaries,
            "warnings": self.warnings,
        }


class SpotifyChartsClient:
    def __init__(
        self,
        access_token: str,
        spotify_app_version: str = "0.0.0.production",
        timeout_seconds: int = 30,
    ) -> None:
        self.access_token = access_token.strip()
        self.spotify_app_version = spotify_app_version.strip() or "0.0.0.production"
        self.timeout_seconds = timeout_seconds

    @staticmethod
    def _extract_error_message(response: requests.Response) -> str:
        try:
            payload = response.json()
            message = payload.get("message") or payload.get("error") or payload.get("detail")
            if isinstance(message, str) and message.strip():
                return message.strip()
        except Exception:
            pass

        body = (response.text or "").strip()
        if not body:
            return f"HTTP {response.status_code}"
        return body[:240]

    def _headers(self) -> dict[str, str]:
        if not self.access_token:
            raise ChartSyncError(
                "Spotify Charts access token is missing. Set SPOTIFY_CHARTS_ACCESS_TOKEN or send access_token in request."
            )

        return {
            "Authorization": f"Bearer {self.access_token}",
            "Accept": "application/json",
            "Content-Type": "application/json",
            "spotify-app-version": self.spotify_app_version,
            "app-platform": "Browser",
            "Origin": "https://charts.spotify.com",
            "Referer": "https://charts.spotify.com/charts/overview/global",
            "User-Agent": "Mozilla/5.0",
        }

    def fetch_chart(self, alias: str, date_value: str = "latest") -> dict[str, Any]:
        normalized_alias = normalize_chart_alias(alias)
        normalized_date = normalize_chart_date(date_value)
        url = f"{CHARTS_API_BASE}{normalized_alias}/{normalized_date}"

        response = requests.get(url, headers=self._headers(), timeout=self.timeout_seconds)
        if response.status_code in (401, 403):
            raise ChartSyncError(
                "Spotify Charts token was rejected. Re-authenticate on charts.spotify.com and refresh SPOTIFY_CHARTS_ACCESS_TOKEN."
            )

        if response.status_code == 400:
            message = self._extract_error_message(response)
            raise ChartSyncError(
                "Spotify Charts request returned 400. This usually means your token is not fully onboarded for Charts access. "
                f"Details: {message}"
            )

        if response.status_code == 404:
            raise ChartSyncError(
                f"Chart not found for alias '{normalized_alias}' and date '{normalized_date}'."
            )

        if not response.ok:
            message = self._extract_error_message(response)
            raise ChartSyncError(f"Spotify Charts request failed ({response.status_code}): {message}")

        try:
            payload = response.json()
        except Exception as exc:
            raise ChartSyncError("Spotify Charts response could not be parsed as JSON") from exc

        if not isinstance(payload, dict):
            raise ChartSyncError("Spotify Charts response format was not recognized")
        return payload


class SpotifyChartsSyncService:
    def __init__(self, client: SpotifyChartsClient) -> None:
        self.client = client

    def sync(
        self,
        songs: list[dict[str, Any]],
        references: list[str] | None = None,
        from_release_date: bool = True,
        max_points_per_chart: int = 40,
    ) -> ChartSyncResult:
        if not songs:
            return ChartSyncResult(
                processed_references=[],
                processed_aliases=[],
                total_requests=0,
                matches=[],
                song_summaries=[],
                warnings=["No songs were provided for chart sync."],
            )

        requested_references = references or list(DEFAULT_CHART_REFERENCES)
        parsed_references = _dedupe_references([parse_chart_reference(value) for value in requested_references])

        song_release_dates: dict[int, date] = {}
        warnings: list[str] = []
        for song in songs:
            song_id = int(song["id"])
            try:
                song_release_dates[song_id] = date.fromisoformat(str(song.get("release_date") or ""))
            except Exception:
                warnings.append(
                    f"Skipping song '{song.get('title', 'Unknown')}' because release_date is invalid."
                )

        if not song_release_dates:
            return ChartSyncResult(
                processed_references=[f"{item.alias}/{item.date_value}" for item in parsed_references],
                processed_aliases=sorted({item.alias for item in parsed_references}),
                total_requests=0,
                matches=[],
                song_summaries=[],
                warnings=warnings or ["No songs had valid release dates for chart sync."],
            )

        global_start_date = min(song_release_dates.values())
        cache: dict[tuple[str, str], dict[str, Any]] = {}
        request_count = 0
        matches: list[dict[str, Any]] = []

        def fetch_cached(alias: str, date_value: str) -> dict[str, Any]:
            nonlocal request_count
            key = (alias, date_value)
            if key in cache:
                return cache[key]
            payload = self.client.fetch_chart(alias, date_value)
            cache[key] = payload
            request_count += 1
            return payload

        for reference in parsed_references:
            try:
                latest_payload = fetch_cached(reference.alias, "latest")
            except ChartSyncError as exc:
                warnings.append(f"{reference.alias}/latest failed: {exc}")
                continue

            dimensions = _extract_dimensions(latest_payload)
            recurrence = str(dimensions.get("recurrence") or "").upper()
            if recurrence not in {"DAILY", "WEEKLY"}:
                warnings.append(
                    f"{reference.alias} returned unsupported recurrence '{recurrence or 'unknown'}'."
                )
                continue

            earliest_date = _parse_iso_date(dimensions.get("earliestDate"))
            latest_date = _parse_iso_date(latest_payload.get("date") or dimensions.get("latestDate"))

            if latest_date is None:
                warnings.append(f"{reference.alias} did not return a valid latest chart date.")
                continue

            if reference.date_value != "latest":
                explicit_date = _parse_iso_date(reference.date_value)
                if explicit_date:
                    latest_date = min(latest_date, explicit_date)

            if from_release_date:
                scan_start_date = global_start_date
                if earliest_date:
                    scan_start_date = max(scan_start_date, earliest_date)
                date_values = _build_scan_dates(
                    recurrence=recurrence,
                    scan_start=scan_start_date,
                    scan_end=latest_date,
                    max_points=max_points_per_chart,
                )
            else:
                date_values = [latest_date.isoformat()]

            if not date_values:
                warnings.append(
                    f"{reference.alias} had no chart dates inside the requested sync range."
                )
                continue

            for chart_date_value in date_values:
                try:
                    payload = fetch_cached(reference.alias, chart_date_value)
                except ChartSyncError as exc:
                    warnings.append(f"{reference.alias}/{chart_date_value} failed: {exc}")
                    continue

                chart_date = _parse_iso_date(payload.get("date") or chart_date_value)
                if chart_date is None:
                    warnings.append(f"{reference.alias}/{chart_date_value} had an invalid chart date.")
                    continue

                entries = payload.get("entries") or []
                if not isinstance(entries, list) or not entries:
                    continue

                display_chart = payload.get("displayChart") or {}
                chart_metadata = display_chart.get("chartMetadata") or {}

                for song in songs:
                    song_id = int(song["id"])
                    release_date = song_release_dates.get(song_id)
                    if release_date is None:
                        continue
                    if from_release_date and chart_date < release_date:
                        continue

                    matched_entry = _find_matching_entry(song=song, entries=entries)
                    if not matched_entry:
                        continue

                    matches.append(
                        _build_match_record(
                            song=song,
                            chart_alias=reference.alias,
                            chart_date=chart_date,
                            recurrence=recurrence,
                            chart_metadata=chart_metadata,
                            entry=matched_entry,
                        )
                    )

        song_summaries = _build_song_summaries(songs=songs, matches=matches)

        return ChartSyncResult(
            processed_references=[f"{item.alias}/{item.date_value}" for item in parsed_references],
            processed_aliases=sorted({item.alias for item in parsed_references}),
            total_requests=request_count,
            matches=matches,
            song_summaries=song_summaries,
            warnings=warnings,
        )


def build_chart_knowledge_text(result: ChartSyncResult) -> str:
    now = datetime.utcnow().isoformat(timespec="seconds")
    lines = [
        "Spotify Charts Sync Summary",
        f"Generated At (UTC): {now}",
        f"Processed References: {', '.join(result.processed_references) if result.processed_references else 'none'}",
        f"Total Chart Requests: {result.total_requests}",
        f"Total Matches: {len(result.matches)}",
        "",
        "Song Insights:",
    ]

    if result.song_summaries:
        for summary in result.song_summaries:
            chart_breakdown = ", ".join(summary.get("charts", [])) or "none"
            lines.append(
                "- "
                f"{summary.get('song_title', 'Unknown')}: "
                f"{summary.get('appearance_count', 0)} appearance(s), "
                f"best rank #{summary.get('best_rank', 'n/a')}, "
                f"first {summary.get('first_chart_date', 'n/a')}, "
                f"latest {summary.get('latest_chart_date', 'n/a')}, "
                f"charts: {chart_breakdown}"
            )
    else:
        lines.append("- No chart appearances were found for the synced songs.")

    if result.matches:
        lines.extend(["", "Recent Matches:"])
        recent = sorted(result.matches, key=lambda row: row.get("chart_date", ""), reverse=True)[:15]
        for match in recent:
            lines.append(
                "- "
                f"{match.get('chart_date', 'n/a')} | {match.get('song_title', 'Unknown')} | "
                f"#{match.get('rank', 'n/a')} on {match.get('chart_alias', 'n/a')}"
            )

    if result.warnings:
        lines.extend(["", "Warnings:"])
        for warning in result.warnings[:20]:
            lines.append(f"- {warning}")

    return "\n".join(lines)


def normalize_chart_alias(alias: str) -> str:
    return alias.strip().lower().replace("_", "-")


def normalize_chart_date(value: str | None) -> str:
    cleaned = (value or "latest").strip().lower()
    if cleaned == "latest":
        return "latest"
    parsed = _parse_iso_date(cleaned)
    if not parsed:
        raise ChartSyncError(f"Invalid chart date '{value}'. Use YYYY-MM-DD or latest.")
    return parsed.isoformat()


def parse_chart_reference(value: str) -> ChartReference:
    cleaned = value.strip()
    if not cleaned:
        raise ChartSyncError("Chart reference cannot be empty")

    if "//" in cleaned:
        parsed = urlparse(cleaned)
        segments = [part for part in parsed.path.split("/") if part]
        if len(segments) >= 4 and segments[0] == "charts" and segments[1] == "view":
            alias = normalize_chart_alias(segments[2])
            date_value = normalize_chart_date(segments[3])
            return ChartReference(alias=alias, date_value=date_value)
        raise ChartSyncError(
            "Chart URL format must be like https://charts.spotify.com/charts/view/<alias>/<date>."
        )

    if "/" in cleaned:
        alias_raw, date_raw = cleaned.split("/", maxsplit=1)
        return ChartReference(
            alias=normalize_chart_alias(alias_raw),
            date_value=normalize_chart_date(date_raw),
        )

    return ChartReference(alias=normalize_chart_alias(cleaned), date_value="latest")


def _dedupe_references(references: list[ChartReference]) -> list[ChartReference]:
    deduped: list[ChartReference] = []
    seen: set[tuple[str, str]] = set()

    for item in references:
        key = (item.alias, item.date_value)
        if key in seen:
            continue
        seen.add(key)
        deduped.append(item)

    return deduped


def _extract_dimensions(payload: dict[str, Any]) -> dict[str, Any]:
    display_chart = payload.get("displayChart") or {}
    chart_metadata = display_chart.get("chartMetadata") or {}
    return chart_metadata.get("dimensions") or {}


def _parse_iso_date(raw: Any) -> date | None:
    if not raw:
        return None

    if isinstance(raw, date):
        return raw

    value = str(raw).strip()
    if not value:
        return None

    if len(value) >= 10:
        value = value[:10]

    try:
        return date.fromisoformat(value)
    except Exception:
        return None


def _build_scan_dates(
    recurrence: str,
    scan_start: date,
    scan_end: date,
    max_points: int,
) -> list[str]:
    if scan_start > scan_end:
        return []

    step_days = 1 if recurrence == "DAILY" else 7

    values: list[str] = []
    cursor = scan_end
    while cursor >= scan_start:
        values.append(cursor.isoformat())
        cursor -= timedelta(days=step_days)

    values.reverse()
    if max_points > 0 and len(values) > max_points:
        return values[-max_points:]
    return values


def _normalize_text(value: str) -> str:
    return re.sub(r"[^a-z0-9]+", "", (value or "").strip().lower())


def _extract_track_id_from_uri(track_uri: str | None) -> str:
    if not track_uri:
        return ""
    match = TRACK_URI_PATTERN.match(track_uri.strip())
    if not match:
        return ""
    return match.group(1)


def _entry_artist_names(entry: dict[str, Any]) -> str:
    track_metadata = entry.get("trackMetadata") or {}
    artists = track_metadata.get("artists") or []
    names = [artist.get("name", "") for artist in artists if artist.get("name")]
    return ", ".join(names)


def _find_matching_entry(song: dict[str, Any], entries: list[dict[str, Any]]) -> dict[str, Any] | None:
    song_track_id = (song.get("spotify_track_id") or "").strip()
    if song_track_id:
        for entry in entries:
            entry_track_id = _extract_track_id_from_uri(
                (entry.get("trackMetadata") or {}).get("trackUri")
            )
            if entry_track_id and entry_track_id.lower() == song_track_id.lower():
                return entry

    song_title = _normalize_text(str(song.get("title") or ""))
    song_artist = _normalize_text(str(song.get("artist_name") or ""))

    if not song_title:
        return None

    for entry in entries:
        track_metadata = entry.get("trackMetadata") or {}
        track_name = _normalize_text(str(track_metadata.get("trackName") or ""))
        if not track_name:
            continue
        if track_name != song_title:
            continue

        if not song_artist:
            return entry

        artist_names = _normalize_text(_entry_artist_names(entry))
        if song_artist and song_artist in artist_names:
            return entry

    return None


def _safe_int(raw: Any) -> int | None:
    if raw is None or raw == "":
        return None
    try:
        return int(raw)
    except Exception:
        try:
            return int(float(raw))
        except Exception:
            return None


def _build_match_record(
    song: dict[str, Any],
    chart_alias: str,
    chart_date: date,
    recurrence: str,
    chart_metadata: dict[str, Any],
    entry: dict[str, Any],
) -> dict[str, Any]:
    chart_entry_data = entry.get("chartEntryData") or {}
    track_metadata = entry.get("trackMetadata") or {}
    labels = track_metadata.get("labels") or []
    label_names = [label.get("name", "") for label in labels if label.get("name")]

    source_url = f"https://charts.spotify.com/charts/view/{chart_alias}/{chart_date.isoformat()}"

    return {
        "song_id": int(song["id"]),
        "song_title": song.get("title") or "Untitled",
        "song_release_date": song.get("release_date") or "",
        "chart_alias": chart_alias,
        "chart_date": chart_date.isoformat(),
        "chart_recurrence": recurrence,
        "chart_type": (chart_metadata.get("dimensions") or {}).get("chartType") or "",
        "chart_name": chart_metadata.get("readableTitle") or chart_alias,
        "rank": _safe_int(chart_entry_data.get("currentRank")),
        "previous_rank": _safe_int(chart_entry_data.get("previousRank")),
        "peak_rank": _safe_int(chart_entry_data.get("peakRank")),
        "appearances_on_chart": _safe_int(chart_entry_data.get("appearancesOnChart")),
        "consecutive_appearances": _safe_int(chart_entry_data.get("consecutiveAppearancesOnChart")),
        "track_uri": track_metadata.get("trackUri") or "",
        "track_name": track_metadata.get("trackName") or (song.get("title") or ""),
        "artist_names": _entry_artist_names(entry),
        "source_labels": ", ".join(label_names),
        "source_url": source_url,
        "raw_entry_json": json.dumps(entry),
    }


def _build_song_summaries(
    songs: list[dict[str, Any]],
    matches: list[dict[str, Any]],
) -> list[dict[str, Any]]:
    grouped: dict[int, list[dict[str, Any]]] = {}
    for match in matches:
        grouped.setdefault(int(match["song_id"]), []).append(match)

    summaries: list[dict[str, Any]] = []
    for song in songs:
        song_id = int(song["id"])
        song_matches = grouped.get(song_id, [])
        if not song_matches:
            continue

        chart_dates = [item.get("chart_date") for item in song_matches if item.get("chart_date")]
        ranks = [item.get("rank") for item in song_matches if isinstance(item.get("rank"), int)]

        chart_counts: dict[str, int] = {}
        for item in song_matches:
            alias = str(item.get("chart_alias") or "unknown")
            chart_counts[alias] = chart_counts.get(alias, 0) + 1

        summaries.append(
            {
                "song_id": song_id,
                "song_title": song.get("title") or "Untitled",
                "appearance_count": len(song_matches),
                "best_rank": min(ranks) if ranks else None,
                "first_chart_date": min(chart_dates) if chart_dates else None,
                "latest_chart_date": max(chart_dates) if chart_dates else None,
                "charts": [f"{alias} ({count})" for alias, count in sorted(chart_counts.items())],
            }
        )

    summaries.sort(key=lambda item: item.get("appearance_count", 0), reverse=True)
    return summaries
