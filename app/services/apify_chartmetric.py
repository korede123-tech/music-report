from __future__ import annotations

from dataclasses import dataclass
import json
import math
import re
from typing import Any

import requests


class ApifyChartmetricError(RuntimeError):
    """Raised when Chartmetric data retrieval via Apify fails."""


@dataclass
class ChartmetricSyncResult:
    profiles: list[dict[str, Any]]
    warnings: list[str]
    requests_made: int

    def as_dict(self) -> dict[str, Any]:
        return {
            "profiles": self.profiles,
            "warnings": self.warnings,
            "requests_made": self.requests_made,
            "profiles_count": len(self.profiles),
        }


class ApifyChartmetricClient:
    def __init__(
        self,
        api_token: str,
        actor_id: str = "canadesk~chartmetric",
        timeout_seconds: int = 180,
    ) -> None:
        self.api_token = api_token.strip()
        self.actor_id = actor_id.strip() or "canadesk~chartmetric"
        self.timeout_seconds = timeout_seconds

    @property
    def run_sync_dataset_url(self) -> str:
        return f"https://api.apify.com/v2/acts/{self.actor_id}/run-sync-get-dataset-items"

    @staticmethod
    def _extract_error_message(response: requests.Response) -> str:
        try:
            payload = response.json()
            if isinstance(payload, dict):
                error_obj = payload.get("error")
                if isinstance(error_obj, dict):
                    error_type = str(error_obj.get("type") or "").strip()
                    message = str(error_obj.get("message") or "").strip()
                    if error_type or message:
                        return f"{error_type}: {message}".strip(": ")

                message = payload.get("message") or payload.get("detail") or payload.get("error")
                if isinstance(message, str) and message.strip():
                    return message.strip()
        except Exception:
            pass

        body = (response.text or "").strip()
        if not body:
            return f"HTTP {response.status_code}"
        return body[:300]

    def _token_or_fail(self) -> str:
        if not self.api_token:
            raise ApifyChartmetricError(
                "Apify token is missing. Set APIFY_API_TOKEN or send token in request."
            )
        return self.api_token

    def run_sync_get_dataset_items(
        self,
        keyword: str,
        mode: str = "1",
        exact: str = "on",
        category: str = "artist",
        operation: str = "gd",
    ) -> list[dict[str, Any]]:
        token = self._token_or_fail()

        payload = {
            "operation": operation,
            "keyword": keyword,
            "category": category,
            "mode": mode,
            "exact": exact,
        }

        response = requests.post(
            self.run_sync_dataset_url,
            params={"token": token},
            json=payload,
            timeout=self.timeout_seconds,
        )

        if response.status_code == 403:
            message = self._extract_error_message(response)
            if "actor-is-not-rented" in message:
                raise ApifyChartmetricError(
                    "Chartmetric actor is not rented on this Apify account yet. Rent it in Apify Console, then run sync again."
                )
            raise ApifyChartmetricError(f"Apify access denied: {message}")

        if response.status_code == 404:
            raise ApifyChartmetricError(
                "Chartmetric actor or run endpoint was not found. Check APIFY_CHARTMETRIC_ACTOR."
            )

        if not response.ok:
            message = self._extract_error_message(response)
            raise ApifyChartmetricError(f"Apify actor request failed ({response.status_code}): {message}")

        try:
            payload_json = response.json()
        except Exception as exc:
            raise ApifyChartmetricError("Apify response could not be parsed as JSON") from exc

        if isinstance(payload_json, list):
            return [item for item in payload_json if isinstance(item, dict)]

        if isinstance(payload_json, dict):
            data = payload_json.get("data")
            if isinstance(data, list):
                return [item for item in data if isinstance(item, dict)]

        raise ApifyChartmetricError("Apify response format was not recognized")


class ChartmetricSyncService:
    def __init__(self, client: ApifyChartmetricClient) -> None:
        self.client = client

    def sync(
        self,
        songs: list[dict[str, Any]],
        keyword_override: str | None = None,
        mode: str = "1",
        exact: str = "on",
    ) -> ChartmetricSyncResult:
        if not songs:
            return ChartmetricSyncResult(
                profiles=[],
                warnings=["No songs found to enrich with Chartmetric data."],
                requests_made=0,
            )

        warnings: list[str] = []
        profiles: list[dict[str, Any]] = []
        requests_made = 0

        normalized_override = (keyword_override or "").strip()

        for song in songs:
            keyword = normalized_override
            if not keyword:
                keyword = str(song.get("artist_name") or "").strip()
            if not keyword:
                keyword = str(song.get("title") or "").strip()

            if not keyword:
                warnings.append(
                    f"Song '{song.get('title', 'Unknown')}' skipped because no artist/title keyword is available."
                )
                continue

            requests_made += 1
            try:
                items = self.client.run_sync_get_dataset_items(
                    keyword=keyword,
                    mode=mode,
                    exact=exact,
                )
            except ApifyChartmetricError as exc:
                warnings.append(f"{keyword}: {exc}")
                continue

            if not items:
                warnings.append(f"{keyword}: actor run succeeded but returned no dataset items.")
                continue

            first_item = items[0]
            metrics = extract_chartmetric_metrics(first_item)
            profiles.append(
                {
                    "song_id": int(song["id"]),
                    "song_title": song.get("title") or "Untitled",
                    "artist_name": song.get("artist_name") or "",
                    "keyword": keyword,
                    "dataset_items_count": len(items),
                    "metrics": metrics,
                    "raw_item": first_item,
                }
            )

        return ChartmetricSyncResult(
            profiles=profiles,
            warnings=warnings,
            requests_made=requests_made,
        )


def extract_chartmetric_metrics(item: dict[str, Any]) -> dict[str, Any]:
    numeric_fields = _flatten_numeric_fields(item)

    interesting_tokens = (
        "track",
        "song",
        "listener",
        "stream",
        "playlist",
        "chart",
        "follower",
        "monthly",
        "engagement",
        "shazam",
        "youtube",
        "instagram",
        "tiktok",
        "facebook",
        "spotify",
        "apple",
        "deezer",
        "amazon",
        "radio",
    )

    extracted: dict[str, float | int] = {}
    for key in sorted(numeric_fields.keys()):
        lowered = key.lower()
        if any(token in lowered for token in interesting_tokens):
            extracted[key] = numeric_fields[key]
        if len(extracted) >= 80:
            break

    track_candidates = []
    for key, value in numeric_fields.items():
        score = _track_metric_score(key)
        if score <= 0:
            continue
        track_candidates.append((score, key, value))

    track_candidates.sort(key=lambda row: (-row[0], row[1]))
    track_number_estimate = track_candidates[0][2] if track_candidates else None
    track_number_source_key = track_candidates[0][1] if track_candidates else ""

    return {
        "track_number_estimate": track_number_estimate,
        "track_number_source_key": track_number_source_key,
        "matched_metric_keys": sorted(extracted.keys())[:120],
        "extracted_metrics": extracted,
        "raw_top_level_keys": sorted(list(item.keys()))[:120],
    }


def build_chartmetric_knowledge_text(profile: dict[str, Any]) -> str:
    metrics = profile.get("metrics") or {}
    extracted_metrics = metrics.get("extracted_metrics") or {}

    lines = [
        "Chartmetric Artist Snapshot",
        f"Song: {profile.get('song_title', 'Unknown')}",
        f"Artist: {profile.get('artist_name') or 'n/a'}",
        f"Keyword: {profile.get('keyword') or 'n/a'}",
        f"Dataset Items Returned: {profile.get('dataset_items_count', 0)}",
    ]

    track_estimate = metrics.get("track_number_estimate")
    track_key = metrics.get("track_number_source_key") or ""
    lines.append(
        f"Track Number Estimate: {track_estimate if track_estimate is not None else 'n/a'}"
        f"{f' (source: {track_key})' if track_key else ''}"
    )

    lines.append("\nSelected Numeric Metrics:")
    if extracted_metrics:
        for key in sorted(extracted_metrics.keys())[:30]:
            lines.append(f"- {key}: {extracted_metrics[key]}")
    else:
        lines.append("- No numeric metrics were extracted from the actor response.")

    return "\n".join(lines)


def _track_metric_score(key: str) -> int:
    lowered = key.lower()

    if any(token in lowered for token in ("track_count", "tracks_count", "number_of_tracks", "num_tracks")):
        return 120
    if "total_tracks" in lowered or "tracks_total" in lowered:
        return 110
    if "song_count" in lowered or "songs_count" in lowered:
        return 100

    score = 0
    if "track" in lowered:
        score += 60
    if "song" in lowered:
        score += 45
    if "count" in lowered or "total" in lowered or "number" in lowered:
        score += 20
    if "id" in lowered:
        score -= 50

    return score


def _flatten_numeric_fields(
    value: Any,
    prefix: str = "",
    output: dict[str, float | int] | None = None,
) -> dict[str, float | int]:
    if output is None:
        output = {}

    if isinstance(value, dict):
        for key, child in value.items():
            next_prefix = f"{prefix}.{key}" if prefix else str(key)
            _flatten_numeric_fields(child, next_prefix, output)
        return output

    if isinstance(value, list):
        if prefix:
            output[f"{prefix}.__len__"] = len(value)

        for index, child in enumerate(value[:3]):
            next_prefix = f"{prefix}[{index}]" if prefix else f"[{index}]"
            _flatten_numeric_fields(child, next_prefix, output)
        return output

    if isinstance(value, bool):
        return output

    if isinstance(value, (int, float)):
        if isinstance(value, float):
            if math.isnan(value) or math.isinf(value):
                return output
        if prefix:
            output[prefix] = int(value) if isinstance(value, bool) else value
        return output

    if isinstance(value, str):
        parsed = _coerce_numeric_string(value)
        if parsed is not None and prefix:
            output[prefix] = parsed
        return output

    return output


def _coerce_numeric_string(raw: str) -> int | float | None:
    value = raw.strip().replace(",", "")
    if not value:
        return None

    if re.fullmatch(r"[-+]?\d+", value):
        try:
            return int(value)
        except Exception:
            return None

    if re.fullmatch(r"[-+]?\d*\.\d+", value):
        try:
            number = float(value)
            if math.isnan(number) or math.isinf(number):
                return None
            return number
        except Exception:
            return None

    return None
