from __future__ import annotations

from datetime import date
import re
import time

import requests


TRACK_ID_PATTERN = re.compile(r"^[A-Za-z0-9]{22}$")
SPOTIFY_TRACK_PATTERN = re.compile(r"(?:open\.spotify\.com/track/|spotify:track:)([A-Za-z0-9]{22})")
ISRC_PATTERN = re.compile(r"^[A-Z]{2}[A-Z0-9]{3}\d{7}$")


def _normalize_release_date(release_raw: str, precision: str) -> date:
    cleaned = release_raw.strip()
    if precision == "day":
        return date.fromisoformat(cleaned)
    if precision == "month":
        return date.fromisoformat(f"{cleaned}-01")
    if precision == "year":
        return date.fromisoformat(f"{cleaned}-01-01")

    if len(cleaned) >= 10:
        return date.fromisoformat(cleaned[:10])
    if len(cleaned) == 7:
        return date.fromisoformat(f"{cleaned}-01")
    if len(cleaned) == 4:
        return date.fromisoformat(f"{cleaned}-01-01")
    raise ValueError("Spotify release date format is invalid")


def _extract_track_id(value: str) -> str | None:
    candidate = value.strip()
    if TRACK_ID_PATTERN.match(candidate):
        return candidate
    match = SPOTIFY_TRACK_PATTERN.search(candidate)
    if match:
        return match.group(1)
    return None


def _extract_isrc(value: str) -> str | None:
    candidate = value.strip().upper()
    if candidate.startswith("ISRC:"):
        candidate = candidate.split(":", maxsplit=1)[1].strip()
    if ISRC_PATTERN.match(candidate):
        return candidate
    return None


class SpotifyResolver:
    def __init__(self, client_id: str, client_secret: str) -> None:
        self.client_id = client_id.strip()
        self.client_secret = client_secret.strip()
        self._access_token = ""
        self._token_expires_at = 0.0

    def _assert_credentials(self) -> None:
        if not self.client_id or not self.client_secret:
            raise RuntimeError(
                "Spotify API credentials are missing. Set SPOTIFY_CLIENT_ID and SPOTIFY_CLIENT_SECRET."
            )

    def _token(self) -> str:
        self._assert_credentials()

        now = time.time()
        if self._access_token and now < self._token_expires_at:
            return self._access_token

        response = requests.post(
            "https://accounts.spotify.com/api/token",
            data={"grant_type": "client_credentials"},
            auth=(self.client_id, self.client_secret),
            timeout=25,
        )

        if not response.ok:
            raise RuntimeError(f"Spotify token request failed ({response.status_code})")

        payload = response.json()
        token = payload.get("access_token")
        expires_in = int(payload.get("expires_in") or 3600)

        if not token:
            raise RuntimeError("Spotify token response did not include access_token")

        self._access_token = token
        self._token_expires_at = now + max(expires_in - 60, 60)
        return self._access_token

    def _get(self, path: str, params: dict | None = None, retry: bool = True) -> dict:
        token = self._token()
        response = requests.get(
            f"https://api.spotify.com/v1{path}",
            headers={"Authorization": f"Bearer {token}"},
            params=params,
            timeout=25,
        )

        if response.status_code == 401 and retry:
            self._access_token = ""
            return self._get(path, params=params, retry=False)

        if response.status_code == 404:
            raise ValueError("Spotify track not found")

        if not response.ok:
            raise RuntimeError(f"Spotify API request failed ({response.status_code})")

        return response.json()

    @staticmethod
    def _serialize_track(track_payload: dict, source_isrc: str | None = None) -> dict:
        album = track_payload.get("album") or {}
        release_raw = album.get("release_date")
        precision = (album.get("release_date_precision") or "day").strip()
        if not release_raw:
            raise ValueError("Spotify did not provide a release date for this track")

        release_date = _normalize_release_date(str(release_raw), precision)
        artists = track_payload.get("artists") or []
        artist_name = ", ".join(artist.get("name", "") for artist in artists if artist.get("name"))

        track_id = track_payload.get("id") or ""
        external_urls = track_payload.get("external_urls") or {}
        spotify_link = external_urls.get("spotify") or f"https://open.spotify.com/track/{track_id}"

        external_ids = track_payload.get("external_ids") or {}
        isrc = (external_ids.get("isrc") or source_isrc or "").upper()

        return {
            "title": (track_payload.get("name") or "Untitled").strip() or "Untitled",
            "artist_name": artist_name,
            "spotify_link": spotify_link,
            "spotify_track_id": track_id,
            "release_date": release_date.isoformat(),
            "release_date_precision": precision,
            "isrc": isrc,
        }

    def resolve(self, spotify_input: str) -> dict:
        value = spotify_input.strip()
        if not value:
            raise ValueError("Provide a Spotify track link, Spotify track URI, track id, or ISRC")

        track_id = _extract_track_id(value)
        if track_id:
            payload = self._get(f"/tracks/{track_id}")
            return self._serialize_track(payload)

        isrc = _extract_isrc(value)
        if isrc:
            payload = self._get("/search", params={"q": f"isrc:{isrc}", "type": "track", "limit": 1})
            items = (payload.get("tracks") or {}).get("items") or []
            if not items:
                raise ValueError(f"No Spotify track found for ISRC {isrc}")
            return self._serialize_track(items[0], source_isrc=isrc)

        payload = self._get("/search", params={"q": value, "type": "track", "limit": 1})
        items = (payload.get("tracks") or {}).get("items") or []
        if not items:
            raise ValueError(
                "No Spotify track found for that input. Try a direct Spotify track link, track URI, track id, ISRC, or a more specific search query."
            )
        return self._serialize_track(items[0])
