from __future__ import annotations

from datetime import date

from pydantic import BaseModel, Field


class SongCreate(BaseModel):
    title: str = Field(min_length=1, max_length=200)
    spotify_link: str = Field(min_length=5, max_length=500)
    release_date: date = Field(description="ISO date, for example 2026-03-01")


class SpotifyResolveRequest(BaseModel):
    spotify_input: str = Field(
        min_length=2,
        max_length=500,
        description="Spotify track link, URI, 22-char track id, or ISRC.",
    )


class SongCreateFromSpotify(BaseModel):
    spotify_input: str = Field(
        min_length=2,
        max_length=500,
        description="Spotify track link, URI, 22-char track id, or ISRC.",
    )


class GenerateReportRequest(BaseModel):
    report_day: int | None = Field(
        default=None,
        description="Optional scheduled report day. Allowed: 1, 3, 7, 14, 21, 30.",
    )


class ChartSyncRequest(BaseModel):
    song_id: int | None = Field(
        default=None,
        ge=1,
        description="Optional song id to sync. If omitted, all songs are checked.",
    )
    chart_references: list[str] = Field(
        default_factory=list,
        description=(
            "Optional chart references. Each value can be an alias (regional-global-weekly), "
            "alias/date, or a full charts.spotify.com chart URL."
        ),
    )
    from_release_date: bool = Field(
        default=True,
        description="If true, scan chart dates from each song release date onward.",
    )
    max_points_per_chart: int = Field(
        default=40,
        ge=1,
        le=400,
        description="Maximum number of chart dates fetched per chart reference.",
    )
    access_token: str | None = Field(
        default=None,
        description="Optional Spotify Charts user access token override.",
    )


class ChartmetricSyncRequest(BaseModel):
    song_id: int | None = Field(
        default=None,
        ge=1,
        description="Optional song id to sync. If omitted, all songs are checked.",
    )
    keyword: str | None = Field(
        default=None,
        max_length=300,
        description="Optional keyword override (artist name) when syncing one song.",
    )
    mode: str = Field(
        default="1",
        pattern="^[123]$",
        description="Chartmetric actor extraction depth. Supported values: 1, 2, 3.",
    )
    exact: str = Field(
        default="on",
        pattern="^(on|off)$",
        description="Exact keyword match mode used by the actor.",
    )
    token: str | None = Field(
        default=None,
        description="Optional Apify API token override.",
    )


class ChatRequest(BaseModel):
    question: str = Field(min_length=1, max_length=2000)
