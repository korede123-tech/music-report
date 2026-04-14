from __future__ import annotations

from datetime import date

from pydantic import BaseModel, Field


class SongCreate(BaseModel):
    title: str = Field(min_length=1, max_length=200)
    spotify_link: str = Field(min_length=5, max_length=500)
    release_date: date = Field(description="ISO date, for example 2026-03-01")


class GenerateReportRequest(BaseModel):
    report_day: int | None = Field(
        default=None,
        description="Optional scheduled report day. Allowed: 1, 3, 7, 14, 21, 30.",
    )


class ChatRequest(BaseModel):
    question: str = Field(min_length=1, max_length=2000)
