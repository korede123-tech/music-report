from __future__ import annotations

from datetime import date, datetime
import difflib
from pathlib import Path
import json
import re
import shutil

import pandas as pd
from fastapi import FastAPI, File, HTTPException, Request, UploadFile
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import FileResponse, HTMLResponse, Response
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates

from app.config import settings
from app.database import (
    create_song_record,
    delete_song_record,
    fetch_song_metrics,
    get_song_record,
    init_db,
    list_chart_appearances,
    list_chartmetric_profiles,
    list_knowledge_docs_for_song,
    list_generated_report_days,
    list_report_records,
    list_song_records,
    replace_song_metrics,
    save_chart_appearances,
    save_chartmetric_profile,
    save_chat_log,
    save_knowledge_doc,
    save_report_record,
    save_upload_record,
    search_knowledge_docs,
)
from app.logger import get_logger
from app.schemas import (
    ChartmetricSyncRequest,
    ChartSyncRequest,
    ChatRequest,
    GenerateReportRequest,
    SongCreate,
    SongCreateFromSpotify,
    SpotifyResolveRequest,
)
from app.services.apify_chartmetric import (
    ApifyChartmetricClient,
    ApifyChartmetricError,
    ChartmetricSyncService,
    build_chartmetric_knowledge_text,
)
from app.services.chatbot import CohereChatbot, extract_document_text, extract_pdf_text
from app.services.charting import create_chart_images
from app.services.csv_processing import build_summary, dataframe_to_records, parse_release_csv
from app.services.pdf_generation import generate_report_pdf
from app.services.scheduling import CHECKPOINTS, determine_next_report, report_label
from app.services.spotify_charts import (
    ChartSyncError,
    SpotifyChartsClient,
    SpotifyChartsSyncService,
    build_chart_knowledge_text,
)
from app.services.spotify_metadata import SpotifyResolver


settings.ensure_directories()
logger = get_logger()
chatbot = CohereChatbot(api_key=settings.cohere_api_key, model=settings.cohere_model)
spotify_resolver = SpotifyResolver(
    client_id=settings.spotify_client_id,
    client_secret=settings.spotify_client_secret,
)


app = FastAPI(
    title="Music Release Reports",
    description="Local app for music release report automation, metrics, PDFs, and chat Q&A.",
    version="1.0.0",
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

app.mount("/static", StaticFiles(directory=str(settings.static_dir)), name="static")
app.mount("/reports", StaticFiles(directory=str(settings.reports_dir)), name="reports")

templates = Jinja2Templates(directory=str(settings.templates_dir))


@app.on_event("startup")
def startup_event() -> None:
    settings.ensure_directories()
    init_db()
    logger.info("Application startup complete")


def _safe_filename(name: str) -> str:
    safe = "".join(char for char in name if char.isalnum() or char in {"-", "_", "."})
    return safe or "upload.csv"


def _summary_to_text(
    song: dict,
    summary: dict,
    dataframe: pd.DataFrame | None = None,
    release_date: date | None = None,
) -> str:
    windows = summary.get("cumulative_windows", {})
    day1_breakdown = summary.get("day1_breakdown", {})
    day1_dates = summary.get("day1_window_dates", [])
    platform_windows = summary.get("platform_windows", {})

    lines = [
        f"Song: {song['title']}",
        f"Spotify Link: {song['spotify_link']}",
        f"Release Date: {song['release_date']}",
        f"Day 1 Rule: {summary.get('day1_rule', 'Day 1 uses release date only.')}",
        f"Day 1 Dates Used: {', '.join(day1_dates) if day1_dates else 'n/a'}",
        f"Day 1 Spotify Streams: {day1_breakdown.get('spotify_streams', 0)}",
        f"Day 1 Apple Music Streams: {day1_breakdown.get('apple_music_streams', 0)}",
        f"Day 1 Shazams: {day1_breakdown.get('number_of_shazams', 0)}",
        f"Day 1 Combined Total: {day1_breakdown.get('combined_total', 0)}",
        "Cumulative Streams:",
    ]
    for window_name, value in windows.items():
        lines.append(f"- {window_name}: {value}")

    spotify_windows = platform_windows.get("spotify_streams", {})
    apple_windows = platform_windows.get("apple_music_streams", {})
    shazam_windows = platform_windows.get("number_of_shazams", {})
    lines.extend(
        [
            "Platform Window Totals:",
            f"- Spotify 24h/3d/7d: {spotify_windows.get('24h', 0)} / {spotify_windows.get('3d', 0)} / {spotify_windows.get('7d', 0)}",
            f"- Apple 24h/3d/7d: {apple_windows.get('24h', 0)} / {apple_windows.get('3d', 0)} / {apple_windows.get('7d', 0)}",
            f"- Shazams 24h/3d/7d: {shazam_windows.get('24h', 0)} / {shazam_windows.get('3d', 0)} / {shazam_windows.get('7d', 0)}",
        ]
    )

    lines.extend(
        [
            f"Cumulative Listeners: {summary.get('latest_cumulative_listeners', 0)}",
            f"Average Replay Rate: {summary.get('avg_replay_rate', 0)}",
            f"Cumulative Saves: {summary.get('latest_cumulative_saves', 0)}",
            f"Cumulative Skips: {summary.get('latest_cumulative_skips', 0)}",
            f"Data Range: {summary.get('first_data_date', 'n/a')} to {summary.get('last_data_date', 'n/a')}",
        ]
    )

    if dataframe is not None and release_date is not None and not dataframe.empty:
        working = dataframe.copy()
        working["date"] = pd.to_datetime(working["date"], errors="coerce").dt.date
        working = working.dropna(subset=["date"])
        working = working[working["date"] >= release_date]
        working = working.sort_values("date").head(14)

        if not working.empty:
            lines.append("Day-by-Day Snapshot (first 14 post-release rows):")
            for index, (_, row) in enumerate(working.iterrows(), start=1):
                lines.append(
                    f"- Day {index} ({row['date'].isoformat()}): "
                    f"Spotify={row['spotify_streams']}, "
                    f"Apple={row['apple_music_streams']}, "
                    f"Shazams={row['number_of_shazams']}, "
                    f"Combined={row['combined_total']}"
                )

    return "\n".join(lines)


def _report_url(file_path: str) -> str:
    full_path = Path(file_path)
    if not full_path.exists():
        return ""
    try:
        relative = full_path.relative_to(settings.reports_dir)
    except ValueError:
        return ""
    return f"/reports/{relative.as_posix()}"


def _q1_song_count_payload(songs: list[dict]) -> dict:
    q1_songs = []
    for song in songs:
        try:
            release = date.fromisoformat(song.get("release_date", ""))
        except Exception:
            continue
        if release.month in (1, 2, 3):
            q1_songs.append(song)

    unique_titles = sorted({(song.get("title") or "Untitled").strip() for song in q1_songs})
    return {
        "rows": len(q1_songs),
        "unique_titles": unique_titles,
    }


def _compact_text(value: str) -> str:
    return re.sub(r"[^a-z0-9]+", "", (value or "").strip().lower())


def _extract_window_days(question: str) -> int | None:
    lowered = question.lower()
    match = re.search(r"\b(\d{1,2})\s*[- ]?(?:day|days)\b", lowered)
    if match:
        value = int(match.group(1))
        if 1 <= value <= 60:
            return value

    if "24h" in lowered or "24 hour" in lowered or "24-hour" in lowered:
        return 1

    return None


def _find_song_from_question(question: str, songs: list[dict]) -> dict | None:
    if not songs:
        return None

    lowered_question = question.lower()
    compact_question = _compact_text(question)
    question_tokens = set(re.findall(r"[a-z0-9]+", lowered_question))

    best_song: dict | None = None
    best_score = 0.0

    for song in songs:
        title = str(song.get("title") or "").strip()
        if not title:
            continue

        lowered_title = title.lower()
        if lowered_title in lowered_question:
            return song

        compact_title = _compact_text(title)
        title_tokens = {token for token in re.findall(r"[a-z0-9]+", lowered_title) if len(token) > 2}

        score = 0.0
        if compact_title:
            score = max(score, difflib.SequenceMatcher(None, compact_title, compact_question).ratio())

        if title_tokens:
            overlap = len(title_tokens & question_tokens) / len(title_tokens)
            score = max(score, overlap * 0.95)

        if score > best_score:
            best_score = score
            best_song = song

    if best_song and (best_score >= 0.45 or len(songs) == 1):
        return best_song

    return None


def _format_metric_number(value: float) -> str:
    rounded = round(value)
    if abs(value - rounded) < 0.0001:
        return f"{int(rounded):,}"
    return f"{value:,.2f}"


def _build_song_window_platform_answer(song: dict, window_days: int) -> str | None:
    rows = fetch_song_metrics(int(song["id"]))
    if not rows:
        return None

    release_date_raw = str(song.get("release_date") or "")
    try:
        release_date = date.fromisoformat(release_date_raw)
    except Exception:
        release_date = None

    normalized_rows: list[dict] = []
    for row in rows:
        try:
            row_date = date.fromisoformat(str(row.get("date") or ""))
        except Exception:
            continue
        normalized_rows.append({"row": row, "date": row_date})

    if not normalized_rows:
        return None

    normalized_rows.sort(key=lambda item: item["date"])

    if release_date is not None:
        release_rows = [item for item in normalized_rows if item["date"] >= release_date]
    else:
        release_rows = normalized_rows

    if not release_rows:
        release_rows = normalized_rows

    window_rows = release_rows[:window_days]
    if not window_rows:
        return None

    spotify_total = sum(float(item["row"].get("spotify_streams") or 0.0) for item in window_rows)
    apple_total = sum(float(item["row"].get("apple_music_streams") or 0.0) for item in window_rows)
    combined_total = spotify_total + apple_total

    start_date = window_rows[0]["date"].isoformat()
    end_date = window_rows[-1]["date"].isoformat()

    lines = [
        f"{window_days}-day Apple Music + Spotify data for {song.get('title', 'Unknown')}:",
        f"- Window used: {start_date} to {end_date} ({len(window_rows)} day(s) available)",
        f"- Spotify streams total: {_format_metric_number(spotify_total)}",
        f"- Apple Music streams total: {_format_metric_number(apple_total)}",
        f"- Combined total: {_format_metric_number(combined_total)}",
        "",
        "Day-by-day:",
    ]

    for index, item in enumerate(window_rows, start=1):
        row = item["row"]
        lines.append(
            f"- Day {index} ({item['date'].isoformat()}): "
            f"Spotify {_format_metric_number(float(row.get('spotify_streams') or 0.0))}, "
            f"Apple Music {_format_metric_number(float(row.get('apple_music_streams') or 0.0))}"
        )

    return "\n".join(lines)


def _resolve_chart_access_token(request_token: str | None = None) -> tuple[str, str]:
    explicit = (request_token or "").strip()
    if explicit:
        return explicit, "request"

    from_settings = settings.spotify_charts_access_token.strip()
    if from_settings:
        return from_settings, "settings"

    try:
        fallback = spotify_resolver._token()
        if fallback:
            return fallback, "spotify_client_credentials"
    except Exception:
        pass

    return "", ""


def _resolve_apify_token(request_token: str | None = None) -> tuple[str, str]:
    explicit = (request_token or "").strip()
    if explicit:
        return explicit, "request"

    from_settings = settings.apify_api_token.strip()
    if from_settings:
        return from_settings, "settings"

    return "", ""


@app.get("/", response_class=HTMLResponse)
def dashboard(request: Request) -> HTMLResponse:
    return templates.TemplateResponse("index.html", {"request": request})


@app.get("/health")
def health_check() -> dict:
    return {"status": "ok", "time": datetime.utcnow().isoformat()}


@app.get("/api/integrations/status")
def integrations_status() -> dict:
    spotify_credentials_configured = bool(
        settings.spotify_client_id.strip() and settings.spotify_client_secret.strip()
    )
    charts_token_configured = bool(settings.spotify_charts_access_token.strip())
    apify_token_configured = bool(settings.apify_api_token.strip())

    warnings: list[str] = []
    if not spotify_credentials_configured:
        warnings.append("Spotify metadata lookup is disabled until SPOTIFY_CLIENT_ID and SPOTIFY_CLIENT_SECRET are set.")
    if not charts_token_configured:
        warnings.append(
            "SPOTIFY_CHARTS_ACCESS_TOKEN is missing. Charts sync will try client-credentials fallback, "
            "which can fail for accounts not fully onboarded to charts.spotify.com."
        )
    if not apify_token_configured:
        warnings.append("APIFY_API_TOKEN is missing. Chartmetric sync is disabled until a token is configured.")

    return {
        "spotify_credentials_configured": spotify_credentials_configured,
        "charts_token_configured": charts_token_configured,
        "apify_token_configured": apify_token_configured,
        "warnings": warnings,
    }


@app.get("/favicon.ico", include_in_schema=False)
def favicon() -> Response:
    icon_path = settings.static_dir / "favicon.ico"
    if icon_path.exists():
        return FileResponse(icon_path)
    return Response(status_code=204)


@app.post("/api/songs")
def create_song(payload: SongCreate) -> dict:
    song = create_song_record(
        title=payload.title.strip(),
        spotify_link=payload.spotify_link.strip(),
        release_date=payload.release_date,
    )
    logger.info("Song created: id=%s title=%s", song["id"], song["title"])
    return song


@app.post("/api/spotify/resolve")
def resolve_spotify_track(payload: SpotifyResolveRequest) -> dict:
    try:
        resolved = spotify_resolver.resolve(payload.spotify_input)
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    except RuntimeError as exc:
        raise HTTPException(status_code=503, detail=str(exc)) from exc
    return resolved


@app.post("/api/songs/from-spotify")
def create_song_from_spotify(payload: SongCreateFromSpotify) -> dict:
    try:
        resolved = spotify_resolver.resolve(payload.spotify_input)
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    except RuntimeError as exc:
        raise HTTPException(status_code=503, detail=str(exc)) from exc

    release_date = date.fromisoformat(resolved["release_date"])
    song = create_song_record(
        title=resolved["title"],
        spotify_link=resolved["spotify_link"],
        release_date=release_date,
        artist_name=resolved.get("artist_name") or None,
        spotify_track_id=resolved.get("spotify_track_id") or None,
        isrc=resolved.get("isrc") or None,
    )
    logger.info(
        "Song created from Spotify metadata: id=%s title=%s track_id=%s",
        song.get("id"),
        song.get("title"),
        song.get("spotify_track_id"),
    )
    return {"song": song, "resolved": resolved}


@app.get("/api/songs")
def list_songs() -> list[dict]:
    songs = list_song_records()
    for song in songs:
        release = date.fromisoformat(song["release_date"])
        generated_days = list_generated_report_days(song["id"])
        song["generated_report_days"] = generated_days
        song["next_report"] = determine_next_report(release, generated_days)
    return songs


@app.delete("/api/songs/{song_id}")
def delete_song(song_id: int) -> dict:
    deleted = delete_song_record(song_id)
    if not deleted:
        raise HTTPException(status_code=404, detail="Song not found")
    logger.info("Song deleted: id=%s", song_id)
    return {"deleted": True, "song_id": song_id}


@app.post("/api/songs/{song_id}/upload-csv")
def upload_song_csv(song_id: int, file: UploadFile = File(...)) -> dict:
    song = get_song_record(song_id)
    if not song:
        raise HTTPException(status_code=404, detail="Song not found")

    suffix = Path(file.filename).suffix.lower()
    metrics_extensions = {".csv", ".xlsx"}
    document_extensions = {".pdf", ".doc", ".docx", ".docs"}
    allowed_extensions = metrics_extensions | document_extensions

    if suffix not in allowed_extensions:
        raise HTTPException(
            status_code=400,
            detail="Upload must be one of: .csv, .xlsx, .pdf, .doc, .docx, .docs",
        )

    timestamp = datetime.utcnow().strftime("%Y%m%d_%H%M%S")
    file_path = settings.upload_dir / f"song_{song_id}_{timestamp}_{_safe_filename(file.filename)}"

    with file_path.open("wb") as target:
        shutil.copyfileobj(file.file, target)

    save_upload_record(song_id=song_id, file_name=file.filename, file_path=str(file_path))

    if suffix in document_extensions:
        try:
            document_text, warnings = extract_document_text(file_path)
        except ValueError as exc:
            raise HTTPException(status_code=400, detail=str(exc)) from exc

        if not document_text.strip():
            document_text = (
                f"Document uploaded for song '{song['title']}' ({file.filename}). "
                "Text extraction returned minimal content."
            )

        save_knowledge_doc(
            title=f"{song['title']} Document - {file.filename}",
            source_path=str(file_path),
            content=document_text,
        )

        logger.info("Document uploaded: song_id=%s file=%s", song_id, file.filename)
        return {
            "song_id": song_id,
            "file": file.filename,
            "ingestion_type": "document",
            "source_format": suffix,
            "rows_loaded": 0,
            "warnings": warnings,
            "summary": None,
        }

    try:
        dataframe, warnings = parse_release_csv(file_path)
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc

    if dataframe.empty:
        raise HTTPException(status_code=400, detail="No valid metric rows were found after parsing")

    replace_song_metrics(song_id, dataframe_to_records(dataframe))

    release_date = date.fromisoformat(song["release_date"])
    try:
        summary, _, _ = build_summary(dataframe, release_date)
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc

    save_knowledge_doc(
        title=f"{song['title']} Metrics Summary",
        source_path=str(file_path),
        content=_summary_to_text(song, summary, dataframe=dataframe, release_date=release_date),
    )

    logger.info("Metrics uploaded: song_id=%s file=%s rows=%s", song_id, file.filename, len(dataframe))
    return {
        "song_id": song_id,
        "file": file.filename,
        "ingestion_type": "metrics",
        "source_format": suffix,
        "rows_loaded": int(len(dataframe)),
        "warnings": warnings,
        "summary": summary,
    }


@app.get("/api/songs/{song_id}/metrics")
def get_song_metrics(song_id: int) -> dict:
    song = get_song_record(song_id)
    if not song:
        raise HTTPException(status_code=404, detail="Song not found")

    rows = fetch_song_metrics(song_id)
    if not rows:
        raise HTTPException(status_code=404, detail="No uploaded metrics found for this song")

    dataframe = pd.DataFrame(rows)
    dataframe["date"] = pd.to_datetime(dataframe["date"]).dt.date

    release_date = date.fromisoformat(song["release_date"])
    summary, timeline_df, windows = build_summary(dataframe, release_date)

    timeline = []
    for _, row in timeline_df.iterrows():
        timeline.append(
            {
                "date": row["date"].isoformat(),
                "day_number": int(row["day_number"]),
                "cumulative_streams": float(round(row["cumulative_combined_total"], 3)),
                "cumulative_listeners": float(round(row["cumulative_spotify_listeners"], 3)),
                "avg_replay_rate": float(round(row["avg_replay_rate"], 5)),
                "cumulative_saves": float(round(row["cumulative_spotify_saves"], 3)),
                "cumulative_skips": float(round(row["cumulative_spotify_skips"], 3)),
            }
        )

    return {
        "song": song,
        "summary": summary,
        "cumulative_windows": windows,
        "timeline": timeline,
    }


@app.post("/api/songs/{song_id}/generate-report")
def generate_song_report(song_id: int, payload: GenerateReportRequest | None = None) -> dict:
    song = get_song_record(song_id)
    if not song:
        raise HTTPException(status_code=404, detail="Song not found")

    rows = fetch_song_metrics(song_id)
    if not rows:
        raise HTTPException(status_code=400, detail="Upload a CSV first before generating a report")

    dataframe = pd.DataFrame(rows)
    dataframe["date"] = pd.to_datetime(dataframe["date"]).dt.date

    release_date = date.fromisoformat(song["release_date"])
    generated_days = list_generated_report_days(song_id)

    requested_day = payload.report_day if payload else None
    if requested_day is not None:
        if requested_day not in CHECKPOINTS:
            raise HTTPException(status_code=400, detail=f"report_day must be one of {CHECKPOINTS}")
        report_day = requested_day
    else:
        next_report = determine_next_report(release_date, generated_days)
        if next_report["status"] == "complete":
            raise HTTPException(status_code=400, detail="All scheduled reports are already complete")
        report_day = int(next_report["report_day"])

    try:
        summary, timeline_df, _ = build_summary(dataframe, release_date)
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc

    chart_paths = create_chart_images(
        song_id=song_id,
        report_day=report_day,
        timeline_df=timeline_df,
        output_dir=settings.chart_dir,
    )
    label = report_label(report_day)
    pdf_path = generate_report_pdf(
        song=song,
        report_day=report_day,
        report_name=label,
        summary=summary,
        chart_paths=chart_paths,
        output_dir=settings.generated_reports_dir,
    )

    report = save_report_record(
        song_id=song_id,
        report_day=report_day,
        pdf_path=str(pdf_path),
        summary=summary,
    )
    report["pdf_url"] = _report_url(report["pdf_path"])
    report["report_label"] = label

    report_context = _summary_to_text(song, summary)
    save_knowledge_doc(
        title=f"{song['title']} {label} Report",
        source_path=str(pdf_path),
        content=report_context,
    )

    logger.info("PDF generated: song_id=%s report_day=%s path=%s", song_id, report_day, pdf_path)
    return report


@app.get("/api/reports")
def list_reports() -> list[dict]:
    reports = list_report_records()
    for report in reports:
        report["file_exists"] = Path(report["pdf_path"]).exists()
        report["pdf_url"] = _report_url(report["pdf_path"])
        report["report_label"] = report_label(int(report["report_day"]))
        report["summary"] = json.loads(report["summary_json"]) if report.get("summary_json") else {}
    return reports


@app.post("/api/charts/sync")
def sync_charts(payload: ChartSyncRequest | None = None) -> dict:
    request_payload = payload or ChartSyncRequest()

    if request_payload.song_id is not None:
        song = get_song_record(request_payload.song_id)
        if not song:
            raise HTTPException(status_code=404, detail="Song not found")
        songs = [song]
    else:
        songs = list_song_records()

    if not songs:
        raise HTTPException(status_code=400, detail="No songs found. Add at least one song first.")

    access_token, token_source = _resolve_chart_access_token(request_payload.access_token)
    if not access_token:
        raise HTTPException(
            status_code=400,
            detail=(
                "Spotify Charts access token is missing. Set SPOTIFY_CHARTS_ACCESS_TOKEN "
                "or send access_token in /api/charts/sync request."
            ),
        )

    chart_client = SpotifyChartsClient(
        access_token=access_token,
        spotify_app_version=settings.spotify_charts_app_version,
    )
    chart_service = SpotifyChartsSyncService(chart_client)

    try:
        result = chart_service.sync(
            songs=songs,
            references=request_payload.chart_references,
            from_release_date=request_payload.from_release_date,
            max_points_per_chart=request_payload.max_points_per_chart,
        )
    except ChartSyncError as exc:
        raise HTTPException(status_code=502, detail=str(exc)) from exc

    grouped_matches: dict[int, list[dict]] = {}
    for match in result.matches:
        grouped_matches.setdefault(int(match["song_id"]), []).append(match)

    replace_aliases = list(result.processed_aliases)
    for song in songs:
        song_id = int(song["id"])
        save_chart_appearances(
            song_id=song_id,
            appearances=grouped_matches.get(song_id, []),
            replace_aliases=replace_aliases,
        )

    knowledge_text = build_chart_knowledge_text(result)
    save_knowledge_doc(
        title=f"Spotify Charts Sync {datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S')} UTC",
        source_path="spotify://charts-sync",
        content=knowledge_text,
    )

    response = result.as_dict()
    response["token_source"] = token_source
    logger.info(
        "Spotify charts sync complete: songs=%s refs=%s matches=%s",
        len(songs),
        len(result.processed_references),
        len(result.matches),
    )
    return response


@app.get("/api/charts/appearances")
def get_chart_appearances(song_id: int | None = None) -> dict:
    if song_id is not None and not get_song_record(song_id):
        raise HTTPException(status_code=404, detail="Song not found")

    items = list_chart_appearances(song_id)
    return {
        "count": len(items),
        "items": items,
    }


@app.post("/api/chartmetric/sync")
def sync_chartmetric(payload: ChartmetricSyncRequest | None = None) -> dict:
    request_payload = payload or ChartmetricSyncRequest()

    if request_payload.song_id is not None:
        song = get_song_record(request_payload.song_id)
        if not song:
            raise HTTPException(status_code=404, detail="Song not found")
        songs = [song]
    else:
        songs = list_song_records()

    if not songs:
        raise HTTPException(status_code=400, detail="No songs found. Add at least one song first.")

    token, token_source = _resolve_apify_token(request_payload.token)
    if not token:
        message = "Apify token is missing. Set APIFY_API_TOKEN or send token in /api/chartmetric/sync request."
        logger.info("Chartmetric sync skipped: %s", message)
        return {
            "profiles": [],
            "warnings": [message],
            "requests_made": 0,
            "profiles_count": 0,
            "actor_id": settings.apify_chartmetric_actor,
            "token_source": token_source,
        }

    client = ApifyChartmetricClient(
        api_token=token,
        actor_id=settings.apify_chartmetric_actor,
    )
    service = ChartmetricSyncService(client)

    try:
        result = service.sync(
            songs=songs,
            keyword_override=request_payload.keyword,
            mode=request_payload.mode,
            exact=request_payload.exact,
        )
    except ApifyChartmetricError as exc:
        raise HTTPException(status_code=502, detail=str(exc)) from exc

    for profile in result.profiles:
        save_chartmetric_profile(
            song_id=int(profile["song_id"]),
            keyword=str(profile.get("keyword") or ""),
            metrics=profile.get("metrics") or {},
            raw_item=profile.get("raw_item") or {},
        )

        save_knowledge_doc(
            title=f"Chartmetric Snapshot - {profile.get('song_title', 'Unknown')}",
            source_path=f"apify://{settings.apify_chartmetric_actor}/{profile.get('keyword', '')}",
            content=build_chartmetric_knowledge_text(profile),
        )

    response = result.as_dict()
    response["actor_id"] = settings.apify_chartmetric_actor
    response["token_source"] = token_source

    logger.info(
        "Chartmetric sync complete: songs=%s profiles=%s warnings=%s",
        len(songs),
        len(result.profiles),
        len(result.warnings),
    )
    return response


@app.get("/api/chartmetric/profiles")
def get_chartmetric_profiles(song_id: int | None = None) -> dict:
    if song_id is not None and not get_song_record(song_id):
        raise HTTPException(status_code=404, detail="Song not found")

    items = list_chartmetric_profiles(song_id)
    for item in items:
        item.pop("metrics_json", None)
        item.pop("raw_json", None)
        item.pop("raw", None)

    return {
        "count": len(items),
        "items": items,
    }


@app.post("/api/reports/upload-pdf")
def upload_reference_pdf(file: UploadFile = File(...)) -> dict:
    if not file.filename.lower().endswith(".pdf"):
        raise HTTPException(status_code=400, detail="Upload must be a PDF file")

    timestamp = datetime.utcnow().strftime("%Y%m%d_%H%M%S")
    output_path = settings.knowledge_dir / f"{timestamp}_{_safe_filename(file.filename)}"

    with output_path.open("wb") as target:
        shutil.copyfileobj(file.file, target)

    extracted_text = extract_pdf_text(output_path)
    if not extracted_text.strip():
        extracted_text = "PDF uploaded, but text extraction returned empty content."

    doc_id = save_knowledge_doc(
        title=file.filename,
        source_path=str(output_path),
        content=extracted_text,
    )
    logger.info("Knowledge PDF uploaded: %s", file.filename)
    return {
        "doc_id": doc_id,
        "title": file.filename,
        "source_path": str(output_path),
        "characters_indexed": len(extracted_text),
    }


@app.post("/api/chat")
def ask_chatbot(payload: ChatRequest) -> dict:
    docs = search_knowledge_docs(payload.question, limit=5)
    songs = list_song_records()

    lowered = payload.question.lower()
    if songs and "q1" in lowered and ("song" in lowered or "track" in lowered) and (
        "how many" in lowered or "count" in lowered
    ):
        q1_data = _q1_song_count_payload(songs)
        unique_count = len(q1_data["unique_titles"])
        answer = (
            f"Q1 release count in your current song pipeline: {q1_data['rows']} row(s), "
            f"{unique_count} unique track title(s).\n"
            f"Unique Q1 titles: {', '.join(q1_data['unique_titles']) if q1_data['unique_titles'] else 'None'}"
        )
        save_chat_log(payload.question, answer)
        return {"answer": answer, "sources": [{"title": "songs table", "source_path": str(settings.db_path)}]}

    window_days = _extract_window_days(payload.question)
    if songs and window_days and ("spotify" in lowered or "apple" in lowered):
        matched_song = _find_song_from_question(payload.question, songs)
        if matched_song:
            answer = _build_song_window_platform_answer(matched_song, window_days)
            if answer:
                related_docs = list_knowledge_docs_for_song(str(matched_song.get("title") or ""), limit=4)
                if not related_docs:
                    related_docs = search_knowledge_docs(
                        f"{matched_song.get('title', '')} {window_days} day spotify apple",
                        limit=3,
                    )

                seen_source_paths: set[str] = set()
                deduped_sources: list[dict] = []
                for doc in related_docs:
                    source_path = str(doc.get("source_path") or "").strip()
                    if not source_path or source_path in seen_source_paths:
                        continue
                    seen_source_paths.add(source_path)
                    deduped_sources.append(
                        {
                            "title": doc.get("title", ""),
                            "source_path": source_path,
                        }
                    )

                save_chat_log(payload.question, answer)
                return {
                    "answer": answer,
                    "sources": [
                        {"title": "daily_metrics", "source_path": str(settings.db_path)},
                        *deduped_sources,
                    ],
                }

    answer = chatbot.ask(payload.question, docs=docs, song_summaries=songs)
    save_chat_log(payload.question, answer)
    logger.info("Chat question answered with %s docs", len(docs))
    return {
        "answer": answer,
        "sources": [
            {"title": doc.get("title", ""), "source_path": doc.get("source_path", "")}
            for doc in docs
        ],
    }
