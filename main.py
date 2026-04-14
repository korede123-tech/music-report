from __future__ import annotations

from datetime import date, datetime
from pathlib import Path
import json
import shutil

import pandas as pd
from fastapi import FastAPI, File, HTTPException, Request, UploadFile
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import HTMLResponse
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates

from app.config import settings
from app.database import (
    create_song_record,
    fetch_song_metrics,
    get_song_record,
    init_db,
    list_generated_report_days,
    list_report_records,
    list_song_records,
    replace_song_metrics,
    save_chat_log,
    save_knowledge_doc,
    save_report_record,
    save_upload_record,
    search_knowledge_docs,
)
from app.logger import get_logger
from app.schemas import ChatRequest, GenerateReportRequest, SongCreate
from app.services.chatbot import CohereChatbot, extract_pdf_text
from app.services.charting import create_chart_images
from app.services.csv_processing import build_summary, dataframe_to_records, parse_release_csv
from app.services.pdf_generation import generate_report_pdf
from app.services.scheduling import CHECKPOINTS, determine_next_report, report_label


settings.ensure_directories()
logger = get_logger()
chatbot = CohereChatbot(api_key=settings.cohere_api_key, model=settings.cohere_model)


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


def _summary_to_text(song: dict, summary: dict) -> str:
    windows = summary.get("cumulative_windows", {})
    lines = [
        f"Song: {song['title']}",
        f"Spotify Link: {song['spotify_link']}",
        f"Release Date: {song['release_date']}",
        "Cumulative Streams:",
    ]
    for window_name, value in windows.items():
        lines.append(f"- {window_name}: {value}")
    lines.extend(
        [
            f"Cumulative Listeners: {summary.get('latest_cumulative_listeners', 0)}",
            f"Average Replay Rate: {summary.get('avg_replay_rate', 0)}",
            f"Cumulative Saves: {summary.get('latest_cumulative_saves', 0)}",
            f"Cumulative Skips: {summary.get('latest_cumulative_skips', 0)}",
            f"Data Range: {summary.get('first_data_date', 'n/a')} to {summary.get('last_data_date', 'n/a')}",
        ]
    )
    return "\n".join(lines)


def _report_url(file_path: str) -> str:
    full_path = Path(file_path)
    try:
        relative = full_path.relative_to(settings.reports_dir)
    except ValueError:
        return ""
    return f"/reports/{relative.as_posix()}"


@app.get("/", response_class=HTMLResponse)
def dashboard(request: Request) -> HTMLResponse:
    return templates.TemplateResponse("index.html", {"request": request})


@app.get("/health")
def health_check() -> dict:
    return {"status": "ok", "time": datetime.utcnow().isoformat()}


@app.post("/api/songs")
def create_song(payload: SongCreate) -> dict:
    song = create_song_record(
        title=payload.title.strip(),
        spotify_link=payload.spotify_link.strip(),
        release_date=payload.release_date,
    )
    logger.info("Song created: id=%s title=%s", song["id"], song["title"])
    return song


@app.get("/api/songs")
def list_songs() -> list[dict]:
    songs = list_song_records()
    for song in songs:
        release = date.fromisoformat(song["release_date"])
        generated_days = list_generated_report_days(song["id"])
        song["generated_report_days"] = generated_days
        song["next_report"] = determine_next_report(release, generated_days)
    return songs


@app.post("/api/songs/{song_id}/upload-csv")
def upload_song_csv(song_id: int, file: UploadFile = File(...)) -> dict:
    song = get_song_record(song_id)
    if not song:
        raise HTTPException(status_code=404, detail="Song not found")

    if not file.filename.lower().endswith(".csv"):
        raise HTTPException(status_code=400, detail="Upload must be a CSV file")

    timestamp = datetime.utcnow().strftime("%Y%m%d_%H%M%S")
    file_path = settings.upload_dir / f"song_{song_id}_{timestamp}_{_safe_filename(file.filename)}"

    with file_path.open("wb") as target:
        shutil.copyfileobj(file.file, target)

    dataframe, warnings = parse_release_csv(file_path)
    if dataframe.empty:
        raise HTTPException(status_code=400, detail="No valid CSV rows were found after parsing")

    replace_song_metrics(song_id, dataframe_to_records(dataframe))
    save_upload_record(song_id=song_id, file_name=file.filename, file_path=str(file_path))

    release_date = date.fromisoformat(song["release_date"])
    try:
        summary, _, _ = build_summary(dataframe, release_date)
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc

    save_knowledge_doc(
        title=f"{song['title']} CSV Summary",
        source_path=str(file_path),
        content=_summary_to_text(song, summary),
    )

    logger.info("CSV uploaded: song_id=%s file=%s rows=%s", song_id, file.filename, len(dataframe))
    return {
        "song_id": song_id,
        "file": file.filename,
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
        report["pdf_url"] = _report_url(report["pdf_path"])
        report["report_label"] = report_label(int(report["report_day"]))
        report["summary"] = json.loads(report["summary_json"]) if report.get("summary_json") else {}
    return reports


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
