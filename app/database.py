from __future__ import annotations

from datetime import date, datetime
import json
import re
import sqlite3
from typing import Any

from app.config import settings


def _connect() -> sqlite3.Connection:
    settings.ensure_directories()
    connection = sqlite3.connect(settings.db_path)
    connection.row_factory = sqlite3.Row
    connection.execute("PRAGMA foreign_keys = ON")
    return connection


def _row_to_dict(row: sqlite3.Row | None) -> dict[str, Any] | None:
    if row is None:
        return None
    return {key: row[key] for key in row.keys()}


def init_db() -> None:
    with _connect() as connection:
        connection.executescript(
            """
            CREATE TABLE IF NOT EXISTS songs (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                title TEXT NOT NULL,
                spotify_link TEXT NOT NULL,
                release_date TEXT NOT NULL,
                created_at TEXT NOT NULL
            );

            CREATE TABLE IF NOT EXISTS uploads (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                song_id INTEGER NOT NULL,
                file_name TEXT NOT NULL,
                file_path TEXT NOT NULL,
                uploaded_at TEXT NOT NULL,
                FOREIGN KEY(song_id) REFERENCES songs(id) ON DELETE CASCADE
            );

            CREATE TABLE IF NOT EXISTS daily_metrics (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                song_id INTEGER NOT NULL,
                date TEXT NOT NULL,
                spotify_streams REAL NOT NULL,
                spotify_listeners REAL NOT NULL,
                spotify_replay_rate REAL NOT NULL,
                spotify_saves REAL NOT NULL,
                save_rate REAL NOT NULL,
                spotify_skips REAL NOT NULL,
                apple_music_streams REAL NOT NULL,
                number_of_shazams REAL NOT NULL,
                combined_total REAL NOT NULL,
                FOREIGN KEY(song_id) REFERENCES songs(id) ON DELETE CASCADE,
                UNIQUE(song_id, date)
            );

            CREATE TABLE IF NOT EXISTS reports (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                song_id INTEGER NOT NULL,
                report_day INTEGER NOT NULL,
                pdf_path TEXT NOT NULL,
                summary_json TEXT,
                generated_at TEXT NOT NULL,
                FOREIGN KEY(song_id) REFERENCES songs(id) ON DELETE CASCADE
            );

            CREATE TABLE IF NOT EXISTS knowledge_docs (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                title TEXT NOT NULL,
                source_path TEXT NOT NULL,
                content TEXT NOT NULL,
                created_at TEXT NOT NULL
            );

            CREATE TABLE IF NOT EXISTS chat_logs (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                question TEXT NOT NULL,
                answer TEXT NOT NULL,
                created_at TEXT NOT NULL
            );

            CREATE INDEX IF NOT EXISTS idx_daily_metrics_song_id ON daily_metrics(song_id);
            CREATE INDEX IF NOT EXISTS idx_reports_song_id ON reports(song_id);
            CREATE INDEX IF NOT EXISTS idx_reports_day ON reports(report_day);
            CREATE INDEX IF NOT EXISTS idx_knowledge_docs_created ON knowledge_docs(created_at);
            """
        )


def create_song_record(title: str, spotify_link: str, release_date: date) -> dict[str, Any]:
    created_at = datetime.utcnow().isoformat()
    with _connect() as connection:
        cursor = connection.execute(
            """
            INSERT INTO songs (title, spotify_link, release_date, created_at)
            VALUES (?, ?, ?, ?)
            """,
            (title, spotify_link, release_date.isoformat(), created_at),
        )
        song_id = cursor.lastrowid
        row = connection.execute("SELECT * FROM songs WHERE id = ?", (song_id,)).fetchone()
    return _row_to_dict(row) or {}


def get_song_record(song_id: int) -> dict[str, Any] | None:
    with _connect() as connection:
        row = connection.execute("SELECT * FROM songs WHERE id = ?", (song_id,)).fetchone()
    return _row_to_dict(row)


def list_song_records() -> list[dict[str, Any]]:
    with _connect() as connection:
        rows = connection.execute("SELECT * FROM songs ORDER BY created_at DESC").fetchall()
    return [{key: row[key] for key in row.keys()} for row in rows]


def save_upload_record(song_id: int, file_name: str, file_path: str) -> dict[str, Any]:
    uploaded_at = datetime.utcnow().isoformat()
    with _connect() as connection:
        cursor = connection.execute(
            """
            INSERT INTO uploads (song_id, file_name, file_path, uploaded_at)
            VALUES (?, ?, ?, ?)
            """,
            (song_id, file_name, file_path, uploaded_at),
        )
        upload_id = cursor.lastrowid
        row = connection.execute("SELECT * FROM uploads WHERE id = ?", (upload_id,)).fetchone()
    return _row_to_dict(row) or {}


def replace_song_metrics(song_id: int, records: list[dict[str, Any]]) -> None:
    with _connect() as connection:
        connection.execute("DELETE FROM daily_metrics WHERE song_id = ?", (song_id,))
        if not records:
            return

        values = [
            (
                song_id,
                record["date"],
                float(record["spotify_streams"]),
                float(record["spotify_listeners"]),
                float(record["spotify_replay_rate"]),
                float(record["spotify_saves"]),
                float(record["save_rate"]),
                float(record["spotify_skips"]),
                float(record["apple_music_streams"]),
                float(record["number_of_shazams"]),
                float(record["combined_total"]),
            )
            for record in records
        ]
        connection.executemany(
            """
            INSERT INTO daily_metrics (
                song_id,
                date,
                spotify_streams,
                spotify_listeners,
                spotify_replay_rate,
                spotify_saves,
                save_rate,
                spotify_skips,
                apple_music_streams,
                number_of_shazams,
                combined_total
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            values,
        )


def fetch_song_metrics(song_id: int) -> list[dict[str, Any]]:
    with _connect() as connection:
        rows = connection.execute(
            "SELECT * FROM daily_metrics WHERE song_id = ? ORDER BY date",
            (song_id,),
        ).fetchall()
    return [{key: row[key] for key in row.keys()} for row in rows]


def save_report_record(song_id: int, report_day: int, pdf_path: str, summary: dict[str, Any]) -> dict[str, Any]:
    generated_at = datetime.utcnow().isoformat()
    with _connect() as connection:
        cursor = connection.execute(
            """
            INSERT INTO reports (song_id, report_day, pdf_path, summary_json, generated_at)
            VALUES (?, ?, ?, ?, ?)
            """,
            (song_id, report_day, pdf_path, json.dumps(summary), generated_at),
        )
        report_id = cursor.lastrowid
        row = connection.execute("SELECT * FROM reports WHERE id = ?", (report_id,)).fetchone()
    return _row_to_dict(row) or {}


def list_generated_report_days(song_id: int) -> list[int]:
    with _connect() as connection:
        rows = connection.execute(
            "SELECT DISTINCT report_day FROM reports WHERE song_id = ? ORDER BY report_day",
            (song_id,),
        ).fetchall()
    return [int(row["report_day"]) for row in rows]


def list_report_records(song_id: int | None = None) -> list[dict[str, Any]]:
    with _connect() as connection:
        if song_id is None:
            rows = connection.execute(
                """
                SELECT r.*, s.title AS song_title
                FROM reports r
                INNER JOIN songs s ON s.id = r.song_id
                ORDER BY r.generated_at DESC
                """
            ).fetchall()
        else:
            rows = connection.execute(
                """
                SELECT r.*, s.title AS song_title
                FROM reports r
                INNER JOIN songs s ON s.id = r.song_id
                WHERE r.song_id = ?
                ORDER BY r.generated_at DESC
                """,
                (song_id,),
            ).fetchall()
    return [{key: row[key] for key in row.keys()} for row in rows]


def save_knowledge_doc(title: str, source_path: str, content: str) -> int:
    created_at = datetime.utcnow().isoformat()
    with _connect() as connection:
        cursor = connection.execute(
            """
            INSERT INTO knowledge_docs (title, source_path, content, created_at)
            VALUES (?, ?, ?, ?)
            """,
            (title, source_path, content, created_at),
        )
        doc_id = cursor.lastrowid
    return int(doc_id)


def _list_recent_knowledge_docs(limit: int = 50) -> list[dict[str, Any]]:
    with _connect() as connection:
        rows = connection.execute(
            "SELECT * FROM knowledge_docs ORDER BY created_at DESC LIMIT ?",
            (limit,),
        ).fetchall()
    return [{key: row[key] for key in row.keys()} for row in rows]


def search_knowledge_docs(question: str, limit: int = 5) -> list[dict[str, Any]]:
    terms = [term.lower() for term in re.findall(r"[a-zA-Z0-9]+", question) if len(term) > 2]
    docs = _list_recent_knowledge_docs(limit=80)
    if not docs:
        return []

    ranked_docs: list[dict[str, Any]] = []
    for doc in docs:
        haystack = f"{doc['title']} {doc['content'][:10000]}".lower()
        score = 0
        for term in terms:
            score += haystack.count(term)
        cloned = dict(doc)
        cloned["score"] = score
        ranked_docs.append(cloned)

    ranked_docs.sort(key=lambda item: (item.get("score", 0), item.get("created_at", "")), reverse=True)
    matching = [doc for doc in ranked_docs if doc.get("score", 0) > 0]
    if matching:
        return matching[:limit]
    return ranked_docs[:limit]


def save_chat_log(question: str, answer: str) -> int:
    created_at = datetime.utcnow().isoformat()
    with _connect() as connection:
        cursor = connection.execute(
            """
            INSERT INTO chat_logs (question, answer, created_at)
            VALUES (?, ?, ?)
            """,
            (question, answer, created_at),
        )
        chat_id = cursor.lastrowid
    return int(chat_id)
