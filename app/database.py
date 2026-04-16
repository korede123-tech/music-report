from __future__ import annotations

from datetime import date, datetime
import difflib
import json
import re
import sqlite3
from typing import Any

from app.config import settings


SEARCH_STOPWORDS = {
    "the",
    "and",
    "for",
    "with",
    "that",
    "this",
    "what",
    "when",
    "where",
    "which",
    "how",
    "from",
    "into",
    "about",
    "your",
    "their",
    "data",
    "report",
    "reports",
}


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
                artist_name TEXT,
                spotify_track_id TEXT,
                isrc TEXT,
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

            CREATE TABLE IF NOT EXISTS chart_appearances (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                song_id INTEGER NOT NULL,
                chart_alias TEXT NOT NULL,
                chart_date TEXT NOT NULL,
                chart_recurrence TEXT,
                chart_type TEXT,
                chart_name TEXT,
                rank INTEGER,
                previous_rank INTEGER,
                peak_rank INTEGER,
                appearances_on_chart INTEGER,
                consecutive_appearances INTEGER,
                track_uri TEXT,
                track_name TEXT,
                artist_names TEXT,
                source_labels TEXT,
                source_url TEXT,
                raw_entry_json TEXT,
                synced_at TEXT NOT NULL,
                FOREIGN KEY(song_id) REFERENCES songs(id) ON DELETE CASCADE,
                UNIQUE(song_id, chart_alias, chart_date)
            );

            CREATE TABLE IF NOT EXISTS chartmetric_profiles (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                song_id INTEGER NOT NULL,
                keyword TEXT NOT NULL,
                metrics_json TEXT NOT NULL,
                raw_json TEXT NOT NULL,
                fetched_at TEXT NOT NULL,
                FOREIGN KEY(song_id) REFERENCES songs(id) ON DELETE CASCADE,
                UNIQUE(song_id)
            );

            CREATE INDEX IF NOT EXISTS idx_daily_metrics_song_id ON daily_metrics(song_id);
            CREATE INDEX IF NOT EXISTS idx_reports_song_id ON reports(song_id);
            CREATE INDEX IF NOT EXISTS idx_reports_day ON reports(report_day);
            CREATE INDEX IF NOT EXISTS idx_knowledge_docs_created ON knowledge_docs(created_at);
            CREATE INDEX IF NOT EXISTS idx_chart_appearances_song_id ON chart_appearances(song_id);
            CREATE INDEX IF NOT EXISTS idx_chart_appearances_date ON chart_appearances(chart_date);
            CREATE INDEX IF NOT EXISTS idx_chartmetric_profiles_song_id ON chartmetric_profiles(song_id);
            """
        )

        # Lightweight migration path for older local DB files.
        song_columns = {
            row["name"]
            for row in connection.execute("PRAGMA table_info(songs)").fetchall()
        }
        if "artist_name" not in song_columns:
            connection.execute("ALTER TABLE songs ADD COLUMN artist_name TEXT")
        if "spotify_track_id" not in song_columns:
            connection.execute("ALTER TABLE songs ADD COLUMN spotify_track_id TEXT")
        if "isrc" not in song_columns:
            connection.execute("ALTER TABLE songs ADD COLUMN isrc TEXT")

        connection.execute(
            "CREATE INDEX IF NOT EXISTS idx_songs_spotify_track_id ON songs(spotify_track_id)"
        )


def create_song_record(
    title: str,
    spotify_link: str,
    release_date: date,
    artist_name: str | None = None,
    spotify_track_id: str | None = None,
    isrc: str | None = None,
) -> dict[str, Any]:
    created_at = datetime.utcnow().isoformat()
    with _connect() as connection:
        cursor = connection.execute(
            """
            INSERT INTO songs (
                title,
                spotify_link,
                release_date,
                artist_name,
                spotify_track_id,
                isrc,
                created_at
            )
            VALUES (?, ?, ?, ?, ?, ?, ?)
            """,
            (
                title,
                spotify_link,
                release_date.isoformat(),
                artist_name,
                spotify_track_id,
                isrc,
                created_at,
            ),
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


def delete_song_record(song_id: int) -> bool:
    with _connect() as connection:
        cursor = connection.execute("DELETE FROM songs WHERE id = ?", (song_id,))
        return int(cursor.rowcount) > 0


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


def save_chart_appearances(
    song_id: int,
    appearances: list[dict[str, Any]],
    replace_aliases: list[str] | None = None,
) -> None:
    synced_at = datetime.utcnow().isoformat()

    with _connect() as connection:
        if replace_aliases:
            placeholders = ",".join("?" for _ in replace_aliases)
            connection.execute(
                f"DELETE FROM chart_appearances WHERE song_id = ? AND chart_alias IN ({placeholders})",
                (song_id, *replace_aliases),
            )

        if not appearances:
            return

        for appearance in appearances:
            connection.execute(
                """
                INSERT INTO chart_appearances (
                    song_id,
                    chart_alias,
                    chart_date,
                    chart_recurrence,
                    chart_type,
                    chart_name,
                    rank,
                    previous_rank,
                    peak_rank,
                    appearances_on_chart,
                    consecutive_appearances,
                    track_uri,
                    track_name,
                    artist_names,
                    source_labels,
                    source_url,
                    raw_entry_json,
                    synced_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(song_id, chart_alias, chart_date)
                DO UPDATE SET
                    chart_recurrence = excluded.chart_recurrence,
                    chart_type = excluded.chart_type,
                    chart_name = excluded.chart_name,
                    rank = excluded.rank,
                    previous_rank = excluded.previous_rank,
                    peak_rank = excluded.peak_rank,
                    appearances_on_chart = excluded.appearances_on_chart,
                    consecutive_appearances = excluded.consecutive_appearances,
                    track_uri = excluded.track_uri,
                    track_name = excluded.track_name,
                    artist_names = excluded.artist_names,
                    source_labels = excluded.source_labels,
                    source_url = excluded.source_url,
                    raw_entry_json = excluded.raw_entry_json,
                    synced_at = excluded.synced_at
                """,
                (
                    song_id,
                    appearance.get("chart_alias"),
                    appearance.get("chart_date"),
                    appearance.get("chart_recurrence"),
                    appearance.get("chart_type"),
                    appearance.get("chart_name"),
                    appearance.get("rank"),
                    appearance.get("previous_rank"),
                    appearance.get("peak_rank"),
                    appearance.get("appearances_on_chart"),
                    appearance.get("consecutive_appearances"),
                    appearance.get("track_uri"),
                    appearance.get("track_name"),
                    appearance.get("artist_names"),
                    appearance.get("source_labels"),
                    appearance.get("source_url"),
                    appearance.get("raw_entry_json"),
                    synced_at,
                ),
            )


def list_chart_appearances(song_id: int | None = None) -> list[dict[str, Any]]:
    with _connect() as connection:
        if song_id is None:
            rows = connection.execute(
                """
                SELECT c.*, s.title AS song_title
                FROM chart_appearances c
                INNER JOIN songs s ON s.id = c.song_id
                ORDER BY c.chart_date DESC, c.rank ASC, c.song_id ASC
                """
            ).fetchall()
        else:
            rows = connection.execute(
                """
                SELECT c.*, s.title AS song_title
                FROM chart_appearances c
                INNER JOIN songs s ON s.id = c.song_id
                WHERE c.song_id = ?
                ORDER BY c.chart_date DESC, c.rank ASC, c.song_id ASC
                """,
                (song_id,),
            ).fetchall()

    return [{key: row[key] for key in row.keys()} for row in rows]


def save_chartmetric_profile(
    song_id: int,
    keyword: str,
    metrics: dict[str, Any],
    raw_item: dict[str, Any],
) -> None:
    fetched_at = datetime.utcnow().isoformat()

    with _connect() as connection:
        connection.execute(
            """
            INSERT INTO chartmetric_profiles (
                song_id,
                keyword,
                metrics_json,
                raw_json,
                fetched_at
            ) VALUES (?, ?, ?, ?, ?)
            ON CONFLICT(song_id)
            DO UPDATE SET
                keyword = excluded.keyword,
                metrics_json = excluded.metrics_json,
                raw_json = excluded.raw_json,
                fetched_at = excluded.fetched_at
            """,
            (
                song_id,
                keyword,
                json.dumps(metrics),
                json.dumps(raw_item),
                fetched_at,
            ),
        )


def list_chartmetric_profiles(song_id: int | None = None) -> list[dict[str, Any]]:
    with _connect() as connection:
        if song_id is None:
            rows = connection.execute(
                """
                SELECT c.*, s.title AS song_title
                FROM chartmetric_profiles c
                INNER JOIN songs s ON s.id = c.song_id
                ORDER BY c.fetched_at DESC
                """
            ).fetchall()
        else:
            rows = connection.execute(
                """
                SELECT c.*, s.title AS song_title
                FROM chartmetric_profiles c
                INNER JOIN songs s ON s.id = c.song_id
                WHERE c.song_id = ?
                ORDER BY c.fetched_at DESC
                """,
                (song_id,),
            ).fetchall()

    items = [{key: row[key] for key in row.keys()} for row in rows]
    for item in items:
        item["metrics"] = json.loads(item.get("metrics_json") or "{}")
        item["raw"] = json.loads(item.get("raw_json") or "{}")
    return items


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


def list_knowledge_docs_for_song(song_title: str, limit: int = 5) -> list[dict[str, Any]]:
    title = (song_title or "").strip()
    if not title:
        return []

    pattern = f"%{title.lower()}%"
    with _connect() as connection:
        rows = connection.execute(
            """
            SELECT *
            FROM knowledge_docs
            WHERE lower(title) LIKE ?
            ORDER BY created_at DESC
            LIMIT ?
            """,
            (pattern, limit),
        ).fetchall()
    return [{key: row[key] for key in row.keys()} for row in rows]


def _search_terms(question: str) -> list[str]:
    tokens = [term.lower() for term in re.findall(r"[a-zA-Z0-9]+", question)]
    filtered = [term for term in tokens if len(term) > 2 and term not in SEARCH_STOPWORDS]
    if filtered:
        return filtered
    return [term for term in tokens if len(term) > 2]


def search_knowledge_docs(question: str, limit: int = 5) -> list[dict[str, Any]]:
    terms = _search_terms(question)
    docs = _list_recent_knowledge_docs(limit=80)
    if not docs:
        return []

    ranked_docs: list[dict[str, Any]] = []
    for doc in docs:
        haystack = f"{doc['title']} {doc['content'][:10000]}".lower()
        title_text = str(doc.get("title") or "").lower()
        haystack_tokens = set(re.findall(r"[a-zA-Z0-9]+", haystack))
        score = 0.0

        for term in terms:
            exact_hits = haystack.count(term)
            if exact_hits > 0:
                score += float(exact_hits * 2)
                if term in title_text:
                    score += 3.0
                continue

            close = difflib.get_close_matches(term, haystack_tokens, n=1, cutoff=0.84)
            if close:
                score += 0.75
                if close[0] in title_text:
                    score += 0.5

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
