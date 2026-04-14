from __future__ import annotations

from datetime import date
from pathlib import Path
import sys


PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from reportlab.lib import colors
from reportlab.lib.units import inch
from reportlab.pdfgen import canvas

from app.config import settings
from app.database import (
    create_song_record,
    init_db,
    list_song_records,
    replace_song_metrics,
    save_knowledge_doc,
    save_report_record,
)
from app.services.charting import create_chart_images
from app.services.chatbot import extract_pdf_text
from app.services.csv_processing import build_summary, dataframe_to_records, parse_release_csv
from app.services.pdf_generation import generate_report_pdf
from app.services.scheduling import report_label


SAMPLE_SONGS = [
    {
        "title": "Neon Skyline",
        "spotify_link": "https://open.spotify.com/track/alpha-neon-skyline",
        "release_date": "2026-03-01",
        "csv_file": "sample_song_alpha.csv",
    },
    {
        "title": "Golden Hourline",
        "spotify_link": "https://open.spotify.com/track/beta-golden-hourline",
        "release_date": "2026-03-15",
        "csv_file": "sample_song_beta.csv",
    },
]


def _get_or_create_song(metadata: dict) -> dict:
    for song in list_song_records():
        if song["title"] == metadata["title"]:
            return song

    return create_song_record(
        title=metadata["title"],
        spotify_link=metadata["spotify_link"],
        release_date=date.fromisoformat(metadata["release_date"]),
    )


def _build_q1_pdf(output_path: Path) -> None:
    output_path.parent.mkdir(parents=True, exist_ok=True)
    pdf = canvas.Canvas(str(output_path), pagesize=(16 * inch, 9 * inch))
    width, height = 16 * inch, 9 * inch

    pdf.setFillColor(colors.HexColor("#080808"))
    pdf.rect(0, 0, width, height, stroke=0, fill=1)
    pdf.setStrokeColor(colors.HexColor("#D4AF37"))
    pdf.setLineWidth(2)
    pdf.rect(0.35 * inch, 0.35 * inch, width - 0.7 * inch, height - 0.7 * inch, stroke=1, fill=0)

    pdf.setFillColor(colors.HexColor("#F3D98B"))
    pdf.setFont("Helvetica-Bold", 34)
    pdf.drawString(0.9 * inch, height - 1.25 * inch, "Q1 Music Campaign Report")

    pdf.setFont("Helvetica", 14)
    pdf.setFillColor(colors.HexColor("#D4AF37"))
    pdf.drawString(0.9 * inch, height - 1.75 * inch, "Reference report for chatbot indexing and testing")

    lines = [
        "Highlights:",
        "- Total Q1 streams across active releases: 4,860,000",
        "- Spotify cumulative listeners: 1,320,000",
        "- Average replay rate across top campaigns: 0.36",
        "- Average save rate across top campaigns: 0.13",
        "- Best performing report checkpoint: Day 7",
        "- Highest save velocity week: Week 2 after release",
        "- Key recommendation: prioritize creator-led clips and playlist retargeting before Day 3",
    ]

    y = height - 2.5 * inch
    for line in lines:
        pdf.setFont("Helvetica", 16 if line == "Highlights:" else 13)
        pdf.setFillColor(colors.HexColor("#F5ECD0"))
        pdf.drawString(1.0 * inch, y, line)
        y -= 0.42 * inch

    pdf.showPage()
    pdf.save()


def main() -> None:
    settings.ensure_directories()
    init_db()

    generated_paths: list[str] = []

    for metadata in SAMPLE_SONGS:
        song = _get_or_create_song(metadata)
        csv_path = settings.sample_data_dir / metadata["csv_file"]
        dataframe, warnings = parse_release_csv(csv_path)
        replace_song_metrics(song["id"], dataframe_to_records(dataframe))

        summary, timeline_df, _ = build_summary(dataframe, date.fromisoformat(song["release_date"]))
        charts = create_chart_images(
            song_id=song["id"],
            report_day=7,
            timeline_df=timeline_df,
            output_dir=settings.chart_dir,
        )
        report_path = generate_report_pdf(
            song=song,
            report_day=7,
            report_name=report_label(7),
            summary=summary,
            chart_paths=charts,
            output_dir=settings.generated_reports_dir,
        )
        save_report_record(song["id"], report_day=7, pdf_path=str(report_path), summary=summary)
        save_knowledge_doc(
            title=f"{song['title']} Day 7 Summary",
            source_path=str(report_path),
            content=f"Song {song['title']} summary: {summary}",
        )
        generated_paths.append(str(report_path))

        if warnings:
            print(f"CSV warnings for {song['title']}: {warnings}")

    q1_pdf_path = settings.sample_data_dir / "Q1_Music_Campaign_Report.pdf"
    _build_q1_pdf(q1_pdf_path)
    q1_text = extract_pdf_text(q1_pdf_path)
    save_knowledge_doc(
        title="Q1 Music Campaign Report",
        source_path=str(q1_pdf_path),
        content=q1_text or "Q1 report was generated but extraction returned empty text.",
    )

    print("Demo data bootstrap complete.")
    print("Generated sample PDFs:")
    for path in generated_paths:
        print(f"- {path}")
    print(f"Reference report: {q1_pdf_path}")


if __name__ == "__main__":
    main()
