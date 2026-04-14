# Music Release Reporter (Local)

Local Python web app that:

- Ingests song campaign CSVs
- Calculates release reporting metrics and due schedules
- Generates widescreen PDF slides in a gold/black visual theme
- Lets you ask Cohere-powered questions across uploaded CSV/PDF report content

The app is fully local and runs at http://localhost:8000.

## Stack

- Python 3.11
- FastAPI backend + SQLite
- React dashboard (served by FastAPI)
- pandas for CSV parsing and calculations
- matplotlib for charting
- reportlab for PDF slides
- Cohere API for report Q&A

## Project Layout

- `main.py`: FastAPI app and endpoints
- `app/database.py`: SQLite schema and persistence helpers
- `app/services/csv_processing.py`: CSV validation, parsing, and cumulative metrics
- `app/services/scheduling.py`: next-due report checkpoint logic
- `app/services/charting.py`: chart image rendering
- `app/services/pdf_generation.py`: PDF slide generation
- `app/services/chatbot.py`: Cohere chat integration + PDF text extraction
- `templates/index.html`: React dashboard
- `static/styles.css`: dashboard styling
- `sample_data/`: CSV test files and Q1 sample reference report

## CSV Columns Expected

`date, spotify_streams, spotify_listeners, spotify_replay_rate, spotify_saves, save_rate, spotify_skips, apple_music_streams, number_of_shazams`

If columns are missing or contain invalid values:

- Missing columns are auto-filled with 0
- Invalid numbers are coerced to 0
- Invalid dates are dropped
- Parsing warnings are returned in upload responses

## Business Rules Implemented

1. Daily combined totals = Spotify streams + Apple Music streams + Shazams
2. Day 1 cumulative (24h) includes release day plus previous day if present
3. Cumulative stream checkpoints: 24h, 3d, 7d, 14d, 21d, 30d
4. Next report due is auto-calculated from song age and already generated checkpoints

## Setup

Use Python 3.11:

```bash
python3.11 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
cp .env.example .env
```

Add your Cohere key into `.env`:

```env
COHERE_API_KEY=your_key_here
COHERE_MODEL=command-a-03-2025
```

Run the app:

```bash
uvicorn main:app --reload
```

Open:

- http://localhost:8000

## Deploy to Vercel

The app is Vercel-ready with:

- [api/index.py](api/index.py) as the serverless entrypoint
- [vercel.json](vercel.json) routing all paths to FastAPI
- Runtime set to Python 3.11
- Writable runtime storage redirected to `/tmp/music_report` on Vercel

### Vercel steps

1. Push this repository to GitHub.
2. In Vercel, import the GitHub repo.
3. Set Environment Variables in Vercel project settings:
	- `COHERE_API_KEY`
	- `COHERE_MODEL` (recommended: `command-a-03-2025`)
4. Deploy.

Notes:

- Vercel serverless file storage is ephemeral. Generated PDFs/DB data are not persistent between deployments or cold starts.
- For persistent production storage, move SQLite/PDF assets to managed storage (for example Postgres + object storage).

## Optional: Bootstrap Demo Data + Sample PDFs

This loads sample songs, ingests sample CSVs, generates sample song PDFs, and creates a Q1 sample report for chatbot testing:

```bash
python scripts/bootstrap_demo.py
```

Outputs:

- Sample song PDFs in `reports/generated/`
- Chart images in `reports/charts/`
- SQLite DB in `data/music_reports.db`
- Q1 reference PDF in `sample_data/Q1_Music_Campaign_Report.pdf`

## Dashboard Workflow

1. Add a song (title, Spotify link, release date)
2. Upload that song's CSV
3. Generate the due report PDF
4. Upload Q1 or legacy PDF into knowledge index
5. Ask questions in the chat panel

## API Endpoints

- `GET /health`
- `POST /api/songs`
- `GET /api/songs`
- `POST /api/songs/{song_id}/upload-csv`
- `GET /api/songs/{song_id}/metrics`
- `POST /api/songs/{song_id}/generate-report`
- `GET /api/reports`
- `POST /api/reports/upload-pdf`
- `POST /api/chat`

## Local Logs

All application actions are logged to:

- `logs/app.log`

## Cohere Notes

- The app uses Cohere chat endpoints for report Q&A and falls back to local indexed snippets if Cohere is unavailable.
- The default model is `command-a-03-2025` (live). Older aliases like `command-r-plus` are deprecated and return errors.
- If you receive a 401/403, verify your API key and account access.
- If you receive model deprecation errors, set `COHERE_MODEL` in `.env` to a live model (for example `command-a-03-2025` or `command-r7b-12-2024`).
- You can still test report Q&A behavior offline because uploaded CSV/PDF content is indexed locally.
