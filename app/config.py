from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
import os

from dotenv import load_dotenv


load_dotenv()


def _is_vercel_runtime() -> bool:
    return bool(os.getenv("VERCEL") or os.getenv("VERCEL_ENV"))


def _runtime_root(base_dir: Path) -> Path:
    if _is_vercel_runtime():
        return Path("/tmp/music_report")
    return base_dir

@dataclass(frozen=True)
class Settings:
    base_dir: Path = Path(__file__).resolve().parent.parent
    runtime_root: Path = Path()
    data_dir: Path = Path()
    upload_dir: Path = Path()
    reports_dir: Path = Path()
    generated_reports_dir: Path = Path()
    chart_dir: Path = Path()
    knowledge_dir: Path = Path()
    log_dir: Path = Path()
    static_dir: Path = base_dir / "static"
    templates_dir: Path = base_dir / "templates"
    sample_data_dir: Path = base_dir / "sample_data"
    db_path: Path = Path()

    cohere_api_key: str = os.getenv("COHERE_API_KEY", "")
    cohere_model: str = os.getenv("COHERE_MODEL", "command-a-03-2025")
    spotify_client_id: str = os.getenv("SPOTIFY_CLIENT_ID", "")
    spotify_client_secret: str = os.getenv("SPOTIFY_CLIENT_SECRET", "")
    spotify_charts_access_token: str = os.getenv("SPOTIFY_CHARTS_ACCESS_TOKEN", "")
    spotify_charts_app_version: str = os.getenv("SPOTIFY_CHARTS_APP_VERSION", "0.0.0.production")
    apify_api_token: str = os.getenv("APIFY_API_TOKEN", "")
    apify_chartmetric_actor: str = os.getenv("APIFY_CHARTMETRIC_ACTOR", "canadesk~chartmetric")
    apify_user_id: str = os.getenv("APIFY_USER_ID", "")

    def __post_init__(self) -> None:
        runtime_root = _runtime_root(self.base_dir)
        object.__setattr__(self, "runtime_root", runtime_root)
        object.__setattr__(self, "data_dir", runtime_root / "data")
        object.__setattr__(self, "upload_dir", runtime_root / "data" / "uploads")
        object.__setattr__(self, "reports_dir", runtime_root / "reports")
        object.__setattr__(self, "generated_reports_dir", runtime_root / "reports" / "generated")
        object.__setattr__(self, "chart_dir", runtime_root / "reports" / "charts")
        object.__setattr__(self, "knowledge_dir", runtime_root / "data" / "knowledge")
        object.__setattr__(self, "log_dir", runtime_root / "logs")
        object.__setattr__(self, "db_path", runtime_root / "data" / "music_reports.db")

    def ensure_directories(self) -> None:
        for directory in (
            self.data_dir,
            self.upload_dir,
            self.reports_dir,
            self.generated_reports_dir,
            self.chart_dir,
            self.knowledge_dir,
            self.log_dir,
        ):
            directory.mkdir(parents=True, exist_ok=True)


settings = Settings()
