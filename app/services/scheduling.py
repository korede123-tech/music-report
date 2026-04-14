from __future__ import annotations

from datetime import date, timedelta


CHECKPOINTS = [1, 3, 7, 14, 21, 30]
LABELS = {
    1: "24-Hour Report",
    3: "3-Day Report",
    7: "7-Day Report",
    14: "14-Day Report",
    21: "21-Day Report",
    30: "30-Day Report",
}


def report_label(report_day: int) -> str:
    return LABELS.get(report_day, f"Day {report_day} Report")


def determine_next_report(release_date: date, generated_days: list[int] | tuple[int, ...]) -> dict:
    today = date.today()
    song_age_days = (today - release_date).days + 1
    generated = {int(day) for day in generated_days}

    if song_age_days < 1:
        next_day = CHECKPOINTS[0]
        due_date = release_date + timedelta(days=next_day - 1)
        return {
            "status": "upcoming",
            "song_age_days": song_age_days,
            "report_day": next_day,
            "label": report_label(next_day),
            "due_date": due_date.isoformat(),
        }

    due_now = [day for day in CHECKPOINTS if day <= song_age_days and day not in generated]
    if due_now:
        target_day = due_now[0]
        due_date = release_date + timedelta(days=target_day - 1)
        return {
            "status": "due_now",
            "song_age_days": song_age_days,
            "report_day": target_day,
            "label": report_label(target_day),
            "due_date": due_date.isoformat(),
        }

    upcoming = [day for day in CHECKPOINTS if day > song_age_days and day not in generated]
    if upcoming:
        target_day = upcoming[0]
        due_date = release_date + timedelta(days=target_day - 1)
        return {
            "status": "upcoming",
            "song_age_days": song_age_days,
            "report_day": target_day,
            "label": report_label(target_day),
            "due_date": due_date.isoformat(),
        }

    return {
        "status": "complete",
        "song_age_days": song_age_days,
        "report_day": None,
        "label": "All scheduled reports completed",
        "due_date": None,
    }
