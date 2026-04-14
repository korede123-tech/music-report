from __future__ import annotations

from pathlib import Path

import matplotlib
import matplotlib.pyplot as plt
import pandas as pd


matplotlib.use("Agg")


PALETTE = {
    "background": "#070707",
    "axis": "#b7993f",
    "line_primary": "#d4af37",
    "line_secondary": "#f3d98b",
    "line_soft": "#d6bc6b",
    "grid": "#383838",
    "text": "#f6e7b0",
}


def _style_axis(axis: plt.Axes) -> None:
    axis.set_facecolor(PALETTE["background"])
    axis.tick_params(colors=PALETTE["text"])
    axis.grid(color=PALETTE["grid"], linestyle="--", alpha=0.45)
    for spine in axis.spines.values():
        spine.set_color(PALETTE["axis"])


def create_chart_images(
    song_id: int,
    report_day: int,
    timeline_df: pd.DataFrame,
    output_dir: Path,
) -> dict[str, str]:
    output_dir.mkdir(parents=True, exist_ok=True)
    dates = pd.to_datetime(timeline_df["date"])

    performance_path = output_dir / f"song_{song_id}_day_{report_day}_performance.png"
    engagement_path = output_dir / f"song_{song_id}_day_{report_day}_engagement.png"

    fig_a, axis_a = plt.subplots(figsize=(12.8, 7.2), facecolor=PALETTE["background"])
    _style_axis(axis_a)
    axis_a.plot(
        dates,
        timeline_df["cumulative_combined_total"],
        color=PALETTE["line_primary"],
        linewidth=2.8,
        label="Cumulative Streams",
    )
    axis_a.plot(
        dates,
        timeline_df["cumulative_spotify_listeners"],
        color=PALETTE["line_secondary"],
        linewidth=2.2,
        label="Cumulative Spotify Listeners",
    )
    axis_a.set_title("Streams + Listeners Momentum", color=PALETTE["text"], fontsize=18)
    axis_a.set_xlabel("Date", color=PALETTE["text"])
    axis_a.set_ylabel("Cumulative Count", color=PALETTE["text"])
    axis_a.legend(facecolor=PALETTE["background"], edgecolor=PALETTE["axis"], labelcolor=PALETTE["text"])
    fig_a.autofmt_xdate(rotation=25)
    fig_a.tight_layout()
    fig_a.savefig(performance_path, dpi=150, facecolor=fig_a.get_facecolor())
    plt.close(fig_a)

    fig_b, axis_b = plt.subplots(figsize=(12.8, 7.2), facecolor=PALETTE["background"])
    _style_axis(axis_b)

    axis_b.plot(
        dates,
        timeline_df["avg_replay_rate"],
        color=PALETTE["line_primary"],
        linewidth=2.4,
        label="Replay Rate (avg)",
    )
    axis_b.plot(
        dates,
        timeline_df["avg_save_rate"],
        color=PALETTE["line_secondary"],
        linewidth=2.1,
        label="Save Rate (avg)",
    )
    axis_b.set_xlabel("Date", color=PALETTE["text"])
    axis_b.set_ylabel("Rate", color=PALETTE["text"])

    axis_b2 = axis_b.twinx()
    _style_axis(axis_b2)
    axis_b2.plot(
        dates,
        timeline_df["cumulative_spotify_saves"],
        color="#f0c35a",
        linewidth=2.1,
        linestyle="-.",
        label="Cumulative Saves",
    )
    axis_b2.plot(
        dates,
        timeline_df["cumulative_spotify_skips"],
        color="#a98b34",
        linewidth=2.1,
        linestyle=":",
        label="Cumulative Skips",
    )
    axis_b2.set_ylabel("Cumulative Count", color=PALETTE["text"])

    lines = axis_b.get_lines() + axis_b2.get_lines()
    labels = [line.get_label() for line in lines]
    axis_b.legend(lines, labels, facecolor=PALETTE["background"], edgecolor=PALETTE["axis"], labelcolor=PALETTE["text"])
    axis_b.set_title("Engagement Quality", color=PALETTE["text"], fontsize=18)

    fig_b.autofmt_xdate(rotation=25)
    fig_b.tight_layout()
    fig_b.savefig(engagement_path, dpi=150, facecolor=fig_b.get_facecolor())
    plt.close(fig_b)

    return {
        "performance": str(performance_path),
        "engagement": str(engagement_path),
    }
