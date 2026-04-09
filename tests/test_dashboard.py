import pandas as pd

from dashboard.app import format_duration, prepare_display_metrics, summarize_dead_letters, truncate_payload


def test_prepare_display_metrics_fills_missing_minutes():
    frame = pd.DataFrame(
        [
            {"timestamp": pd.Timestamp("2026-04-08 08:05:00"), "total_revenue": 100.0, "active_users": 10, "event_count": 5, "anomaly_detected": False},
            {"timestamp": pd.Timestamp("2026-04-08 08:07:00"), "total_revenue": 300.0, "active_users": 25, "event_count": 9, "anomaly_detected": True},
        ]
    )

    prepared = prepare_display_metrics(frame, points=10)

    assert len(prepared) == 3
    assert prepared["timestamp"].dt.strftime("%H:%M").tolist() == ["08:05", "08:06", "08:07"]
    assert prepared.loc[1, "total_revenue"] == 0.0
    assert prepared.loc[1, "active_users"] == 0
    assert prepared.loc[1, "event_count"] == 0



def test_summarize_dead_letters_groups_by_reason():
    frame = pd.DataFrame(
        [
            {"invalid_reason": "invalid_timestamp"},
            {"invalid_reason": "invalid_timestamp"},
            {"invalid_reason": "negative_amount"},
        ]
    )

    summary = summarize_dead_letters(frame)

    assert summary.to_dict("records") == [
        {"invalid_reason": "invalid_timestamp", "invalid_count": 2},
        {"invalid_reason": "negative_amount", "invalid_count": 1},
    ]



def test_format_duration_and_truncate_payload():
    assert format_duration(45) == "45s"
    assert format_duration(125) == "2m 5s"
    assert format_duration(3725) == "1h 2m"
    assert truncate_payload("abcdef", limit=10) == "abcdef"
    assert truncate_payload("abcdefghijklmnopqrstuvwxyz", limit=10) == "abcdefg..."
