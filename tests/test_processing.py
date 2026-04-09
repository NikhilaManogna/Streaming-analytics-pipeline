from processing.spark_streaming import build_dead_letter_records, build_postgres_records


def test_build_postgres_records_flags_anomalies_and_rounds_values():
    rows = [
        {"timestamp": "2026-04-08 08:05:00", "total_revenue": 4999.994, "active_users": 10, "event_count": 12},
        {"timestamp": "2026-04-08 08:06:00", "total_revenue": 5001.117, "active_users": 20, "event_count": 22},
    ]

    records = build_postgres_records(rows, threshold=5000)

    assert records[0] == ("2026-04-08 08:05:00", 4999.99, 10, 12, False)
    assert records[1] == ("2026-04-08 08:06:00", 5001.12, 20, 22, True)


def test_build_dead_letter_records_preserves_reason_and_payload():
    rows = [
        {"kafka_timestamp": "2026-04-08 08:05:00", "raw_payload": "{bad json}", "invalid_reason": "invalid_timestamp"}
    ]

    records = build_dead_letter_records(rows)

    assert records == [("2026-04-08 08:05:00", "{bad json}", "invalid_timestamp")]