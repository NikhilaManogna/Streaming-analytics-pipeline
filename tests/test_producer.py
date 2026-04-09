from producer.producer import generate_event, generate_invalid_event


def test_generate_event_shape_and_values():
    event = generate_event()

    assert set(event) == {"event_id", "user_id", "event_type", "timestamp", "amount"}
    assert isinstance(event["user_id"], int)
    assert event["event_type"] in {"click", "order", "payment"}
    assert isinstance(event["timestamp"], str)

    if event["event_type"] == "click":
        assert event["amount"] == 0.0
    else:
        assert event["amount"] > 0



def test_generate_invalid_event_breaks_expected_rules():
    event = generate_invalid_event()

    assert set(event) == {"event_id", "user_id", "event_type", "timestamp", "amount"}
    assert (
        event["event_id"] is None
        or event["user_id"] is None
        or event["event_type"] not in {"click", "order", "payment"}
        or event["timestamp"] == "not-a-valid-timestamp"
        or event["amount"] < 0
    )
