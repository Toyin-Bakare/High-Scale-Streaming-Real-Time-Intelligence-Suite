from datetime import datetime, timezone
from aggregator.windowing import window_bounds

def test_window_bounds_tumbling():
    ts = datetime(2026, 2, 7, 12, 0, 59, tzinfo=timezone.utc)
    ws, we = window_bounds(ts, 60)
    assert ws.isoformat() == "2026-02-07T12:00:00+00:00"
    assert we.isoformat() == "2026-02-07T12:01:00+00:00"
