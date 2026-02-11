from datetime import datetime, timezone, timedelta
from aggregator.state import WindowState

def test_state_updates_and_snapshot():
    st = WindowState()
    ws = datetime(2026,2,7,12,0,0,tzinfo=timezone.utc)
    we = ws + timedelta(seconds=60)

    st.update("requests", ws, we, {"customer_id":"c1"}, value=1.0, user_id="u1")
    st.update("requests", ws, we, {"customer_id":"c1"}, value=1.0, user_id="u2")
    st.update("requests", ws, we, {"customer_id":"c1"}, value=1.0, user_id="u2")

    ready = st.snapshot_ready(now=we + timedelta(seconds=1))
    assert len(ready) == 1
    metric, _, _, dims, cell = ready[0]
    assert metric == "requests"
    assert cell.count == 3
    assert len(cell.unique_users) == 2
