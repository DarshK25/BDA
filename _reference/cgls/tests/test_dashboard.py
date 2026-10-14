"""Smoke-test the Streamlit page in DEMO mode (fake feed, no Kafka)."""
import os
import time

os.environ["CGLS_DEMO"] = "1"

from streamlit.testing.v1 import AppTest


def test_dashboard_renders_and_reroutes():
    at = AppTest.from_file("src/cgls/dashboard/app.py", default_timeout=30)
    at.run()
    deadline = time.time() + 15
    while time.time() < deadline and not at.metric:           # demo feed starts a moment after load
        time.sleep(1)
        at.run()
    assert not at.exception, at.exception
    labels = [m.label for m in at.metric]
    assert "Roads reporting" in labels and "Data age" in labels
    assert at.metric[0].value == "19/19"
