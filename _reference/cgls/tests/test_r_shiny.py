"""Shiny server logic, tested headless with shiny::testServer (skipped when R is not installed)."""
import os
import shutil
import subprocess

import pandas as pd
import pytest

pytestmark = pytest.mark.skipif(not shutil.which("Rscript"), reason="R not installed")

R_TEST = r'''
library(shiny)
app <- source("r/app.R")$value
testServer(app, {
  session$setInputs(minutes = 15, refresh = 5, segment = "Santacruz-Bandra")
  stopifnot(nzchar(output$n_heavy))
  for (o in c("heat", "trend", "weather")) stopifnot(!is.null(output[[o]]$src))
  cat("SHINY_OK\n")
})
'''


def test_shiny_server_renders_plots(tmp_path):
    import random

    from cgls.network import SEGMENTS
    rng = random.Random(1)
    rows, now = [], 1_800_000_000_000
    for k in range(40):
        for s in SEGMENTS:
            sc = rng.uniform(0.2, 0.9)
            rows.append(dict(segment_id=s["segment_id"], name=s["name"], window_start_ms=now + k * 5000 - 30000,
                             window_end_ms=now + k * 5000, avg_speed_kmph=30.0, free_flow_kmph=s["free_flow_kmph"],
                             speed_ratio=0.6, occupancy_pct=50.0, veh_per_lane=6.0, rain_mm_hr=rng.choice([0.0, 5.0, 12.0]),
                             visibility_km=9.0, gps_pings=50, vehicles=20, level=int(sc * 4), confidence=0.9, score=sc,
                             forecast_score=sc, forecast_level=int(sc * 4), level_name=["FREE", "MODERATE", "HEAVY", "SEVERE"][min(3, int(sc * 4))],
                             forecast_level_name="MODERATE", est_speed_kmph=30.0, incident_suspected=False,
                             forecast_source="gbt", emitted_ms=now + k * 5000 + 9000, batch_id=k, dt="2027-01-15"))
    os.makedirs(tmp_path / "export")
    pd.DataFrame(rows).to_csv(tmp_path / "export" / "congestion.csv", index=False)
    r = subprocess.run(["Rscript", "-e", R_TEST], env=dict(os.environ, CGLS_DATA_DIR=str(tmp_path)),
                       capture_output=True, text=True, timeout=300)
    assert "SHINY_OK" in r.stdout, r.stdout[-1500:] + r.stderr[-1500:]
