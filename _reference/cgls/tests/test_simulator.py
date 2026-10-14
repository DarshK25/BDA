import glob
import json
import os

from cgls import config
from cgls.simulator import producer


def test_simulator_writes_all_three_streams(tmp_path, monkeypatch):
    monkeypatch.setattr(config, "GROUND_TRUTH_DIR", str(tmp_path / "gt"))
    rc = producer.main(["--sink", "files", "--out-dir", str(tmp_path / "ev"), "--duration", "6",
                        "--vehicles", "50", "--dirty-rate", "0.2", "--no-events"])
    assert rc == 0
    counts = {}
    for t in config.INPUT_TOPICS:
        files = glob.glob(str(tmp_path / "ev" / t / "part-*.json"))
        assert files, f"no files for {t}"
        counts[t] = sum(1 for f in files for _ in open(f))
    assert counts[config.TOPIC_GPS] > 40 and counts[config.TOPIC_SENSOR] >= 19
    bad = 0
    for f in glob.glob(str(tmp_path / "ev" / config.TOPIC_GPS / "part-*.json")):
        for line in open(f):
            try:
                m = json.loads(line)
                bad += m["segment_id"] is None or not (0 <= m["speed_kmph"] <= 150)
            except json.JSONDecodeError:
                bad += 1
    assert bad > 0, "dirty-rate 0.2 should have produced corrupt records"
    assert os.path.exists(tmp_path / "gt" / "gt.csv")
