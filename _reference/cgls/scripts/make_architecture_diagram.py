"""Regenerates docs/architecture.png  (python scripts/make_architecture_diagram.py)"""
import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
from matplotlib.patches import FancyBboxPatch, FancyArrowPatch

fig, ax = plt.subplots(figsize=(13, 7))
ax.set_xlim(0, 13); ax.set_ylim(-0.6, 6.6); ax.axis("off")

def box(x, y, w, h, text, fc, fs=9):
    ax.add_patch(FancyBboxPatch((x, y), w, h, boxstyle="round,pad=0.03,rounding_size=0.12", fc=fc, ec="#333", lw=1.2))
    ax.text(x + w / 2, y + h / 2, text, ha="center", va="center", fontsize=fs)

def arrow(x1, y1, x2, y2):
    ax.add_patch(FancyArrowPatch((x1, y1), (x2, y2), arrowstyle="-|>", mutation_scale=14, lw=1.4, color="#333"))

for x, t in [(1.0, "1. DATA SOURCES"), (4.0, "2. INGESTION"), (7.45, "3. STREAM PROCESSING"), (11.45, "4. SERVING & STORAGE")]:
    ax.text(x, 6.3, t, ha="center", fontsize=10, fontweight="bold", color="#1d4ed8")

box(0.1, 4.6, 1.8, 0.9, "GPS trackers\n(300 vehicles,\nping every 5 s)", "#e0f2fe")
box(0.1, 3.3, 1.8, 0.9, "IoT road sensors\n(19 segments,\noccupancy + count)", "#e0f2fe")
box(0.1, 2.0, 1.8, 0.9, "Weather station\n(rain, visibility)", "#e0f2fe")
ax.text(1.0, 1.5, "Python simulator\n(+ hidden ground truth)", ha="center", fontsize=8, style="italic")

box(3.0, 4.6, 2.0, 0.9, "topic: gps_events", "#fde68a")
box(3.0, 3.3, 2.0, 0.9, "topic: sensor_events", "#fde68a")
box(3.0, 2.0, 2.0, 0.9, "topic: weather_events", "#fde68a")
ax.text(4.0, 5.8, "Apache Kafka (KRaft)\n3 partitions per topic", ha="center", fontsize=8.5)
for y in (5.05, 3.75, 2.45):
    arrow(1.9, y, 3.0, y)

ax.add_patch(FancyBboxPatch((5.7, 1.0), 3.5, 4.9, boxstyle="round,pad=0.03,rounding_size=0.15", fc="#fff7ed", ec="#c2410c", lw=1.8))
ax.text(7.45, 5.65, "Apache Spark Structured Streaming", ha="center", fontsize=9.5, fontweight="bold", color="#c2410c")
steps = ["Parse JSON + validate\n(drop corrupt / impossible)", "Unify 3 streams + drop duplicates\n(weather fan-out to segments)",
         "30 s sliding window / 5 s\n+ watermark (per segment)", "Features: speed ratio, occupancy,\nveh/lane, rain, visibility",
         "MLlib RandomForest\n-> level + confidence", "GBT 60 s forecast (road + neighbours)\n+ incident detection"]
ys = [4.85, 4.15, 3.45, 2.75, 2.05, 1.35]
for s, y in zip(steps, ys):
    box(5.9, y, 3.1, 0.55, s, "#ffffff", fs=7.8)
for y in ys[:-1]:
    arrow(7.45, y, 7.45, y - 0.15)
arrow(5.0, 5.05, 5.7, 4.6); arrow(5.0, 3.75, 5.7, 3.75); arrow(5.0, 2.45, 5.7, 2.9)

box(10.0, 4.95, 2.9, 0.75, "Kafka topics\ncongestion_updates, incident_alerts", "#fde68a", fs=8)
box(10.0, 3.7, 2.9, 0.95, "Streamlit dashboard: live map, KPIs,\nalerts, A* route suggestion", "#dcfce7", fs=8)
box(10.0, 2.55, 2.9, 0.85, "Parquet data lake\n(stands in for HDFS) + checkpoints", "#f3e8ff", fs=8)
box(10.0, 1.4, 2.9, 0.85, "CSV export -> R: ggplot2 report\n+ Shiny analytics app", "#fee2e2", fs=8)
box(10.0, 0.3, 2.9, 0.8, "evaluate.py: accuracy vs ground\ntruth, freshness, gaps", "#f3f4f6", fs=8)
arrow(9.2, 3.6, 10.0, 5.3); arrow(9.2, 2.9, 10.0, 2.95); arrow(9.2, 2.2, 10.0, 1.8)
arrow(11.45, 4.95, 11.45, 4.65); arrow(11.45, 2.55, 11.45, 2.25); arrow(11.45, 1.4, 11.45, 1.1)
ax.text(9.45, 4.55, "foreachBatch", fontsize=7.5, color="#444", rotation=55, ha="center")
ax.text(6.5, -0.35, "Fault tolerance: Kafka retains the data; the Spark checkpoint stores offsets and window state, so a restart resumes without loss.",
        fontsize=8.3, style="italic", color="#444", ha="center")
fig.savefig("docs/architecture.png", dpi=170, bbox_inches="tight", facecolor="white")
