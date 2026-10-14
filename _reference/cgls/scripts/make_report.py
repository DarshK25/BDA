"""Builds docs/Report_CGLS.docx with the same headings as the reference document.
Yellow-highlighted text = things YOU must fill in or confirm (name, your real results, screenshots).
Run:  python scripts/make_report.py   (needs python-docx)"""
import re
from docx import Document
from docx.enum.text import WD_ALIGN_PARAGRAPH, WD_COLOR_INDEX
from docx.shared import Inches, Pt

FONT = "Times New Roman"
doc = Document()
sec = doc.sections[0]
sec.page_width, sec.page_height = Inches(8.5), Inches(11)
for side in ("left_margin", "right_margin", "top_margin", "bottom_margin"):
    setattr(sec, side, Inches(1))
st = doc.styles["Normal"]
st.font.name = FONT
st.font.size = Pt(12)
st.element.rPr.rFonts.set("{http://schemas.openxmlformats.org/wordprocessingml/2006/main}eastAsia", FONT)


def para(text="", bold=False, size=None, align=None, italic=False, font=None, space_after=6):
    """Text inside [[double brackets]] is highlighted yellow (placeholder)."""
    p = doc.add_paragraph()
    p.paragraph_format.space_after = Pt(space_after)
    if align:
        p.alignment = align
    for chunk in re.split(r"(\[\[.*?\]\])", text):
        if not chunk:
            continue
        ph = chunk.startswith("[[")
        r = p.add_run(chunk[2:-2] if ph else chunk)
        r.bold, r.italic = bold, italic
        r.font.name = font or FONT
        if size:
            r.font.size = Pt(size)
        if ph:
            r.font.highlight_color = WD_COLOR_INDEX.YELLOW
    return p


def bullet(text, bold_prefix=None):
    p = doc.add_paragraph(style="List Bullet")
    p.paragraph_format.space_after = Pt(2)
    if bold_prefix:
        r = p.add_run(bold_prefix)
        r.bold = True
        r.font.name = FONT
    for chunk in re.split(r"(\[\[.*?\]\])", text):
        if not chunk:
            continue
        ph = chunk.startswith("[[")
        r = p.add_run(chunk[2:-2] if ph else chunk)
        r.font.name = FONT
        if ph:
            r.font.highlight_color = WD_COLOR_INDEX.YELLOW
    return p


def code(text):
    for line in text.strip("\n").split("\n"):
        p = para(line if line else " ", font="Courier New", size=8.5, space_after=0)


def excerpt(path, start, end):
    src = open(path, encoding="utf-8").read().split("\n")
    i = next(k for k, l in enumerate(src) if start in l)
    j = next(k for k, l in enumerate(src) if k > i and end in l)
    return "\n".join(src[i:j]).rstrip()


C = WD_ALIGN_PARAGRAPH.CENTER
# ----------------------------------------------------------------- cover block
para("", space_after=0)
para("DEPARTMENT OF COMPUTER ENGINEERING", bold=True, size=14, align=C)
para("NBA Accredited", size=9, align=C)
para("ACADEMIC YEAR 2025-26", bold=True, align=C)
para("Complex Engineering Problem Report", bold=True, align=C)
para("Subject: Big Data Analytics                       Sem: VII", bold=True)
para("Name of Student: [[YOUR NAME]]", bold=True)
para("Student ID: [[YOUR ID]]", bold=True)
para("Class/Div: [[BE/B - confirm]]", bold=True)

# ----------------------------------------------------------------- Task / Title / Problem
para("Task:", bold=True)
para("Design and implement a real-time analytics pipeline for a Smart City Transportation System that processes "
     "live data from multiple sources such as GPS trackers, IoT sensors, and weather data to predict traffic "
     "congestion and suggest alternate routes dynamically.")
para("Title: Real-Time Analytics using Apache Spark Streaming", bold=True)
para("Problem Statement:", bold=True)
para("To develop a real-time data processing system using Apache Spark Streaming (Structured Streaming API) that "
     "collects and analyses GPS, IoT-sensor and weather data to provide congestion updates every five seconds, "
     "forecasts congestion 60 seconds ahead, and recommends alternate routes when the usual route becomes slow. "
     "The system also detects suspected accidents from the stream, and analytics are reported with R (ggplot2, Shiny). "
     "Because live city feeds are not available to students, the three data sources are produced by a simulator that "
     "also records the hidden true congestion level, which allows the accuracy of the pipeline to be measured.")

# ----------------------------------------------------------------- Sustainability
para("Sustainability Context:", bold=True)
bullet("Congestion-aware routing can reduce idling time, and therefore fuel use and local air pollution; in our "
       "demonstration an accident on one road led to a suggested detour that saves [[X]] minutes on the "
       "Andheri-Dadar trip (measured in the dashboard).")
bullet("Supports SDG 11 - Sustainable Cities and Communities (target 11.2: safe, affordable, sustainable transport systems).")
bullet("Gives transport authorities a live, data-driven view of where capacity is lost (accidents, rain) so that signal "
       "timing, public-transport and enforcement resources can be directed there.")

# ----------------------------------------------------------------- Tools
para("Tools and Technologies:", bold=True)
bullet(" Apache Kafka 3.8 (KRaft mode), three input topics and one output topic", "Data Ingestion:")
bullet(" Apache Spark 3.5.3 Structured Streaming (event-time windows, watermark, de-duplication, checkpointing) and Spark MLlib (RandomForest classifier, Gradient-Boosted-Tree forecaster)", "Processing:")
bullet(" Parquet files (columnar data lake) with Spark checkpoints; HDFS-compatible - only the path changes on a cluster", "Storage:")
bullet(" Streamlit + Plotly live dashboard reading the Kafka output topic; R with ggplot2 (report) and Shiny (interactive analytics) reading the exported CSV", "Visualization:")
bullet(" A* and Dijkstra route search on a live-weighted road graph (Python, NetworkX)", "Routing:")
bullet(" Python 3.11 (PySpark, pandas, kafka-python) and R 4.x (ggplot2, Shiny, dplyr)", "Programming Languages:")
bullet(" Docker Compose (one command starts the whole system on Windows)", "Deployment:")
para("Note: HDFS and Tableau/Power BI were not used. HDFS offers no benefit on a single laptop, and Tableau/Power BI cannot "
     "consume a live Kafka stream; Parquet and Streamlit were used instead.", italic=True, size=10)

# ----------------------------------------------------------------- Architecture
para("Architecture Diagram", bold=True)
doc.add_picture("docs/architecture.png", width=Inches(6.4))
doc.paragraphs[-1].alignment = C
para("Data flow: simulator → Kafka (gps_events, sensor_events, weather_events) → Spark Structured Streaming "
     "(validate, de-duplicate, window, features, MLlib classifier, GBT forecaster, incident detection) → Kafka "
     "(congestion_updates, incident_alerts) → dashboard and A* route planner; results are also written to a Parquet data lake "
     "and a CSV that the R analytics read.", size=10, italic=True)

# ----------------------------------------------------------------- Code
para("Code/Programs:", bold=True)
para("Key parts of the implementation (full source in the project folder, src/cgls/).", size=10, italic=True)
para("1. Windowed aggregation of the unified GPS + sensor + weather stream (streaming/job.py)", bold=True, size=11)
code(excerpt("src/cgls/streaming/job.py", "def window_aggregate", "# ---------"))
para("2. Prediction with the trained MLlib RandomForest (streaming/job.py)", bold=True, size=11)
code(excerpt("src/cgls/streaming/job.py", "def predict(", "# ---------"))
para("3. Incident detection rule and learned forecast input (streaming/job.py, ml/forecaster.py)", bold=True, size=11)
code("""
# incident: speed collapses while sensor occupancy jumps
if not active and drop >= 0.20 and rise >= 15.0 and speed_ratio < 0.65:
    raise_alert(segment)                       # published once on Kafka topic incident_alerts

# forecast features: own history + neighbouring roads + weather -> GBT predicts congestion 60 s ahead
FORECAST_FEATURES = ["speed_ratio", "occupancy_pct", "speed_ratio_l15", "speed_ratio_l30", "speed_ratio_l45",
                     "occupancy_l15", "occupancy_l30", "occupancy_l45", "rain_mm_hr", "visibility_km",
                     "nb_mean_ratio", "nb_min_ratio"]
""")
para("4. Starting the streaming query with checkpointing (streaming/job.py)", bold=True, size=11)
code("""
query = (result.writeStream.outputMode("append")
         .foreachBatch(BatchProcessor(spark))
         .option("checkpointLocation", "/checkpoints/congestion_v1")
         .trigger(processingTime="5 seconds")
         .start())
query.awaitTermination()
""")
para("5. A* route search with an admissible heuristic (routing.py)", bold=True, size=11)
code(excerpt("src/cgls/routing.py", "def search(", "raise nx.NetworkXNoPath"))
para("6. R: congestion heat map with ggplot2 (r/analysis.R)", bold=True, size=11)
code(excerpt("r/analysis.R", "p1 <- ggplot", "save(p1"))

# ----------------------------------------------------------------- Screenshots
para("Expected Screenshots:", bold=True)
para("Sample of the console output printed by the Spark job every five seconds (format to expect; replace it with your own screenshot):", size=10, italic=True)
code("""
============================================================================================================
Batch 13  |  window ending 2026-10-03 14:23:55 UTC  |  19 window rows  |  result is 11.6s old
============================================================================================================
segment              road  gps  speed km/h  occ %  rain      NOW conf   in 60s
    S05  Santacruz-Bandra  102        12.9   90.0   1.2   SEVERE 100%   SEVERE
    S06      Bandra-Mahim   58        23.9   55.0   1.2    HEAVY  84%    HEAVY
    S15      Bandra-Kurla   74        30.2   53.0   1.2 MODERATE  68%    HEAVY
    S01    Borivali-Malad  103        44.3   38.0   1.2 MODERATE  97% MODERATE
""")
for n, t in [
    (1, "docker compose ps - all services running"),
    (2, "Spark console table - normal traffic"),
    (3, "Spark console table - accident on S05 (SEVERE)"),
    (4, "Live dashboard - normal traffic"),
    (5, "Live dashboard - accident, with suggested alternate route and minutes saved"),
    (6, "Spark UI, Structured Streaming tab (input rate, batch duration)"),
    (7, "Kafka console-consumer showing messages of the input topics and congestion_updates"),
    (8, "File Explorer - Parquet data lake (data/lake/segment_congestion)"),
    (9, "Model training output (accuracy vs. speed-only baseline)"),
    (10, "Forecaster training output (GBT vs. baselines)"),
    (11, "Console line 'INCIDENT SUSPECTED' and the red alert on the dashboard"),
    (12, "R: ggplot2 charts (heat map, incident timeline, forecast quality) and the Shiny app")]:
    para(f"[[Insert screenshot {n}: {t}]]")

# ----------------------------------------------------------------- Testing
para("Testing & Validation:", bold=True)
para("Fill the 'Actual Result' column only with what you observed when you ran each test (commands in README.md, section 5).", size=10, italic=True)
rows = [
    ("1. Stream simulated GPS, sensor and weather data through Kafka", "A new congestion table for all 19 roads every 5 s; result age about 10-12 s", "[[observed batch interval and 'result is N s old']]"),
    ("2. Send corrupt records (invalid JSON, speed -5 or 999, missing segment)", "Records are dropped; the job keeps running", "[[observed]]"),
    ("3. Simulated accident on Santacruz-Bandra (S05)", "S05 reaches HEAVY within about 30-40 s; incident alert within about 60 s; no alert on other roads", "[[seconds until HEAVY / until alert; false alarms]]"),
    ("4. Route Andheri → Dadar before and during the accident", "Suggested route changes and shows minutes saved", "[[usual vs. best route, minutes]]"),
    ("5. Heavy rain (12 mm/h) starts", "Rain value rises; congestion estimates increase", "[[observed]]"),
    ("6. Kill the Spark container for ~40 s, then restart it", "Resumes from the checkpoint; no gaps in 5-second windows", "[[window_sequence_gaps = ?]]"),
    ("7. Increase input from ~100 to ~1000 messages/s", "Batch duration stays below the 5 s trigger", "[[input rate, batch duration, result age]]"),
    ("8. Compare estimates with simulator ground truth (evaluate.py)", "High accuracy for 'now'; forecast compared with naive baseline", "[[accuracy now / forecast / baseline / freshness p50, p95]]"),
    ("9. Run automated tests (pytest)", "All tests pass", "[[N passed ?]]"),
    ("10. Send every GPS message twice (network retry)", "A 30 s window counts each ping once", "[[observed / test passes]]"),
    ("11. Compare the GBT forecaster with 'nothing changes' and linear-trend baselines on unseen simulated worlds", "GBT has lower error", "[[RMSE and level accuracy of the three]]"),
    ("12. A* versus Dijkstra on random speed patterns", "Same route cost; A* expands no more junctions", "[[automated test result]]"),
    ("13. Generate the R report and open the Shiny app", "8 PNG charts; app refreshes by itself", "[[observed]]"),
]
tbl = doc.add_table(rows=1, cols=3)
tbl.style = "Table Grid"
for c, h in zip(tbl.rows[0].cells, ("Test Case Scenario", "Expected Result", "Actual Result")):
    c.text = ""
    r = c.paragraphs[0].add_run(h)
    r.bold = True
    r.font.name = FONT
for row in rows:
    cells = tbl.add_row().cells
    for c, t in zip(cells, row):
        c.text = ""
        pp = c.paragraphs[0]
        for chunk in re.split(r"(\[\[.*?\]\])", t):
            if not chunk:
                continue
            ph = chunk.startswith("[[")
            r = pp.add_run(chunk[2:-2] if ph else chunk)
            r.font.name = FONT
            r.font.size = Pt(10)
            if ph:
                r.font.highlight_color = WD_COLOR_INDEX.YELLOW
para("")

# ----------------------------------------------------------------- Conclusion
para("Conclusion:", bold=True)
para("A real-time pipeline built on Kafka and Spark Structured Streaming ingests three heterogeneous streams, "
     "validates them, aggregates them in 30-second event-time windows that slide every five seconds, and uses a Spark MLlib "
     "RandomForest to estimate the congestion level of each road, a Gradient-Boosted-Tree model to forecast it 60 seconds ahead from the road's own history and its neighbours, and a rule to flag suspected accidents. On simulated data the estimate matched the hidden true level "
     "in [[X]]% of windows (a speed-only rule: [[Y]]%), and results were published about [[Z]] seconds after the end of each "
     "window; the 60-second forecast had an error of [[E1]] against [[E2]] for the naive 'nothing changes' guess. A dashboard and an A*-based route planner turn the live estimates into alternate-route suggestions; during a "
     "simulated accident the suggested detour saved [[M]] minutes. Checkpointing allowed the job to restart after a forced "
     "failure without gaps. R (ggplot2 and Shiny) provided the exploratory analytics on the exported results.")
para("Limitations: the data is simulated and the model is trained on synthetic data; the 60-second trend forecast performed "
     "[[better than / about the same as]] the naive baseline but the gain is modest because most congestion simply persists; the incident rule is hand-tuned; Spark ran in local mode, "
     "so multi-machine scaling was not demonstrated. Future work: real city feeds, an ML forecaster on lagged features, a Spark "
     "cluster, an LSTM forecaster, MQTT-connected sensors, and routing that accounts for the effect of many drivers rerouting at once. "
     "Apache Flink was considered for stream processing; Spark Structured Streaming was used because the problem statement specifies Spark and it provides the required event-time windows, watermarks and checkpointing.")

doc.save("docs/Report_CGLS.docx")
print("saved docs/Report_CGLS.docx")
