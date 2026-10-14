# CGLS - Congestion GPS Live Streaming
### Real-Time Analytics using Apache Spark Streaming - Smart City Transportation

A complete, runnable Big Data pipeline:

```
GPS trackers ┐                                          ┌─► Kafka ─► Live dashboard (Python): map, alerts, A* routes
IoT sensors  ├─► Kafka ─► Spark Structured Streaming ───┼─► Parquet data lake (HDFS substitute)
Weather      ┘            dedupe · windows · MLlib      └─► CSV ─► R: ggplot2 report + Shiny analytics app
                          RandomForest · GBT forecaster
                          incident detection
```

Everything runs in Docker. **You do not need Hadoop, Spark, Kafka or Java installed on Windows.**

> **Honesty note (important for your report and viva).** The data comes from a *simulator*, because a
> student cannot get live city feeds. The simulator has a hidden "true" congestion value per road,
> so we can measure how accurate the pipeline really is. Everything downstream of the simulator is
> real: Kafka, Spark, the ML model, the dashboard. Say "simulated data" in your report.

---------------------------------------------------------------------------------------------------

## 0. What is in this folder

| Path | What it is |
|---|---|
| `docker-compose.yml`, `Dockerfile`, `requirements.txt` | the whole environment, one command to start |
| `src/cgls/simulator/producer.py` | creates GPS, IoT-sensor and weather messages and sends them to Kafka |
| `src/cgls/streaming/job.py` | **the Spark Structured Streaming job (the heart of the project)** |
| `src/cgls/ml/train.py` | trains the MLlib RandomForest classifier (current congestion level) |
| `src/cgls/ml/forecaster.py` | trains the MLlib GBT forecaster (congestion 60 s ahead) on fast offline simulations |
| `src/cgls/ml/evaluate.py` | measures accuracy, freshness and gaps against the simulator's ground truth |
| `src/cgls/routing.py` | dynamic route suggestion: A* + Dijkstra, two edge-weight models (speed, weather-penalty) |
| `r/analysis.R`, `r/app.R`, `r/common.R` | **R analytics**: ggplot2 report (8 charts) and a Shiny app |
| `src/cgls/dashboard/app.py` | live map dashboard (Streamlit) |
| `src/cgls/network.py`, `traffic_model.py`, `features.py` | road network, the simulated "physics", feature maths |
| `tests/` | automated tests (Python + R) |
| `data/` | **outputs appear here**: `lake/` (Parquet), `models/`, `reports/`, `ground_truth/` |
| `docs/architecture.png` | architecture diagram for the report |
| `docs/Report_CGLS.docx` | report using the same headings as the reference document - fill in your results |
| `data/export/congestion.csv` | flat export that the R code reads |

---------------------------------------------------------------------------------------------------

## 1. One-time setup on Windows

### 1.1 Check the machine
* Windows 10 (22H2) or Windows 11, 64-bit, virtualization enabled (Task Manager → Performance → CPU → "Virtualization: Enabled").
* **RAM: 8 GB works, 16 GB is comfortable.** Disk: about **8 GB free** is enough (images ≈ 4 GB, data grows slowly). You have 30 GB, so you are fine.

### 1.2 Docker Desktop
You already have Docker. Make sure it uses the **WSL 2 backend**: Docker Desktop → *Settings → General → "Use the WSL 2 based engine"* ticked.
Start Docker Desktop and wait until the whale icon says **"Engine running"**.

Open **PowerShell** and check:

```powershell
docker --version
docker compose version
```
Both must print a version (Compose v2, e.g. `v2.2x`). If `docker compose` is not found, update Docker Desktop.

### 1.3 Give Docker enough memory (only if you have 8 GB RAM)
Create the file `C:\Users\<your-name>\.wslconfig` with:

```
[wsl2]
memory=5GB
processors=4
```
Then in PowerShell run `wsl --shutdown` and start Docker Desktop again.

### 1.4 Put the project in your `cgls` folder
Copy the contents of the zip **into** your `cgls` folder so that `docker-compose.yml` sits directly inside it, e.g. `C:\Users\<you>\cgls\docker-compose.yml`.
Open PowerShell in that folder (File Explorer → click the address bar → type `powershell` → Enter).

```powershell
cd C:\Users\<you>\cgls
dir
```
You should see `docker-compose.yml`, `Dockerfile`, `src`, `tests`, ...

> Tip: keep the project on your normal Windows drive (not inside `\\wsl$`); Docker handles it.

---------------------------------------------------------------------------------------------------

## 2. Build and test (about 10 minutes the first time)

```powershell
docker compose build
```
This downloads Python, Java, PySpark and the Spark-Kafka connector. It prints a lot; wait for `Successfully built` / `Built`.
If you see `connector pre-download skipped`, that is fine - it will download on first run instead.

Run the automated tests (they use a real local Spark, no Kafka needed):

```powershell
docker compose --profile tools run --rm tools
```
Expected last line: `N passed` (all pass; R tests are skipped in this image because it has no R, that is normal) - takes about 3 minutes. **Take a screenshot** (report → Testing & Validation).

Train the model once and read its accuracy (the streaming job would also do this automatically on first start):

```powershell
docker compose --profile tools run --rm tools python -m cgls.ml.train
```
You will see accuracy, F1, a "GPS speed only" baseline and feature importances. **Screenshot this.**
It also writes `data\models\metrics.json`.

Train the 60-second forecaster (about 2-3 minutes; it simulates many random accidents and rain episodes much faster than real time and tests itself on worlds it has never seen):

```powershell
docker compose --profile tools run --rm tools python -m cgls.ml.forecaster
```
Output compares the **GBT forecaster** with two baselines ("assume nothing changes" and "linear trend"). **Screenshot this** - it is your forecasting result. Writes `data\models\forecaster_metrics.json`.

---------------------------------------------------------------------------------------------------

## 3. Start the whole system

```powershell
docker compose up -d
```
What starts, in order: `kafka` → `kafka-init` (creates 5 topics, then exits - that is normal) → `simulator`, `spark`, `dashboard`.

Check that containers are up:
```powershell
docker compose ps
```
(`kafka-init` shows `exited (0)` - good.)

Watch the Spark job print a result table every 5 seconds:
```powershell
docker compose logs -f spark
```
(Press `Ctrl+C` to stop *watching*; the system keeps running.)
After ~20-40 s you will see blocks like:

```
============================================================================================
Batch 7  |  window ending 2026-10-03 14:23:35 UTC  |  19 window rows  |  result is 11.0s old
============================================================================================
segment              road  gps  speed km/h  occ %  rain      NOW conf   in 60s
    S05  Santacruz-Bandra  102        12.9   90.0   1.2   SEVERE 100%   SEVERE
    ...
```
**This table is your "Expected Screenshots" section.**

Open in your browser:

| URL | What |
|---|---|
| http://localhost:8501 | **Live dashboard** (map, KPIs, route planner) |
| http://localhost:4040 | **Spark UI** → tab *Structured Streaming*: input rate, batch duration |
| http://localhost:3838 | **R Shiny analytics** (optional, see 3.2) |

The first minute the dashboard says "Waiting for the first result"; the model may be trained on first start.

### 3.1 What you should see in the first 5 minutes (the built-in demo script)
The simulator plans two events (seconds after *simulator* start):

| Time | Event | What to look for |
|---|---|---|
| 0-90 s | normal traffic | mostly yellow/green roads |
| **90 s** | **accident on S05 (Santacruz-Bandra)** | congestion builds over about a minute (like a real queue): S05 turns HEAVY about 30 s later; the console prints `!!! INCIDENT SUSPECTED on S05` and the dashboard shows a red alert about 45-60 s after the crash; neighbours S04, S06, S14, S15 get worse with a delay (spill-back) |
| ~2-3 min | route planner (Andheri → Dadar) | blue "suggested" route differs from grey "usual" route; banner says *saves X min* |
| **150 s** | **heavy rain** | `rain` column rises, more roads get worse |
| 330 s | accident clears | roads recover, suggestion goes back to usual route |

Make dashboard screenshots at the *normal*, *accident* and *recovered* moments.

### 3.2 R analytics (ggplot2 + Shiny) - optional but recommended for a Big Data course
The Spark job also appends every result to `data\export\congestion.csv`. The R tools read that file (and the simulator's `ground_truth\gt.csv`).
Let the system run for at least 5 minutes, then:

```powershell
docker compose --profile r build                        # first time only, about 5 minutes
docker compose --profile r run --rm r-report             # writes 8 PNG charts + summary.csv to data\reports\r\
docker compose --profile r up -d r-analytics             # Shiny app -> http://localhost:3838
```
Charts: congestion heat map, GPS-speed vs sensor-occupancy, rain effect, incident timeline (estimate vs forecast vs truth), forecast quality, confusion matrix, result freshness, road ranking. **Add 3-4 of them to the report.**
The Shiny app has tabs (heat map, road trend, weather impact, forecast quality, table), a time slider and refreshes itself.

---------------------------------------------------------------------------------------------------

## 4. Look inside the pipeline (great for the viva)

**See raw messages in Kafka**
```powershell
docker compose exec kafka /opt/kafka/bin/kafka-console-consumer.sh --bootstrap-server kafka:9092 --topic gps_events --max-messages 5
docker compose exec kafka /opt/kafka/bin/kafka-console-consumer.sh --bootstrap-server kafka:9092 --topic sensor_events --max-messages 5
docker compose exec kafka /opt/kafka/bin/kafka-console-consumer.sh --bootstrap-server kafka:9092 --topic weather_events --max-messages 3
docker compose exec kafka /opt/kafka/bin/kafka-console-consumer.sh --bootstrap-server kafka:9092 --topic congestion_updates --max-messages 3
docker compose exec kafka /opt/kafka/bin/kafka-console-consumer.sh --bootstrap-server kafka:9092 --topic incident_alerts --from-beginning --max-messages 3
```

**See the data lake** - open `cgls\data\lake\segment_congestion` in File Explorer: one folder per micro-batch, each holding a Parquet file.
Read it with Python inside Docker:
```powershell
docker compose --profile tools run --rm tools python -c "import pandas as pd; df=pd.read_parquet('/data/lake/segment_congestion'); print(df.shape); print(df[['segment_id','level_name','forecast_level_name','est_speed_kmph']].tail(10))"
```

---------------------------------------------------------------------------------------------------

## 5. Testing & Validation - do these and record YOUR results

Fill the report table only with what you actually observe. Suggested test cases:

| # | Test | How | What to record |
|---|---|---|---|
| T1 | Live stream end to end | `docker compose logs -f spark`; dashboard open | a new table every ~5 s; "result is N s old" (typically ~10-12 s); dashboard *Data age* |
| T2 | Corrupt data does not crash the job | simulator already sends 1 % corrupt records. Also inject one by hand: `echo not-json \| docker compose exec -T kafka /opt/kafka/bin/kafka-console-producer.sh --bootstrap-server kafka:9092 --topic gps_events` | job keeps printing batches; no error |
| T3 | Accident detection | watch S05 after t+90 s | seconds until S05 reaches HEAVY/SEVERE; seconds until `INCIDENT SUSPECTED` appears; whether any other road was flagged (false alarm) |
| T4 | Dynamic rerouting | dashboard, Andheri → Dadar, before/after the accident | usual vs. best route minutes, route names |
| T5 | Weather effect | after t+150 s | `rain` column and levels in the table |
| T6 | **Fault tolerance (replaces "node failure")** | see 5.1 | batch numbers continue; evaluate shows `window_sequence_gaps: 0` |
| T7 | Throughput | see 5.2 | Spark UI input rows/s, batch duration, "result is N s old" |
| T8 | Prediction quality | `docker compose --profile tools run --rm tools python -m cgls.ml.evaluate` (after ≥ 5 min of running) | accuracy now, forecast vs. persistence baseline, freshness |
| T9 | Unit/integration tests | section 2 | `N passed` |
| T10 | Duplicate messages | simulator already re-sends 1 % of GPS pings; automated test `test_duplicate_gps_messages_are_counted_once` | test passes (window counts 30, not 60) |
| T11 | Forecaster vs baselines | `python -m cgls.ml.forecaster` (section 2) and `evaluate` | RMSE / level accuracy of GBT vs persistence vs trend |
| T12 | A* vs Dijkstra | automated tests in `tests/test_routing.py`; dashboard caption "A* expanded N junctions, Dijkstra M" | same route, A* explores no more nodes |
| T13 | R analytics | section 3.2 | the PNG charts; Shiny page screenshot |

### 5.1 T6 - Fault tolerance (checkpoint recovery)
```powershell
docker compose kill spark          # simulates a crash
# wait ~40 seconds - Kafka keeps collecting events meanwhile
docker compose up -d spark         # restart
docker compose logs -f spark       # the job resumes and catches up on the missed events
```
After a few minutes run evaluate (T8) and check `window_sequence_gaps` (0 = no 5-second step is missing anywhere).
Explain in the report: *Kafka retains messages, the Spark checkpoint stores the read offsets and the window state, the Parquet sink is idempotent (one folder per batch id), so a crash loses no data.* The Kafka output topic is **at-least-once**: after a crash the last batch can be published twice; the dashboard keeps only the newest value per road so duplicates are harmless.

> We run Spark in *local mode* in one container, so "node failure" here means "driver/executor process failure". True multi-node
> failover needs a Spark cluster (see section 8, extensions).

### 5.2 T7 - Throughput test (100 → 1000+ messages/s)
```powershell
docker compose stop simulator
docker compose run --rm --no-deps simulator python -m cgls.simulator.producer --vehicles 500  --no-events --duration 120
docker compose run --rm --no-deps simulator python -m cgls.simulator.producer --vehicles 5000 --no-events --duration 120
```
`--vehicles N` means about N/5 GPS messages per second (500 → ~100 msg/s, 5000 → ~1000 msg/s). While each runs, open http://localhost:4040 → *Structured Streaming* and note **Input Rate**, **Process Rate** and **Batch Duration**; also note "result is N s old" in `docker compose logs spark`.
The system keeps up as long as batch duration stays below the 5 s trigger. Restart the normal simulator afterwards: `docker compose up -d simulator`.

### 5.3 Custom scenarios
```powershell
docker compose stop simulator
docker compose run --rm --no-deps simulator python -m cgls.simulator.producer --scenario rush --incident-segment S07 --incident-after 30
docker compose run --rm --no-deps simulator python -m cgls.simulator.producer --help
```

---------------------------------------------------------------------------------------------------

## 6. Screenshot checklist (for the report)

1. `docker compose ps` (all services up)
2. Spark console table during normal traffic
3. Spark console table during the accident (S05 SEVERE)
4. Dashboard: normal / accident / reroute banner / after recovery
5. Spark UI → Structured Streaming tab (input rate, batch duration)
6. Kafka console-consumer output (a few messages of each topic)
7. File Explorer showing `data\lake\segment_congestion\...parquet`
8. Training output (accuracy vs baseline) and `evaluate` output
9. Test run: all tests passed (`docker compose --profile tools run --rm tools`)
10. Fault-tolerance test: logs before kill and after restart
11. Forecaster training output (GBT vs baselines)
12. Console line `!!! INCIDENT SUSPECTED ...` and the red alert on the dashboard
13. R charts (heat map, incident timeline, forecast quality, confusion matrix) and the Shiny page

---------------------------------------------------------------------------------------------------

## 7. Stop, restart, clean up

| Goal | Command |
|---|---|
| stop everything, keep data | `docker compose stop` |
| start again | `docker compose up -d` |
| remove containers (keeps your `data\` files) | `docker compose down` |
| **fresh start** (also wipes Kafka + Spark checkpoint) | `docker compose down -v` |
| see disk used by Docker | `docker system df` |
| free disk (removes unused images/cache) | `docker system prune -a` |

Run **`docker compose down -v`** whenever you change `job.py` in a way that changes the query, or if Spark complains about the checkpoint.
Disk use: ≈ 4 GB images; the Parquet lake grows roughly 1-2 MB per hour. Delete `data\lake` any time.

---------------------------------------------------------------------------------------------------

## 8. Troubleshooting (Windows)

| Symptom | Fix |
|---|---|
| `error during connect ... docker daemon is not running` | start Docker Desktop, wait for "Engine running" |
| `port is already allocated` (8501 / 4040 / 29092) | close the program using it, or change the left number in `ports:` in `docker-compose.yml` |
| a container shows `Exited (137)` | out of memory → close apps, set `.wslconfig` (1.3), or use fewer vehicles |
| dashboard stuck on "Waiting for the first result" > 2 min | `docker compose logs spark` and look for the first error. First start trains the model (~1 min). |
| Spark error `Subscribe topic not found` / checkpoint errors | `docker compose down -v` then `docker compose up -d` |
| `no space left on device` | `docker system prune -a` (deletes unused images; rebuild with `docker compose build`) |
| build fails downloading packages | check internet / VPN / proxy; just run `docker compose build` again |
| map has no background tiles | the browser needs internet for OpenStreetMap tiles; roads still draw |
| edited `.py` file on Windows has no effect | `src\` is mounted live: `docker compose restart spark` (or `dashboard`, `simulator`) |
| `docker compose --profile r build` is slow / fails | it installs R packages from Debian; retry on a good connection. The rest of the project does not need it. |
| R charts say "No data" | wait until `data\export\congestion.csv` exists (the Spark job has printed at least one batch) |
| scripts with weird `\r` errors | files must use LF endings; `.gitattributes` enforces it if you use git |

**Dashboard without Kafka (to show the UI quickly):**
`docker compose run --rm --no-deps -p 8502:8501 -e CGLS_DEMO=1 dashboard` → http://localhost:8502

---------------------------------------------------------------------------------------------------

## 9. How it works (read before your viva)

**Why Structured Streaming and not "Spark Streaming (DStreams)"?** DStreams is the old RDD API, deprecated since Spark 3.4/4.0. Structured Streaming is the current Spark streaming engine (same idea: micro-batches every few seconds) with event-time windows, watermarks and exactly-once checkpointing.

**Pipeline steps (`streaming/job.py`)**
1. *Read*: one Kafka source subscribed to the three input topics.
2. *Parse & validate*: JSON → typed columns. Corrupt JSON, missing keys and impossible speeds (< 0 or > 150 km/h) are dropped.
3. *Unify & de-duplicate*: GPS, sensor and weather events get one schema; weather is city-wide so it is fanned out to each road (stream-static cross join). A network retry that re-sends the same message is dropped (`dropDuplicates` on message key + event time, state bounded by the watermark).
4. *Window*: 30-second windows sliding every 5 seconds per road, using **event time** and a 2-second **watermark** (how long to wait for late events). Append mode → every window is emitted exactly once when final.
5. *Features*: speed ratio (GPS speed / free-flow speed), sensor occupancy, vehicles per lane, rain, visibility. Missing sources are handled (e.g. a window without GPS falls back to the sensor).
6. *Predict*: MLlib RandomForest → congestion level (FREE / MODERATE / HEAVY / SEVERE) with a probability, giving a continuous congestion score.
7. *Forecast*: a **GBT regressor (MLlib)** predicts the congestion index 60 s ahead from the road's own history (now, 15, 30, 45 s ago), the state of **neighbouring roads** (jams spill back) and weather. It is trained offline on fast simulations with random accidents/rain and tested on unseen simulated worlds. Until 45 s of history exist after a start-up, it falls back to "nothing changes".
8. *Incident detection* (a simple complex-event-processing rule): speed ratio falls by ≥ 0.20 within 30 s **and** sensor occupancy rises by ≥ 15 points **and** the road is now slow → alert (once), cleared when speed recovers. Published on Kafka topic `incident_alerts`.
9. *Sinks* (`foreachBatch`): console table, Parquet lake, CSV for R, Kafka topics `congestion_updates` and `incident_alerts`.

**Routing (`routing.py`)**: the city is a graph; each road's weight is its current travel time. Two models (switch in the dashboard sidebar): `speed` = length / live speed, and `penalty` = free-flow minutes × (1 + 2.5·congestion + 0.4·weather penalty). The best route is found with **A\*** (heuristic = straight-line distance at the city's top speed, which never overestimates, so the answer is optimal) and checked against Dijkstra in the tests. The *usual* route is the empty-road fastest path re-priced at live speeds. If the best is different and saves ≥ 0.5 min → "take the alternate route".

**Ideas from the Gemini/Google overview: what we used and what we did not**

| Suggestion | Decision |
|---|---|
| Apache Flink for stream processing | **Not used.** The assignment is "Apache Spark Streaming"; a second engine would double the complexity. Spark Structured Streaming offers the same ideas we needed (event time, watermarks, state, exactly-once checkpoints). Say this in the viva: Flink is a true record-at-a-time engine with lower latency; Spark works in micro-batches (5 s here). |
| Kafka as central log with topics per source | Used (`gps_events`, `sensor_events`, `weather_events`). |
| Event-time + watermarks for out-of-order data | Used (2 s watermark). |
| Data cleansing: duplicates, missing values, normalisation | Used: duplicate removal; missing GPS or sensor data in a window is filled from the other source; speeds are normalised to *speed ratio* (speed / free-flow). |
| Windowed joins with weather | Used (weather fanned out per road, averaged in the same window). Windows are 30 s because the demo runs in minutes; a real city would use 5 min. |
| LSTM forecaster | **Replaced by a gradient-boosted-tree forecaster** that runs inside Spark MLlib with no deep-learning stack. An LSTM can be trained on the same lag table later (see extensions). |
| Complex event processing for accidents | Used in simplified form (rule in section "Incident detection"). |
| A* with dynamic weights `base × (1 + α·congestion + β·weather)` | Used. |
| MQTT for IoT devices and for pushing reroute advice | **Not used**: our sensors and the dashboard talk through Kafka. Mention as future work. |
| Triton / Flink ML serving | Not needed: the model is applied in Spark itself. |
| R scripts, ggplot2, Shiny (your idea) | Added (`r/`). |

**Where HDFS went**: HDFS needs a Hadoop cluster; on one laptop it adds complexity but no benefit. We store results as **Parquet** files (the same columnar format used on HDFS/S3 data lakes) in `data/lake`. Changing the path to `hdfs://...` or `s3a://...` is the only change needed on a real cluster. Say this in the viva.

**Where Tableau / Power BI went**: they are batch BI tools with no native live-streaming feed; a Streamlit page reading the Kafka output topic updates every 3 s.

**Big Data "V"s**: Velocity - continuous streams, 5 s updates; Variety - three different sources and schemas (GPS, IoT, weather); Volume - 1000+ msg/s is ~86 M messages/day on a laptop and scales with Kafka partitions + Spark cores; Veracity - corrupt records are filtered and noise is smoothed by windows.

### Known limitations (put these in the report - it shows maturity)
* Data is simulated; the ML model is trained on synthetic data. With real data, only `generate_dataset()` in `train.py` changes.
* The 60-second forecaster is better than the naive "nothing changes" guess, but only modestly (on unseen simulated worlds: error 0.072 vs 0.084, level accuracy 84.3 % vs 82.6 %). Most of the time congestion simply persists, and the start of an accident cannot be predicted from past data. Report your own numbers.
* The incident rule is hand-written (thresholds 0.20 / 15 / 0.65); it worked on the simulator without false alarms but real traffic would need tuning or a learned detector.
* Spark runs in local mode in one container, so this demonstrates the programming model, checkpoint recovery and per-core parallelism, not multi-machine scaling.
* Congestion-based routing ignores turn restrictions, one-way roads and the fact that many drivers rerouting at once changes traffic (a real system needs that feedback).

### Possible extensions (if you have time)
Spark standalone cluster with 2 workers in `docker-compose.yml`; an **LSTM** forecaster (PyTorch, trained on the table built by `forecaster.build_table()`, compared with the GBT numbers); MQTT bridge for the sensors; Prometheus/Grafana metrics; replace the simulator by public data (e.g. Open-Meteo for real weather).
