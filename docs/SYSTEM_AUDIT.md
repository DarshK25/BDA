# MapYourWay system audit

Audit date: 2026-10-09. Scope: the current `src/mapyourway` application, its
Compose deployment, tests, data contracts, and R consumers. `_reference/cgls`
was examined as design material, not counted as implemented MapYourWay behavior.

## Verdict

**This is a partial real-data prototype, not a working end-to-end implementation
of the full problem statement yet.** TomTom and Open-Meteo calls work, the
application image builds, and several library functions work. However, the
current streaming query is rejected by Spark, the existing broker has a topic
migration conflict, real traffic normalization is incorrect, and live routing
and future congestion forecasting remain unimplemented.

## Checks performed

| Check | Result | Evidence / scope |
|---|---|---|
| Python syntax / compilation | PASS | 19 current package/test Python files parsed; compilation passed. |
| Windows virtualenv imports | PASS | The existing import script loaded its seven selected modules. |
| Existing pytest suite | PASS, limited | 4 tests passed; reported combined statement/branch coverage 17%. Streaming, ML, routing, feature engineering and dashboard rendering have no coverage in this suite. |
| Compose validation | PASS | `docker compose config --quiet`, using the Windows Docker CLI. |
| Application image build | PASS | `docker compose build tools` completed, including Spark-Kafka jar downloads. Build was slow and emitted Spark startup/shutdown errors during jar prefetch; image export nevertheless completed. |
| Default container test command | FAIL | `docker compose run --rm --no-deps tools` rejects all `--cov*` options. |
| Container tests with coverage options bypassed | PASS, limited | `docker compose run --rm --no-deps tools python -m pytest -o addopts= -q`: 4 passed. |
| Compose startup against existing broker | FAIL | Three dotted input topics collide with legacy underscore names. Init nevertheless exits 0; producer subsequently reports Kafka metadata timeouts. |
| Live Open-Meteo | PASS | Real response normalized successfully, including temperature, visibility, humidity, wind, weather code and source time. |
| Live TomTom S01 | PASS | Real response: speed 15 km/h, free-flow speed 23 km/h, confidence 1.0; one derived sensor and three derived probes emitted to an in-memory collector. This isolated check did not depend on Kafka. |
| A* / Dijkstra / rerouting library smoke | PASS, limited | Normal Andheri-Dadar route comparison and a congested-edge rerouting scenario passed. Not an exhaustive graph/property test. |
| Canonical pandas feature smoke | PASS | Basic default-index rows produced expected ratio, occupancy and vehicles-per-lane. |
| Spark metadata and fallback | PASS | 19 metadata rows; ratio 0.4 classified as heavy with confidence 0.5. |
| Spark streaming plan validation | FAIL | Actual `compute_windowed_features` plan, with schema-compatible rate streams, rejected by Spark 3.5.3 for both update and append modes. |
| Boundary/failure probes | FAILURES REPRODUCED | Empty API response accepted; rapid retry on empty cache; missing closure/source time; old dashboard message wins; non-default pandas index adds a phantom row. |
| Dashboard visualization functions | FAILURES REPRODUCED | A moderate prediction and a missing segment both map to green. |
| Dashboard server | STARTED only | Streamlit announced port 8501. Browser interaction/end-to-end rendering is not certified. An isolated AppTest attempt timed out. |
| Model train/save/load | NOT VERIFIED | Initial native-Windows training timed out; reduced smoke training failed with `Python worker failed to connect back`. No trained model quality or saved-model round-trip is claimed. |
| R / Shiny runtime | NOT VERIFIED | Rscript exists but required R packages are absent locally; R image was not built. Findings below are source review. |
| Kafka -> Spark -> Parquet/CSV -> dashboard/R | BLOCKED | No successful complete path demonstrated because of topic and streaming-plan blockers. |

Windows Python 3.10.10 / PySpark 3.5.3 and the built Linux Python 3.11 / Java 17
image were used. WSL itself lacks the project dependencies and Docker integration;
Windows interoperability allowed the Docker and Python checks. Native-Windows
stream startup also encounters missing `HADOOP_HOME` / `winutils.exe`; the Spark
plan rejection was independently confirmed through Spark's analyzer, without
needing a filesystem checkpoint.

## P0: restore a runnable pipeline

### 1. Unsupported streaming query

**Confirmed by Spark's analyzer.**

- GPS and sensor windowed streaming aggregations are joined, followed by a
  stream-stream weather left outer join (`src/mapyourway/streaming/job.py:167-244`).
- The sink selects `outputMode("update")` (`job.py:487-499`).
- Spark reports:
  `Join between two streaming DataFrames/Datasets is not supported in Update output mode, only in Append output mode`.
- Merely changing the mode does not fix it. Append mode reports:
  `Stream-stream LeftOuter join ... is not supported without a watermark in the join keys, or a watermark on the nullable side and an appropriate range condition`.

Pending: redesign the streaming/stateful stages and weather enrichment, then
test the real query with late events and sparse weather. A unified event
aggregation or separated queries with an explicitly timestamped weather lookup
are possible designs. The reference's unified-event pipeline is useful design
material, but its weather cadence and synthetic assumptions need adaptation.

### 2. Existing Kafka topics block the new names, but init reports success

**Confirmed in broker/init/producer logs.**

Existing topics are `mapyourway.gps_events`, `mapyourway.sensor_events`, and
`mapyourway.weather_events`. Kafka rejects the new `mapyourway.gps.events`,
`mapyourway.sensor.events`, and `mapyourway.weather.events` because dots and
underscores collide in metric naming. This is an upgrade/environment blocker;
it is not evidence that dotted names fail on an empty broker.

`docker-compose.yml:57-63` does not stop on a failed creation command. Its final
topic-list command succeeds, producing exit code 0 even though the input topics
are missing. Dependent services start and fail later with metadata timeouts.

Pending: choose/document a migration strategy for existing topics and offsets;
make init fail immediately on error and verify every required topic. Preserve
existing data when performing migration.

### 3. The documented container test runner fails

**Confirmed in the freshly built image.** `pytest.ini:19-23` unconditionally
requires pytest-cov options, while `requirements.txt:24-25` installs only pytest.
The local virtualenv has pytest-cov already, hiding the clean-install failure.

Pending: declare the test plugin dependency or make coverage an explicit optional
command. Add a clean-image test job so local environment extras cannot mask this.

## P1: correct the real-data behavior

### 4. Spark discards TomTom free-flow speed and misclassifies traffic

**Confirmed by a live response, schema inspection and Spark metadata checks.**

The producer emits `free_flow_speed_kmph`, but `GPS_SCHEMA` drops it
(`job.py:35-42`). Spark instead joins static `network.py` values and divides by
those (`job.py:104-119,248-250`).

Observed example:

| S01 observation | Ratio | Existing rule's class |
|---|---:|---|
| TomTom: 15 / 23 km/h | 0.652 | Moderate |
| Spark: 15 / configured 60 km/h | 0.250 | Severe |

The derived sensor occupancy uses TomTom's denominator, so the live feature
vector can internally disagree as well. Preserve/aggregate the observed
free-flow value and distinguish static fallback metadata from source values.

### 5. Cached traffic is presented as fresh indefinitely

**Confirmed field loss; expiry behavior established by code review.**

Snapshots contain no source observation time (`real_producer.py:252-261`). Cached
events receive a new `ts_ms` on every emission (`real_producer.py:452-465`) and
failed refreshes do not expire the old value. Spark drops provenance, and the
dashboard's age is time since message receipt (`live_state.py:43-45`), not time
since the traffic was observed.

Pending: retain observation/fetch/emission times and snapshot IDs; enforce a
per-segment TTL; expose stale/unknown states; store source provenance in the lake.
At the default 900-second refresh, the real traffic information updates every
15 minutes. Re-emission every 10 seconds does not provide new traffic evidence.

### 6. Failed startup bypasses the API quota interval

**Reproduced using a fake clock and failed API polls.** With an empty snapshot
cache, `if not snapshot_cache or ...` (`real_producer.py:443`) polls every loop,
irrespective of the configured interval. The probe made three failed poll
attempts within two clock seconds with a 900-second setting.

Pending: schedule attempts independently of cache contents, with bounded retry,
backoff and 429 handling. Sequential HTTP calls and Kafka send blocking also
pause weather and regular emission in the single producer loop.

### 7. Closures, source confidence and travel times are lost

**Reproduced on the production ProbeFleet path.** `ProbeFleet.events`
(`real_producer.py:106-118`) emits neither road closure nor the source travel
times. The aggregate/no-fleet branch does include them, but `main()` uses the
fleet. Spark schemas additionally discard source/confidence fields. Final
confidence 0.5 is an arbitrary fallback value, with no `prediction_source` field.

Pending: retain closure and quality fields through Kafka, Spark, storage and UI;
distinguish model confidence from source confidence; label rule-based results.
Routing must exclude closed roads rather than treating zero speed as 5 km/h
(`routing.py:41-49`).

### 8. Invalid or absent observations can appear as good traffic/weather

- **Reproduced:** a TomTom HTTP 200 response containing `{}` is counted as one
  successful segment and publishes free-flow traffic (`real_producer.py:234-243`).
- **Source review:** actual visibility `0` is converted to 10 km by
  `float(current.get("visibility", 10000.0) or 10000.0)` (`real_producer.py:174`).
- **Source review:** missing weather becomes rain 0 / visibility 10 with no
  missing-data flag (`job.py:257-262`).
- Kafka send futures are not checked: `flush()` waits for completion but does
  not mean each send succeeded. Async failures can be missed.

Pending: validate mandatory fields and units, distinguish zero from missing,
preserve unknown values/quality flags, and observe publish acknowledgements.

### 9. Dashboard can regress and mislead

**Reproduced:** a 12:00 message overwrites a 12:10 message for the same segment
(`live_state.py:37-57`). There is no event-time monotonicity or duplicate guard.

**Reproduced:** at ratio 0.75 the Spark fallback calls traffic moderate, but the
map is green. The dashboard recomputes a different classification rather than
using `congestion_level` (`app.py:106-118`). Missing roads default to free-flow
and also appear green (`app.py:76-85`); averages and free-road counts include them.

Pending: use one class definition, reject older updates, indicate missing/stale
roads, and distinguish service connection from end-to-end data health.
`history` holds 360 messages total, not one hour of network observations; with
19 roads every 10 seconds that is roughly three minutes even before revisions.

### 10. Source geometry is not validated against the configured road

**Source review.** Each segment uses a straight-line midpoint to request the
nearest TomTom road fragment (`real_producer.py:223-230`). Returned geometry,
direction and coverage are not checked. One nearby fragment is applied to an
entire multi-kilometre corridor. Probes and map lines interpolate directly
between junction coordinates. Routing uses an undirected graph and estimated
lengths (`network.py:61-85`, `routing.py:43-49`).

Pending: validate/map-match source road fragments, define directionality and
real road geometry/lengths, and document the limits of segment-level coverage.
The tested network totals **97.31 km**, not the README's approximately 25 km.

### 11. Storage/recovery promises need correction

**Configuration and source review; sink recovery was blocked upstream.**

- `CHECKPOINT_DIR=/checkpoints` is ignored. Reproduced with Compose-equivalent
  environment: `config.CHECKPOINT_DIR` is `/data/checkpoints`
  (`config.py:42`), so the dedicated checkpoint volume is unused by the job.
- Kafka has no persistent data volume (`docker-compose.yml:24-48`). Removing or
  recreating its container risks losing retained events and consumer offsets.
- CSV always appends; replaying a micro-batch duplicates rows
  (`job.py:401-411`). Kafka, Parquet and CSV are separate, non-atomic writes.
- Batch-ID Parquet overwrite is only idempotent within the same checkpoint/run
  identity. A reset can reuse a batch path; revised windows in distinct batches
  have no historical upsert/deduplication contract.
- Only Parquet batch directories are bounded; CSV/checkpoint growth is not.
- Every row in a batch is placed under the first row's date, including batches
  crossing midnight (`job.py:386-398`).
- Deduplication keys use `ts_ms` while the watermark is on `event_time`
  (`job.py:148-164`); verify state eviction under a sustained run instead of
  assuming this deduplication state is bounded.

Pending: explicit durable state paths, tested restart/replay semantics, canonical
record identity, idempotent exports and lifecycle policies for each storage sink.

## Major unfinished requirements

### 12. Dynamic route suggestions are not integrated

The library works in basic scenarios. The dashboard has no origin/destination
controls, route overlays or time-saving display, and no application producer
publishes `TOPIC_ROUTES`. `routing.py`'s comment about live updates every five
seconds does not correspond to a running integration.

Pending: feed valid latest predictions into graph weights; implement route
selection, alternatives and rerouting with closure/staleness handling; publish
or otherwise serve results; demonstrate route changes on a controlled replay.

### 13. Current classification is not future congestion forecasting

The live fallback classifies the current speed ratio. The trainer also uses
current features and current labels. `ml/forecaster.py` is a classifier wrapper,
not a horizon-based forecast, and the streaming job does not call it.

Pending: define a forecast horizon appropriate to the 15-minute observation
cadence, retain independent historical observations, build time/lag/neighbor
features, and evaluate future labels against a persistence baseline. There is
only a RandomForest training implementation; the advertised GBT model is absent
from the active package. The reference implementation's forecast is not active.

### 14. Model validation can learn its own labels and leak time

**Reproduced label handling:** any existing `congestion_level`, including the
stream's fallback classification, becomes `label_source="provided"`
(`ml/train.py:120-125`). Thus training on the output lake does not create
independent ground truth, despite appearing to use supplied labels.

`randomSplit` (`train.py:173`) mixes adjacent overlapping windows and repetitions
of a cached observation between training and test data. Evaluation accepts data
without independent labels and derives the rule again; no holdout lineage check
is implemented (`ml/evaluate.py:45-48`).

Pending: preserve label/prediction provenance, independent evaluation targets,
chronological/grouped splits by source snapshot, explicit baselines, class-wise
metrics and versioned model promotion. Saving a model does not hot-reload the
already-running stream, which loads its model once.

### 15. R analytics contains incompatible semantics

**Source review, not an executed R/Shiny validation.** `r/common.R:27-32`:

- Converts `record_count` to `vehicle_count`: event messages, derived probes and
  overlapping windows are not unique real vehicles. The Shiny "Total Vehicles"
  KPI is therefore not a measured vehicle total.
- Divides class IDs by 3 only when the current dataset's maximum exceeds 1.
  Class 1 is initially interpreted as 1.0/severe; appending class 2 or 3 changes
  those historical values to 0.333. The mapping must depend on the schema, not
  which classes happen to occur in a file.
- Extracts hour/day in UTC, not Mumbai local time (`common.R:38-41`).

Shiny refresh is manual; source attribution is absent from its output.
`r/analysis.R` produces JSON, not the PNG reports mentioned in Compose comments.
Finish and execute the R data-contract/report tests after correcting exports.

### 16. Additional feature-path defects

- **Reproduced:** `create_model_features` on one row with index `[9]` returns two
  rows. A RangeIndex time-feature frame is concatenated with the original index
  (`features.py:147-149`).
- **Reproduced:** training normalization converts `flow_per_lane=8` to
  `veh_per_lane=0` when vehicle-count/lanes columns are absent; default zero wins
  before the fallback (`train.py:94-102`).
- **Reproduced:** `avg_occupancy=0.6` remains `occupancy_pct=0.6` in the Spark
  trainer, while the pandas path interprets fractional occupancy as 60%.
- **Source review:** `create_lag_features` forward-fills across roads globally
  after grouped shift (`features.py:78-86`), permitting cross-road leakage.

Normalize schemas/units once and test parity on actual lake and supported
historical inputs, including non-default indices and multiple segments.

### 17. Reproducibility, documentation and demonstration evidence

- Only four current automated tests exist. Reference tests target a different
  package/contract and are not a validation of the active application.
- `test_imports.py` catches import exceptions and still prints "All imports
  successful" and exits 0; this was observed using the dependency-free WSL Python.
- `.dockerignore` exists locally but is ignored by `.gitignore:62-63` and is not
  tracked. A clean checkout will not inherit its secret/venv/reference exclusions
  despite `Dockerfile:27` using `COPY . /app`.
- Essential current runtime/test files were already untracked at audit start;
  a clean checkout of the current commit does not contain this working tree.
- README-listed `demo_astar.py`, `demo_predict.py` and `show_lake.py` are missing;
  the scripts and docs directories had no project content at audit start.
- The application is road-corridor analytics near named railway corridors; it
  does not ingest railway GPS, timetables or train occupancy.
- Fuel/emissions reductions are motivation, not measured results. There is no
  baseline comparison, emissions model or quantified sustainability evaluation.

## Prioritized completion checklist

### Phase A: end-to-end correctness

- [ ] Fix topic migration and fail-fast initialization.
- [ ] Redesign and start the Spark query; test sparse/missing weather and late data.
- [ ] Fix test dependencies and run the default clean-image test command.
- [ ] Correct source free-flow normalization and preserve observation metadata.
- [ ] Validate one real observation through Kafka -> Spark -> Kafka + Parquet + CSV.
- [ ] Show all 19 configured roads as valid, stale or unknown on the dashboard.

### Phase B: real-data reliability

- [ ] Add snapshot IDs, source/fetch/emission timestamps, TTLs and quality flags.
- [ ] Bound failed API retries, account for quotas and check Kafka acknowledgements.
- [ ] Preserve closures/confidence; validate source fragments against intended roads.
- [ ] Correct dashboard ordering, classification and missing-road behavior.
- [ ] Fix checkpoint path, Kafka persistence, replay-safe exports and retention.
- [ ] Execute restart, duplicate, out-of-order, malformed-event and API-outage tests.

### Phase C: complete the problem statement

- [ ] Connect routing to valid live traffic and build the route-planning UI.
- [ ] Collect timestamped historical observations with clear provenance.
- [ ] Define forecast horizon, labels and temporal holdouts; implement forecasting.
- [ ] Demonstrate model save/load/inference in the Linux image and benchmark quality.
- [ ] Correct/test R analytics and generate actual reports in Mumbai local time.
- [ ] Quantify route time savings and any fuel/emissions estimates with assumptions.
- [ ] Finish runnable demo scripts, architecture/report material and accurate README claims.
- [ ] Version the complete active application and Docker exclusions.

## Suggested release acceptance tests

1. Fresh install: all expected topics exist; init exits nonzero for any failure.
2. Startup: actual Spark query starts and produces output, rather than just imports.
3. Source parity: TomTom 15/23 stays approximately 0.652 through storage and UI.
4. Freshness: a source outage becomes stale/unknown after a documented TTL.
5. Ordering: older/replayed records cannot regress the displayed segment state.
6. Data quality: malformed payloads, missing weather, zero visibility and closures
   remain distinguishable from free-flow/clear conditions.
7. Recovery: restart during each sink write; verify record identity, offsets and
   export consistency, including a midnight-crossing batch.
8. Routing: a jam/closure changes the best route; A* and Dijkstra agree on cost.
9. Forecasting: chronological held-out real observations beat or transparently
   fail to beat a persistence baseline at the declared horizon.
10. R/reporting: class mapping is stable as new classes arrive; no derived event
    count is reported as a measured unique-vehicle count.

## Audit execution notes

No application fixes were made as part of this audit. Temporary diagnostic
scripts were removed after use. Existing worktree changes were preserved.
The application Docker image was built; topic init created the congestion and
routes topics while retaining legacy input topics. No existing topics were
deleted. Services started for the audit were stopped afterward; stop-induced
exit code 137 is not presented as an application crash finding. Coverage output
was regenerated by the existing test command.

An expanded Compose configuration printed during diagnostics included the
producer's API key. The value is not repeated in this report. Treat the diagnostic
transcript as containing that credential and replace it before sharing the output.
