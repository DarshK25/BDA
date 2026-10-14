# Live Map Quality Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Make the live MapYourWay dashboard update in place and render the 19 Mumbai corridors along sourced road-following polylines.

**Architecture:** Keep Kafka state, routing, segment IDs, and Spark contracts unchanged. Add a checked-in GeoJSON-shaped geometry asset loaded by `network.py`; make the dashboard consume segment geometry through one helper. Move dynamic dashboard rendering into one Streamlit fragment driven by one state snapshot, with stable widget/chart identity and Plotly map `uirevision`.

**Tech Stack:** Python 3.10/3.11, Streamlit 1.41, Plotly 5.24, NetworkX, pytest, OSRM/OSM-derived static geometry.

**Spec:** `docs/superpowers/specs/2026-10-11-live-map-quality-design.md`

## Global Constraints

- Preserve existing segment IDs (`S01`–`S19`), node names, Kafka topics, Spark output fields, and route contracts.
- Do not add a frontend framework or a runtime geometry API dependency.
- Geometry coordinates are GeoJSON order `[longitude, latitude]`.
- Keep existing `length_km` and routing weights in this visual-fidelity pass.
- Do not commit changes unless the user explicitly requests a commit.
- Verify with the project test command and live Docker/browser checks before claiming completion.

## Review Focus

- Missing or malformed geometry asset: network import must fall back to endpoint coordinates rather than fail startup; test in Task 1.
- A segment with a straight two-point geometry: rendering must still work and not assume a third vertex; test in Task 2.
- Fragment refresh with a changed sidebar control: controls must remain stable and the live region must use the current selection; test in Task 3.
- Empty/stale Kafka snapshot: the fragment must retain the waiting/stale/error behavior instead of raising; test in Task 3.
- User-panned map during traffic updates: Plotly layout must carry a stable `uirevision`; test in Task 2.

---

### Task 1: Add and load road-following segment geometry

**Files:**
- Create: `src/mapyourway/network_geometry.json`
- Modify: `src/mapyourway/network.py:9-91`
- Test: `tests/test_core.py`

**Interfaces:**
- Produces `segment["geometry"]` as a list of `[lon, lat]` pairs for every active segment.
- Preserves `lat1`, `lon1`, `lat2`, and `lon2` for compatibility.

- [ ] **Step 1: Write the failing tests**

Add tests that assert all `SEGMENTS` have a non-empty `geometry`, each point has two finite numeric values, and at least one known corridor has more than two points. Add a fallback-loader test using a missing asset path or a helper-level empty mapping.

- [ ] **Step 2: Run the focused tests to verify they fail**

Run: `venv\Scripts\python.exe -m pytest -o addopts= -p no:cacheprovider tests/test_core.py -k geometry -q`

Expected: FAIL because the active network has no `geometry` field.

- [ ] **Step 3: Add the sourced geometry asset**

Create `network_geometry.json` with one GeoJSON-style coordinate list for each
of `S01` through `S19`, plus `source`, `license`, and `retrieved_at` metadata.
Use the OSRM driving route response for each existing node pair and simplify
only by retaining route-shape vertices; do not replace the route with hand-drawn
straight lines.

- [ ] **Step 4: Implement the geometry loader and network enrichment**

Add a small loader in `network.py` that reads the sibling JSON asset, validates
coordinate shape, and returns `{}` on missing/invalid asset. Update
`_build_segments()` to set `geometry` from the asset or the two endpoint pairs.

- [ ] **Step 5: Run the focused tests to verify they pass**

Run: `venv\Scripts\python.exe -m pytest -o addopts= -p no:cacheprovider tests/test_core.py -k geometry -q`

Expected: PASS, including fallback behavior.

### Task 2: Render traffic and route overlays along geometry

**Files:**
- Modify: `src/mapyourway/dashboard/app.py:68-108`
- Test: `tests/test_core.py`

**Interfaces:**
- Add a private `_segment_coordinates(segment) -> tuple[list[float], list[float]]` helper.
- `create_traffic_map()` uses that helper for traffic and route traces.

- [ ] **Step 1: Write the failing tests**

Add a test that builds a map for one populated segment and asserts its traffic
trace uses the segment geometry coordinate count, and that the figure contains a
stable `layout.mapbox.uirevision` value.

- [ ] **Step 2: Run the focused test to verify it fails**

Run: `venv\Scripts\python.exe -m pytest -o addopts= -p no:cacheprovider tests/test_core.py -k map_geometry -q`

Expected: FAIL because the current traces contain exactly two endpoint values
and no `uirevision`.

- [ ] **Step 3: Implement geometry-aware traces**

Use `_segment_coordinates()` for all traffic and route traces. Add stable
Plotly `uirevision` at both layout and mapbox layout levels. Keep route colors,
traffic colors, node markers, hover text, and map dimensions unchanged.

- [ ] **Step 4: Run the focused test to verify it passes**

Run: `venv\Scripts\python.exe -m pytest -o addopts= -p no:cacheprovider tests/test_core.py -k map_geometry -q`

Expected: PASS.

### Task 3: Convert full-page refresh into a live fragment

**Files:**
- Modify: `src/mapyourway/dashboard/app.py:1-225`
- Test: `tests/test_core.py` or a new `tests/test_dashboard_rendering.py`

**Interfaces:**
- Keep `get_live_state()` cached and unchanged as the Kafka resource boundary.
- Add a live-region renderer that accepts `state`, `show_details`, `origin`,
  `destination`, `use_forecast`, and `run_every`-independent current controls.
- Sidebar widgets remain outside the fragment and use explicit stable keys.

- [ ] **Step 1: Write the failing test**

Add source-level or helper-level tests asserting the app no longer imports or
calls `st_autorefresh`, uses `st.fragment`, reads one snapshot for a render, and
uses stable keys for the traffic map and speed chart. Keep pure helper tests
independent of a live Streamlit server.

- [ ] **Step 2: Run the focused test to verify it fails**

Run: `venv\Scripts\python.exe -m pytest -o addopts= -p no:cacheprovider tests/test_dashboard_rendering.py -q`

Expected: FAIL because the current app uses `st_autorefresh` and renders the
whole page after the timer.

- [ ] **Step 3: Implement the fragment structure**

Remove `streamlit_autorefresh`. Keep title, sidebar controls, and tabs/placeholders
outside a single `@st.fragment(run_every=f"{refresh_rate}s")`. The fragment must
call `state.get_snapshot()` once, compute route/metrics/figures from that data,
and update stable placeholders for sidebar status, KPIs, map, analytics, and
table. Keep forecast fallback enabled as a runtime message rather than disabling
the sidebar widget based on a snapshot that the fragment owns.

- [ ] **Step 4: Run focused tests and the full local suite**

Run:
`venv\Scripts\python.exe -m pytest -o addopts= -p no:cacheprovider tests/test_dashboard_rendering.py -q`

Then:
`venv\Scripts\python.exe -m pytest -o addopts= -p no:cacheprovider --cov=src/mapyourway --cov-branch --cov-report=term-missing -q`

Expected: all tests pass.

### Task 4: Verify the integrated live dashboard

**Files:**
- Modify: only if verification finds a regression in Tasks 1–3.

- [ ] **Step 1: Validate Compose configuration**

Run: `docker compose config --quiet`

Expected: exit code 0.

- [ ] **Step 2: Rebuild/restart only services needed for source mounts**

Run the existing Compose startup for `kafka-init`, `model-init`, `spark`, and
`dashboard`, then wait for Kafka health, Spark query startup, and dashboard
health. Do not delete persisted Kafka topics or checkpoints.

- [ ] **Step 3: Check end-to-end data and UI behavior**

Confirm Spark logs show `Streaming query started`, Kafka output offsets advance,
the dashboard reports fresh roads/messages, and browser inspection shows the map
still has stable zoom/center metadata and curved segment traces. Change the
refresh slider once and confirm subsequent updates remain fragment-scoped.

- [ ] **Step 4: Review the working diff and report evidence**

Run `git status --short` and `git diff --stat`; report exact tests, Docker status,
browser checks, and any remaining data-source or topology limitations.
