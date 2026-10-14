# Live Map Quality Design

## Intent

Make the working MapYourWay dashboard feel like a stable live map rather than a
page that blinks on every data tick, and make the 19 modeled Mumbai corridors
follow real road alignments instead of drawing endpoint-to-endpoint chords.

## Goals

- Update live metrics, traffic colors, route overlays, analytics, and the table
  without rerunning the whole Streamlit page on every refresh interval.
- Preserve the user's map pan and zoom while traffic values change.
- Replace every straight two-point segment overlay with a sourced road-following
  polyline while preserving the existing segment IDs, topic contracts, node
  names, and routing behavior.
- Keep all live outputs derived from one immutable dashboard snapshot per update.
- Keep the dashboard useful during Kafka delays, stale observations, and route
  errors.

## Non-goals

- Rebuild the traffic network into every physical lane or directed OSM way.
- Change Kafka schemas, Spark feature contracts, segment IDs, or model behavior.
- Claim that a static route geometry asset makes TomTom midpoint observations
  road-perfect; source matching remains a later data-quality task.
- Add a new frontend framework or browser-side websocket layer.

## Design decisions

1. Use `st.fragment(run_every="Ns")` for the live region, with sidebar widgets
   outside the fragment. The fragment will read one `LiveState` snapshot and
   render all dynamic outputs from it.
2. Use stable widget/chart keys and Plotly `uirevision` so user controls and map
   view survive fragment reruns.
3. Store one checked-in JSON geometry asset under `src/mapyourway/`, sourced
   from OSRM's driving route geometry over OpenStreetMap road data. Coordinates
   use GeoJSON order `[longitude, latitude]`; an attribution/source record is
   included in the asset.
4. Keep the existing endpoint fields and `length_km` values initially. Geometry
   improves visual fidelity without silently changing route weights or traffic
   identities.

## Acceptance criteria

- A fragment rerun updates displayed live values without recreating the title,
  sidebar controls, or tab shell.
- A map trace for every segment contains more than two coordinates where the
  sourced route has bends, and route overlays use the same geometry as traffic
  traces.
- Existing pure-Python tests pass, plus focused tests cover geometry loading,
  trace coordinates, stable Plotly layout revision, and live snapshot rendering
  helpers.
- A clean Docker dashboard check shows message counts/fresh roads continuing to
  update while the browser map remains interactive and Spark/Kafka remain live.
