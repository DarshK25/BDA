"""MapYourWay live map.

The basemap is Esri's light gray canvas: coastline, creeks and parks, with
streets kept very faint and no API key. Carto's no-label tiles now return a
watermark instead of a map, so they are not used. The dark and coloured
lines are our OpenStreetMap network and our congestion scores. TomTom's own
traffic tiles stay off unless the checkbox is turned on, and then they sit
underneath our lines as a check, not as the answer.
"""
from __future__ import annotations

import json
import os
from datetime import datetime, timedelta, timezone

import pandas as pd
import streamlit as st
import streamlit.components.v1 as components

from mapyourway import config
from mapyourway.dashboard.publisher import CONFIG_PATH, start
from mapyourway.network import PLACES, get_network_summary, place_names

SUMMARY = get_network_summary()

SOURCE_LABEL = {
    "tomtom_direct": "TomTom sample on this segment",
    "tomtom_same_road": "speed ratio from the nearest TomTom sample on this same road",
    "urban_default": "not measured yet — router uses free-flow speed",
}


def _load_json(path: str) -> dict:
    if not os.path.exists(path):
        return {}
    try:
        with open(path, encoding="utf-8") as handle:
            return json.load(handle)
    except (OSError, json.JSONDecodeError):
        return {}


def _age(iso_stamp: str | None) -> str:
    if not iso_stamp:
        return "waiting"
    try:
        stamp = datetime.fromisoformat(iso_stamp)
    except ValueError:
        return "waiting"
    if stamp.tzinfo is None:
        stamp = stamp.replace(tzinfo=timezone.utc)
    seconds = max(0, int((datetime.now(timezone.utc) - stamp).total_seconds()))
    if seconds < 15:
        return f"{seconds} s ago"
    return f"{seconds // 60} min {seconds % 60} s ago"


def _map_html(show_local: bool, show_tomtom: bool, origin: str, destination: str) -> str:
    tomtom_url = "null"
    if show_tomtom:
        tile_url = config.tomtom_flow_tile_url()
        if tile_url:
            tomtom_url = json.dumps(tile_url)
    local_flag = "true" if show_local else "false"
    major_places = {
        "Borivali", "Kandivali", "Malad", "Goregaon", "Andheri", "Bandra",
        "BKC", "Ghatkopar", "Dadar", "Worli", "Fort", "Nariman Point", "Colaba",
    }
    places = [
        {"name": place["name"], "lat": place["lat"], "lon": place["lon"]}
        for place in PLACES
        if place["name"] in major_places or place["name"] in {origin, destination}
    ]
    return f"""
    <!DOCTYPE html>
    <html>
    <head>
      <meta charset="utf-8" />
      <style>
        html, body, #map {{ margin:0; height:100%; background:#e6e4e0; }}
        #status {{
          position:absolute; left:10px; bottom:8px; z-index:800;
          background:rgba(255,255,255,0.92); color:#1c2430;
          font:12px/1.3 sans-serif; padding:4px 8px; border-radius:4px;
        }}
        .place-label {{
          background:rgba(255,255,255,0.88); border:0; box-shadow:none;
          color:#273444; font:600 11px/1.1 sans-serif; padding:2px 4px;
        }}
        .place-label::before {{ display:none; }}
        .trip-label {{
          background:#17233c; color:white; border:0; box-shadow:none;
          font:700 12px/1.1 sans-serif; padding:4px 7px;
        }}
        .trip-label::before {{ display:none; }}
      </style>
    </head>
    <body>
      <div id="map"></div>
      <div id="status">Loading streets…</div>
      <script>
        const SHOW_LOCAL = {local_flag};
        const TOMTOM_URL = {tomtom_url};
        const PLACES = {json.dumps(places)};
        const ORIGIN = {json.dumps(origin)};
        const DESTINATION = {json.dumps(destination)};
        const LIVE = {{}};
        let roadsLayer = null;
        let corridorLines = [];
        window.__ok = 0;
        window.__fail = '';

        function roadStyle(feature) {{
          const props = feature.properties || {{}};
          const info = LIVE[feature.id] || LIVE[props.segment_id];
          if (!info) {{
            return {{color:'#8a929b', weight: 1.15, opacity: 0.42}};
          }}
          return {{
            color: info.color || '#1f8f4d',
            weight: Math.max(2.4, info.width || 2),
            opacity: info.opacity == null ? 0.95 : info.opacity
          }};
        }}

        function setLine(current, coordinates, options) {{
          if (current) {{
            current.remove();
          }}
          if (!coordinates || !coordinates.length) return null;
          const latlngs = coordinates.map((point) => [point[1], point[0]]);
          return window.L.polyline(latlngs, options).addTo(window.__map);
        }}

        async function refresh() {{
          try {{
            const response = await fetch('/app/static/view.json?t=' + Date.now());
            if (!response.ok) return;
            const view = await response.json();
            Object.keys(LIVE).forEach((key) => delete LIVE[key]);
            Object.assign(LIVE, view.segments || {{}});
            if (roadsLayer) roadsLayer.setStyle(roadStyle);
            const route = view.route || {{}};
            corridorLines.forEach((line) => line.remove());
            corridorLines = [];
            (route.corridors || []).forEach((corridor) => {{
              const halo = setLine(null, corridor.line, {{
                color: corridor.color || '#143d8f',
                weight: corridor.role === 'fastest' ? 9 : 6,
                opacity: corridor.role === 'fastest' ? 0.8 : 0.6,
                dashArray: corridor.role === 'fastest' ? null : '8 9'
              }});
              if (halo) corridorLines.push(halo);
            }});
            if (roadsLayer) roadsLayer.bringToFront();
            const measured = view.sampled_total || 0;
            document.getElementById('status').textContent =
              window.__ok + ' streets · ' + measured + ' live samples · ' +
              (route.corridors || []).length + ' routes';
          }} catch (err) {{
            window.__fail = String(err && err.message || err);
          }}
        }}

        (async function () {{
          try {{
            const cssText = await (await fetch('/app/static/leaflet.css')).text();
            const styleTag = document.createElement('style');
            styleTag.textContent = cssText;
            document.head.appendChild(styleTag);
            window.eval(await (await fetch('/app/static/leaflet.js')).text());
            const L = window.L || window.leaflet;
            window.L = L;
            const map = L.map('map', {{
              zoomControl: true,
              attributionControl: true,
              preferCanvas: true
            }}).setView([19.07, 72.86], 11);
            window.__map = map;
            L.tileLayer(
              'https://server.arcgisonline.com/ArcGIS/rest/services/Canvas/World_Light_Gray_Base/MapServer/tile/{{z}}/{{y}}/{{x}}',
              {{
                maxZoom: 16,
                attribution: 'Basemap © Esri · Roads © OpenStreetMap'
              }}
            ).addTo(map);
            if (TOMTOM_URL) {{
              L.tileLayer(TOMTOM_URL, {{opacity: 0.5, maxZoom: 18}}).addTo(map);
            }}
            PLACES.forEach((place) => {{
              const selected = place.name === ORIGIN || place.name === DESTINATION;
              const color = place.name === ORIGIN ? '#2563eb' :
                (place.name === DESTINATION ? '#dc2626' : '#475569');
              const marker = L.circleMarker([place.lat, place.lon], {{
                radius: selected ? 7 : 3,
                color: color,
                weight: selected ? 3 : 1,
                fillColor: selected ? '#ffffff' : color,
                fillOpacity: selected ? 1 : 0.7
              }}).addTo(map);
              marker.bindTooltip(
                (place.name === ORIGIN ? 'START · ' : (place.name === DESTINATION ? 'END · ' : '')) + place.name,
                {{
                  permanent: true,
                  direction: selected ? 'top' : 'right',
                  offset: selected ? [0, -7] : [4, 0],
                  className: selected ? 'trip-label' : 'place-label'
                }}
              );
            }});
            const geojson = await (await fetch('/app/static/network.geojson')).json();
            if (!SHOW_LOCAL) {{
              geojson.features = geojson.features.filter((feature) => !feature.properties.local);
            }}
            window.__ok = geojson.features.length;
            roadsLayer = L.geoJSON(geojson, {{
              style: roadStyle,
              interactive: false
            }}).addTo(map);
            await refresh();
            setInterval(refresh, 5000);
          }} catch (err) {{
            window.__fail = String(err && err.stack || err);
            document.getElementById('status').textContent = 'Map failed: ' + (err && err.message || err);
          }}
        }})();
      </script>
    </body>
    </html>
    """


def _live_panels(origin: str, destination: str) -> None:
    latest = _load_json(config.LATEST_JSON)
    view = _load_json(os.path.join(os.path.dirname(__file__), "static", "view.json"))
    segments = latest.get("segments") or {}
    measured = [row for row in segments.values() if row.get("speed_source") in {"tomtom_direct", "tomtom_same_road"}]
    severe = [row for row in measured if row.get("level") == "severe"]
    avg_speed = sum(row["speed_kmph"] for row in measured) / len(measured) if measured else None
    weather = latest.get("weather") or {}
    model = latest.get("model") or {}

    c1, c2, c3, c4, c5 = st.columns(5)
    c1.metric("Spark update", _age(latest.get("updated_at")))
    c2.metric("Roads measured", f"{len(measured)}")
    c3.metric("Severe measured roads", f"{len(severe)}")
    c4.metric("Avg measured speed", "—" if avg_speed is None else f"{avg_speed:.0f} km/h")
    rain = weather.get("rain_mm_hr")
    temp = weather.get("temp_c")
    c5.metric("Mumbai weather", "—" if temp is None else f"{temp} °C", f"rain {rain} mm" if rain is not None else None)

    route = view.get("route") or {}
    if route.get("error"):
        st.warning(f"No connected drive from {origin} to {destination}: {route['error']}")
    elif route.get("best_min") is not None and route.get("origin") == origin and route.get("destination") == destination:
        left, right = st.columns(2)
        with left:
            st.markdown("**Usual route, driven at today's speeds**")
            st.write(route.get("usual_label") or "—")
            st.write(f"{route['usual_min']} min")
        with right:
            if route.get("reroute"):
                st.markdown("**Suggested route**")
                st.write(route.get("best_label") or "—")
                st.write(f"{route['best_min']} min · saves {route['saving_min']} min")
            else:
                st.markdown("**Suggested route**")
                st.write("The usual route is still the fastest.")
                st.write(f"{route['best_min']} min")
        corridors = route.get("corridors") or []
        if corridors:
            st.markdown("**Other complete drives, timed at the same live speeds**")
            role_name = {"fastest": "Fastest now", "usual": "Usual road", "alternate": "Another way"}
            for index, corridor in enumerate(corridors, start=1):
                st.write(
                    f"{index}. {role_name.get(corridor.get('role'), 'Another way')} · "
                    f"{corridor.get('minutes')} min · {corridor.get('km')} km · {corridor.get('label')}"
                )
        st.caption(
            f"A* expanded {route.get('expanded_astar', '—')} junctions; "
            f"Dijkstra expanded {route.get('expanded_dijkstra', '—')}."
        )
    else:
        st.info("The router is waiting for the first Spark batch.")

    if model:
        if model.get("kind") == "random_forest":
            steady = ""
            if model.get("mae") is not None and model.get("mae") < 0.01:
                steady = " It is near zero right now because speeds are barely changing between batches, which is normal at a quiet hour."
            st.caption(
                f"Next-step model: random forest trained on {model.get('rows')} live rows "
                f"from this stream. Holdout MAE {model.get('mae')} congestion points "
                f"(0 is perfect, 1 is the whole scale). This is not a published benchmark.{steady}"
            )
        else:
            st.caption(
                f"Next-step model: persistence ({model.get('rows', 0)} rows so far). "
                "A random forest replaces it after a few hundred real observations."
            )

    if measured:
        table = pd.DataFrame(measured)
        table = table.sort_values("congestion", ascending=False).head(12)
        table["how we know"] = table["speed_source"].map(SOURCE_LABEL)
        st.markdown("**Slowest measured roads right now**")
        st.dataframe(
            table[["name", "level", "speed_kmph", "free_flow_kmph", "occupancy_pct", "congestion", "forecast", "how we know"]],
            use_container_width=True,
            hide_index=True,
        )
    else:
        st.info("No TomTom sample has landed yet. Pale green streets are being treated as free-flowing until a sample arrives.")


def main() -> None:
    st.set_page_config(page_title="MapYourWay Mumbai", layout="wide")
    names = place_names()
    st.sidebar.title("Trip")
    if len(names) < 2:
        st.sidebar.error("The road network has not been built yet.")
        origin = destination = ""
    else:
        origin = st.sidebar.selectbox("From", names, index=names.index("Borivali") if "Borivali" in names else 0)
        destination = st.sidebar.selectbox(
            "To", names, index=names.index("Colaba") if "Colaba" in names else len(names) - 1
        )
    show_local = st.sidebar.checkbox("Show local streets", value=True)
    show_tomtom = st.sidebar.checkbox("Show provider traffic overlay", value=False)
    if show_tomtom and not config.tomtom_flow_tile_url():
        st.sidebar.warning("TomTom overlay needs TOM_TOM_API_KEY.")
    st.sidebar.caption(
        f"{SUMMARY.get('num_roads', 0)} road segments · {SUMMARY.get('num_junctions', 0)} junctions · "
        f"{SUMMARY.get('total_length_km', 0)} km from OpenStreetMap."
    )
    st.sidebar.markdown(
        "Unused streets are gray. Roads on the candidate routes are green, yellow, orange, or red "
        "using live traffic where sampled and free-flow where no sample exists. "
        "The solid blue border is the fastest route; dashed borders are alternatives."
    )

    os.makedirs(os.path.dirname(CONFIG_PATH), exist_ok=True)
    temporary = CONFIG_PATH + ".tmp"
    with open(temporary, "w", encoding="utf-8") as handle:
        json.dump({"origin": origin, "destination": destination, "show_local": show_local}, handle)
    os.replace(temporary, CONFIG_PATH)
    start()

    st.title("MapYourWay — Mumbai")
    st.caption(
        "Basemap is Esri's light gray canvas: Mumbai's coast, creeks and parks, with streets kept faint and no API key. "
        "CARTO's no-label tiles now draw an \"API key required\" watermark, so they are not used. "
        "Gray streets are outside the selected trip. Route streets carry the congestion colour. "
        "The solid blue border is fastest; dashed borders are alternatives. "
        "Road geometry © OpenStreetMap contributors. Basemap © Esri."
    )
    components.html(_map_html(show_local, show_tomtom, origin, destination), height=680, scrolling=False)

    @st.fragment(run_every=timedelta(seconds=5))
    def _refresh() -> None:
        _live_panels(origin, destination)

    _refresh()


main()
