"""Live dashboard (Streamlit). Open http://localhost:8501

Reads the congestion_updates Kafka topic that the Spark job writes to and shows:
  * a live map - every road coloured by congestion level
  * KPIs, a city-wide congestion trend and a table of the worst roads
  * dynamic route suggestion: pick origin/destination, see usual vs. best route right now
"""
from __future__ import annotations

import os
import time

import pandas as pd
import plotly.graph_objects as go
import streamlit as st

from cgls import config
from cgls import routing
from cgls.dashboard.live_state import LiveState, start_demo_feed, start_kafka_consumer
from cgls.network import NODES, SEGMENT_BY_ID, SEGMENTS
from cgls.traffic_model import LEVEL_NAMES

COLORS = ["#2e9d4f", "#f2c200", "#f07f1e", "#d62828"]    # free, moderate, heavy, severe

st.set_page_config(page_title="Smart City Traffic - Live", layout="wide", page_icon="🚦")


@st.cache_resource
def get_state() -> LiveState:
    state = LiveState()
    if os.getenv("CGLS_DEMO") == "1":
        start_demo_feed(state)
    else:
        start_kafka_consumer(state, config.KAFKA_BOOTSTRAP)
    return state


def build_map(latest: dict, highlight: dict | None) -> go.Figure:
    fig = go.Figure()
    for s in SEGMENTS:
        m = latest.get(s["segment_id"])
        lvl = m["level"] if m else None
        color = COLORS[lvl] if lvl is not None else "#9aa0a6"
        hover = (f"<b>{s['name']}</b> ({s['segment_id']})<br>"
                 + (f"{LEVEL_NAMES[lvl]} (confidence {m['confidence']:.0%})<br>speed {(m.get('avg_speed_kmph') if m.get('avg_speed_kmph') is not None else float('nan')):.0f} km/h"
                    f" (free-flow {s['free_flow_kmph']:.0f})<br>occupancy {m['occupancy_pct']:.0f}%<br>"
                    f"in 60 s: {m['forecast_level_name']}" if m else "no data yet"))
        fig.add_trace(go.Scattermapbox(lat=[s["lat1"], s["lat2"]], lon=[s["lon1"], s["lon2"]], mode="lines",
                                       line=dict(width=7, color=color), hoverinfo="text", text=hover, showlegend=False))
    if highlight:
        for key, color, width in (("usual", "#555555", 4), ("best", "#1d4ed8", 5)):
            r = highlight.get(key)
            if r is None or (key == "usual" and not highlight["reroute"]):
                continue
            lats = [NODES[n][0] for n in r.nodes]
            lons = [NODES[n][1] for n in r.nodes]
            fig.add_trace(go.Scattermapbox(lat=lats, lon=lons, mode="lines", line=dict(width=width, color=color),
                                           hoverinfo="skip", showlegend=False,
                                           name="usual" if key == "usual" else "suggested"))
    fig.add_trace(go.Scattermapbox(lat=[v[0] for v in NODES.values()], lon=[v[1] for v in NODES.values()],
                                   mode="markers+text", text=list(NODES), textposition="top right",
                                   marker=dict(size=9, color="#111"), hoverinfo="skip", showlegend=False))
    fig.update_layout(mapbox=dict(style="open-street-map", center=dict(lat=19.12, lon=72.87), zoom=10.3),
                      margin=dict(l=0, r=0, t=0, b=0), height=640)
    return fig


def speeds_for_routing(latest: dict, use_forecast: bool) -> dict[str, float]:
    out = {}
    for sid, m in latest.items():
        ff = SEGMENT_BY_ID[sid]["free_flow_kmph"]
        score = m["forecast_score"] if use_forecast else m["score"]
        out[sid] = ff * (1.0 - 0.8 * score)       # same relation the model was trained on
    return out


# ------------------------------------------------------------------ sidebar (outside the refreshing fragment)
st.sidebar.title("🚦 Route planner")
names = sorted(NODES)
origin = st.sidebar.selectbox("From", names, index=names.index("Andheri"))
dest = st.sidebar.selectbox("To", names, index=names.index("Dadar"))
use_fc = st.sidebar.checkbox("Plan with the 60-second forecast", value=False)
weight_model = st.sidebar.radio("Edge-weight model", ["speed (length / live speed)", "penalty (1 + a·congestion + b·weather)"])
st.sidebar.caption("Roads are re-priced every 5 s from the live Spark output. If a faster road exists "
                   "the suggestion changes by itself.")
state = get_state()
st.title("Smart City Traffic - live congestion & routing")
st.caption(f"Pipeline: simulated GPS + IoT + weather → Kafka → Spark Structured Streaming (MLlib) → Kafka → this page"
           + ("   |   **DEMO MODE (no Kafka)**" if state.mode == "demo" else ""))


@st.fragment(run_every="3s")
def live():
    latest, history, n_msgs, err = state.snapshot()
    if not latest:
        st.info("Waiting for the first result from Spark (it takes about 15-30 s after start-up) ..."
                + (f"\n\nKafka consumer: {err}" if err else ""))
        return

    newest = max(m["window_end_ms"] for m in latest.values())
    age = time.time() - newest / 1000.0
    levels = [m["level"] for m in latest.values()]
    speeds = [m["avg_speed_kmph"] for m in latest.values() if m.get("avg_speed_kmph") is not None]
    k1, k2, k3, k4, k5 = st.columns(5)
    k1.metric("Roads reporting", f"{len(latest)}/{len(SEGMENTS)}")
    k2.metric("Heavy / severe", sum(1 for x in levels if x >= 2))
    k3.metric("Avg speed", f"{sum(speeds) / len(speeds):.0f} km/h" if speeds else "-")
    k4.metric("Rain", f"{sum(m['rain_mm_hr'] for m in latest.values()) / len(latest):.1f} mm/h")
    k5.metric("Data age", f"{age:.0f} s", help="seconds since the end of the newest 30-second window")

    if origin == dest:
        cmp_ = None
    elif weight_model.startswith("speed"):
        cmp_ = routing.compare(origin, dest, speeds_for_routing(latest, use_fc))
    else:
        scores = {sid: (m["forecast_score"] if use_fc else m["score"]) for sid, m in latest.items()}
        wx = routing.weather_penalty(sum(m["rain_mm_hr"] for m in latest.values()) / len(latest),
                                     sum(m["visibility_km"] for m in latest.values()) / len(latest))
        cmp_ = routing.compare_graph(origin, dest, routing.build_graph_penalty(scores, wx))

    suspected = [m for m in latest.values() if m.get("incident_suspected")]
    for m in suspected:
        st.error(f"🚨 **Suspected incident on {m['name']}** - speed collapsed while sensor occupancy jumped.")

    left, right = st.columns([3, 2])
    with left:
        st.plotly_chart(build_map(latest, cmp_), use_container_width=True, key=f"map{int(time.time())}")
        st.caption("Green free · yellow moderate · orange heavy · red severe.  "
                   "Blue line = suggested route, grey = usual route (only drawn when they differ).")
    with right:
        st.subheader(f"{origin} → {dest}")
        if cmp_ is None:
            st.write("Pick two different places.")
        else:
            u, b = cmp_["usual"], cmp_["best"]
            if cmp_["reroute"]:
                st.success(f"**Take the alternate route - saves {cmp_['saving_min']:.1f} min**")
            else:
                st.info("The usual route is still the best choice right now.")
            c1, c2 = st.columns(2)
            c1.metric("Usual route now", f"{u.minutes:.0f} min", help=f"{u.km:.1f} km")
            c2.metric("Best route now", f"{b.minutes:.0f} min", help=f"{b.km:.1f} km")
            st.write(f"**Best:** {b.label()}  ({b.km:.1f} km)")
            st.write(f"**Usual:** {u.label()}  ({u.km:.1f} km)")
            st.caption(f"A* expanded {cmp_['expanded_astar']} junctions, Dijkstra {cmp_['expanded_dijkstra']} (same answer).")
            alt = pd.DataFrame([{"route": r.label(), "minutes": round(r.minutes, 1), "km": round(r.km, 1)}
                                for r in cmp_["alternatives"]])
            st.dataframe(alt, hide_index=True, use_container_width=True)

        if len(history) > 2:
            st.subheader("City-wide congestion score")
            df = pd.DataFrame(history, columns=["window_end_ms", "congestion score", "speed ratio"])
            df["time"] = pd.to_datetime(df["window_end_ms"], unit="ms")
            st.line_chart(df.set_index("time")[["congestion score"]], height=170)

    st.subheader("Roads ranked by congestion")
    rows = [{"road": m["name"], "now": m["level_name"], "speed km/h": m.get("avg_speed_kmph"),
             "free-flow": m["free_flow_kmph"], "occupancy %": m["occupancy_pct"],
             "in 60 s": m["forecast_level_name"], "forecast by": m.get("forecast_source", "-"), "confidence": round(m["confidence"], 2),
             "GPS pings": m["gps_pings"]} for m in latest.values()]
    st.dataframe(pd.DataFrame(rows).sort_values("occupancy %", ascending=False),
                 hide_index=True, use_container_width=True)
    st.caption(f"{n_msgs} updates received · Kafka consumer: {'OK' if not err else err}")


live()
