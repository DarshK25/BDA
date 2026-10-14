from mapyourway.network import SEGMENT_BY_ID
from mapyourway.routing import compare

free = compare("Borivali", "Colaba", {})
print("FREE", round(free["best"].minutes, 1), "min", free["best"].label()[:180])
slow = {
    segment_id: 8.0
    for segment_id, segment in SEGMENT_BY_ID.items()
    if "western express" in segment["name"].lower() or "sea link" in segment["name"].lower()
}
jammed = compare("Borivali", "Colaba", slow)
print("JAMMED WEH", round(jammed["best"].minutes, 1), "min", "reroute", jammed["reroute"])
print(jammed["best"].label()[:240])
