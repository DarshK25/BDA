"""
Network topology definition for the CGLS system.
Defines 13 junctions and 19 roads forming the city traffic network.
"""

from typing import Dict, List, Tuple, Optional
from dataclasses import dataclass
import json


@dataclass
class Junction:
    """Represents an intersection in the traffic network."""
    id: str
    name: str
    lat: float
    lon: float
    traffic_light: bool = True


@dataclass
class Road:
    """Represents a road segment connecting two junctions."""
    id: str
    name: str
    from_junction: str
    to_junction: str
    length_km: float
    lanes: int
    speed_limit_kmh: float
    capacity_vehicles: int


# ============================================================================
# JUNCTION DEFINITIONS (13 junctions)
# ============================================================================

JUNCTIONS: Dict[str, Junction] = {
    "J1": Junction("J1", "North Plaza", 40.7580, -73.9855, True),
    "J2": Junction("J2", "Central Square", 40.7489, -73.9680, True),
    "J3": Junction("J3", "East Gate", 40.7489, -73.9500, True),
    "J4": Junction("J4", "South Terminal", 40.7300, -73.9680, True),
    "J5": Junction("J5", "West Hub", 40.7489, -73.9860, True),
    "J6": Junction("J6", "Market Cross", 40.7420, -73.9680, True),
    "J7": Junction("J7", "University Corner", 40.7580, -73.9680, True),
    "J8": Junction("J8", "Harbor Junction", 40.7300, -73.9860, True),
    "J9": Junction("J9", "Airport Link", 40.7300, -73.9500, True),
    "J10": Junction("J10", "Industrial Park", 40.7200, -73.9680, True),
    "J11": Junction("J11", "Shopping District", 40.7380, -73.9770, True),
    "J12": Junction("J12", "Tech Park", 40.7580, -73.9770, True),
    "J13": Junction("J13", "Medical Center", 40.7380, -73.9590, True),
}


# ============================================================================
# ROAD DEFINITIONS (19 roads)
# ============================================================================

ROADS: Dict[str, Road] = {
    # North-South arterials
    "R1": Road("R1", "Broadway North", "J1", "J7", 1.2, 3, 50, 180),
    "R2": Road("R2", "Broadway South", "J7", "J2", 1.5, 3, 50, 180),
    "R3": Road("R3", "Main Street", "J2", "J6", 1.0, 4, 60, 240),
    "R4": Road("R4", "South Avenue", "J6", "J4", 1.8, 3, 50, 180),
    
    # East-West arterials
    "R5": Road("R5", "Northern Boulevard", "J1", "J12", 1.0, 2, 45, 120),
    "R6": Road("R6", "Central Drive", "J7", "J2", 1.3, 4, 60, 240),
    "R7": Road("R7", "Market Road", "J2", "J3", 1.5, 3, 50, 180),
    "R8": Road("R8", "Harbor Road", "J5", "J8", 2.0, 2, 40, 100),
    
    # Connecting roads
    "R9": Road("R9", "West Connector", "J5", "J2", 1.4, 3, 50, 180),
    "R10": Road("R10", "East Connector", "J2", "J3", 1.6, 3, 50, 180),
    "R11": Road("R11", "Park Avenue", "J12", "J7", 0.8, 2, 45, 120),
    "R12": Road("R12", "University Drive", "J7", "J13", 1.1, 2, 40, 100),
    "R13": Road("R13", "Shopping Loop", "J11", "J6", 0.9, 3, 45, 150),
    
    # Secondary roads
    "R14": Road("R14", "Industrial Way", "J4", "J10", 1.3, 2, 40, 100),
    "R15": Road("R15", "Airport Express", "J3", "J9", 2.5, 4, 80, 320),
    "R16": Road("R16", "Service Road", "J6", "J13", 1.0, 2, 40, 100),
    "R17": Road("R17", "Tech Boulevard", "J12", "J11", 0.7, 2, 45, 120),
    "R18": Road("R18", "Medical Avenue", "J13", "J3", 0.8, 2, 45, 120),
    "R19": Road("R19", "Harbor Loop", "J8", "J4", 1.5, 2, 40, 100),
}


# ============================================================================
# ADJACENCY GRAPH
# ============================================================================

def build_adjacency_graph() -> Dict[str, List[Tuple[str, str]]]:
    """
    Build adjacency list representation of the road network.
    Returns: Dict mapping junction_id -> [(neighbor_junction_id, road_id), ...]
    """
    graph: Dict[str, List[Tuple[str, str]]] = {j_id: [] for j_id in JUNCTIONS}
    
    for road_id, road in ROADS.items():
        graph[road.from_junction].append((road.to_junction, road_id))
        # Bidirectional roads (can be made unidirectional if needed)
        graph[road.to_junction].append((road.from_junction, road_id))
    
    return graph


ADJACENCY_GRAPH = build_adjacency_graph()


# ============================================================================
# UTILITY FUNCTIONS
# ============================================================================

def get_road_by_junctions(from_j: str, to_j: str) -> Optional[Road]:
    """Find the road connecting two junctions."""
    for road in ROADS.values():
        if (road.from_junction == from_j and road.to_junction == to_j) or \
           (road.to_junction == from_j and road.from_junction == to_j):
            return road
    return None


def get_connected_roads(junction_id: str) -> List[Road]:
    """Get all roads connected to a specific junction."""
    connected = []
    for road in ROADS.values():
        if road.from_junction == junction_id or road.to_junction == junction_id:
            connected.append(road)
    return connected


def calculate_travel_time(road: Road, current_speed_kmh: float) -> float:
    """
    Calculate travel time in minutes for a road segment.
    
    Args:
        road: Road object
        current_speed_kmh: Current average speed on the road
    
    Returns:
        Travel time in minutes
    """
    if current_speed_kmh <= 0:
        current_speed_kmh = 1.0  # Prevent division by zero
    
    time_hours = road.length_km / current_speed_kmh
    return time_hours * 60  # Convert to minutes


def get_network_summary() -> Dict:
    """Return summary statistics about the network."""
    total_length = sum(road.length_km for road in ROADS.values())
    total_capacity = sum(road.capacity_vehicles for road in ROADS.values())
    avg_speed_limit = sum(road.speed_limit_kmh for road in ROADS.values()) / len(ROADS)
    
    return {
        "num_junctions": len(JUNCTIONS),
        "num_roads": len(ROADS),
        "total_length_km": round(total_length, 2),
        "total_capacity": total_capacity,
        "avg_speed_limit_kmh": round(avg_speed_limit, 2),
        "roads_with_traffic_lights": sum(1 for j in JUNCTIONS.values() if j.traffic_light),
    }


def export_network_geojson(filepath: str) -> None:
    """Export network topology as GeoJSON for visualization."""
    features = []
    
    # Export junctions as points
    for junction in JUNCTIONS.values():
        features.append({
            "type": "Feature",
            "geometry": {
                "type": "Point",
                "coordinates": [junction.lon, junction.lat]
            },
            "properties": {
                "id": junction.id,
                "name": junction.name,
                "type": "junction",
                "traffic_light": junction.traffic_light
            }
        })
    
    # Export roads as lines
    for road in ROADS.values():
        from_j = JUNCTIONS[road.from_junction]
        to_j = JUNCTIONS[road.to_junction]
        features.append({
            "type": "Feature",
            "geometry": {
                "type": "LineString",
                "coordinates": [
                    [from_j.lon, from_j.lat],
                    [to_j.lon, to_j.lat]
                ]
            },
            "properties": {
                "id": road.id,
                "name": road.name,
                "type": "road",
                "length_km": road.length_km,
                "lanes": road.lanes,
                "speed_limit": road.speed_limit_kmh,
                "capacity": road.capacity_vehicles
            }
        })
    
    geojson = {
        "type": "FeatureCollection",
        "features": features
    }
    
    with open(filepath, 'w') as f:
        json.dump(geojson, f, indent=2)


if __name__ == "__main__":
    # Print network summary
    summary = get_network_summary()
    print("Network Summary:")
    print(json.dumps(summary, indent=2))
    
    # Example: Find connected roads
    print("\nRoads connected to Central Square (J2):")
    for road in get_connected_roads("J2"):
        print(f"  {road.id}: {road.name}")
