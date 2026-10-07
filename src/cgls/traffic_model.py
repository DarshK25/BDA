"""
Traffic physics model for CGLS.
Simulates congestion behavior: occupancy -> speed relationship.
"""

import math
from typing import Dict, Tuple
from dataclasses import dataclass


@dataclass
class TrafficState:
    """Represents current traffic conditions on a road segment."""
    occupancy: float  # 0.0 to 1.0 (fraction of capacity)
    speed_kmh: float  # Current average speed
    vehicle_count: int  # Number of vehicles
    flow_rate: float  # Vehicles per hour


class TrafficModel:
    """
    Models traffic flow dynamics using fundamental traffic theory.
    Based on the speed-density relationship.
    """
    
    def __init__(self, free_flow_speed: float = 50.0, jam_density: float = 1.0):
        """
        Initialize traffic model.
        
        Args:
            free_flow_speed: Speed at zero density (km/h)
            jam_density: Maximum occupancy (typically 1.0)
        """
        self.v_free = free_flow_speed
        self.k_jam = jam_density
        
    def calculate_speed(self, occupancy: float) -> float:
        """
        Calculate speed based on occupancy using Greenshields model.
        
        v = v_free * (1 - k/k_jam)
        
        Args:
            occupancy: Current occupancy (0.0 to 1.0)
        
        Returns:
            Speed in km/h
        """
        if occupancy <= 0:
            return self.v_free
        
        if occupancy >= self.k_jam:
            return 5.0  # Minimum speed in heavy congestion
        
        # Greenshields model
        speed = self.v_free * (1 - occupancy / self.k_jam)
        
        # Apply minimum speed constraint
        return max(5.0, speed)
    
    def calculate_flow(self, occupancy: float, speed: float) -> float:
        """
        Calculate traffic flow (vehicles per hour).
        
        q = k * v
        
        Args:
            occupancy: Current occupancy
            speed: Current speed (km/h)
        
        Returns:
            Flow rate (vehicles/hour)
        """
        return occupancy * speed
    
    def calculate_congestion_level(self, occupancy: float, speed: float, 
                                   free_flow_speed: float) -> float:
        """
        Calculate normalized congestion level (0.0 to 1.0).
        
        Uses both occupancy and speed reduction.
        
        Args:
            occupancy: Current occupancy
            speed: Current speed (km/h)
            free_flow_speed: Expected free-flow speed (km/h)
        
        Returns:
            Congestion level (0.0 = free flow, 1.0 = jammed)
        """
        # Speed-based component
        speed_ratio = speed / free_flow_speed if free_flow_speed > 0 else 1.0
        speed_congestion = 1.0 - speed_ratio
        
        # Occupancy-based component
        occupancy_congestion = occupancy
        
        # Weighted combination (70% speed, 30% occupancy)
        congestion = 0.7 * speed_congestion + 0.3 * occupancy_congestion
        
        # Clamp to [0, 1]
        return max(0.0, min(1.0, congestion))
    
    def get_traffic_state(self, vehicle_count: int, capacity: int, 
                         road_length_km: float) -> TrafficState:
        """
        Calculate complete traffic state for a road segment.
        
        Args:
            vehicle_count: Number of vehicles on the segment
            capacity: Maximum capacity of the segment
            road_length_km: Length of the road segment (km)
        
        Returns:
            TrafficState object with all metrics
        """
        # Calculate occupancy
        occupancy = vehicle_count / capacity if capacity > 0 else 0.0
        occupancy = min(1.0, occupancy)
        
        # Calculate speed from occupancy
        speed = self.calculate_speed(occupancy)
        
        # Calculate flow
        flow = self.calculate_flow(occupancy, speed)
        
        return TrafficState(
            occupancy=occupancy,
            speed_kmh=speed,
            vehicle_count=vehicle_count,
            flow_rate=flow
        )
    
    def predict_travel_time(self, road_length_km: float, 
                           current_speed: float) -> float:
        """
        Predict travel time for a road segment.
        
        Args:
            road_length_km: Road length (km)
            current_speed: Current average speed (km/h)
        
        Returns:
            Travel time in minutes
        """
        if current_speed <= 0:
            current_speed = 1.0
        
        time_hours = road_length_km / current_speed
        return time_hours * 60  # Convert to minutes


def simulate_congestion_buildup(initial_vehicles: int, capacity: int, 
                               duration_minutes: int, 
                               arrival_rate: float = 10.0) -> list:
    """
    Simulate congestion buildup over time.
    
    Args:
        initial_vehicles: Starting number of vehicles
        capacity: Road capacity
        duration_minutes: Simulation duration
        arrival_rate: Vehicles arriving per minute
    
    Returns:
        List of TrafficState objects over time
    """
    model = TrafficModel()
    states = []
    current_vehicles = initial_vehicles
    
    for minute in range(duration_minutes):
        # Add arriving vehicles
        current_vehicles += int(arrival_rate)
        
        # Some vehicles exit (based on speed)
        state = model.get_traffic_state(current_vehicles, capacity, 1.0)
        exit_rate = state.speed_kmh / 60.0  # Simplified exit model
        current_vehicles = max(0, current_vehicles - int(exit_rate))
        
        # Cap at capacity
        current_vehicles = min(current_vehicles, capacity)
        
        # Record state
        final_state = model.get_traffic_state(current_vehicles, capacity, 1.0)
        states.append(final_state)
    
    return states


def classify_congestion(congestion_level: float) -> str:
    """
    Classify congestion level into human-readable categories.
    
    Args:
        congestion_level: Numeric congestion (0.0 to 1.0)
    
    Returns:
        Category string
    """
    if congestion_level < 0.3:
        return "low"
    elif congestion_level < 0.6:
        return "medium"
    elif congestion_level < 0.8:
        return "high"
    else:
        return "severe"


if __name__ == "__main__":
    # Demo the traffic model
    model = TrafficModel(free_flow_speed=50.0)
    
    print("Traffic Model Demo")
    print("=" * 50)
    
    for occupancy in [0.1, 0.3, 0.5, 0.7, 0.9]:
        speed = model.calculate_speed(occupancy)
        flow = model.calculate_flow(occupancy, speed)
        congestion = model.calculate_congestion_level(occupancy, speed, 50.0)
        category = classify_congestion(congestion)
        
        print(f"\nOccupancy: {occupancy:.1%}")
        print(f"  Speed: {speed:.1f} km/h")
        print(f"  Flow: {flow:.1f} veh/h")
        print(f"  Congestion: {congestion:.2f} ({category})")
