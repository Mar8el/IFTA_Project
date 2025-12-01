from fastapi import FastAPI, Query
from pydantic import BaseModel
from typing import List
import random
from datetime import datetime
import pytz

app = FastAPI()

eastern_timezone = pytz.timezone("US/Eastern")

# Oregon instead of OR and Indiana instead of IN. To avoid potential issues with dbt models later 
STATES = ["AL", "AR", "AZ", "CA", "CO", "CT", "DE", "FL", "GA", "ID", "IL", "INDIANA", "IA",
          "KS", "KY", "LA", "ME", "MD", "MA", "MI", "MN", "MS", "MO", "MT", "NE", "NV",
          "NH", "NJ", "NM", "NY", "NC", "ND", "OH", "OK", "OREGON", "PA", "RI", "SC", "SD",
          "TN", "TX", "UT", "VT", "VA", "WA", "WV", "WI", "WY"]

class StateMeters(BaseModel):
    state: str
    meters: float | None

class VehicleDay(BaseModel):
    vehicle_id: str
    total_meters: float
    per_state: List[StateMeters]

def abcd(vehicles: int = 500):
    results = []
    for i in range(vehicles):
        vehicle_id = f"VEH-{i:04d}"
        total_meters = round(max(50000, min(random.gauss(507750, 150000), 965500)), 1)
        states = random.sample(STATES, 3)  # Pick exactly 3 states
        meters = [round(total_meters / 3, 1) for _ in range(3)]
        if meters:
            meters[-1] = round(total_meters - sum(meters[:-1]), 1)
        
        per_state = [{"state": s, "meters": None} for s in STATES]
        for s, m in zip(states, meters):
            per_state[STATES.index(s)] = {"state": s, "meters": m}
        
        results.append({
            "vehicle_id": vehicle_id,
            "total_meters": total_meters,
            "per_state": per_state
        })
    
    return results

@app.get("/abcd", response_model=List[VehicleDay])
def abcd_endpoint(vehicles: int = Query(500, ge=1, le=5000)):
    return abcd(vehicles)
