from fastapi import FastAPI, Response, Query
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse
from functools import lru_cache
import pandas as pd
from datetime import datetime
from typing import Optional, Tuple
import threading


app = FastAPI(title="SmartFactory IoT Stats")


# --- CORS: allow all origins ---
app.add_middleware(
   CORSMiddleware,
   allow_origins=["*"],
   allow_methods=["*"],
   allow_headers=["*"],
)


# --- Load data once (lazy, cached) ---
@lru_cache(maxsize=1)
def load_data() -> pd.DataFrame:
   df = pd.read_csv("sensors.csv")
   # Ensure required columns exist
   required = {"timestamp", "location", "sensor", "value"}
   missing = required - set(df.columns)
   if missing:
       raise ValueError(f"CSV missing columns: {missing}")


   # Parse timestamp and standardize types
   df["timestamp"] = pd.to_datetime(df["timestamp"], errors="coerce", utc=True)
   df = df.dropna(subset=["timestamp"])
   # Normalize helper columns for case-insensitive filters
   df["location_norm"] = df["location"].astype(str).str.strip().str.lower()
   df["sensor_norm"] = df["sensor"].astype(str).str.strip().str.lower()
   # Ensure numeric value
   df["value"] = pd.to_numeric(df["value"], errors="coerce")
   df = df.dropna(subset=["value"])
   return df


# --- Simple in-memory response cache ---
_cache = {}
_lock = threading.Lock()


def _cache_key(
   location: Optional[str],
   sensor: Optional[str],
   start_date: Optional[str],
   end_date: Optional[str],
) -> Tuple:
   # Normalize key for identical requests
   norm = lambda s: s.strip().lower() if isinstance(s, str) else None
   return (
       norm(location),
       norm(sensor),
       norm(start_date),
       norm(end_date),
   )


def _parse_iso(date_str: Optional[str]) -> Optional[pd.Timestamp]:
   if not date_str:
       return None
   # Accept common formats; convert to UTC
   try:
       ts = pd.to_datetime(date_str, utc=True)
       if pd.isna(ts):
           return None
       return ts
   except Exception:
       return None


@app.get("/stats")
def stats(
   response: Response,
   location: Optional[str] = Query(None),
   sensor: Optional[str] = Query(None),
   start_date: Optional[str] = Query(None, description="ISO8601, e.g. 2024-01-01 or 2024-01-01T00:00:00Z"),
   end_date: Optional[str] = Query(None, description="ISO8601, e.g. 2024-01-31 or 2024-01-31T23:59:59Z"),
):
   # Check cache
   key = _cache_key(location, sensor, start_date, end_date)
   with _lock:
       if key in _cache:
           payload = _cache[key]
           return JSONResponse(content=payload, headers={"X-Cache": "HIT"})


   df = load_data()


   # Filtering
   flt = df
   if location:
       flt = flt[flt["location_norm"] == location.strip().lower()]
   if sensor:
       flt = flt[flt["sensor_norm"] == sensor.strip().lower()]


   start_ts = _parse_iso(start_date)
   end_ts = _parse_iso(end_date)


   if start_ts is not None:
       flt = flt[flt["timestamp"] >= start_ts]
   if end_ts is not None:
       flt = flt[flt["timestamp"] <= end_ts]


   # Compute stats
   if flt.empty:
       stats_obj = {"count": 0, "avg": None, "min": None, "max": None}
   else:
       vals = flt["value"]
       stats_obj = {
           "count": int(vals.count()),
           "avg": float(vals.mean()),
           "min": float(vals.min()),
           "max": float(vals.max()),
       }


   payload = {"stats": stats_obj}


   # Store in cache and return MISS
   with _lock:
       _cache[key] = payload


   return JSONResponse(content=payload, headers={"X-Cache": "MISS"})
