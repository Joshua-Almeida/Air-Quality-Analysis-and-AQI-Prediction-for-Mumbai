"""
Small read-only API between the Spark output files and the React dashboard.

Spark writes Delta tables to output/delta/. The `deltalake` library reads them
here without starting Spark, and it only sees *committed* versions, so the
dashboard never shows a half-written micro-batch.

Run on your laptop (from the project root):
    uvicorn api.server:app --host 127.0.0.1 --port 8077
Then open http://localhost:5173 (React dev server) or, after `npm run build`
in dashboard/, http://127.0.0.1:8077 directly.
"""
import json
import math
import os
from datetime import datetime, timezone

import pandas as pd
from deltalake import DeltaTable
from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from fastapi.staticfiles import StaticFiles

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DELTA_DIR = os.path.join(ROOT, "output", "delta")
BATCH_DIR = os.path.join(ROOT, "output", "batch")
ML_DIR = os.path.join(ROOT, "output", "ml")
DASHBOARD_DIST = os.path.join(ROOT, "dashboard", "dist")

TABLES = ["city_hourly", "city_rolling_24h", "top10_ranking", "alerts"]
BATCH_FILES = {
    "pm25_by_month", "pm25_by_hour", "lockdown_by_city", "lockdown_overall",
    "mumbai_vs_delhi", "city_overall", "validation_report",
}

app = FastAPI(title="India AQI stream API")
app.add_middleware(CORSMiddleware, allow_origins=["*"], allow_methods=["GET"], allow_headers=["*"])

# Re-reading a table only when its Delta version changes keeps polling cheap.
_cache = {}


def read_table(name):
    """Latest committed snapshot of a Delta table, or an empty frame if it does not exist yet."""
    path = os.path.join(DELTA_DIR, name)
    if not os.path.exists(os.path.join(path, "_delta_log")):
        return pd.DataFrame()
    table = DeltaTable(path)
    version = table.version()
    cached = _cache.get(name)
    if cached and cached[0] == version:
        return cached[1]
    df = table.to_pandas()
    # Spark stored the CPCB local times as UTC labels; drop the zone so "13:00" stays "13:00"
    for col in df.columns:
        if isinstance(df[col].dtype, pd.DatetimeTZDtype):
            df[col] = df[col].dt.tz_localize(None)
    _cache[name] = (version, df)
    return df


def records(df):
    """DataFrame -> JSON-safe list of dicts (timestamps as text, NaN as null)."""
    out = []
    for row in df.to_dict(orient="records"):
        clean = {}
        for k, v in row.items():
            if isinstance(v, pd.Timestamp):
                clean[k] = v.strftime("%Y-%m-%d %H:%M")
            elif isinstance(v, float) and math.isnan(v):
                clean[k] = None
            elif hasattr(v, "item"):          # numpy scalar -> python
                clean[k] = v.item()
            else:
                clean[k] = v
        out.append(clean)
    return out


@app.get("/api/status")
def status():
    hourly = read_table("city_hourly")
    rolling = read_table("city_rolling_24h")
    top10 = read_table("top10_ranking")
    alerts = read_table("alerts")
    tables = {}
    for name in TABLES:
        path = os.path.join(DELTA_DIR, name)
        if os.path.exists(os.path.join(path, "_delta_log")):
            tables[name] = DeltaTable(path).version()
    last_update = None
    if not top10.empty and "updated_at" in top10:
        last_update = top10["updated_at"].max()
    return {
        "server_time": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
        "latest_hour": records(pd.DataFrame({"t": [hourly["hour"].max()]}))[0]["t"] if not hourly.empty else None,
        "latest_window_end": records(pd.DataFrame({"t": [rolling["window_end"].max()]}))[0]["t"] if not rolling.empty else None,
        "first_hour": records(pd.DataFrame({"t": [hourly["hour"].min()]}))[0]["t"] if not hourly.empty else None,
        "hours_streamed": int(hourly["hour"].nunique()) if not hourly.empty else 0,
        "cities": int(hourly["city"].nunique()) if not hourly.empty else 0,
        "alerts_total": int(len(alerts)),
        "last_ranking_update_utc": last_update.strftime("%Y-%m-%dT%H:%M:%SZ") if last_update is not None else None,
        "table_versions": tables,
    }


@app.get("/api/cities/latest")
def cities_latest():
    """Latest hourly AQI for every city, plus its rolling 24h average and a 24-hour trail."""
    hourly = read_table("city_hourly")
    if hourly.empty:
        return []
    hourly = hourly.sort_values("hour")
    latest = hourly.groupby("city").tail(1).set_index("city")

    rolling = read_table("city_rolling_24h")
    if not rolling.empty:
        last_roll = rolling.sort_values("window_end").groupby("city").tail(1).set_index("city")
        latest["avg_aqi_24h"] = last_roll["avg_aqi"]
        latest["avg_pm25_24h"] = last_roll["avg_pm25"]

    trail = hourly.groupby("city").tail(24).groupby("city")["avg_aqi"].apply(list)
    latest["trail"] = trail
    latest = latest.reset_index().sort_values("avg_aqi", ascending=False)
    return records(latest)


@app.get("/api/ranking")
def ranking():
    top10 = read_table("top10_ranking")
    if top10.empty:
        return []
    return records(top10.sort_values("rank"))


@app.get("/api/alerts")
def alerts(limit: int = 60):
    df = read_table("alerts")
    if df.empty:
        return {"total": 0, "items": []}
    df = df.sort_values(["event_time", "aqi"], ascending=[False, False])
    return {"total": int(len(df)), "items": records(df.head(limit))}


@app.get("/api/city/{name}/history")
def city_history(name: str, hours: int = 72):
    hourly = read_table("city_hourly")
    rolling = read_table("city_rolling_24h")
    h = hourly[hourly["city"] == name].sort_values("hour").tail(hours) if not hourly.empty else hourly
    r = rolling[rolling["city"] == name].sort_values("window_end").tail(hours) if not rolling.empty else rolling
    if h.empty and r.empty:
        raise HTTPException(404, f"No streamed data for {name} yet")
    return {
        "city": name,
        "hourly": records(h[["hour", "avg_aqi", "max_aqi", "avg_pm25", "stations"]]) if not h.empty else [],
        "rolling": records(r[["window_end", "avg_pm25", "avg_aqi"]]) if not r.empty else [],
    }


@app.get("/api/batch/{name}")
def batch_result(name: str):
    if name not in BATCH_FILES:
        raise HTTPException(404, f"Unknown batch result {name}")
    path = os.path.join(BATCH_DIR, f"{name}.csv")
    if not os.path.exists(path):
        raise HTTPException(404, f"{name}.csv not found - run spark_jobs/batch_analysis.py")
    return records(pd.read_csv(path))


@app.get("/api/ml")
def ml_results():
    path = os.path.join(ML_DIR, "metrics.json")
    if not os.path.exists(path):
        raise HTTPException(404, "Model not trained yet - run spark_jobs/train_model.py")
    with open(path) as f:
        metrics = json.load(f)
    fi = pd.read_csv(os.path.join(ML_DIR, "feature_importance.csv"))
    cm = pd.read_csv(os.path.join(ML_DIR, "confusion_matrix.csv"))
    return {
        "metrics": metrics,
        "feature_importance": records(fi),
        "confusion": {"labels": [c for c in cm.columns if c != "actual"], "rows": records(cm)},
    }


# Serve the built React app (dashboard/dist) at "/" when it exists
if os.path.isdir(DASHBOARD_DIST):
    app.mount("/", StaticFiles(directory=DASHBOARD_DIST, html=True), name="dashboard")
