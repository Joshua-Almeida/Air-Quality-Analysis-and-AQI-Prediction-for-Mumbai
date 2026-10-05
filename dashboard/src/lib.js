import { useEffect, useState } from "react";

// ---------------------------------------------------------------- AQI bands
// Official CPCB categories. Colours are the CPCB hues, lifted slightly so
// they hold contrast on a graphite ground.
export const BANDS = [
  { name: "Good", min: 0, max: 50, key: "good" },
  { name: "Satisfactory", min: 51, max: 100, key: "sat" },
  { name: "Moderate", min: 101, max: 200, key: "mod" },
  { name: "Poor", min: 201, max: 300, key: "poor" },
  { name: "Very Poor", min: 301, max: 400, key: "vpoor" },
  { name: "Severe", min: 401, max: 500, key: "severe" },
];
export const AQI_MAX = 500;
export const HOME_CITY = "Mumbai";

export function bandOf(aqi) {
  if (aqi == null || Number.isNaN(aqi)) return null;
  return BANDS.find((b) => aqi <= b.max) ?? BANDS[BANDS.length - 1];
}

export function bandByName(name) {
  return BANDS.find((b) => b.name === name) ?? null;
}

/** Position of an AQI value on the shared 0-500 rail, as a 0..1 fraction. */
export function railX(aqi) {
  return Math.max(0, Math.min(1, (aqi ?? 0) / AQI_MAX));
}

// ---------------------------------------------------------------- polling
/** Fetch a JSON endpoint now and then every `intervalMs`. */
export function usePoll(path, intervalMs = 3000) {
  const [state, setState] = useState({ data: null, error: null, loading: true });

  useEffect(() => {
    if (!path) return undefined;
    let alive = true;
    const load = async () => {
      try {
        const res = await fetch(path);
        if (!res.ok) {
          const body = await res.json().catch(() => ({}));
          throw new Error(body.detail || `${res.status} ${res.statusText}`);
        }
        const data = await res.json();
        if (alive) setState({ data, error: null, loading: false });
      } catch (err) {
        if (alive) setState((s) => ({ ...s, error: err.message, loading: false }));
      }
    };
    load();
    const id = setInterval(load, intervalMs);
    return () => {
      alive = false;
      clearInterval(id);
    };
  }, [path, intervalMs]);

  return state;
}

/** Re-render every `ms` so "updated 4 s ago" keeps counting between polls. */
export function useNow(ms = 1000) {
  const [now, setNow] = useState(() => Date.now());
  useEffect(() => {
    const id = setInterval(() => setNow(Date.now()), ms);
    return () => clearInterval(id);
  }, [ms]);
  return now;
}

// ---------------------------------------------------------------- formatting
const MONTHS = ["Jan", "Feb", "Mar", "Apr", "May", "Jun", "Jul", "Aug", "Sep", "Oct", "Nov", "Dec"];

/** "2019-11-04 06:00" -> { date: "04 Nov 2019", time: "06:00" } */
export function splitStamp(stamp) {
  if (!stamp) return { date: "--", time: "--:--" };
  const [d, t] = stamp.split(" ");
  const [y, m, day] = d.split("-");
  return { date: `${day} ${MONTHS[Number(m) - 1]} ${y}`, time: t?.slice(0, 5) ?? "" };
}

export function shortStamp(stamp) {
  const { date, time } = splitStamp(stamp);
  return `${date.slice(0, 6)} ${time}`;
}

export function monthName(m) {
  return MONTHS[m - 1];
}

export function fmt(n, digits = 0) {
  if (n == null || Number.isNaN(n)) return "--";
  return Number(n).toLocaleString("en-IN", { maximumFractionDigits: digits, minimumFractionDigits: digits });
}

export function ago(isoUtc, now) {
  if (!isoUtc) return null;
  const s = Math.max(0, Math.round((now - Date.parse(isoUtc)) / 1000));
  if (s < 60) return `${s} s ago`;
  if (s < 3600) return `${Math.floor(s / 60)} min ago`;
  return `${Math.floor(s / 3600)} h ago`;
}

/** Add hours to a "YYYY-MM-DD HH:MM" label without touching time zones. */
export function addHours(stamp, h) {
  const d = new Date(stamp.replace(" ", "T") + ":00Z");
  d.setUTCHours(d.getUTCHours() + h);
  return d.toISOString().slice(0, 16).replace("T", " ");
}
