import { useState } from "react";
import { AlertLog, FocusPanel, Ledger, RankingPanel } from "./components/Live.jsx";
import { ArchiveView, ModelView } from "./components/Archive.jsx";
import { HOME_CITY, ago, fmt, shortStamp, splitStamp, useNow, usePoll } from "./lib.js";

const TABS = [
  { id: "live", label: "Live stream" },
  { id: "archive", label: "Archive · batch" },
  { id: "model", label: "Model" },
];

// A ranking older than this (wall-clock) means Spark has stopped emitting.
const STALE_AFTER_S = 45;

function StatusRail({ status, apiError, tab, setTab }) {
  const now = useNow(1000);
  const { date, time } = splitStamp(status?.latest_hour);
  const updated = ago(status?.last_ranking_update_utc, now);
  const ageS = status?.last_ranking_update_utc ? (now - Date.parse(status.last_ranking_update_utc)) / 1000 : Infinity;
  const live = !apiError && ageS < STALE_AFTER_S;

  return (
    <header className="rail-bar">
      <div className="brand">
        <h1>India air quality <span>live replay</span></h1>
        <p>CPCB hourly readings · Kafka → Spark → Delta Lake</p>
      </div>

      <div className="clock" aria-live="polite">
        <span className="clock-label">Replay clock</span>
        <span className="clock-time">{time}</span>
        <span className="clock-date">{date}</span>
      </div>

      <dl className="pipeline">
        <div>
          <dt><i className={`lamp ${apiError ? "off" : "on"}`} aria-hidden="true" />API</dt>
          <dd>{apiError ? "offline" : "connected"}</dd>
        </div>
        <div>
          <dt><i className={`lamp ${live ? "on pulse" : "off"}`} aria-hidden="true" />Stream</dt>
          <dd>{apiError ? "--" : live ? `updated ${updated}` : updated ? `idle · last ${updated}` : "no output yet"}</dd>
        </div>
        <div>
          <dt>24 h windows closed to</dt>
          <dd>{status?.latest_window_end ? shortStamp(status.latest_window_end) : "--"}</dd>
        </div>
        <div>
          <dt>Streamed</dt>
          <dd>{fmt(status?.hours_streamed ?? 0)} h · {status?.cities ?? 0} cities</dd>
        </div>
      </dl>

      <nav className="tabs" role="tablist" aria-label="Views">
        {TABS.map((t) => (
          <button key={t.id} type="button" role="tab" aria-selected={tab === t.id}
            className={tab === t.id ? "on" : ""} onClick={() => setTab(t.id)}>
            {t.label}
          </button>
        ))}
      </nav>
    </header>
  );
}

export default function App() {
  const [tab, setTab] = useState("live");
  const [focus, setFocus] = useState(HOME_CITY);

  const status = usePoll("/api/status", 3000);
  const cities = usePoll(tab === "live" ? "/api/cities/latest" : null, 3000);
  const ranking = usePoll(tab === "live" ? "/api/ranking" : null, 3000);
  const alerts = usePoll(tab === "live" ? "/api/alerts?limit=60" : null, 3000);

  const apiError = status.error;

  return (
    <div className="app">
      <StatusRail status={status.data} apiError={apiError} tab={tab} setTab={setTab} />

      {apiError && (
        <div className="banner" role="alert">
          Cannot reach the API ({apiError}). Start it from the project root with{" "}
          <code>uvicorn api.server:app --host 127.0.0.1 --port 8077</code>.
        </div>
      )}

      <main>
        {tab === "live" && (
          <div className="grid grid-live">
            <RankingPanel ranking={ranking.data ?? []} focus={focus} onFocus={setFocus} />
            <FocusPanel city={focus} cities={cities.data ?? []} ranking={ranking.data ?? []}
              onReset={() => setFocus(HOME_CITY)} />
            <AlertLog alerts={alerts.data} />
            <Ledger cities={cities.data ?? []} focus={focus} onFocus={setFocus} />
          </div>
        )}
        {tab === "archive" && <ArchiveView />}
        {tab === "model" && <ModelView />}
      </main>

      <footer className="foot">
        <span>Data: CPCB via Kaggle “Air Quality Data in India (2015–2020)”. Replayed history, not today’s air.</span>
        <span>Polling every 3 s</span>
      </footer>
    </div>
  );
}
