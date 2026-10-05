import { useMemo, useState } from "react";
import { AqiRail, TraceChart } from "./Charts.jsx";
import { BANDS, HOME_CITY, bandByName, bandOf, fmt, railX, shortStamp, splitStamp, usePoll } from "../lib.js";

/** Category chip: colour AND name, never colour alone. */
export function BandChip({ aqi, name, compact = false }) {
  const band = name ? bandByName(name) : bandOf(aqi);
  if (!band) return <span className="chip chip-none">no data</span>;
  return <span className={`chip bg-${band.key} ${compact ? "chip-compact" : ""}`}>{band.name}</span>;
}

export function Panel({ title, meta, children, className = "", id }) {
  return (
    <section className={`panel ${className}`} aria-labelledby={id}>
      <header className="panel-head">
        <h2 id={id}>{title}</h2>
        {meta && <div className="panel-meta">{meta}</div>}
      </header>
      <div className="panel-body">{children}</div>
    </section>
  );
}

function Empty({ children }) {
  return <div className="empty">{children}</div>;
}

// ---------------------------------------------------------------- ranking
const ROW = 46;

export function RankingPanel({ ranking, focus, onFocus }) {
  const windowEnd = ranking[0]?.window_end;
  return (
    <Panel
      id="ranking-title"
      title="Top 10 most polluted"
      className="panel-ranking"
      meta={windowEnd ? <>by 24 h average AQI, window to <b>{shortStamp(windowEnd)}</b></> : "by 24 h average AQI"}
    >
      {ranking.length === 0 ? (
        <Empty>
          No finished 24-hour window yet. Windows close once the watermark passes their end, about
          two simulated hours after the newest reading.
        </Empty>
      ) : (
        <>
          <ol className="rank-list" style={{ height: ranking.length * ROW }}>
            {ranking.map((r) => (
              <li
                key={r.city}
                className={`rank-row ${r.city === HOME_CITY ? "is-home" : ""} ${r.city === focus ? "is-focus" : ""}`}
                style={{ transform: `translateY(${(r.rank - 1) * ROW}px)` }}
              >
                <button type="button" onClick={() => onFocus(r.city)} aria-label={`Focus ${r.city}, rank ${r.rank}`}>
                  <span className="rank-n">{r.rank}</span>
                  <span className="rank-city">{r.city}</span>
                  <span className="rank-bar" aria-hidden="true">
                    <span className={`rank-fill bg-${bandOf(r.avg_aqi)?.key}`} style={{ transform: `scaleX(${railX(r.avg_aqi)})` }} />
                  </span>
                  <span className="rank-val">{fmt(r.avg_aqi)}</span>
                  <BandChip aqi={r.avg_aqi} compact />
                </button>
              </li>
            ))}
          </ol>
          <div className="rank-scale" aria-hidden="true">
            <span />
            <span />
            <span className="rank-scale-axis">
              {[0, 100, 200, 300, 400, 500].map((v) => (
                <span key={v} style={{ left: `${railX(v) * 100}%` }}>{v}</span>
              ))}
            </span>
            <span />
            <span />
          </div>
        </>
      )}
    </Panel>
  );
}

// ---------------------------------------------------------------- focus city
export function FocusPanel({ city, cities, ranking, onReset }) {
  const history = usePoll(city ? `/api/city/${encodeURIComponent(city)}/history?hours=72` : null, 3000);
  const row = cities.find((c) => c.city === city);
  const isHome = city === HOME_CITY;

  const byRolling = useMemo(
    () => [...cities].filter((c) => c.avg_aqi_24h != null).sort((a, b) => b.avg_aqi_24h - a.avg_aqi_24h),
    [cities]
  );
  const rank = byRolling.findIndex((c) => c.city === city) + 1;
  const median = useMemo(() => {
    const vals = cities.map((c) => c.avg_aqi).filter((v) => v != null).sort((a, b) => a - b);
    return vals.length ? vals[Math.floor(vals.length / 2)] : null;
  }, [cities]);
  const worst = ranking[0];

  const { date, time } = splitStamp(row?.hour);

  return (
    <Panel
      id="focus-title"
      title={
        <>
          <span className={`city-mark ${isHome ? "home" : ""}`} aria-hidden="true" />
          {city}
          {row?.state && <span className="h-sub">{row.state}</span>}
        </>
      }
      className={`panel-focus ${isHome ? "is-home" : ""}`}
      meta={
        isHome ? (
          "home city · pinned"
        ) : (
          <button type="button" className="link" onClick={onReset}>Back to {HOME_CITY}</button>
        )
      }
    >
      {!row ? (
        <Empty>
          No readings for {city} have been streamed yet. {city === HOME_CITY && "Mumbai stations report from late 2018 onward in this dataset."}
        </Empty>
      ) : (
        <>
          <div className="focus-top">
            <div className="focus-reading">
              <span className="focus-aqi">{fmt(row.avg_aqi)}</span>
              <div className="focus-reading-side">
                <BandChip aqi={row.avg_aqi} />
                <span className="focus-when">AQI at {time}, {date}</span>
              </div>
            </div>
            <dl className="kv">
              <div><dt>24 h average</dt><dd>{fmt(row.avg_aqi_24h)}</dd></div>
              <div><dt>Rank by 24 h</dt><dd>{rank ? <>{rank}<small> / {byRolling.length}</small></> : "--"}</dd></div>
              <div><dt>PM2.5 now</dt><dd>{fmt(row.avg_pm25, 1)}<small> µg/m³</small></dd></div>
              <div><dt>Worst station</dt><dd>{fmt(row.max_aqi)}</dd></div>
            </dl>
          </div>
          <AqiRail
            markers={[
              { label: city, value: row.avg_aqi, kind: isHome ? "home" : "focus" },
              { label: "India median", value: median, kind: "ref" },
              ...(worst && worst.city !== city ? [{ label: worst.city, value: worst.avg_aqi, kind: "ref worst" }] : []),
            ]}
          />
          {history.data ? (
            <TraceChart hourly={history.data.hourly} rolling={history.data.rolling}
              accent={isHome ? "var(--accent)" : "var(--ink)"} />
          ) : (
            <Empty>Loading trace…</Empty>
          )}
        </>
      )}
    </Panel>
  );
}

// ---------------------------------------------------------------- alerts
export function AlertLog({ alerts, threshold = 300 }) {
  const items = alerts?.items ?? [];
  return (
    <Panel
      id="alerts-title"
      title="Alert log"
      className="panel-alerts"
      meta={<>{fmt(alerts?.total ?? 0)} readings above AQI {threshold}</>}
    >
      {items.length === 0 ? (
        <Empty>No station has crossed AQI {threshold} yet.</Empty>
      ) : (
        <ol className="log">
          {items.map((a) => {
            const band = bandOf(a.aqi);
            return (
              <li key={`${a.event_time}-${a.station_id}`} className={`log-line sev-${band?.key}`}>
                <span className="log-time">{shortStamp(a.event_time)}</span>
                <span className={`log-sev bg-${band?.key}`}>{band?.name}</span>
                <span className="log-where">
                  <b>{a.city}</b> {a.station_name?.replace(/ - [A-Z]+$/, "").replace(`, ${a.city}`, "")}
                </span>
                <span className="log-aqi">{fmt(a.aqi)}</span>
              </li>
            );
          })}
        </ol>
      )}
    </Panel>
  );
}

// ---------------------------------------------------------------- ledger
const COLUMNS = [
  { key: "city", label: "City", num: false },
  { key: "avg_aqi", label: "AQI now", num: true },
  { key: "avg_aqi_24h", label: "24 h avg", num: true },
  { key: "avg_pm25", label: "PM2.5", num: true },
  { key: "avg_pm10", label: "PM10", num: true },
  { key: "stations", label: "Stations", num: true },
];

export function Ledger({ cities, focus, onFocus }) {
  const [sort, setSort] = useState({ key: "avg_aqi", dir: -1 });
  const rows = useMemo(() => {
    const out = [...cities];
    out.sort((a, b) => {
      const va = a[sort.key], vb = b[sort.key];
      if (va == null) return 1;
      if (vb == null) return -1;
      return (typeof va === "string" ? va.localeCompare(vb) : va - vb) * sort.dir;
    });
    return out;
  }, [cities, sort]);

  const toggle = (key) =>
    setSort((s) => (s.key === key ? { key, dir: -s.dir } : { key, dir: key === "city" ? 1 : -1 }));

  return (
    <Panel
      id="ledger-title"
      title="Every city, latest hour"
      className="panel-ledger"
      meta={<>{cities.length} cities reporting · click a row to inspect it</>}
    >
      {cities.length === 0 ? (
        <Empty>Waiting for the first finished hour from Spark.</Empty>
      ) : (
        <div className="table-wrap">
          <table className="ledger">
            <thead>
              <tr>
                {COLUMNS.map((c) => (
                  <th key={c.key} className={c.num ? "num" : ""} aria-sort={sort.key === c.key ? (sort.dir > 0 ? "ascending" : "descending") : "none"}>
                    <button type="button" onClick={() => toggle(c.key)}>
                      {c.label}
                      <svg className={`sort ${sort.key === c.key ? "on" : ""} ${sort.dir > 0 ? "up" : ""}`} width="8" height="8" viewBox="0 0 8 8" aria-hidden="true">
                        <path d="M1 2.5 4 5.5 7 2.5" fill="none" stroke="currentColor" strokeWidth="1.4" />
                      </svg>
                    </button>
                  </th>
                ))}
                <th>Category</th>
                <th className="strip-h">Last 24 hours</th>
              </tr>
            </thead>
            <tbody>
              {rows.map((c) => (
                <tr key={c.city} className={`${c.city === HOME_CITY ? "is-home" : ""} ${c.city === focus ? "is-focus" : ""}`}
                  onClick={() => onFocus(c.city)}>
                  <td>
                    <button type="button" className="row-btn" onClick={(e) => { e.stopPropagation(); onFocus(c.city); }}>
                      {c.city}
                    </button>
                    <span className="state">{c.state}</span>
                  </td>
                  <td className="num strong">{fmt(c.avg_aqi)}</td>
                  <td className="num">{fmt(c.avg_aqi_24h)}</td>
                  <td className="num">{fmt(c.avg_pm25, 1)}</td>
                  <td className="num">{fmt(c.avg_pm10, 1)}</td>
                  <td className="num">{c.stations}</td>
                  <td><BandChip aqi={c.avg_aqi} compact /></td>
                  <td>
                    <span className="strip" aria-label={`Hourly categories for the last ${c.trail?.length ?? 0} hours`}>
                      {(c.trail ?? []).map((v, i) => (
                        <i key={i} className={`bg-${bandOf(v)?.key}`} title={`${fmt(v)}`} />
                      ))}
                    </span>
                  </td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      )}
      <div className="band-key" aria-label="CPCB AQI categories">
        {BANDS.map((b) => (
          <span key={b.key}><i className={`bg-${b.key}`} />{b.name} <small>{b.min}–{b.max === 500 ? "500+" : b.max}</small></span>
        ))}
      </div>
    </Panel>
  );
}
