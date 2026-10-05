import { useEffect, useRef, useState } from "react";
import { AQI_MAX, BANDS, railX, shortStamp, addHours, fmt } from "../lib.js";

/** Width of a container element, kept up to date with ResizeObserver. */
export function useWidth() {
  const ref = useRef(null);
  const [width, setWidth] = useState(0);
  useEffect(() => {
    if (!ref.current) return undefined;
    const ro = new ResizeObserver(([entry]) => setWidth(Math.floor(entry.contentRect.width)));
    ro.observe(ref.current);
    return () => ro.disconnect();
  }, []);
  return [ref, width];
}

/**
 * The shared AQI rail: one 0-500 scale split into the six CPCB bands.
 * Markers are placed by value, so every rail on the page reads the same way.
 */
export function AqiRail({ markers = [], showLabels = true }) {
  return (
    <div className="rail">
      <div className="rail-track">
        {BANDS.map((b, i) => {
          const start = i === 0 ? 0 : BANDS[i - 1].max;
          return (
            <span
              key={b.key}
              className={`rail-seg bg-${b.key}`}
              style={{ left: `${railX(start) * 100}%`, width: `${((b.max - start) / AQI_MAX) * 100}%` }}
              title={`${b.name}: ${b.min}-${b.max}`}
            />
          );
        })}
        {markers.map((m) =>
          m.value == null ? null : (
            <span key={m.label} className={`rail-marker ${m.kind ?? ""}`} style={{ left: `${railX(m.value) * 100}%` }}>
              <span className="rail-marker-label">
                {m.label} <b>{fmt(m.value)}</b>
              </span>
            </span>
          )
        )}
      </div>
      {showLabels && (
        <div className="rail-scale" aria-hidden="true">
          {[0, 50, 100, 200, 300, 400, 500].map((v) => (
            <span key={v} style={{ left: `${railX(v) * 100}%` }}>
              {v}
            </span>
          ))}
        </div>
      )}
    </div>
  );
}

/**
 * 72-hour trace for one city: hourly city AQI (thin) and rolling 24 h average (thick),
 * drawn over faint CPCB band stripes on the same 0-500 scale as the rail.
 */
export function TraceChart({ hourly = [], rolling = [], accent = "var(--accent)" }) {
  const [ref, width] = useWidth();
  const [hover, setHover] = useState(null);
  const height = 220;
  const pad = { l: 34, r: 8, t: 8, b: 22 };
  const hours = hourly.map((h) => h.hour);
  const n = hours.length;
  const innerW = Math.max(10, width - pad.l - pad.r);
  const innerH = height - pad.t - pad.b;
  const x = (i) => pad.l + (n <= 1 ? innerW : (i / (n - 1)) * innerW);
  // Zoom to the smallest CPCB band boundary that fits the data, so a clean city
  // still gets a readable trace while the band stripes keep their meaning.
  const peak = Math.max(0, ...hourly.map((h) => h.avg_aqi ?? 0), ...rolling.map((r) => r.avg_aqi ?? 0));
  const yMax = [100, 200, 300, 400, 500].find((b) => b >= peak * 1.1) ?? AQI_MAX;
  const y = (v) => pad.t + innerH - (Math.min(v, yMax) / yMax) * innerH;
  const yTicks = [0, 50, 100, 200, 300, 400, 500].filter((v) => v <= yMax);

  // A rolling window ending at 07:00 covers readings up to 06:00, so plot it at 06:00.
  const index = new Map(hours.map((h, i) => [h, i]));
  const rollPts = rolling
    .map((r) => ({ i: index.get(addHours(r.window_end, -1)), v: r.avg_aqi }))
    .filter((p) => p.i != null);

  const path = (pts) => pts.map((p, k) => `${k ? "L" : "M"}${x(p.i).toFixed(1)},${y(p.v).toFixed(1)}`).join("");
  const hourlyPath = path(hourly.map((h, i) => ({ i, v: h.avg_aqi })));
  const rollingPath = path(rollPts);

  const dayTicks = hours
    .map((h, i) => ({ h, i }))
    .filter(({ h }) => h.endsWith("00:00"));

  const onMove = (e) => {
    if (!n) return;
    const rect = e.currentTarget.getBoundingClientRect();
    const px = e.clientX - rect.left;
    const i = Math.round(((px - pad.l) / innerW) * (n - 1));
    setHover(Math.max(0, Math.min(n - 1, i)));
  };

  const hv = hover != null ? hourly[hover] : hourly[n - 1];
  const hvRoll = hv ? rollPts.find((p) => p.i === (hover ?? n - 1)) : null;

  return (
    <div className="trace" ref={ref}>
      <div className="trace-readout" aria-live="off">
        <span>{hv ? shortStamp(hv.hour) : "--"}</span>
        <span>
          hourly <b>{fmt(hv?.avg_aqi)}</b>
        </span>
        <span>
          24 h avg <b>{fmt(hvRoll?.v)}</b>
        </span>
        <span>
          stations <b>{hv?.stations ?? "--"}</b>
        </span>
      </div>
      {width > 0 && (
        <svg width={width} height={height} onMouseMove={onMove} onMouseLeave={() => setHover(null)} role="img"
          aria-label="Hourly AQI and rolling 24 hour average for the last 72 hours">
          {BANDS.map((b, i) => {
            const start = i === 0 ? 0 : BANDS[i - 1].max;
            if (start >= yMax) return null;
            return (
              <rect key={b.key} className={`band-stripe fill-${b.key}`} x={pad.l} width={innerW}
                y={y(b.max)} height={y(start) - y(b.max)} />
            );
          })}
          {yTicks.map((v) => (
            <g key={v}>
              <line className="gridline" x1={pad.l} x2={pad.l + innerW} y1={y(v)} y2={y(v)} />
              <text className="axis" x={pad.l - 6} y={y(v) + 4} textAnchor="end">{v}</text>
            </g>
          ))}
          {dayTicks.map(({ h, i }) => (
            <g key={h}>
              <line className="gridline day" x1={x(i)} x2={x(i)} y1={pad.t} y2={pad.t + innerH} />
              <text className="axis" x={x(i) + 4} y={height - 6}>{shortStamp(h).slice(0, 6)}</text>
            </g>
          ))}
          <path d={hourlyPath} className="trace-hourly" style={{ stroke: accent }} />
          <path d={rollingPath} className="trace-rolling" style={{ stroke: accent }} />
          {hover != null && (
            <line className="crosshair" x1={x(hover)} x2={x(hover)} y1={pad.t} y2={pad.t + innerH} />
          )}
          {n > 0 && <circle className="trace-head" cx={x(n - 1)} cy={y(hourly[n - 1].avg_aqi)} r="3.5" style={{ fill: accent }} />}
        </svg>
      )}
      <div className="legend">
        <span><i className="key thin" style={{ background: accent }} /> hourly city average</span>
        <span><i className="key thick" style={{ background: accent }} /> rolling 24 h average</span>
      </div>
    </div>
  );
}

/** Multi-series line chart over a shared categorical x axis. */
export function LineChart({ series, xLabels, height = 220, yLabel, formatX = (v) => v, tickEvery = 1 }) {
  const [ref, width] = useWidth();
  const pad = { l: 40, r: 10, t: 10, b: 24 };
  const innerW = Math.max(10, width - pad.l - pad.r);
  const innerH = height - pad.t - pad.b;
  const all = series.flatMap((s) => s.values.filter((v) => v != null));
  const max = Math.max(10, ...all) * 1.08;
  const n = xLabels.length;
  const x = (i) => pad.l + (n <= 1 ? 0 : (i / (n - 1)) * innerW);
  const y = (v) => pad.t + innerH - (v / max) * innerH;
  const ticks = niceTicks(max);

  return (
    <div className="chart" ref={ref}>
      {width > 0 && (
        <svg width={width} height={height} role="img" aria-label={yLabel}>
          {ticks.map((t) => (
            <g key={t}>
              <line className="gridline" x1={pad.l} x2={pad.l + innerW} y1={y(t)} y2={y(t)} />
              <text className="axis" x={pad.l - 6} y={y(t) + 4} textAnchor="end">{t}</text>
            </g>
          ))}
          {xLabels.map((lab, i) =>
            i % tickEvery === 0 ? (
              <text key={lab} className="axis" x={x(i)} y={height - 6} textAnchor="middle">{formatX(lab)}</text>
            ) : null
          )}
          {series.map((s) => {
            let d = "";
            let pen = false;
            s.values.forEach((v, i) => {
              if (v == null) { pen = false; return; }
              d += `${pen ? "L" : "M"}${x(i).toFixed(1)},${y(v).toFixed(1)}`;
              pen = true;
            });
            return <path key={s.name} d={d} className="line" style={{ stroke: s.color, strokeWidth: s.width ?? 2 }} />;
          })}
        </svg>
      )}
      <div className="legend">
        {series.map((s) => (
          <span key={s.name}><i className="key thick" style={{ background: s.color }} /> {s.name}</span>
        ))}
      </div>
    </div>
  );
}

/** Vertical bars, one per category. */
export function ColumnChart({ labels, values, height = 200, colorFor, format = (v) => fmt(v) }) {
  const [ref, width] = useWidth();
  const pad = { l: 40, r: 6, t: 18, b: 24 };
  const innerW = Math.max(10, width - pad.l - pad.r);
  const innerH = height - pad.t - pad.b;
  const max = Math.max(1, ...values) * 1.1;
  const bw = innerW / values.length;
  const ticks = niceTicks(max);
  return (
    <div className="chart" ref={ref}>
      {width > 0 && (
        <svg width={width} height={height} role="img">
          {ticks.map((t) => (
            <g key={t}>
              <line className="gridline" x1={pad.l} x2={pad.l + innerW} y1={pad.t + innerH - (t / max) * innerH} y2={pad.t + innerH - (t / max) * innerH} />
              <text className="axis" x={pad.l - 6} y={pad.t + innerH - (t / max) * innerH + 4} textAnchor="end">{t}</text>
            </g>
          ))}
          {values.map((v, i) => {
            const h = (v / max) * innerH;
            return (
              <g key={labels[i]}>
                <rect x={pad.l + i * bw + bw * 0.18} width={bw * 0.64} y={pad.t + innerH - h} height={h}
                  className="column" style={{ fill: colorFor ? colorFor(v, i) : undefined }} />
                <text className="axis value" x={pad.l + i * bw + bw / 2} y={pad.t + innerH - h - 5} textAnchor="middle">{format(v)}</text>
                <text className="axis" x={pad.l + i * bw + bw / 2} y={height - 6} textAnchor="middle">{labels[i]}</text>
              </g>
            );
          })}
        </svg>
      )}
    </div>
  );
}

/** Horizontal bars, optionally diverging around zero (for % change). */
export function BarList({ rows, max, diverging = false, format = (v) => fmt(v, 1), highlight }) {
  const m = max ?? Math.max(...rows.map((r) => Math.abs(r.value)), 1);
  return (
    <ul className={`barlist ${diverging ? "diverging" : ""}`}>
      {rows.map((r) => {
        const w = (Math.abs(r.value) / m) * (diverging ? 50 : 100);
        const left = diverging ? (r.value < 0 ? 50 - w : 50) : 0;
        return (
          <li key={r.label} className={highlight === r.label ? "is-home" : ""}>
            <span className="barlist-label">{r.label}</span>
            <span className="barlist-track">
              {diverging && <span className="barlist-zero" />}
              <span className={`barlist-bar ${r.value < 0 ? "neg" : "pos"} ${r.className ?? ""}`}
                style={{ left: `${left}%`, width: `${w}%` }} />
            </span>
            <span className="barlist-value">{format(r.value)}</span>
          </li>
        );
      })}
    </ul>
  );
}

function niceTicks(max) {
  const raw = max / 4;
  const mag = 10 ** Math.floor(Math.log10(raw));
  const step = [1, 2, 2.5, 5, 10].map((s) => s * mag).find((s) => s >= raw) ?? raw;
  const out = [];
  for (let v = 0; v <= max; v += step) out.push(Math.round(v * 100) / 100);
  return out;
}
