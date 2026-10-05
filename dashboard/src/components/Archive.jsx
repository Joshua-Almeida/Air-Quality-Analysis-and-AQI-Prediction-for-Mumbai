import { BarList, ColumnChart, LineChart } from "./Charts.jsx";
import { BandChip, Panel } from "./Live.jsx";
import { BANDS, HOME_CITY, bandOf, fmt, monthName, usePoll } from "../lib.js";

function Missing({ what, command }) {
  return (
    <div className="empty">
      {what} has not been computed yet. Run <code>{command}</code>.
    </div>
  );
}

const BATCH_CMD = ".\\scripts\\spark-submit.ps1 spark_jobs/batch_analysis.py";

/** Batch results from Spark SQL over the full 2015-2020 data. */
export function ArchiveView() {
  const month = usePoll("/api/batch/pm25_by_month", 30000);
  const hour = usePoll("/api/batch/pm25_by_hour", 30000);
  const lockCity = usePoll("/api/batch/lockdown_by_city", 30000);
  const lockAll = usePoll("/api/batch/lockdown_overall", 30000);
  const md = usePoll("/api/batch/mumbai_vs_delhi", 30000);
  const overall = usePoll("/api/batch/city_overall", 30000);
  const validation = usePoll("/api/batch/validation_report", 10000);

  const months = month.data ?? [];
  const hours = hour.data ?? [];

  const mdRows = md.data ?? [];
  const ym = [...new Set(mdRows.map((r) => r.year_month))].sort();
  const valueFor = (city) => ym.map((k) => mdRows.find((r) => r.year_month === k && r.city === city)?.avg_pm25 ?? null);

  const l19 = lockAll.data?.find((r) => r.year === 2019);
  const l20 = lockAll.data?.find((r) => r.year === 2020);
  const change = l19 && l20 ? ((l20.avg_pm25 - l19.avg_pm25) / l19.avg_pm25) * 100 : null;

  const v = validation.data?.[0];

  return (
    <div className="grid grid-archive">
      <Panel id="val-title" title="Stream vs batch check" className="panel-validation"
        meta="rolling 24 h PM2.5 recomputed in batch and compared window by window">
        {!v ? (
          <Missing what="The validation" command=".\scripts\spark-submit.ps1 spark_jobs/validate_stream_vs_batch.py" />
        ) : (
          <div className="validation">
            <span className={`verdict ${v.result === "PASS" ? "ok" : "bad"}`}>{v.result}</span>
            <dl className="kv kv-wide">
              <div><dt>Windows compared</dt><dd>{fmt(v.windows)}</dd></div>
              <div><dt>Full 24 h windows</dt><dd>{fmt(v.complete_windows)}</dd></div>
              <div><dt>Count mismatches</dt><dd>{fmt(v.count_mismatches)}</dd></div>
              <div><dt>Max |difference|</dt><dd>{fmt(v.max_abs_diff, 4)}</dd></div>
              <div><dt>Duplicates</dt><dd>{fmt(v.duplicate_windows)}</dd></div>
              <div><dt>Stream covered</dt><dd className="small">{v.stream_from?.slice(0, 16)}<br />→ {v.stream_to?.slice(0, 16)}</dd></div>
            </dl>
          </div>
        )}
      </Panel>

      <Panel id="lock-title" title="Lockdown: 25 Mar – 30 Jun, 2019 vs 2020" className="panel-lockdown"
        meta={l19 ? `${l20.cities} cities with data in both years` : null}>
        {!lockCity.data ? (
          <Missing what="The lockdown comparison" command={BATCH_CMD} />
        ) : (
          <>
            <div className="lock-head">
              <div><span className="lock-year">2019</span><span className="lock-num">{fmt(l19?.avg_pm25, 1)}</span><small>µg/m³ PM2.5</small></div>
              <div><span className="lock-year">2020</span><span className="lock-num">{fmt(l20?.avg_pm25, 1)}</span><small>µg/m³ PM2.5</small></div>
              <div><span className="lock-year">change</span><span className="lock-num down">{fmt(change, 1)}%</span><small>same cities, same dates</small></div>
            </div>
            <BarList diverging highlight={HOME_CITY} format={(x) => `${x > 0 ? "+" : ""}${fmt(x, 1)}%`}
              rows={lockCity.data.map((r) => ({ label: r.city, value: r.pm25_change_pct }))} />
          </>
        )}
      </Panel>

      <Panel id="month-title" title="PM2.5 by month" className="panel-month" meta="all stations, 2015–2020">
        {!month.data ? <Missing what="Seasonality" command={BATCH_CMD} /> : (
          <ColumnChart labels={months.map((r) => monthName(r.month))} values={months.map((r) => r.avg_pm25)}
            colorFor={(_, i) => `var(--b-${bandOf(months[i].avg_aqi)?.key})`} format={(x) => fmt(x)} />
        )}
      </Panel>

      <Panel id="hour-title" title="PM2.5 by hour of day" className="panel-hour" meta="winter nights trap pollution near the ground">
        {!hour.data ? <Missing what="The daily cycle" command={BATCH_CMD} /> : (
          <LineChart xLabels={hours.map((r) => r.hour)} tickEvery={3} formatX={(h) => `${String(h).padStart(2, "0")}h`}
            series={[{ name: "average PM2.5 (µg/m³)", color: "var(--ink)", values: hours.map((r) => r.avg_pm25) }]} />
        )}
      </Panel>

      <Panel id="md-title" title="Mumbai vs Delhi" className="panel-md" meta="monthly average PM2.5 (µg/m³)">
        {!md.data ? <Missing what="The city comparison" command={BATCH_CMD} /> : (
          <LineChart xLabels={ym} tickEvery={Math.max(1, Math.ceil(ym.length / 8))} formatX={(k) => `${monthName(Number(k.slice(5)))} ${k.slice(2, 4)}`}
            series={[
              { name: "Delhi", color: "var(--ink-2)", values: valueFor("Delhi") },
              { name: "Mumbai", color: "var(--accent)", values: valueFor("Mumbai"), width: 2.5 },
            ]} />
        )}
      </Panel>

      <Panel id="overall-title" title="Long-run city average" className="panel-overall" meta="mean AQI over every reading">
        {!overall.data ? <Missing what="The city ranking" command={BATCH_CMD} /> : (
          <ol className="overall">
            {overall.data.map((r, i) => (
              <li key={r.city} className={r.city === HOME_CITY ? "is-home" : ""}>
                <span className="rank-n">{i + 1}</span>
                <span>{r.city}</span>
                <span className="num">{fmt(r.avg_aqi)}</span>
                <BandChip aqi={r.avg_aqi} compact />
              </li>
            ))}
          </ol>
        )}
      </Panel>
    </div>
  );
}

/** Random Forest results from Spark MLlib. */
export function ModelView() {
  const ml = usePoll("/api/ml", 30000);
  if (!ml.data) {
    return (
      <div className="grid">
        <Panel id="ml-title" title="AQI category model" className="panel-wide">
          <Missing what="The Random Forest" command=".\scripts\spark-submit.ps1 spark_jobs/train_model.py" />
        </Panel>
      </div>
    );
  }
  const { metrics, feature_importance: fi, confusion } = ml.data;
  const order = BANDS.map((b) => b.name).filter((n) => confusion?.labels?.includes(n));
  const maxCell = Math.max(1, ...(confusion?.rows ?? []).flatMap((r) => order.map((o) => r[o] ?? 0)));

  return (
    <div className="grid grid-model">
      <Panel id="ml-title" title="Random Forest · predicting AQI category" className="panel-metrics"
        meta={`trained on 2015–2019, tested on 2020 · ${metrics.params.trees} trees, depth ${metrics.params.max_depth}`}>
        <dl className="kv kv-big">
          <div><dt>Weighted F1</dt><dd>{fmt(metrics.f1_weighted, 3)}</dd></div>
          <div><dt>Accuracy</dt><dd>{fmt(metrics.accuracy, 3)}</dd></div>
          <div><dt>Train rows</dt><dd>{fmt(metrics.train_rows)}</dd></div>
          <div><dt>Test rows</dt><dd>{fmt(metrics.test_rows)}</dd></div>
        </dl>
        <h3 className="sub-h">F1 per category</h3>
        <BarList max={1} format={(x) => fmt(x, 2)}
          rows={order.map((n) => ({ label: n, value: metrics.per_class_f1[n] ?? 0, className: `bg-${BANDS.find((b) => b.name === n).key}` }))} />
      </Panel>

      <Panel id="fi-title" title="Feature importance" className="panel-fi" meta="share of impurity reduction across all trees">
        <BarList format={(x) => fmt(x * 100, 1) + "%"}
          rows={fi.map((r) => ({ label: r.feature, value: r.importance }))} />
      </Panel>

      <Panel id="cm-title" title="Confusion matrix" className="panel-cm" meta="rows: actual · columns: predicted (2020 test set)">
        <div className="matrix" style={{ gridTemplateColumns: `minmax(6rem, auto) repeat(${order.length}, 1fr)` }}>
          <span />
          {order.map((o) => <span key={o} className="m-head">{o}</span>)}
          {order.map((actual) => {
            const row = confusion.rows.find((r) => r.actual === actual) ?? {};
            return [
              <span key={`${actual}-h`} className="m-row">{actual}</span>,
              ...order.map((pred) => {
                const v = row[pred] ?? 0;
                return (
                  <span key={`${actual}-${pred}`} className={`m-cell ${actual === pred ? "diag" : ""}`}
                    style={{ "--a": Math.sqrt(v / maxCell) }}>
                    {fmt(v)}
                  </span>
                );
              }),
            ];
          })}
        </div>
      </Panel>
    </div>
  );
}
