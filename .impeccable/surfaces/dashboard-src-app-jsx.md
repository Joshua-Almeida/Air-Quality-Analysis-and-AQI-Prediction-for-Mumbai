---
version: 1
slug: "dashboard-src-app-jsx"
primary_target: "dashboard/src/App.jsx"
related_targets: []
---

## Scope

Whole dashboard surface (dashboard/src). Mode: Operate. Audience: student presenting to examiners on a projector/laptop while the Kafka -> Spark -> Delta stream runs. Task: see at a glance which cities are worst now, how Mumbai compares, that alerts are firing, and that the stream is alive.

## Direction contract

THESIS: A live operations console for India's air. Persistent panels show the pipeline at work (ranking, Mumbai, alerts, every city) instead of the KPI-card grid with a hero metric.

OWN-WORLD: Graphite ground; panels tiled edge to edge and divided by 1px seams, one elevation step, no shadows. The CPCB AQI bands (Good to Severe) are the only "syntax" colours and appear only as state: category chips, bars, log severities. Mumbai owns one reserved cool accent no band uses. Compact sans, tabular mono figures, small-caps panel headers.

STORY: The viewer sees the replay clock advance, the top-10 reshuffle, alert lines stream in, and Mumbai's trace grow; they understand the watermark lag; Archive and Model panels hold the batch and ML results.

FIRST VIEWPORT: Status rail across the top (pipeline lamps, large replay clock, watermark, freshness, panel tabs). Below: ranking panel left (bars on one shared 0-500 AQI rail with band ticks), Mumbai panel centre (large AQI, category, rank, 72 h trace), alert log right. All-cities ledger spans the bottom.

FORM: dark-first developer console (catalog challenger, chosen by user over assigned PUC slip), seed key 36b72957. Signature interaction: ranking rows slide vertically to their new rank on each update; log lines push in from the top; motion along one axis only.

FINISH: unreviewed and undocumented is unfinished; this build ends with the finish review, the verdict, DESIGN.md, and every shipping raster carrying its provenance
