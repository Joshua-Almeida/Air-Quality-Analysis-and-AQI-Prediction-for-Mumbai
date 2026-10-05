# Product

<!-- impeccable:product-schema 1 -->

## Platform

web

## Stack

React (Vite) single-page dashboard with plain CSS, served by a small FastAPI read-only API that reads Delta Lake tables written by Spark. Chosen by the user over Streamlit.

## Users

A computer engineering student presenting a Big Data Analytics course project, and the professor/examiners watching. The primary viewing situation is a live demo on a projector or laptop screen while Kafka and Spark are running, so the screen must be legible from a distance and visibly "alive" as new data lands.

## Product Purpose

Show, in real time, how air quality changes across Indian cities as CPCB hourly readings (2015-2020, replayed through Kafka) are processed by Spark Structured Streaming: the latest AQI per city, a live top-10 most polluted ranking, threshold alerts, and a highlight on Mumbai. Success means an examiner understands within seconds that data is streaming, which cities are worst right now, and how Mumbai compares.

## Positioning

It is a working streaming pipeline, not a static chart: every number comes from event-time windows with a watermark, written to Delta tables, and is validated against a batch recomputation.

## Operating Context

- Replayed time, not real time: one simulated hour arrives every ~2 seconds, so the dashboard shows both the simulated event clock (e.g. "14 Nov 2019, 06:00") and freshness of the last update.
- Polling every 3 seconds; tables can be empty when the stream has just started.
- Secondary view shows batch analysis results (seasonality, hour-of-day, 2019 vs 2020 lockdown, Mumbai vs Delhi) and the Random Forest results.

## Capabilities and Constraints

- Data: 26 cities, ~2.0M hourly station readings after cleaning; AQI uses the official CPCB categories Good, Satisfactory, Moderate, Poor, Very Poor, Severe.
- Rolling 24 h windows lag the newest hour by the watermark (2 simulated hours); alerts are per station reading above AQI 300 by default.
- Runs locally only; no auth, no deployment.

## Brand Commitments

The user asked for a dashboard that does not look AI-generated. No other brand assets exist.

## Evidence on Hand

Real CPCB data from the Kaggle dataset "Air Quality Data in India (2015-2020)" in data/raw/. No testimonials or external claims; none may be invented.

## Product Principles

1. Truth over decoration: every figure on screen maps to a named Spark output.
2. Readable from the back of a room.
3. Show the stream is live, and how far behind the watermark the numbers are.
4. Mumbai is the project's home city and gets a permanent place, not a buried row.

## Accessibility & Inclusion

AQI category must never be communicated by colour alone (always paired with the category name or number). Sufficient contrast for projector use.
