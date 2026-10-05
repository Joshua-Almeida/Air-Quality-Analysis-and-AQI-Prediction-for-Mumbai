"""
Step 8 - Report charts (matplotlib).

Reads the small result files the Spark jobs wrote and saves PNGs to
reports/figures/. Runs on your laptop (host venv), no Spark needed:

    python reports/make_charts.py

Charts that need a result which has not been produced yet are skipped with a
message, so you can run this at any point.
"""
import json
import os

import matplotlib

matplotlib.use("Agg")  # write files only, never open a window
import matplotlib.pyplot as plt
import pandas as pd

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
BATCH = os.path.join(ROOT, "output", "batch")
ML = os.path.join(ROOT, "output", "ml")
DELTA = os.path.join(ROOT, "output", "delta")
FIG = os.path.join(ROOT, "reports", "figures")

# Official CPCB category colours (print versions)
BANDS = [
    (50, "Good", "#00B050"),
    (100, "Satisfactory", "#92D050"),
    (200, "Moderate", "#E6C619"),
    (300, "Poor", "#FF9900"),
    (400, "Very Poor", "#FF0000"),
    (float("inf"), "Severe", "#C00000"),
]
BAND_COLOR = {name: color for _, name, color in BANDS}
MUMBAI = "#1f77d0"
DELHI = "#555555"

plt.rcParams.update({
    "figure.dpi": 150,
    "savefig.bbox": "tight",
    "font.size": 10,
    "axes.spines.top": False,
    "axes.spines.right": False,
    "axes.grid": True,
    "grid.alpha": 0.25,
    "axes.titleweight": "bold",
    "axes.titlesize": 12,
})


def band_color(aqi):
    for upper, _, color in BANDS:
        if aqi <= upper:
            return color
    return BANDS[-1][2]


def load(path):
    if not os.path.exists(path):
        print(f"  skip: {os.path.relpath(path, ROOT)} not found")
        return None
    return pd.read_csv(path)


def save(fig, name):
    os.makedirs(FIG, exist_ok=True)
    out = os.path.join(FIG, name)
    fig.savefig(out)
    plt.close(fig)
    print(f"  wrote reports/figures/{name}")


def chart_month():
    df = load(os.path.join(BATCH, "pm25_by_month.csv"))
    if df is None:
        return
    months = ["Jan", "Feb", "Mar", "Apr", "May", "Jun", "Jul", "Aug", "Sep", "Oct", "Nov", "Dec"]
    fig, ax = plt.subplots(figsize=(8, 3.8))
    ax.bar([months[m - 1] for m in df.month], df.avg_pm25, color=[band_color(a) for a in df.avg_aqi], edgecolor="black", linewidth=0.4)
    for x, y in zip(range(len(df)), df.avg_pm25):
        ax.text(x, y + 2, f"{y:.0f}", ha="center", fontsize=8)
    ax.set_ylabel("Average PM2.5 (µg/m³)")
    ax.set_title("PM2.5 by month, all Indian stations 2015-2020")
    ax.text(0.99, 0.97, "bar colour = CPCB category of the month's average AQI", transform=ax.transAxes,
            ha="right", va="top", fontsize=8, color="#555")
    save(fig, "01_pm25_by_month.png")


def chart_hour():
    df = load(os.path.join(BATCH, "pm25_by_hour.csv"))
    if df is None:
        return
    fig, ax = plt.subplots(figsize=(8, 3.6))
    ax.plot(df.hour, df.avg_pm25, marker="o", color="#333", linewidth=2)
    ax.set_xticks(range(0, 24, 2))
    ax.set_xlabel("Hour of day (local time)")
    ax.set_ylabel("Average PM2.5 (µg/m³)")
    ax.set_title("PM2.5 by hour of day")
    lo = df.loc[df.avg_pm25.idxmin()]
    hi = df.loc[df.avg_pm25.idxmax()]
    ax.annotate(f"lowest {lo.avg_pm25:.0f} at {int(lo.hour):02d}:00", (lo.hour, lo.avg_pm25), xytext=(0, -18),
                textcoords="offset points", ha="center", fontsize=8)
    ax.annotate(f"highest {hi.avg_pm25:.0f} at {int(hi.hour):02d}:00", (hi.hour, hi.avg_pm25), xytext=(0, 8),
                textcoords="offset points", ha="center", fontsize=8)
    save(fig, "02_pm25_by_hour.png")


def chart_lockdown():
    overall = load(os.path.join(BATCH, "lockdown_overall.csv"))
    by_city = load(os.path.join(BATCH, "lockdown_by_city.csv"))
    if overall is None or by_city is None:
        return

    fig, ax = plt.subplots(figsize=(6.5, 3.8))
    metrics = [("avg_pm25", "PM2.5"), ("avg_pm10", "PM10"), ("avg_no2", "NO2")]
    width = 0.38
    for i, (col, label) in enumerate(metrics):
        v19 = overall.loc[overall.year == 2019, col].iloc[0]
        v20 = overall.loc[overall.year == 2020, col].iloc[0]
        ax.bar(i - width / 2, v19, width, color="#999", label="2019" if i == 0 else None)
        ax.bar(i + width / 2, v20, width, color="#2b8a3e", label="2020 (lockdown)" if i == 0 else None)
        ax.text(i + width / 2, v20 + 2, f"{(v20 - v19) / v19 * 100:+.0f}%", ha="center", fontsize=9, fontweight="bold")
    ax.set_xticks(range(len(metrics)), [m[1] for m in metrics])
    ax.set_ylabel("µg/m³")
    cities = int(overall.cities.iloc[0])
    ax.set_title(f"25 Mar - 30 Jun: 2019 vs 2020 ({cities} cities in both years)")
    ax.legend(frameon=False)
    save(fig, "03_lockdown_overall.png")

    df = by_city.sort_values("pm25_change_pct")
    fig, ax = plt.subplots(figsize=(7, 0.32 * len(df) + 1))
    colors = [MUMBAI if c == "Mumbai" else ("#2b8a3e" if v < 0 else "#d9480f") for c, v in zip(df.city, df.pm25_change_pct)]
    ax.barh(df.city, df.pm25_change_pct, color=colors)
    ax.axvline(0, color="black", linewidth=0.8)
    for y, v in enumerate(df.pm25_change_pct):
        ax.text(v - 1 if v < 0 else v + 1, y, f"{v:+.0f}%", va="center", ha="right" if v < 0 else "left", fontsize=8)
    lo, hi = df.pm25_change_pct.min(), df.pm25_change_pct.max()
    ax.set_xlim(min(lo, 0) - 10, max(hi, 0) + 10)
    ax.set_xlabel("Change in average PM2.5, 2020 vs 2019 (%)")
    ax.set_title("Lockdown effect by city (Mumbai highlighted)")
    ax.grid(axis="y", visible=False)
    save(fig, "04_lockdown_by_city.png")


def chart_mumbai_delhi():
    df = load(os.path.join(BATCH, "mumbai_vs_delhi.csv"))
    if df is None:
        return
    df["date"] = pd.to_datetime(df.year_month + "-01")
    fig, ax = plt.subplots(figsize=(9, 3.8))
    for city, color in (("Delhi", DELHI), ("Mumbai", MUMBAI)):
        d = df[df.city == city]
        ax.plot(d.date, d.avg_pm25, label=city, color=color, linewidth=2.2 if city == "Mumbai" else 1.6)
    ax.axvspan(pd.Timestamp("2020-03-25"), pd.Timestamp("2020-06-30"), color="#2b8a3e", alpha=0.12, label="2020 lockdown")
    ax.set_ylabel("Monthly average PM2.5 (µg/m³)")
    ax.set_title("Mumbai vs Delhi, monthly PM2.5")
    ax.legend(frameon=False)
    save(fig, "05_mumbai_vs_delhi.png")


def chart_validation():
    pairs = load(os.path.join(BATCH, "validation_pairs.csv"))
    report = load(os.path.join(BATCH, "validation_report.csv"))
    if pairs is None or report is None:
        return
    r = report.iloc[0]
    fig, ax = plt.subplots(figsize=(4.6, 4.4))
    ax.scatter(pairs.batch_avg_pm25, pairs.avg_pm25, s=6, alpha=0.5, color="#333")
    lim = max(pairs.avg_pm25.max(), pairs.batch_avg_pm25.max()) * 1.05
    ax.plot([0, lim], [0, lim], color="#d9480f", linewidth=1, label="stream = batch")
    ax.set_xlim(0, lim)
    ax.set_ylim(0, lim)
    ax.set_xlabel("Batch: rolling 24 h PM2.5")
    ax.set_ylabel("Streaming: rolling 24 h PM2.5")
    ax.set_title(f"Stream vs batch: {r.result}")
    ax.text(0.03, 0.97, f"{int(r.windows):,} windows\nmax |diff| = {r.max_abs_diff:.4f}", transform=ax.transAxes,
            va="top", fontsize=9)
    ax.legend(frameon=False, loc="lower right")
    save(fig, "06_stream_vs_batch.png")


def chart_top10():
    path = os.path.join(DELTA, "top10_ranking")
    if not os.path.exists(os.path.join(path, "_delta_log")):
        print("  skip: streaming top10_ranking table not found")
        return
    from deltalake import DeltaTable

    df = DeltaTable(path).to_pandas().sort_values("rank", ascending=False)
    window_end = pd.Timestamp(df.window_end.iloc[0]).strftime("%d %b %Y %H:%M")
    fig, ax = plt.subplots(figsize=(7, 4))
    colors = [MUMBAI if c == "Mumbai" else band_color(a) for c, a in zip(df.city, df.avg_aqi)]
    ax.barh(df.city, df.avg_aqi, color=colors, edgecolor="black", linewidth=0.4)
    for y, v in enumerate(df.avg_aqi):
        ax.text(v + 4, y, f"{v:.0f}", va="center", fontsize=8)
    ax.set_xlim(0, max(500, df.avg_aqi.max() * 1.1))
    ax.set_xlabel("24 h average AQI")
    ax.set_title(f"Live top-10 most polluted cities\n(streaming snapshot, window ending {window_end})")
    ax.grid(axis="y", visible=False)
    save(fig, "07_stream_top10_snapshot.png")


def chart_ml():
    metrics_path = os.path.join(ML, "metrics.json")
    if not os.path.exists(metrics_path):
        print("  skip: output/ml/metrics.json not found")
        return
    with open(metrics_path) as f:
        metrics = json.load(f)

    fi = load(os.path.join(ML, "feature_importance.csv")).sort_values("importance")
    fig, ax = plt.subplots(figsize=(6.5, 4.6))
    ax.barh(fi.feature, fi.importance * 100, color="#333")
    for y, v in enumerate(fi.importance * 100):
        ax.text(v + 0.3, y, f"{v:.1f}%", va="center", fontsize=8)
    ax.set_xlabel("Importance (% of total impurity reduction)")
    ax.set_title("Random Forest feature importance")
    ax.grid(axis="y", visible=False)
    save(fig, "08_feature_importance.png")

    cm = pd.read_csv(os.path.join(ML, "confusion_matrix.csv"), index_col="actual")
    share = cm.div(cm.sum(axis=1).replace(0, 1), axis=0)
    fig, ax = plt.subplots(figsize=(6.4, 5.2))
    im = ax.imshow(share.values, cmap="Blues", vmin=0, vmax=1)
    ax.set_xticks(range(len(cm.columns)), cm.columns, rotation=30, ha="right")
    ax.set_yticks(range(len(cm.index)), cm.index)
    for i in range(len(cm.index)):
        for j in range(len(cm.columns)):
            ax.text(j, i, f"{cm.values[i, j]:,}\n{share.values[i, j]:.0%}", ha="center", va="center", fontsize=7,
                    color="white" if share.values[i, j] > 0.55 else "black")
    ax.set_xlabel("Predicted category")
    ax.set_ylabel("Actual category")
    ax.grid(False)
    ax.set_title(f"Confusion matrix, 2020 test set\naccuracy {metrics['accuracy']:.3f} · weighted F1 {metrics['f1_weighted']:.3f}")
    fig.colorbar(im, ax=ax, fraction=0.046, label="share of actual row")
    save(fig, "09_confusion_matrix.png")


def main():
    print("Making report charts...")
    for chart in (chart_month, chart_hour, chart_lockdown, chart_mumbai_delhi, chart_validation, chart_top10, chart_ml):
        chart()


if __name__ == "__main__":
    main()
