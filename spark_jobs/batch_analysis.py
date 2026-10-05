"""
Step 5 - Batch analysis with Spark SQL on the full 2015-2020 dataset.

Questions answered (each saved as a small CSV in output/batch/):
  pm25_by_month.csv         Which months are the most polluted? (seasonality)
  pm25_by_hour.csv          Which hours of the day are the worst? (daily cycle)
  lockdown_by_city.csv      How much cleaner was the 2020 lockdown period than
  lockdown_overall.csv      the same dates in 2019?
  mumbai_vs_delhi.csv       Month-by-month comparison of the two megacities
  city_overall.csv          Long-run average AQI of every city

Run:  .\scripts\spark-submit.ps1 spark_jobs/batch_analysis.py
"""
import os

from spark_jobs.common import BATCH_DIR, CLEAN_PARQUET, get_spark

# India's national lockdown began on 25 March 2020; the dataset ends on 1 July 2020.
LOCKDOWN_START, LOCKDOWN_END = "03-25", "06-30"


def save(df, name):
    """Results are tiny, so collect them to pandas and write one clean CSV."""
    os.makedirs(BATCH_DIR, exist_ok=True)
    pdf = df.toPandas()
    pdf.to_csv(os.path.join(BATCH_DIR, name), index=False)
    print(f"\n=== {name} ({len(pdf)} rows) ===")
    print(pdf.head(15).to_string(index=False))
    return pdf


def main():
    spark = get_spark("batch_analysis")
    spark.read.parquet(CLEAN_PARQUET).createOrReplaceTempView("aq")

    # ---- PM2.5 by month of year (all years pooled) --------------------------
    save(spark.sql("""
        SELECT month,
               ROUND(AVG(pm25), 2) AS avg_pm25,
               ROUND(AVG(aqi), 1)  AS avg_aqi,
               COUNT(*)            AS readings
        FROM aq
        GROUP BY month
        ORDER BY month
    """), "pm25_by_month.csv")

    # ---- PM2.5 by hour of day ----------------------------------------------
    save(spark.sql("""
        SELECT hour,
               ROUND(AVG(pm25), 2) AS avg_pm25,
               ROUND(AVG(aqi), 1)  AS avg_aqi,
               COUNT(*)            AS readings
        FROM aq
        GROUP BY hour
        ORDER BY hour
    """), "pm25_by_hour.csv")

    # ---- 2019 vs 2020 lockdown, same calendar dates -------------------------
    # Only cities that reported in BOTH years are compared, so a city that
    # simply did not exist in 2019 cannot distort the result.
    spark.sql(f"""
        SELECT city, year, pm25, pm10, no2, aqi
        FROM aq
        WHERE year IN (2019, 2020)
          AND date_format(datetime, 'MM-dd') BETWEEN '{LOCKDOWN_START}' AND '{LOCKDOWN_END}'
    """).createOrReplaceTempView("lockdown_period")

    save(spark.sql("""
        WITH per_city AS (
            SELECT city,
                   AVG(CASE WHEN year = 2019 THEN pm25 END) AS pm25_2019,
                   AVG(CASE WHEN year = 2020 THEN pm25 END) AS pm25_2020,
                   AVG(CASE WHEN year = 2019 THEN no2  END) AS no2_2019,
                   AVG(CASE WHEN year = 2020 THEN no2  END) AS no2_2020,
                   AVG(CASE WHEN year = 2019 THEN aqi  END) AS aqi_2019,
                   AVG(CASE WHEN year = 2020 THEN aqi  END) AS aqi_2020
            FROM lockdown_period
            GROUP BY city
        )
        SELECT city,
               ROUND(pm25_2019, 2) AS pm25_2019,
               ROUND(pm25_2020, 2) AS pm25_2020,
               ROUND(100 * (pm25_2020 - pm25_2019) / pm25_2019, 1) AS pm25_change_pct,
               ROUND(no2_2019, 2)  AS no2_2019,
               ROUND(no2_2020, 2)  AS no2_2020,
               ROUND(100 * (no2_2020 - no2_2019) / no2_2019, 1) AS no2_change_pct,
               ROUND(aqi_2019, 1)  AS aqi_2019,
               ROUND(aqi_2020, 1)  AS aqi_2020
        FROM per_city
        WHERE pm25_2019 IS NOT NULL AND pm25_2020 IS NOT NULL
        ORDER BY pm25_change_pct
    """), "lockdown_by_city.csv")

    save(spark.sql("""
        WITH both_years AS (
            SELECT city FROM lockdown_period
            GROUP BY city HAVING COUNT(DISTINCT year) = 2
        )
        SELECT year,
               ROUND(AVG(pm25), 2) AS avg_pm25,
               ROUND(AVG(pm10), 2) AS avg_pm10,
               ROUND(AVG(no2), 2)  AS avg_no2,
               ROUND(AVG(aqi), 1)  AS avg_aqi,
               COUNT(DISTINCT city) AS cities,
               COUNT(*)            AS readings
        FROM lockdown_period
        WHERE city IN (SELECT city FROM both_years)
        GROUP BY year
        ORDER BY year
    """), "lockdown_overall.csv")

    # ---- Mumbai vs Delhi, month by month -----------------------------------
    save(spark.sql("""
        SELECT date_format(datetime, 'yyyy-MM') AS year_month,
               city,
               ROUND(AVG(pm25), 2) AS avg_pm25,
               ROUND(AVG(aqi), 1)  AS avg_aqi,
               COUNT(*)            AS readings
        FROM aq
        WHERE city IN ('Mumbai', 'Delhi')
        GROUP BY date_format(datetime, 'yyyy-MM'), city
        ORDER BY year_month, city
    """), "mumbai_vs_delhi.csv")

    # ---- Long-run city averages --------------------------------------------
    save(spark.sql("""
        SELECT city,
               FIRST(state)        AS state,
               ROUND(AVG(aqi), 1)  AS avg_aqi,
               ROUND(AVG(pm25), 2) AS avg_pm25,
               COUNT(DISTINCT station_id) AS stations,
               MIN(datetime)       AS first_reading,
               MAX(datetime)       AS last_reading
        FROM aq
        GROUP BY city
        ORDER BY avg_aqi DESC
    """), "city_overall.csv")

    spark.stop()


if __name__ == "__main__":
    main()
