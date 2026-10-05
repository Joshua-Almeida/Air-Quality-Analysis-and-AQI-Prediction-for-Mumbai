"""
Step 7 (optional) - Random Forest that predicts the AQI category (AQI_Bucket).

Features : the 12 pollutant readings of that hour + hour of day + month + city
Label    : AQI_Bucket (Good / Satisfactory / Moderate / Poor / Very Poor / Severe)
Split    : by time -- train on 2015-2019, test on 2020. A random split would
           let the model "peek" at neighbouring hours of the same day, which
           makes the score look better than it really is.

Outputs in output/ml/:
  metrics.json             accuracy, weighted F1/precision/recall, per-class F1
  feature_importance.csv   how much each feature contributed to the trees' splits
  confusion_matrix.csv     actual vs predicted counts

Run (stop the streaming job first, this needs more memory):
    $env:SPARK_DRIVER_MEMORY = "3g"
    .\scripts\spark-submit.ps1 spark_jobs/train_model.py --sample 0.3
"""
import argparse
import json
import os

import pandas as pd
from pyspark.ml import Pipeline
from pyspark.ml.classification import RandomForestClassifier
from pyspark.ml.evaluation import MulticlassClassificationEvaluator
from pyspark.ml.feature import StringIndexer, VectorAssembler
from pyspark.sql import functions as F

from spark_jobs.common import BUCKET_ORDER, CLEAN_PARQUET, ML_DIR, POLLUTANTS, get_spark


def parse_args():
    p = argparse.ArgumentParser()
    p.add_argument("--sample", type=float, default=0.3, help="fraction of rows to use (memory)")
    p.add_argument("--trees", type=int, default=40)
    p.add_argument("--depth", type=int, default=10)
    return p.parse_args()


def main():
    args = parse_args()
    spark = get_spark("aqi_bucket_random_forest", shuffle_partitions=16)
    os.makedirs(ML_DIR, exist_ok=True)

    df = (
        spark.read.parquet(CLEAN_PARQUET)
        .select(*POLLUTANTS, "hour", "month", "year", "city", "aqi_bucket")
        .dropna()
        .sample(fraction=args.sample, seed=42)
    )
    # Fewer partitions = fewer trees being grown in parallel = less memory on a laptop
    train = df.filter(F.col("year") < 2020).repartition(6).cache()
    test = df.filter(F.col("year") == 2020).cache()
    print(f"train rows: {train.count():,}   test rows: {test.count():,}")

    # Turn text columns into numbers the trees can use
    label_indexer = StringIndexer(inputCol="aqi_bucket", outputCol="label", handleInvalid="skip")
    city_indexer = StringIndexer(inputCol="city", outputCol="city_idx", handleInvalid="keep")
    feature_cols = POLLUTANTS + ["hour", "month", "city_idx"]
    assembler = VectorAssembler(inputCols=feature_cols, outputCol="features")
    rf = RandomForestClassifier(
        labelCol="label", featuresCol="features",
        numTrees=args.trees, maxDepth=args.depth,
        maxBins=64,            # must be >= number of cities (city is categorical)
        maxMemoryInMB=96,      # split-statistics buffer per task; the 256 MB default ran out of heap
        seed=42,
    )
    model = Pipeline(stages=[label_indexer, city_indexer, assembler, rf]).fit(train)
    predictions = model.transform(test).cache()

    # ---- Metrics -----------------------------------------------------------
    def metric(name):
        return MulticlassClassificationEvaluator(labelCol="label", predictionCol="prediction", metricName=name).evaluate(predictions)

    labels = model.stages[0].labels               # index -> bucket name
    metrics = {
        "train_rows": train.count(),
        "test_rows": predictions.count(),
        "accuracy": round(metric("accuracy"), 4),
        "f1_weighted": round(metric("f1"), 4),
        "precision_weighted": round(metric("weightedPrecision"), 4),
        "recall_weighted": round(metric("weightedRecall"), 4),
        "per_class_f1": {},
        "params": {"trees": args.trees, "max_depth": args.depth, "sample": args.sample},
    }
    for idx, name in enumerate(labels):
        f1 = MulticlassClassificationEvaluator(
            labelCol="label", predictionCol="prediction", metricName="fMeasureByLabel", metricLabel=float(idx)
        ).evaluate(predictions)
        metrics["per_class_f1"][name] = round(f1, 4)

    with open(os.path.join(ML_DIR, "metrics.json"), "w") as f:
        json.dump(metrics, f, indent=2)
    print(json.dumps(metrics, indent=2))

    # ---- Feature importance ------------------------------------------------
    importances = model.stages[-1].featureImportances.toArray()
    fi = pd.DataFrame({"feature": feature_cols, "importance": importances}).sort_values("importance", ascending=False)
    fi["feature"] = fi["feature"].replace({"city_idx": "city"})
    fi.to_csv(os.path.join(ML_DIR, "feature_importance.csv"), index=False)
    print(fi.to_string(index=False))

    # ---- Confusion matrix (actual x predicted) -----------------------------
    cm = predictions.groupBy("label", "prediction").count().toPandas()
    cm["actual"] = cm["label"].astype(int).map(lambda i: labels[i])
    cm["predicted"] = cm["prediction"].astype(int).map(lambda i: labels[i])
    matrix = cm.pivot_table(index="actual", columns="predicted", values="count", fill_value=0)
    order = [b for b in BUCKET_ORDER if b in labels]
    matrix = matrix.reindex(index=order, columns=order, fill_value=0).astype(int)
    matrix.to_csv(os.path.join(ML_DIR, "confusion_matrix.csv"))
    print(matrix)

    spark.stop()


if __name__ == "__main__":
    main()
