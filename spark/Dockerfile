# Official Apache Spark image: Spark 3.5.9, Scala 2.12, Java 17, Python 3 (Ubuntu)
FROM apache/spark:3.5.9-scala2.12-java17-python3-ubuntu

USER root

# numpy is required by Spark MLlib; pandas/pyarrow are handy for small outputs
RUN pip install --no-cache-dir numpy==1.26.4 pandas==2.2.3 pyarrow==17.0.0

# Quieter logs: warnings and errors only
COPY log4j2.properties /opt/spark/conf/log4j2.properties

# Put spark-submit / pyspark on the PATH
ENV PATH="/opt/spark/bin:${PATH}"
# Make pyspark and our own package (spark_jobs/common.py) importable
ENV PYTHONPATH="/app:/opt/spark/python:/opt/spark/python/lib/py4j-0.10.9.7-src.zip"

WORKDIR /app
