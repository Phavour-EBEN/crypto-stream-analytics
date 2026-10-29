FROM python:3.11-slim-bookworm

# Spark needs Java
RUN apt-get update \
 && apt-get install -y --no-install-recommends openjdk-17-jre-headless procps \
 && rm -rf /var/lib/apt/lists/*

RUN pip install --no-cache-dir pyspark==3.5.1

WORKDIR /app
COPY stream_processor.py .

# 3g driver memory: the VM has 8 GB, shared with Redpanda (1 GB) and the producer
CMD ["spark-submit", \
     "--master", "local[*]", \
     "--driver-memory", "3g", \
     "--packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1,org.apache.hadoop:hadoop-azure:3.3.4", \
     "stream_processor.py"]
