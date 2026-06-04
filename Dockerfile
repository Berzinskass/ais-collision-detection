# PySpark 4.x runs on the JVM, so the image needs both Python and a JRE (>=17).
FROM python:3.11-slim

# --- system deps: a headless JDK for Spark + procps (Spark calls `ps`) -------
RUN apt-get update \
    && apt-get install -y --no-install-recommends \
        default-jdk-headless \
        procps \
    && rm -rf /var/lib/apt/lists/*

# JAVA_HOME is auto-resolved from the default-jdk symlink.
ENV JAVA_HOME=/usr/lib/jvm/default-java
ENV PYTHONUNBUFFERED=1
# Spark binds to the loopback interface inside the container.
ENV SPARK_LOCAL_IP=127.0.0.1

WORKDIR /app

# --- python deps (cached layer) ----------------------------------------------
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# --- application code --------------------------------------------------------
COPY src/ ./src/
COPY tests/ ./tests/

# Mount points for input data and generated output.
VOLUME ["/app/data", "/app/output"]

ENV AIS_DATA_GLOB=data/*.csv
ENV AIS_OUTPUT_DIR=output
ENV SPARK_DRIVER_MEMORY=4g

# Default command runs the detection pipeline over whatever CSVs are in /app/data.
CMD ["python", "-m", "src.main"]
