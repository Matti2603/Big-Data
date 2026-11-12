# Airflow mit Java und PySpark: die Spark-Jobs laufen im lokalen Modus direkt
# im Scheduler-Container (LocalExecutor) und lesen/schreiben HDFS.
FROM apache/airflow:2.10.5-python3.11

USER root
RUN apt-get update \
 && apt-get install -y --no-install-recommends openjdk-17-jre-headless procps \
 && rm -rf /var/lib/apt/lists/* \
 && ln -s "/usr/lib/jvm/java-17-openjdk-$(dpkg --print-architecture)" /opt/java
ENV JAVA_HOME=/opt/java

USER airflow
COPY requirements.txt /tmp/requirements.txt
RUN pip install --no-cache-dir "apache-airflow==${AIRFLOW_VERSION}" -r /tmp/requirements.txt
