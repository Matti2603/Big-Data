# HDFS als Single-Node (NameNode + DataNode in einem Container).
# Eigenes Image statt apache/hadoop, weil es dieses nur für amd64 gibt; das
# lean-Paket von Hadoop ist reines Java und läuft damit auch nativ auf arm64.
FROM eclipse-temurin:11-jre-jammy

ARG HADOOP_VERSION=3.4.2

RUN apt-get update \
 && apt-get install -y --no-install-recommends curl procps \
 && rm -rf /var/lib/apt/lists/*

# Download mit Prüfsumme; dlcdn ist schneller, archive.apache.org hält alte Versionen
RUN set -eux; \
    f="hadoop-${HADOOP_VERSION}-lean.tar.gz"; \
    for base in https://dlcdn.apache.org/hadoop/common https://archive.apache.org/dist/hadoop/common; do \
      curl -fsSL -o "/tmp/$f" "$base/hadoop-${HADOOP_VERSION}/$f" && break; \
    done; \
    curl -fsSL "https://archive.apache.org/dist/hadoop/common/hadoop-${HADOOP_VERSION}/$f.sha512" \
      | grep -oE '[0-9a-fA-F]{128}' | tr 'A-F' 'a-f' > /tmp/sum; \
    echo "$(cat /tmp/sum)  /tmp/$f" | sha512sum -c -; \
    tar -xzf "/tmp/$f" -C /opt; \
    mv "/opt/hadoop-${HADOOP_VERSION}" /opt/hadoop; \
    rm -rf "/tmp/$f" /tmp/sum /opt/hadoop/share/doc

ENV HADOOP_HOME=/opt/hadoop \
    HADOOP_CONF_DIR=/opt/hadoop/etc/hadoop \
    PATH=/opt/hadoop/bin:/opt/hadoop/sbin:$PATH

RUN useradd --create-home --uid 1001 hadoop \
 && mkdir -p /data /opt/hadoop/logs \
 && chown -R hadoop:hadoop /data /opt/hadoop/logs

COPY core-site.xml hdfs-site.xml /opt/hadoop/etc/hadoop/
COPY entrypoint.sh healthcheck.sh /usr/local/bin/

USER hadoop
VOLUME /data
EXPOSE 9000 9870 9864

HEALTHCHECK --interval=10s --timeout=5s --start-period=30s --retries=30 CMD ["healthcheck.sh"]
ENTRYPOINT ["entrypoint.sh"]
