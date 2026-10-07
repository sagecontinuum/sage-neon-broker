# apache/kafka plus the Prometheus JMX exporter Java agent.
# Docker Compose builds this automatically ("docker compose up -d --build").
# Both the mirror-kafka and mirrormaker containers run this image.

ARG KAFKA_IMAGE=apache/kafka:4.3.1
FROM ${KAFKA_IMAGE}

USER root
RUN mkdir -p /opt/jmx-exporter

ARG JMX_EXPORTER_VERSION=1.6.0
RUN wget https://github.com/prometheus/jmx_exporter/releases/download/${JMX_EXPORTER_VERSION}/jmx_prometheus_javaagent-${JMX_EXPORTER_VERSION}.jar \
    -O /opt/jmx-exporter/jmx_prometheus_javaagent.jar
