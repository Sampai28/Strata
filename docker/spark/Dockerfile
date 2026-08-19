# Official Apache Spark, plus the one jar that is not bundled with it.
#
# spark-avro ships separately from the Spark distribution. The usual way to get
# it is `--packages org.apache.spark:spark-avro_2.12:3.5.3`, which resolves from
# Maven at job submission time — that means every single spark-submit pays an
# Ivy resolution cost, and the whole thing breaks without network access. Baking
# the jar into an image layer costs one download at build time and makes every
# subsequent run offline-capable and deterministic.
#
# Without this, experiment 2 cannot test Avro at all, which would leave the
# format comparison as columnar-versus-CSV and lose the row-oriented-binary
# middle ground that makes the projection result meaningful.

FROM apache/spark:3.5.3

USER root

ARG SPARK_VERSION=3.5.3
ARG SCALA_BINARY=2.12
ARG MAVEN_CENTRAL=https://repo1.maven.org/maven2

ADD ${MAVEN_CENTRAL}/org/apache/spark/spark-avro_${SCALA_BINARY}/${SPARK_VERSION}/spark-avro_${SCALA_BINARY}-${SPARK_VERSION}.jar \
    /opt/spark/jars/spark-avro_${SCALA_BINARY}-${SPARK_VERSION}.jar

# ADD leaves downloaded files as 0600 root-owned, which the spark user then
# cannot read — the failure surfaces as ClassNotFoundException for the Avro data
# source rather than as a permission error, which is a long way to travel for a
# chmod.
RUN chmod 0644 /opt/spark/jars/spark-avro_${SCALA_BINARY}-${SPARK_VERSION}.jar

# Event-log directory for the history server, and a writable home for the
# client container's Ivy/Spark scratch files.
RUN mkdir -p /opt/spark/work-dir /opt/strata /home/spark \
    && chown -R 185:0 /opt/spark/work-dir /opt/strata /home/spark

ENV HOME=/home/spark
ENV PYSPARK_PYTHON=python3
ENV PYSPARK_DRIVER_PYTHON=python3

USER 185
WORKDIR /opt/strata
