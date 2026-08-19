# Official Apache Hive, plus the PostgreSQL JDBC driver.
#
# Hive does not bundle a Postgres driver. Without it, `schematool -initSchema`
# fails at startup with a ClassNotFoundException for org.postgresql.Driver,
# which reads like a Hive problem and is really a missing jar.

FROM apache/hive:4.0.0

USER root

ARG POSTGRES_JDBC_VERSION=42.7.4
ARG MAVEN_CENTRAL=https://repo1.maven.org/maven2

ADD ${MAVEN_CENTRAL}/org/postgresql/postgresql/${POSTGRES_JDBC_VERSION}/postgresql-${POSTGRES_JDBC_VERSION}.jar \
    /opt/hive/lib/postgresql-${POSTGRES_JDBC_VERSION}.jar

RUN chmod 0644 /opt/hive/lib/postgresql-${POSTGRES_JDBC_VERSION}.jar

USER hive
