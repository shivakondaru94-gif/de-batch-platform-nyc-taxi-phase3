# Spark 3.5 ships Hadoop 3.3.x but not the S3A connector, so the image is
# extended with the jars needed to read/write the MinIO lake and to load the
# serving database. Pinning exact versions keeps the build reproducible.
#
# NOTE ON THE REGISTRY: this originally read bitnami/spark:3.5.1. In 2025
# Bitnami withdrew every versioned tag from the public docker.io/bitnami
# catalogue, so that pinned tag stopped resolving and the build broke. The
# archived images live under bitnamilegacy, whose oldest available 3.5 tag is
# 3.5.3. A pinned tag is therefore not immutable: only a digest is. See
# docs/security.md (T6).
FROM bitnamilegacy/spark:3.5.6

USER root

ARG HADOOP_AWS_VERSION=3.3.4
ARG AWS_SDK_VERSION=1.12.262
ARG POSTGRES_JDBC_VERSION=42.7.3
ARG MAVEN=https://repo1.maven.org/maven2

RUN install_packages curl && \
    curl -fsSL -o /opt/bitnami/spark/jars/hadoop-aws-${HADOOP_AWS_VERSION}.jar \
      ${MAVEN}/org/apache/hadoop/hadoop-aws/${HADOOP_AWS_VERSION}/hadoop-aws-${HADOOP_AWS_VERSION}.jar && \
    curl -fsSL -o /opt/bitnami/spark/jars/aws-java-sdk-bundle-${AWS_SDK_VERSION}.jar \
      ${MAVEN}/com/amazonaws/aws-java-sdk-bundle/${AWS_SDK_VERSION}/aws-java-sdk-bundle-${AWS_SDK_VERSION}.jar && \
    curl -fsSL -o /opt/bitnami/spark/jars/postgresql-${POSTGRES_JDBC_VERSION}.jar \
      ${MAVEN}/org/postgresql/postgresql/${POSTGRES_JDBC_VERSION}/postgresql-${POSTGRES_JDBC_VERSION}.jar

# The base image is built to run under an arbitrary uid and so carries no
# /etc/passwd entry for it. Hadoop starts by invoking the JVM's UnixLoginModule,
# which asks the OS for the current user name, receives null, and throws a
# NullPointerException inside UnixPrincipal. Because that is an NPE rather than
# a LoginException, it aborts the login even though the module is optional, and
# HADOOP_USER_NAME is never consulted. Giving the uid a real passwd entry is
# what actually fixes it.
RUN echo 'spark:x:1001:0:spark:/tmp:/sbin/nologin' >> /etc/passwd

# Drop back to the unprivileged user: containers must not run as root.
USER 1001
