FROM apache/airflow:2.9.3-python3.11

# The scheduler submits work to the Spark cluster and runs the ingestion
# job, so it needs the Spark provider, a JRE for spark-submit, and boto3
# to talk to the MinIO lake.
USER root
# The JRE's install path embeds the CPU architecture (...-amd64 on Intel,
# ...-arm64 on Apple Silicon). A stable symlink keeps JAVA_HOME correct on
# both, so the same image builds and runs on either kind of machine.
RUN apt-get update && \
    apt-get install -y --no-install-recommends openjdk-17-jre-headless curl && \
    ln -s /usr/lib/jvm/java-17-openjdk-$(dpkg --print-architecture) /usr/lib/jvm/java-17 && \
    apt-get clean && rm -rf /var/lib/apt/lists/*
ENV JAVA_HOME=/usr/lib/jvm/java-17

USER airflow
# Dependencies are specified in requirements.txt rather than inline, so the
# dependency specification is a reviewable file of its own.
COPY requirements.txt /tmp/requirements.txt
RUN pip install --no-cache-dir -r /tmp/requirements.txt
