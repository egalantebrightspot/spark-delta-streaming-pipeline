FROM bitnami/spark:3.5

USER root

RUN install_packages curl

COPY requirements.txt /tmp/requirements.txt
RUN pip install --no-cache-dir -r /tmp/requirements.txt && \
    rm /tmp/requirements.txt

# Pre-install Delta Lake JARs in Spark's classpath so master and workers
# share them without runtime Maven downloads.
RUN python3 -c "\
import urllib.request, os, pathlib; \
jar_dir = pathlib.Path(os.environ['SPARK_HOME']) / 'jars'; \
base = 'https://repo1.maven.org/maven2/io/delta'; \
jars = [ \
    ('delta-spark_2.12/3.3.2/delta-spark_2.12-3.3.2.jar', 'delta-spark_2.12-3.3.2.jar'), \
    ('delta-storage/3.3.2/delta-storage-3.3.2.jar', 'delta-storage-3.3.2.jar'), \
]; \
[urllib.request.urlretrieve(f'{base}/{p}', str(jar_dir / n)) or print(f'  {n}') for p, n in jars]"

WORKDIR /app

COPY pipeline/       ./pipeline/
COPY generator/      ./generator/
COPY monitoring/     ./monitoring/
COPY scripts/        ./scripts/
COPY notebooks/      ./notebooks/
COPY tests/          ./tests/
COPY infra/docker/config.docker.yaml ./config.docker.yaml
COPY infra/docker/entrypoint.sh      ./entrypoint.sh

RUN chmod +x ./entrypoint.sh && \
    mkdir -p /app/data && \
    chown -R 1001:1001 /app

ENV PYTHONPATH=/app
ENV PIPELINE_CONFIG_PATH=/app/config.docker.yaml
ENV DELTA_JARS_PREINSTALLED=1

USER 1001

ENTRYPOINT ["/app/entrypoint.sh"]
CMD ["help"]
