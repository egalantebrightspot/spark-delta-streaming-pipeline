FROM bitnami/spark:3.5.0

USER root

COPY requirements.txt /app/requirements.txt
RUN pip install --no-cache-dir -r /app/requirements.txt

COPY pipeline/ /app/pipeline/
COPY generator/ /app/generator/

WORKDIR /app

USER 1001
