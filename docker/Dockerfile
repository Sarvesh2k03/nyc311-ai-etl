# Airflow image with the pipeline's own dependencies baked in, so the DAG's
# tasks run dbt and the pipeline package directly rather than shelling out to a
# separate environment.
FROM apache/airflow:2.10.5-python3.12

USER root
RUN apt-get update \
 && apt-get install -y --no-install-recommends git \
 && rm -rf /var/lib/apt/lists/*

USER airflow
COPY requirements-pipeline.txt /tmp/requirements-pipeline.txt
RUN pip install --no-cache-dir -r /tmp/requirements-pipeline.txt

# The pipeline package is bind-mounted at /opt/project, not copied, so edits on
# the host take effect without a rebuild.
ENV PYTHONPATH=/opt/project
WORKDIR /opt/project
