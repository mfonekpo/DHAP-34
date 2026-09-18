FROM apache/airflow:2.11.2-python3.11

# Only named volumes are writable, so no host UID mapping is necessary.
USER root
RUN mkdir -p /opt/airflow/work /opt/airflow/project /opt/airflow/scripts \
    && chown -R airflow:root /opt/airflow/work /opt/airflow/project /opt/airflow/scripts \
    && chmod 0770 /opt/airflow/work
USER airflow

COPY --chown=airflow:root requirements.txt /opt/airflow/requirements.txt
RUN pip install --no-cache-dir \
    --constraint https://raw.githubusercontent.com/apache/airflow/constraints-2.11.2/constraints-3.11.txt \
    -r /opt/airflow/requirements.txt \
    && pip check

COPY --chown=airflow:root scripts/airflow_env.py /opt/airflow/scripts/airflow_env.py

# Preserve the official entrypoint's database checks and signal handling, after
# building connection settings from individual environment variables safely.
ENTRYPOINT ["/usr/bin/dumb-init", "--", "python", "/opt/airflow/scripts/airflow_env.py"]
CMD ["airflow", "version"]
