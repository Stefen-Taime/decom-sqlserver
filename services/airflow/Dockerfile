FROM apache/airflow:3.3.2-python3.12@sha256:9df9c8be4096b9cc626bd7cb1f2b8c712eef66c59c615f8c7e6200871ca10bd1
COPY --chown=airflow:root constraints-3.3.2-python3.12.txt /opt/airflow/constraints.txt
RUN pip install --no-cache-dir 'apache-airflow==3.3.2' 'apache-airflow-providers-fab==3.9.0' 'paramiko==5.0.0' --constraint /opt/airflow/constraints.txt
COPY --chown=airflow:root scripts/init-container.py /opt/lab/init-container.py
COPY --chown=airflow:root scripts/health.py /opt/lab/health.py
