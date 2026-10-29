FROM python:3.12-slim-bookworm@sha256:e697c81897302a4c4333bade000d98038fbb21eb064f8ee1fd42d02ebcd9d337
ENV PYTHONUNBUFFERED=1 PYTHONDONTWRITEBYTECODE=1
WORKDIR /app
COPY warehouse/loader/requirements.txt ./
RUN pip install --no-cache-dir -r requirements.txt
COPY cdc/archiver/archive.py ./archive.py
COPY warehouse/loader/*.py ./
COPY purge/app/audit_core.py purge/app/row_values.py ./
COPY warehouse/sql/load-batch.sql ./load-batch.sql
USER 10004:10004
CMD ["python", "load.py", "--watch"]
