FROM python:3.12-slim-bookworm@sha256:e697c81897302a4c4333bade000d98038fbb21eb064f8ee1fd42d02ebcd9d337
ENV PYTHONUNBUFFERED=1 PYTHONDONTWRITEBYTECODE=1
WORKDIR /app
COPY warehouse/loader/requirements.txt ./
RUN pip install --no-cache-dir -r requirements.txt
COPY cdc/archiver/archive.py ./
COPY warehouse/loader/load.py warehouse/loader/sales_contract.py ./
COPY warehouse/loader/resolve.py warehouse/loader/resolve_core.py ./
COPY purge/app/ ./
COPY purge/scripts/source.py ./source_observer.py
COPY purge/scripts/checkpoint-input.py purge/scripts/verify-checkpoint-host.py purge/scripts/source.py /host-scripts/
COPY --chown=10010:10010 purge/tests/ /tests/
USER 10010:10010
CMD ["python","audit.py"]
