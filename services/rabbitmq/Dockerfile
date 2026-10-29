FROM python:3.12-slim-bookworm@sha256:e697c81897302a4c4333bade000d98038fbb21eb064f8ee1fd42d02ebcd9d337
ENV PYTHONUNBUFFERED=1 PYTHONDONTWRITEBYTECODE=1 ARROW_NUM_THREADS=1 OMP_NUM_THREADS=1
WORKDIR /app
COPY export/requirements.txt ./export-requirements.txt
RUN pip install --no-cache-dir -r export-requirements.txt pika==1.4.4
COPY export/app/common.py ./
COPY export/contracts/ ./contracts/
COPY export/tests/test_contract.py ./regression/test_contract.py
COPY rabbitmq/app/ ./
COPY rabbitmq/tests/ ./tests/
USER 10009:10009
ENTRYPOINT ["python"]
CMD ["invoice_app.py", "watch"]
