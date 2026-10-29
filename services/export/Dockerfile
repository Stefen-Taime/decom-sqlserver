FROM python:3.12-slim-bookworm@sha256:e697c81897302a4c4333bade000d98038fbb21eb064f8ee1fd42d02ebcd9d337
ENV PYTHONUNBUFFERED=1 PYTHONDONTWRITEBYTECODE=1 ARROW_NUM_THREADS=1 OMP_NUM_THREADS=1
WORKDIR /app
COPY requirements.txt ./
RUN pip install --no-cache-dir -r requirements.txt
COPY app/ ./
COPY contracts/ ./contracts/
COPY tests/ ./tests/
USER 10005:10005
ENTRYPOINT ["python"]
CMD ["export.py", "--help"]
