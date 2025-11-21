FROM python:3.12-slim-bookworm

RUN apt-get update && apt-get install -y --no-install-recommends \
  msmtp \
  msmtp-mta \
  bsd-mailx \
  ca-certificates \
  && rm -rf /var/lib/apt/lists/*

COPY --from=ghcr.io/astral-sh/uv:latest /uv /uvx /bin/

WORKDIR /app
COPY . .
RUN uv sync

EXPOSE 8000
ENTRYPOINT ["uv", "run", "gunicorn", "-k", "uvicorn.workers.UvicornWorker", "-b", "0.0.0.0:8000", "main:app"]