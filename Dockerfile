FROM python:3.12-slim as builder

RUN apt-get update && \
    apt-get install -y --no-install-recommends \
    git \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

COPY requirements.txt .
RUN python -m venv /opt/venv
ENV PATH="/opt/venv/bin:$PATH"
RUN pip install --no-cache-dir -r requirements.txt

FROM python:3.12-slim

COPY --from=builder /opt/venv /opt/venv
ENV PATH="/opt/venv/bin:$PATH"

ENV DOCKER_CONTAINER=1 \
    PYTHONUNBUFFERED=1

RUN apt-get update && \
    apt-get install -y --no-install-recommends \
    git \
    cron \
    && rm -rf /var/lib/apt/lists/* && \
    mkdir -p /var/log && \
    touch /var/log/cache-mover.log

WORKDIR /app

COPY . .
RUN pip install --no-cache-dir -r requirements.txt  # Install requirements in the final image too
COPY docker-entrypoint.sh /usr/local/bin/
RUN chmod +x /usr/local/bin/docker-entrypoint.sh

HEALTHCHECK --interval=30s --timeout=30s --start-period=5s --retries=3 \
    CMD ps aux | grep '[c]ache-mover.py' || exit 1

ENTRYPOINT ["docker-entrypoint.sh"]