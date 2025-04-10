FROM python:3.13-slim

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

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY . .
COPY docker-entrypoint.sh /usr/local/bin/
RUN chmod +x /usr/local/bin/docker-entrypoint.sh

HEALTHCHECK --interval=30s --timeout=30s --start-period=5s --retries=3 \
    CMD pgrep cron || exit 1

ENTRYPOINT ["docker-entrypoint.sh"]