FROM python:3.13.4-slim

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    DATA_DIR=/data

RUN groupadd --gid 10001 bot \
    && useradd --uid 10001 --gid 10001 --no-create-home --shell /usr/sbin/nologin bot \
    && mkdir -p /app /data \
    && chown 10001:10001 /data

WORKDIR /app
ARG APP_VERSION=1.6.0
LABEL org.opencontainers.image.version=${APP_VERSION}
COPY requirements.lock ./requirements.lock
RUN python -m pip install --no-cache-dir --require-hashes -r requirements.lock

COPY Welcome_Bot.py healthcheck.py ./
USER 10001:10001

HEALTHCHECK --interval=30s --timeout=5s --start-period=40s --retries=3 \
    CMD ["python", "/app/healthcheck.py"]

CMD ["python", "Welcome_Bot.py"]
