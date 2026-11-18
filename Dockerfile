ARG PYTHON_TAG=3.15-slim

# Build stage
FROM python:${PYTHON_TAG} AS builder

ENV PIP_DISABLE_PIP_VERSION_CHECK=1

WORKDIR /app

# Install dependencies in a virtual environment
COPY requirements.txt .
RUN python -m venv /opt/venv && \
    /opt/venv/bin/pip install --no-cache-dir --only-binary=:all: -r requirements.txt

# Runtime stage
FROM python:${PYTHON_TAG}

ARG REVISION=unknown

LABEL org.opencontainers.image.title="prometheus-helper" \
      org.opencontainers.image.description="Prometheus health checker" \
      org.opencontainers.image.version="0.1.0" \
      org.opencontainers.image.vendor="wlanboy" \
      org.opencontainers.image.source="https://github.com/wlanboy/prometheus_helper" \
      org.opencontainers.image.revision="${REVISION}" \
      org.opencontainers.image.licenses="Apache-2.0" \
      org.opencontainers.image.base.name="python:3.15-slim"

# Verhindert, dass Python .pyc Dateien schreibt und sorgt für sofortige Log-Ausgabe
ENV PYTHONDONTWRITEBYTECODE=1 PYTHONUNBUFFERED=1 PATH="/opt/venv/bin:$PATH"

# Create non-root user
RUN groupadd -r -g 1000 appuser && useradd -r -u 1000 -g appuser appuser

WORKDIR /app

# Copy virtual environment from builder
COPY --from=builder /opt/venv /opt/venv

# Copy application files
COPY --chown=appuser:appuser main.py server.py /app/

# Use non-root user
USER appuser

EXPOSE 9877

# server.py beendet sich bei KeyboardInterrupt sauber; SIGTERM hätte als PID 1 keinen Handler
STOPSIGNAL SIGINT

# Konfiguration per Environment (PROMETHEUS_URL, ...), weitere Optionen als Argumente an docker run anhängen
ENTRYPOINT ["python", "server.py"]
