# JEV trading engine image (backtest / paper only; live trading is blocked in code).
# Build from the repository root:  docker build -f infra/docker/trading-engine.Dockerfile -t jev-trading-engine .
FROM python:3.12-slim

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PIP_NO_CACHE_DIR=1 \
    PIP_DISABLE_PIP_VERSION_CHECK=1

WORKDIR /app

RUN useradd --create-home --uid 10001 jev

COPY pyproject.toml README.md ./
COPY packages ./packages
COPY apps ./apps
COPY config ./config
# Editable install: the code runs from /app, so config/default.yaml is found next to it.
RUN pip install --upgrade pip setuptools && pip install -e ".[postgres]" \
    && mkdir -p /app/data && chown jev:jev /app/data

USER jev

# No secrets are baked into the image: they come from the environment at runtime (see .env.example).
ENTRYPOINT ["python", "-m", "apps.trading_engine"]
CMD ["--help"]
