# ---------- Builder stage ----------
FROM python:3.10-slim AS builder

RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential \
    libpq-dev \
    git \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

COPY README.md /app/README.md
RUN pip install --no-cache-dir --retries 3 --timeout 60 poetry python-dotenv

COPY pyproject.toml poetry.lock ./

# Accept GitHub token for glapy private repo (passed via --build-arg in CI/CD)
ARG GITHUB_TOKEN=""
ENV GITHUB_ACCESS_TOKEN_GLAPY=${GITHUB_TOKEN}

RUN poetry config installer.max-workers 1 && \
    poetry config installer.parallel false && \
    poetry config virtualenvs.in-project true && \
    for i in 1 2 3; do \
        poetry install --no-root --only main && break || \
        (echo "Poetry install attempt $i failed, retrying in 15 seconds..." && sleep 15); \
    done

COPY . .
RUN poetry install --only main

# ---------- Runtime stage ----------
FROM python:3.10-slim

RUN apt-get update && apt-get install -y --no-install-recommends \
    libpq5 \
    && rm -rf /var/lib/apt/lists/* \
    && pip install --no-cache-dir poetry

WORKDIR /app

COPY --from=builder /app /app

ARG GITHUB_TOKEN=""
ENV GITHUB_ACCESS_TOKEN_GLAPY=${GITHUB_TOKEN}
ENV PYTHONPATH=/app
ENV PATH="/app/.venv/bin:$PATH"

CMD ["poetry", "run", "python"]
