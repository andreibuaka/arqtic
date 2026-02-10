# Multi-stage build for minimal image size
# Stage 1: Install dependencies
FROM python:3.14-slim AS builder

COPY --from=ghcr.io/astral-sh/uv:latest /uv /usr/local/bin/uv

WORKDIR /app

# Copy dependency files first (layer cache optimization)
COPY pyproject.toml uv.lock ./

# Install production dependencies only
RUN uv sync --frozen --no-dev --no-install-project

# Strip bytecode caches, test dirs, and static data we don't need at runtime
RUN find /app/.venv -type d -name "__pycache__" -exec rm -rf {} + 2>/dev/null; \
    find /app/.venv -type d -name "tests" -exec rm -rf {} + 2>/dev/null; \
    find /app/.venv -name "*.pyc" -delete 2>/dev/null; \
    rm -rf /app/.venv/lib/*/site-packages/matplotlib/mpl-data/sample_data 2>/dev/null; \
    rm -rf /app/.venv/lib/*/site-packages/matplotlib/mpl-data/fonts/afm 2>/dev/null; \
    rm -rf /app/.venv/lib/*/site-packages/matplotlib/mpl-data/fonts/pdfcorefonts 2>/dev/null; \
    true

# Stage 2: Runtime image
FROM python:3.14-slim

WORKDIR /app

# Copy the venv from builder
COPY --from=builder /app/.venv /app/.venv

# Copy application code
COPY config.py ./
COPY pipeline/ ./pipeline/
COPY dashboard/ ./dashboard/
COPY forecast/ ./forecast/

# Put venv on PATH
ENV PATH="/app/.venv/bin:$PATH"

# Default: dashboard mode (override with command for pipeline job)
CMD ["streamlit", "run", "dashboard/app.py", "--server.port=8080", "--server.address=0.0.0.0", "--server.headless=true"]
