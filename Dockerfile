ARG INSTALL_PYTHON_VERSION=${INSTALL_PYTHON_VERSION:-PYTHON_VERSION_NOT_SET}

# ============================================================================
# Builder Stage: Install dependencies and project
# ============================================================================
FROM python:${INSTALL_PYTHON_VERSION} AS builder

# Python environment variables
ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PYTHONFAULTHANDLER=1 \
    PYTHONHASHSEED=random \
    DEBIAN_FRONTEND=noninteractive

# Install system packages FIRST (UV install script needs curl)
RUN apt-get update && \
    apt-get -y --no-install-recommends install curl git && \
    apt-get autoremove -y && \
    apt-get clean -y && \
    rm -rf /var/lib/apt/lists/*

# Install UV using install script (now curl is available)
ADD https://astral.sh/uv/install.sh /install.sh
RUN sh /install.sh && rm /install.sh
ENV PATH="/root/.local/bin:$PATH"

# UV optimization environment variables
ENV UV_COMPILE_BYTECODE=1 \
    UV_LINK_MODE=copy

WORKDIR /app

# Install dependencies ONLY (cached layer - only invalidated when deps change)
# Using bind mounts is more efficient than COPY
RUN --mount=type=cache,target=/root/.cache/uv \
    --mount=type=bind,source=uv.lock,target=uv.lock \
    --mount=type=bind,source=pyproject.toml,target=pyproject.toml \
    uv sync --frozen --no-install-project --no-dev

# Smoke test: Verify critical dependencies installed
RUN .venv/bin/python -c "import pytz; print(f'✓ pytz {pytz.__version__} installed')" && \
    .venv/bin/python -c "import pandas; print(f'✓ pandas {pandas.__version__} installed')" && \
    .venv/bin/python -c "import psycopg2; print(f'✓ psycopg2 installed')"

# Copy project files (changes frequently, so separate layer)
COPY .env ./
COPY project ./project

# Install the project itself into .venv
RUN --mount=type=cache,target=/root/.cache/uv \
    uv sync --frozen --no-dev

# Final smoke test: Verify project imports work
RUN .venv/bin/python -c "from project.logger_config import logger; print('✓ Project imports working')"


# ============================================================================
# Production Stage: Minimal runtime image
# ============================================================================
FROM python:${INSTALL_PYTHON_VERSION} AS production

# Non-root user configuration
ARG USERNAME=user
ARG USER_UID=1000
ARG USER_GID=$USER_UID

# Python environment variables
ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PYTHONFAULTHANDLER=1 \
    PYTHONHASHSEED=random \
    DEBIAN_FRONTEND=noninteractive

# Install minimal runtime dependencies
RUN apt-get update && \
    apt-get -y --no-install-recommends install \
        cron \
        nano \
        procps \
        iputils-ping && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*

# Create non-root user
RUN addgroup --gid $USER_GID --system $USERNAME && \
    adduser --no-create-home --shell /bin/false --disabled-password \
            --uid $USER_UID --system --group $USERNAME

WORKDIR /app

# Create logs directory with proper permissions
RUN mkdir -p /app/logs && \
    chown -R $USER_UID:$USER_GID /app

# Copy virtualenv and project from builder
COPY --from=builder --chown=$USER_UID:$USER_GID /app/.venv /app/.venv
COPY --from=builder --chown=$USER_UID:$USER_GID /app/project /app/project
COPY --from=builder --chown=$USER_UID:$USER_GID /app/.env /app/.env

# Switch to non-root user
USER $USERNAME

# Add .venv/bin to PATH
ENV PATH="/app/.venv/bin:$PATH"

# Health check: Verify Python can import critical modules
HEALTHCHECK --interval=30s --timeout=10s --start-period=5s --retries=3 \
    CMD /app/.venv/bin/python -c "import pytz, pandas, psycopg2; import project.logger_config" || exit 1

# Default command (can be overridden in docker-compose)
# CMD ["/app/.venv/bin/python", "/app/scheduler_jobs.py"]
