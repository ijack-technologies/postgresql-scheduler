ARG INSTALL_PYTHON_VERSION=${INSTALL_PYTHON_VERSION:-PYTHON_VERSION_NOT_SET}

FROM python:${INSTALL_PYTHON_VERSION} AS builder

# Use Docker BuildKit for better caching and faster builds
ARG DOCKER_BUILDKIT=1
ARG BUILDKIT_INLINE_CACHE=1
# Enable BuildKit for Docker-Compose
ARG COMPOSE_DOCKER_CLI_BUILD=1

# Python package installation stuff
ARG PIP_NO_CACHE_DIR=1
ARG PIP_DISABLE_PIP_VERSION_CHECK=1
ARG PIP_DEFAULT_TIMEOUT=100

# Don't write .pyc bytecode
ENV PYTHONDONTWRITEBYTECODE=1
# Don't buffer stdout. Write it immediately to the Docker log
ENV PYTHONUNBUFFERED=1
ENV PYTHONFAULTHANDLER=1
ENV PYTHONHASHSEED=random

# UV configuration
ENV UV_CACHE_DIR=/root/.cache/uv
ENV UV_SYSTEM_PYTHON=1
ENV UV_COMPILE_BYTECODE=1
ENV UV_LINK_MODE=copy

# Tell apt-get we're never going to be able to give manual feedback:
ENV DEBIAN_FRONTEND=noninteractive

WORKDIR /project

# Install cron for scheduled jobs and procps for "ps" tool to check running procs
RUN apt-get update && \
    apt-get -y --no-install-recommends install curl git cron nano procps wget dos2unix && \
    # Clean up
    apt-get autoremove -y && \
    apt-get clean -y && \
    rm -rf /var/lib/apt/lists/*

# Install UV (Python package manager) - latest version
RUN curl -LsSf https://astral.sh/uv/install.sh | sh

# IMPORTANT: Copy dependency files FIRST for better layer caching
# UV will only reinstall if these files change
COPY pyproject.toml uv.lock ./

# Install dependencies using UV with cache mount
# This layer is cached unless pyproject.toml or uv.lock changes
RUN --mount=type=cache,target=/root/.cache/uv \
    # Create virtual environment at /venv
    /root/.local/bin/uv venv /venv && \
    # Activate the venv and install dependencies into it
    # UV_PROJECT_ENVIRONMENT tells uv sync to use /venv instead of creating .venv
    UV_PROJECT_ENVIRONMENT=/venv /root/.local/bin/uv sync --frozen --no-install-project --no-dev && \
    # Verify critical dependencies are installed (smoke test)
    /venv/bin/python -c "import pytz; print(f'✓ pytz {pytz.__version__} installed')" && \
    /venv/bin/python -c "import pandas; print(f'✓ pandas {pandas.__version__} installed')" && \
    /venv/bin/python -c "import psycopg2; print(f'✓ psycopg2 installed')"

# Make sure our packages are in the PATH
ENV PATH="/venv/bin:$PATH"

# Copy project files (these change frequently, so do this AFTER dependencies)
COPY .env ./
COPY project ./

# Install the project itself (fast since dependencies already installed)
RUN --mount=type=cache,target=/root/.cache/uv \
    UV_PROJECT_ENVIRONMENT=/venv /root/.local/bin/uv pip install --no-deps -e .

# Verify the project can import (final smoke test)
RUN /venv/bin/python -c "from project.logger_config import logger; print('✓ Project imports working')"




# Final stage of multi-stage build ############################################################
FROM python:${INSTALL_PYTHON_VERSION} as production

# For setting up the non-root user in the container
ARG USERNAME=user
ARG USER_UID=1000
ARG USER_GID=$USER_UID

# Use Docker BuildKit for better caching and faster builds
ARG DOCKER_BUILDKIT=1
ARG BUILDKIT_INLINE_CACHE=1
# Enable BuildKit for Docker-Compose
ARG COMPOSE_DOCKER_CLI_BUILD=1

# Don't write .pyc bytecode
ENV PYTHONDONTWRITEBYTECODE=1
# Don't buffer stdout. Write it immediately to the Docker log
ENV PYTHONUNBUFFERED=1
ENV PYTHONFAULTHANDLER=1
ENV PYTHONHASHSEED=random

# Tell apt-get we're never going to be able to give manual feedback:
ENV DEBIAN_FRONTEND=noninteractive

# Add a new non-root user and change ownership of the workdir
RUN addgroup --gid $USER_GID --system $USERNAME && \
    adduser --no-create-home --shell /bin/false --disabled-password --uid $USER_UID --system --group $USERNAME && \
    # chown -R $USER_UID:$USER_GID /project && \
    # Get curl and netcat for Docker healthcheck
    apt-get update && \
    apt-get -y --no-install-recommends install curl cron nano procps wget iputils-ping && \
    apt-get clean && \
    # Delete index files we don't need anymore:
    rm -rf /var/lib/apt/lists/*

WORKDIR /project

# Make the logs directory writable by the non-root user
RUN mkdir -p /project/logs && \
    chmod 755 /project/logs && \
    chown -R $USER_UID:$USER_GID /project && \
    echo "Main directory permissions: $(ls -la /project)"

# Copy in files and change ownership to the non-root user
COPY --chown=$USER_UID:$USER_GID --from=builder /venv /venv
COPY --chown=$USER_UID:$USER_GID project .env ./

# Set the user so nobody can run as root on the Docker host (security)
USER $USERNAME

# Make sure we use the virtualenv
ENV PATH="/venv/bin:$PATH"
RUN echo PATH = $PATH

# Copy my preferred .bashrc to /root/ so that it's automatically "sourced" when the container starts
COPY .bashrc /$USERNAME

# Add health check to ensure Python can import critical modules
HEALTHCHECK --interval=30s --timeout=10s --start-period=5s --retries=3 \
    CMD /venv/bin/python -c "import pytz, pandas, psycopg2; import project.logger_config" || exit 1

# CMD ["/venv/bin/python3", "/project/main_scheduler.py"]
