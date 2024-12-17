ARG INSTALL_PYTHON_VERSION=${INSTALL_PYTHON_VERSION:-PYTHON_VERSION_NOT_SET}

FROM python:${INSTALL_PYTHON_VERSION} AS builder

ARG POETRY_VERSION=1.8.3

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

# The following only runs in the "builder" build stage of this multi-stage build.
RUN \
    # Use a virtual environment for easy transfer of builder packages
    python -m venv /venv && \
    /venv/bin/pip install --upgrade pip setuptools wheel poetry-plugin-export && \
    /venv/bin/pip install "poetry==$POETRY_VERSION"

# Poetry exports the requirements to stdout in a "requirements.txt" file format,
# and pip installs them in the /venv virtual environment. We need to copy in both
# pyproject.toml AND poetry.lock for this to work!
COPY pyproject.toml poetry.lock ./
# COPY pyproject.toml ./
RUN /venv/bin/poetry config virtualenvs.create false && \
    # Export the requirements to stdout, and install them in the virtual environment
    /venv/bin/poetry export --no-interaction --no-ansi --without-hashes --format requirements.txt \
    $(test "$ENVIRONMENT" != "production" && echo "--with dev") \
    | /venv/bin/pip install -r /dev/stdin --no-cache-dir

# Make sure our packages are in the PATH
# ENV PATH="/project/node_modules/.bin:$PATH"
ENV PATH="/venv/bin:$PATH"

COPY .env entrypoint.sh ./
# Copy the project files into the container, in the /project workdir
COPY project ./




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
    # chmod 777 /project/logs
    chown -R $USER_UID:$USER_GID /project && \
    echo "Main directory permissions: $(ls -la /project)"

# Copy in files and change ownership to the non-root user
COPY --chown=$USER_UID:$USER_GID --from=builder /venv /venv
COPY --chown=$USER_UID:$USER_GID project .env ./

# Set the user so nobody can run as root on the Docker host (security)
USER $USERNAME

# Add the / folder so we can import from project.utils, etc.
# Add the /venv/bin folder so we can run python, etc.
ENV PATH="/:/venv/bin:$PATH"
RUN echo PATH = $PATH

# Copy my preferred .bashrc to /root/ so that it's automatically "sourced" when the container starts
COPY .bashrc /$USERNAME

CMD ["/venv/bin/python3", "/project/main_scheduler.py"]
