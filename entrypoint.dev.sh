#!/bin/bash

# Enable exit on non 0
set -euo pipefail

if [ ! -d "/workspace/.venv" ]; then
  echo "Virtual environment not found, running poetry install..."
  # Create a virtualenv at .venv in /workspace directory so we can easily see the site-packages
  poetry config virtualenvs.create true && \
  poetry config virtualenvs.in-project true && \
  poetry config virtualenvs.path "/workspace/.venv" && \
  poetry install --no-interaction --no-ansi --no-root
else
  echo "Virtual environment found at /workspace/.venv; skipping poetry install"
fi

# Activate the virtual environment
source /workspace/.venv/bin/activate

# Execute the main command passed to the container
exec "$@"
