#!/bin/bash

# Enable exit on non 0
set -euo pipefail

# Activate the virtual environment if it exists
if [ -d "/workspace/.venv" ] && [ -f "/workspace/.venv/bin/activate" ]; then
  source /workspace/.venv/bin/activate
fi

# Execute the main command passed to the container
exec "$@"