#!/bin/bash

# Enable exit on non 0
set -e
set -x

# Set the current working directory to the directory in which the script is located, for CI/CD
cd "$(dirname "$0")"
echo "Current working directory: $(pwd)"

# Use Ruff to check everything (without applying fixes)
echo ""
echo "Running ruff linter (check only)..."
ruff check ../project --config ../pyproject.toml
ruff check ../test --config ../pyproject.toml

# Check formatting (without applying)
echo ""
echo "Running ruff formatter (check only)..."
ruff format ../project --check --config ../pyproject.toml
ruff format ../test --check --config ../pyproject.toml

echo ""
echo "Lint check complete!"

exit 0