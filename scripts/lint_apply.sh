#!/bin/bash

# Enable exit on non 0
set -e
set -x

# Set the current working directory to the directory in which the script is located, for CI/CD
cd "$(dirname "$0")"
# cd ..
echo "Current working directory: $(pwd)"

# Use Ruff to lint everything
echo ""
echo "Running ruff linter..."
# Run the linter
ruff check ../project --fix --config ../pyproject.toml
ruff check ../test --fix --config ../pyproject.toml

# Run the formatter
echo ""
echo "Running ruff formatter..."
ruff format ../project --config ../pyproject.toml
ruff format ../test --config ../pyproject.toml

# # Don't need this if using Ruff
# # Remove unused imports and unused variables
# echo ""
# echo "Running autoflake..."
# autoflake --in-place --remove-unused-variables --remove-all-unused-imports --verbose --recursive ../project
# autoflake --in-place --remove-unused-variables --remove-all-unused-imports --verbose --recursive ../test

# # Don't need this if using Ruff
# echo ""
# echo "Running autopep8 to remove whitespace (NOTE this doesn't change multi-line strings!)..."
# autopep8 --in-place --recursive --exclude="*/migrations/*" --select="W291,W293" ../project
# autopep8 --in-place --recursive --exclude="*/migrations/*" --select="W291,W293" ../test

# # Opinionated but lovely auto-formatting
# echo ""
# echo "Running black..."
# black ../project
# black ../test

# # Nice sorting of imports (don't need this if using Ruff. It also sorts slightly differently)
# echo ""
# echo "Running isort..."
# isort --profile black ../project
# isort --profile black ../test
