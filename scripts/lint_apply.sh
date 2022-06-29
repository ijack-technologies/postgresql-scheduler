#!/bin/bash

# Enable exit on non 0
set -e
set -x

# Set the current working directory to the directory in which the script is located, for CI/CD
cd "$(dirname "$0")"
# cd ..
echo "Current working directory: $(pwd)"

# Remove unused imports and unused variables
echo ""
echo "Running autoflake..."
autoflake --in-place --remove-unused-variables --remove-all-unused-imports --verbose --recursive ../cron_d
autoflake --in-place --remove-unused-variables --remove-all-unused-imports --verbose --recursive ../test

echo ""
echo "Running autopep8 to remove whitespace (NOTE this doesn't change multi-line strings!)..."
autopep8 --in-place --recursive --exclude="*/migrations/*" --select="W291,W293" ../cron_d
autopep8 --in-place --recursive --exclude="*/migrations/*" --select="W291,W293" ../test

# Opinionated but lovely auto-formatting
echo ""
echo "Running black..."
black ../cron_d
black ../test

# Nice sorting of imports
echo ""
echo "Running isort..."
isort --profile black ../cron_d
isort --profile black ../test
