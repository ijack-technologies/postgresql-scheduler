#!/bin/bash

# Enable exit on non 0
set -e
set -x

# Set the current working directory to the directory in which the script is located, for CI/CD
cd "$(dirname "$0")"
# cd ..
echo "Current working directory: $(pwd)"

# Nice sorting of imports
echo ""
echo "Running isort..."
isort --profile black ../cron_d --check-only
isort --profile black ../test --check-only

# Remove unused imports and unused variables
echo ""
echo "Running autoflake..."
autoflake --in-place --remove-unused-variables --remove-all-unused-imports --verbose --recursive ../cron_d
autoflake --in-place --remove-unused-variables --remove-all-unused-imports --verbose --recursive ../test

# Opinionated but lovely auto-formatting
echo ""
echo "Running black..."
black ../cron_d --check
black ../test --check

echo ""
echo "Running flake8..."
# flake8 ../cron_d
# flake8 ../test
flake8 ../cron_d --ignore 'E402,E501,W503,E203,E741,C901'
flake8 ../test --ignore 'E402,E501,W503,E203,E741,C901'

# echo ""
# echo "Running mypy..."
# mypy --config-file ../mypy.ini ../cron_d --disallow-untyped-defs

# Security checks with Bandit and Safety
echo ""
echo "Running bandit..."
bandit -r "../cron_d"
bandit -r "../test" --configfile "../.bandit_4_tests.yml"

echo ""
echo "Running safety..."
safety check

# For Jinja2 template blocks
echo ""
echo "Running curlylint..."
curlylint ../cron_d/templates --parse-only

echo ""
echo "Lint check complete!"

exit 0