#!/bin/bash

set -e

# Set the current working directory to the directory in which the script is located, for CI/CD
cd "$(dirname "$0")"
cd ..
echo "Current working directory: $(pwd)"

# Load environment variables from dotenv / .env file in Bash, and remove comments
export $(cat .env | sed 's/#.*//g' | xargs)

# Repository Setup
# For localhost use "host.docker.internal"
export REPO_NAME=ijack_private
# export REPO_URL=http://host.docker.internal:81/
export REPO_URL=https://pypi.myijack.com
# poetry config repositories.${REPO_NAME} $REPO_URL

# Security
# poetry config pypi-token.${REPO_NAME} $PYPI_TOKEN_PRIVATE
# The following assumes the repository is already configured.
# Poetry config stuff is saved in ~/config.toml and ~/auth.toml
poetry config http-basic.${REPO_NAME} $PYPI_USERNAME_PRIVATE $PYPI_PASSWORD_PRIVATE