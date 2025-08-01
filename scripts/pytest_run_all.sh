#!/bin/bash

# Enable exit on non 0
set -e

# Set the current working directory to the directory in which the script is located, for CI/CD
cd "$(dirname "$0")"
cd ..
echo "Current working directory: $(pwd)"

# Run the linter first since it's really fast
sh /workspace/scripts/lint_apply.sh

echo ""
echo "Running pytest..."

# --exitfirst = stop the execution of the tests instantly on first error or failed test
pytest /workspace/test/ -v --durations=0

echo ""
echo "pytest complete!"

exit 0
