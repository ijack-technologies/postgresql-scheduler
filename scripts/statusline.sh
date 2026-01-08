#!/bin/bash
# ~/.claude/statusline.sh - Claude Code Status Line
# Shows: [Model] Context% | Branch (with ANSI colors)

# ANSI color codes (from rcom/scripts/dev-servers.sh patterns)
CYAN='\033[0;36m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
RED='\033[0;31m'
MAGENTA='\033[0;35m'
NC='\033[0m' # No Color

# Read JSON from stdin
input=$(cat)

# Parse model name (jq required)
MODEL=$(echo "$input" | jq -r '.model.display_name // "Claude"')

# Calculate context percentage
CONTEXT_SIZE=$(echo "$input" | jq -r '.context_window.context_window_size // 200000')
USAGE=$(echo "$input" | jq -r '.context_window.current_usage // null')

if [ "$USAGE" != "null" ]; then
    INPUT_TOKENS=$(echo "$USAGE" | jq -r '.input_tokens // 0')
    CACHE_CREATE=$(echo "$USAGE" | jq -r '.cache_creation_input_tokens // 0')
    CACHE_READ=$(echo "$USAGE" | jq -r '.cache_read_input_tokens // 0')
    CURRENT=$((INPUT_TOKENS + CACHE_CREATE + CACHE_READ))
    PERCENT=$((CURRENT * 100 / CONTEXT_SIZE))
else
    PERCENT=0
fi

# Color-code percentage based on usage level
if [ "$PERCENT" -lt 50 ]; then
    PERCENT_COLOR="$GREEN"
elif [ "$PERCENT" -lt 80 ]; then
    PERCENT_COLOR="$YELLOW"
else
    PERCENT_COLOR="$RED"
fi

# Get git branch (works in any git repo)
BRANCH=""
if git rev-parse --git-dir > /dev/null 2>&1; then
    BRANCH=$(git branch --show-current 2>/dev/null)
    [ -z "$BRANCH" ] && BRANCH="detached"
fi

# Build output with colors
OUTPUT="${CYAN}[${MODEL}]${NC} ${PERCENT_COLOR}${PERCENT}%${NC}"
if [ -n "$BRANCH" ]; then
    OUTPUT="${OUTPUT} ${NC}|${NC} ${MAGENTA}${BRANCH}${NC}"
fi

echo -e "$OUTPUT"
