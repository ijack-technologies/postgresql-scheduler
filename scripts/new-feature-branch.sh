#!/bin/bash

# Script to create a new feature branch with timestamp
# Usage: ./scripts/new-feature-branch.sh
#
# This script:
# 1. Stashes all uncommitted changes (modified and untracked files)
# 2. Switches to main branch and pulls latest changes
# 3. Deletes the old feature branch (if not already on main)
# 4. Creates a new feature branch with format: username/YYYY-MM-DD-HHMM
# 5. Pushes the new branch to origin
# 6. Restores all stashed changes to the new branch

# Color codes for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

# Error handling function
error_exit() {
    echo -e "${RED}Error: $1${NC}" >&2
    exit 1
}

# Warning function (doesn't exit)
warning() {
    echo -e "${YELLOW}Warning: $1${NC}" >&2
}

# Get current branch
CURRENT_BRANCH=$(git branch --show-current)
if [ -z "$CURRENT_BRANCH" ]; then
    error_exit "Could not determine current branch"
fi

# Get GitHub username from git config (try github.user first)
GITHUB_USERNAME=$(git config github.user 2>/dev/null || true)

# If not set, try user.email to extract username
if [ -z "$GITHUB_USERNAME" ]; then
    USER_EMAIL=$(git config user.email 2>/dev/null || true)
    # Extract username from email if it's a GitHub noreply email
    if [[ "$USER_EMAIL" =~ ^([^@]+)@users\.noreply\.github\.com$ ]]; then
        # Handle both formats: "username@users.noreply.github.com" and "ID+username@users.noreply.github.com"
        EMAIL_PREFIX="${BASH_REMATCH[1]}"
        if [[ "$EMAIL_PREFIX" =~ \+(.+)$ ]]; then
            GITHUB_USERNAME="${BASH_REMATCH[1]}"
        else
            GITHUB_USERNAME="$EMAIL_PREFIX"
        fi
    fi
fi

# If still not found, use the first part of user.name as fallback
if [ -z "$GITHUB_USERNAME" ]; then
    GITHUB_USERNAME=$(git config user.name 2>/dev/null | awk '{print tolower($1)}' || echo "user")
fi

# Get current date and time in YYYY-MM-DD-HHMM format
CURRENT_DATETIME=$(date +%Y-%m-%d-%H%M)

# New branch name with timestamp
NEW_BRANCH="${GITHUB_USERNAME}/${CURRENT_DATETIME}"

echo -e "${YELLOW}Current branch: ${CURRENT_BRANCH}${NC}"
echo -e "${YELLOW}New branch will be: ${NEW_BRANCH}${NC}"
echo ""

# Check for uncommitted changes and stash them if present
STASH_CREATED=false
if ! git diff-index --quiet HEAD -- || [ -n "$(git ls-files --others --exclude-standard)" ]; then
    echo -e "${YELLOW}Detected uncommitted changes. Stashing them temporarily...${NC}"

    # Count stashes before
    STASH_COUNT_BEFORE=$(git stash list | wc -l)

    # Attempt to stash changes
    # Note: git stash may return exit code 1 for warnings (e.g., "failed to remove: Device or resource busy")
    # but still successfully create the stash. We check if stash was created rather than relying on exit code.
    if git stash push -u -m "Auto-stash for new-feature-branch script on $(date '+%Y-%m-%d %H:%M:%S')" 2>&1 | tee /tmp/stash-output.txt; then
        : # Stash command completed
    fi

    # Check if warnings occurred but stash was still created
    if grep -q "warning: failed to remove" /tmp/stash-output.txt; then
        warning "Some files could not be removed (device busy), but stash was created"
    fi

    # Count stashes after
    STASH_COUNT_AFTER=$(git stash list | wc -l)

    # Verify stash was actually created
    if [ "$STASH_COUNT_AFTER" -gt "$STASH_COUNT_BEFORE" ]; then
        STASH_CREATED=true
        echo -e "${GREEN}✅ Stash entry created${NC}"

        # Verify working directory is actually clean after stash
        # This handles "device busy" cases where stash is created but files remain
        if ! git diff-index --quiet HEAD -- || [ -n "$(git ls-files --others --exclude-standard)" ]; then
            echo -e "${YELLOW}Working directory still has changes after stash (likely due to device busy)${NC}"
            echo -e "${YELLOW}Attempting to restore clean state with git checkout...${NC}"

            # Force checkout to discard working directory changes (stash has them saved)
            if git checkout -- .; then
                echo -e "${GREEN}✅ Working directory cleaned${NC}"
            else
                # If checkout fails, try git restore
                if git restore .; then
                    echo -e "${GREEN}✅ Working directory cleaned with git restore${NC}"
                else
                    error_exit "Failed to clean working directory after stash. Please manually run: git checkout -- ."
                fi
            fi

            # Clean untracked files that couldn't be removed
            git clean -fd 2>/dev/null || true
        fi
        echo ""
    else
        # Check if there are still uncommitted changes
        if ! git diff-index --quiet HEAD -- || [ -n "$(git ls-files --others --exclude-standard)" ]; then
            error_exit "Failed to stash changes. Please commit or stash manually and try again."
        else
            echo -e "${GREEN}No changes needed to be stashed${NC}"
            echo ""
        fi
    fi
fi

# Check if we're already on main
if [ "$CURRENT_BRANCH" == "main" ]; then
    echo -e "${YELLOW}Already on main branch, skipping deletion step${NC}"
else
    # Switch to main first
    echo -e "${GREEN}Switching to main branch...${NC}"
    if ! git checkout main; then
        error_exit "Failed to switch to main branch"
    fi

    # Delete the old feature branch (only if it exists)
    if git show-ref --verify --quiet "refs/heads/$CURRENT_BRANCH"; then
        echo -e "${GREEN}Deleting branch ${CURRENT_BRANCH}...${NC}"
        if ! git branch -D "$CURRENT_BRANCH"; then
            warning "Could not delete old branch $CURRENT_BRANCH (may be checked out elsewhere)"
        fi
    fi
fi

# Pull latest changes from origin
echo -e "${GREEN}Pulling latest changes from origin/main...${NC}"
if ! git pull origin main; then
    error_exit "Failed to pull latest changes from origin/main"
fi

# Create and checkout new feature branch
echo -e "${GREEN}Creating new feature branch: ${NEW_BRANCH}${NC}"
if ! git checkout -b "$NEW_BRANCH"; then
    error_exit "Failed to create new branch $NEW_BRANCH"
fi

# Push the new branch to origin
echo -e "${GREEN}Pushing new branch to origin...${NC}"
if ! git push -u origin "$NEW_BRANCH"; then
    warning "Failed to push branch to origin. You can push manually later with: git push -u origin $NEW_BRANCH"
fi

# Pop the stash if we created one
if [ "$STASH_CREATED" = true ]; then
    echo ""
    echo -e "${GREEN}Applying stashed changes to new branch...${NC}"
    if git stash pop; then
        echo -e "${GREEN}✅ Stashed changes have been restored${NC}"
    else
        warning "Failed to apply stashed changes. Your changes are still in the stash."
        echo -e "${YELLOW}You can manually apply them with: git stash pop${NC}"
    fi
fi

echo ""
echo -e "${GREEN}✅ Successfully created new feature branch: ${NEW_BRANCH}${NC}"
echo -e "${YELLOW}You can now start working on your new feature!${NC}"
