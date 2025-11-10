#!/bin/bash

# Generic GitHub Issue Creator Script
# Usage: ./create-github-issue.sh [OPTIONS]
#
# DRY, SOLID, KISS design for creating GitHub issues across any repository

set -e

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# Default values
DEFAULT_REPO=$(gh repo view --json nameWithOwner -q '.nameWithOwner' 2>/dev/null || echo "")
DEFAULT_ASSIGNEE="@me"
DEFAULT_LABELS=""
DEFAULT_PROJECT=""

# Function to print usage
usage() {
    cat <<EOF
Usage: $(basename "$0") [OPTIONS]

Create a GitHub issue with proper formatting and optional project board assignment.

OPTIONS:
    -r, --repo REPO         Repository (owner/name format)
                            Default: current repo (${DEFAULT_REPO})
    -t, --title TITLE       Issue title (required)
    -b, --body BODY         Issue body (supports markdown)
    -f, --body-file FILE    Read issue body from file
    -l, --labels LABELS     Comma-separated labels (e.g., "bug,urgent")
    -a, --assignee USER     Assignee username or "@me" for yourself
                            Default: @me
    -p, --project ID        Project board ID (e.g., 12 for IJACK Roadmap)
    -T, --template NAME     Issue template name (bug, feature, idea, etc.)
    -i, --interactive       Interactive mode (prompts for all fields)
    -h, --help              Show this help message

EXAMPLES:
    # Create bug report in current repo
    $(basename "$0") -t "Fix authentication error" -l "bug" -b "Users can't login"

    # Create feature request with file body
    $(basename "$0") -r owner/repo -t "Add dark mode" -f issue-body.md -l "enhancement"

    # Create issue in different repo and add to project
    $(basename "$0") -r mccarthysean/myrepo -t "Update README" -p 5 -a mccarthysean

    # Interactive mode
    $(basename "$0") -i

ISSUE TEMPLATES:
    Available templates (use with -T or --template):
    - bug         : Bug report
    - feature     : Feature request
    - idea        : Idea/enhancement
    - service     : Service request
    - user-story  : User story
    - epic        : Epic/large initiative

EOF
}

# Function to check GitHub CLI authentication
check_gh_auth() {
    if ! gh auth status &>/dev/null; then
        echo -e "${YELLOW}⚠️  GitHub CLI not authenticated${NC}"
        echo -e "${BLUE}🔐 Authenticating now...${NC}"
        gh auth login --web
    fi
}

# Function to get current repo
get_current_repo() {
    gh repo view --json nameWithOwner -q '.nameWithOwner' 2>/dev/null || echo ""
}

# Function to validate repo exists
validate_repo() {
    local repo=$1
    if ! gh repo view "$repo" &>/dev/null; then
        echo -e "${RED}❌ Repository '$repo' not found or not accessible${NC}" >&2
        return 1
    fi
    return 0
}

# Function to add Claude Code attribution footer
add_claude_footer() {
    local body=$1
    echo "$body"
    echo ""
    echo "---"
    echo ""
    echo "🤖 Generated with [Claude Code](https://claude.com/claude-code)"
}

# Function to create issue interactively
interactive_mode() {
    echo -e "${BLUE}📝 Interactive GitHub Issue Creator${NC}"
    echo ""

    # Repository
    local current_repo=$(get_current_repo)
    read -p "Repository [${current_repo}]: " repo
    repo=${repo:-$current_repo}

    # Title
    read -p "Issue Title (required): " title
    while [[ -z "$title" ]]; do
        echo -e "${RED}Title cannot be empty${NC}"
        read -p "Issue Title (required): " title
    done

    # Labels
    read -p "Labels (comma-separated) [none]: " labels

    # Assignee
    read -p "Assignee [@me]: " assignee
    assignee=${assignee:-@me}

    # Body
    echo "Issue Body (markdown supported, Ctrl+D when done):"
    body=$(cat)

    # Project
    read -p "Project Board ID [none]: " project

    # Create the issue
    create_issue "$repo" "$title" "$body" "$labels" "$assignee" "$project"
}

# Function to create the issue
create_issue() {
    local repo=$1
    local title=$2
    local body=$3
    local labels=$4
    local assignee=$5
    local project=$6

    # Validate inputs
    if [[ -z "$repo" ]]; then
        echo -e "${RED}❌ Repository required. Use -r flag or run in git repository${NC}" >&2
        exit 1
    fi

    if [[ -z "$title" ]]; then
        echo -e "${RED}❌ Title required. Use -t flag${NC}" >&2
        exit 1
    fi

    # Validate repo exists
    if ! validate_repo "$repo"; then
        exit 1
    fi

    # Add Claude footer to body
    local full_body=$(add_claude_footer "$body")

    # Build gh issue create command
    local cmd=(gh issue create --repo "$repo" --title "$title" --body "$full_body")

    # Add optional parameters
    if [[ -n "$labels" ]]; then
        cmd+=(--label "$labels")
    fi

    if [[ -n "$assignee" ]]; then
        cmd+=(--assignee "$assignee")
    fi

    echo -e "${BLUE}🚀 Creating issue in ${repo}...${NC}"

    # Execute issue creation
    local issue_url
    issue_url=$("${cmd[@]}")

    if [[ $? -eq 0 ]]; then
        echo ""
        echo -e "${GREEN}✅ Issue created successfully!${NC}"
        echo -e "${BLUE}📍 Issue URL: ${issue_url}${NC}"

        # Add to project board if specified
        if [[ -n "$project" ]]; then
            echo ""
            echo -e "${BLUE}📋 Adding to project board #${project}...${NC}"

            # Extract owner from repo
            local owner=$(echo "$repo" | cut -d'/' -f1)

            if gh project item-add "$project" --owner "$owner" --url "$issue_url" &>/dev/null; then
                echo -e "${GREEN}✅ Added to project board${NC}"
            else
                echo -e "${YELLOW}⚠️  Could not add to project board (may need to do manually)${NC}"
                echo -e "${YELLOW}   Run: gh project item-add ${project} --owner ${owner} --url ${issue_url}${NC}"
            fi
        fi

        echo ""
        return 0
    else
        echo -e "${RED}❌ Failed to create issue${NC}" >&2
        return 1
    fi
}

# Parse command line arguments
REPO=""
TITLE=""
BODY=""
BODY_FILE=""
LABELS=""
ASSIGNEE=""
PROJECT=""
TEMPLATE=""
INTERACTIVE=false

while [[ $# -gt 0 ]]; do
    case $1 in
        -r|--repo)
            REPO="$2"
            shift 2
            ;;
        -t|--title)
            TITLE="$2"
            shift 2
            ;;
        -b|--body)
            BODY="$2"
            shift 2
            ;;
        -f|--body-file)
            BODY_FILE="$2"
            shift 2
            ;;
        -l|--labels)
            LABELS="$2"
            shift 2
            ;;
        -a|--assignee)
            ASSIGNEE="$2"
            shift 2
            ;;
        -p|--project)
            PROJECT="$2"
            shift 2
            ;;
        -T|--template)
            TEMPLATE="$2"
            shift 2
            ;;
        -i|--interactive)
            INTERACTIVE=true
            shift
            ;;
        -h|--help)
            usage
            exit 0
            ;;
        *)
            echo -e "${RED}❌ Unknown option: $1${NC}" >&2
            usage
            exit 1
            ;;
    esac
done

# Check GitHub CLI authentication
check_gh_auth

# Interactive mode
if [[ "$INTERACTIVE" == true ]]; then
    interactive_mode
    exit $?
fi

# Use defaults if not specified
REPO=${REPO:-$(get_current_repo)}
ASSIGNEE=${ASSIGNEE:-$DEFAULT_ASSIGNEE}

# Read body from file if specified
if [[ -n "$BODY_FILE" ]]; then
    if [[ -f "$BODY_FILE" ]]; then
        BODY=$(cat "$BODY_FILE")
    else
        echo -e "${RED}❌ Body file not found: $BODY_FILE${NC}" >&2
        exit 1
    fi
fi

# Create the issue
create_issue "$REPO" "$TITLE" "$BODY" "$LABELS" "$ASSIGNEE" "$PROJECT"
