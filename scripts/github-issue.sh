#!/bin/bash
# Unified GitHub Issue Management Script
# Usage: ./scripts/github-issue.sh <command> [options]
#
# Commands:
#   create   Create new issue (auto-adds to IJACK Roadmap Project #12)
#   edit     Edit existing issue
#   comment  Add comment to issue
#   view     View issue details
#   close    Close issue
#   reopen   Reopen closed issue
#
# DRY, SOLID, KISS design for GitHub issue management

set -e

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# === IJACK Roadmap Project #12 Configuration ===
PROJECT_ID="PVT_kwDODGeA9M4BCOFY"
PROJECT_NUMBER=12
OWNER="ijack-technologies"

# Field IDs
FIELD_STARTED="PVTF_lADODGeA9M4BCOFYzg0fxWM"
FIELD_STORY_POINTS="PVTSSF_lADODGeA9M4BCOFYzg0gHRU"
FIELD_PRIORITY="PVTSSF_lADODGeA9M4BCOFYzg04jjg"
FIELD_STATUS="PVTSSF_lADODGeA9M4BCOFYzg0ftEw"
FIELD_SPRINT="PVTIF_lADODGeA9M4BCOFYzg0gwlI"

# Story Points option IDs
declare -A STORY_POINTS_MAP=(
    [1]="f83665ee"
    [2]="a1503678"
    [3]="d67f489f"
    [5]="6e6356f7"
    [8]="ae8610c7"
    [13]="b57d8761"
)

# Priority option IDs
declare -A PRIORITY_MAP=(
    ["very-low"]="b0e548f3"
    ["low"]="b04f73f7"
    ["high"]="0a472eab"
    ["very-high"]="820ac6bb"
)

# Status option IDs
declare -A STATUS_MAP=(
    ["backlog"]="f75ad846"
    ["ready"]="0bb5b0e0"
    ["in-progress"]="47fc9ee4"
    ["done"]="98236657"
)

# Default values
DEFAULT_ASSIGNEE="@me"
DEFAULT_STORY_POINTS="3"
DEFAULT_PRIORITY="low"
DEFAULT_STATUS="ready"

# === Helper Functions ===

usage() {
    cat <<EOF
Usage: $(basename "$0") <command> [options]

Commands:
    create      Create new issue (auto-adds to IJACK Roadmap)
    edit        Edit existing issue
    comment     Add comment to issue
    view        View issue details
    close       Close issue
    reopen      Reopen closed issue
    list        List recent issues

Smart Issue Detection:
    - Auto-detects issue from branch name (e.g., feature/123-fix-bug)
    - Use -S/--search to find issues by keyword
    - Shows recent issues if no match found

Common Options:
    -h, --help              Show help for command

Run '$(basename "$0") <command> --help' for command-specific options.
EOF
}

usage_create() {
    cat <<EOF
Usage: $(basename "$0") create [options]

Create a new GitHub issue with automatic IJACK Roadmap integration.

Options:
    -t, --title TITLE       Issue title (required)
    -b, --body BODY         Issue body (markdown supported)
    -f, --body-file FILE    Read body from file
    -l, --labels LABELS     Comma-separated labels
    -a, --assignee USER     Assignee (@me or username, default: @me)
    -p, --points POINTS     Story points: 1,2,3,5,8,13 (default: 3)
    -P, --priority LEVEL    Priority: very-low,low,high,very-high (default: low)
    -s, --status STATUS     Status: backlog,ready,in-progress,done (default: ready)
    --no-roadmap            Skip adding to IJACK Roadmap
    -h, --help              Show this help

Examples:
    $(basename "$0") create -t "Fix bug" -l "bug" -p 2 -P high
    $(basename "$0") create -t "Add feature" -b "Description" -l "enhancement" -p 5
EOF
}

usage_edit() {
    cat <<EOF
Usage: $(basename "$0") edit [issue-number] [options]

Edit an existing GitHub issue. If no issue number provided, will auto-detect
from branch name or show recent issues.

Options:
    -t, --title TITLE       New issue title
    -b, --body BODY         New issue body
    --add-label LABEL       Add label to issue
    --remove-label LABEL    Remove label from issue
    -a, --assignee USER     Update assignee
    -S, --search TERM       Search for issue by keyword
    -h, --help              Show this help

Examples:
    $(basename "$0") edit 123 -t "Updated title"
    $(basename "$0") edit -t "New title"              # Auto-detect from branch
    $(basename "$0") edit -S "auth bug" -t "Updated"  # Search and edit
    $(basename "$0") edit --add-label "urgent" --remove-label "low-priority"
EOF
}

usage_comment() {
    cat <<EOF
Usage: $(basename "$0") comment [issue-number] [options]

Add a comment to an existing GitHub issue. If no issue number provided,
will auto-detect from branch name or show recent issues.

Options:
    -b, --body BODY         Comment body (required)
    -f, --body-file FILE    Read comment from file
    -S, --search TERM       Search for issue by keyword
    -h, --help              Show this help

Examples:
    $(basename "$0") comment 123 -b "This is fixed in PR #456"
    $(basename "$0") comment -b "Update: Fixed"      # Auto-detect from branch
    $(basename "$0") comment -S "auth" -b "Working on this"
EOF
}

usage_view() {
    cat <<EOF
Usage: $(basename "$0") view [issue-number] [options]

View details of a GitHub issue. If no issue number provided,
will auto-detect from branch name or show recent issues.

Options:
    -S, --search TERM       Search for issue by keyword
    -h, --help              Show this help

Examples:
    $(basename "$0") view 123
    $(basename "$0") view                # Auto-detect from branch
    $(basename "$0") view -S "auth bug"  # Search and view
EOF
}

usage_close() {
    cat <<EOF
Usage: $(basename "$0") close [issue-number] [options]

Close a GitHub issue with optional comment. If no issue number provided,
will auto-detect from branch name or show recent issues.

Options:
    -b, --body BODY         Closing comment
    -r, --reason REASON     Close reason: completed,not_planned (default: completed)
    -S, --search TERM       Search for issue by keyword
    -h, --help              Show this help

Examples:
    $(basename "$0") close 123
    $(basename "$0") close -b "Fixed"             # Auto-detect from branch
    $(basename "$0") close -S "auth" -b "Done"    # Search and close
EOF
}

usage_reopen() {
    cat <<EOF
Usage: $(basename "$0") reopen [issue-number] [options]

Reopen a closed GitHub issue. If no issue number provided,
will auto-detect from branch name or show recent closed issues.

Options:
    -b, --body BODY         Reopen comment
    -S, --search TERM       Search for issue by keyword
    -h, --help              Show this help

Examples:
    $(basename "$0") reopen 123
    $(basename "$0") reopen -S "auth" -b "Regression found"
EOF
}

usage_list() {
    cat <<EOF
Usage: $(basename "$0") list [options]

List GitHub issues.

Options:
    -s, --state STATE       Issue state: open,closed,all (default: open)
    -l, --labels LABELS     Filter by labels (comma-separated)
    -S, --search TERM       Search for issues by keyword
    -n, --limit N           Number of issues to show (default: 10)
    -h, --help              Show this help

Examples:
    $(basename "$0") list
    $(basename "$0") list -s closed -n 20
    $(basename "$0") list -l "bug" -S "auth"
EOF
}

check_gh_auth() {
    if ! gh auth status &>/dev/null; then
        echo -e "${YELLOW}⚠️  GitHub CLI not authenticated${NC}"
        echo -e "${BLUE}🔐 Authenticating now...${NC}"
        gh auth login --web
    fi
}

get_current_repo() {
    gh repo view --json nameWithOwner -q '.nameWithOwner' 2>/dev/null || echo ""
}

add_claude_footer() {
    local body=$1
    echo "$body"
    echo ""
    echo "---"
    echo ""
    echo "🤖 Generated with [Claude Code](https://claude.com/claude-code)"
}

# Validate labels exist in the repository
validate_labels() {
    local labels=$1
    if [[ -z "$labels" ]]; then
        return 0
    fi

    echo -e "${BLUE}🏷️  Validating labels...${NC}"
    local available_labels=$(gh label list --limit 200 --json name --jq '.[].name' 2>/dev/null)

    if [[ -z "$available_labels" ]]; then
        echo -e "${YELLOW}⚠️  Could not fetch labels, proceeding anyway${NC}"
        return 0
    fi

    local invalid_labels=()
    IFS=',' read -ra label_array <<< "$labels"

    for label in "${label_array[@]}"; do
        # Trim whitespace
        label=$(echo "$label" | xargs)
        if ! echo "$available_labels" | grep -qFx "$label"; then
            invalid_labels+=("$label")
        fi
    done

    if [[ ${#invalid_labels[@]} -gt 0 ]]; then
        echo -e "${RED}❌ Invalid labels detected:${NC}"
        for label in "${invalid_labels[@]}"; do
            echo -e "   - ${RED}$label${NC}"
        done
        echo ""
        echo -e "${YELLOW}Available labels in this repository:${NC}"
        echo "$available_labels" | head -20
        if [[ $(echo "$available_labels" | wc -l) -gt 20 ]]; then
            echo "   ... (run 'gh label list' to see all)"
        fi
        echo ""
        echo -e "${YELLOW}Tip: Labels are case-sensitive and may include emojis${NC}"
        return 1
    fi

    echo -e "${GREEN}✅ All labels valid${NC}"
    return 0
}

extract_issue_number() {
    local input=$1
    # Handle both "#123" and "123" and full URL
    if [[ "$input" =~ ^https://github.com/.*/issues/([0-9]+) ]]; then
        echo "${BASH_REMATCH[1]}"
    else
        echo "${input#\#}"
    fi
}

# Smart issue detection - find issue from branch name or list recent issues
detect_issue() {
    local search_term="$1"
    local issue_number=""

    # Try to extract issue number from current branch name (e.g., feature/123-fix-bug or issue-456)
    local branch_name=$(git branch --show-current 2>/dev/null || echo "")
    if [[ -n "$branch_name" ]]; then
        # Look for issue number patterns in branch name
        if [[ "$branch_name" =~ [/-]([0-9]+)[/-] ]] || [[ "$branch_name" =~ [/-]([0-9]+)$ ]] || [[ "$branch_name" =~ ^([0-9]+)[/-] ]]; then
            local potential_issue="${BASH_REMATCH[1]}"
            # Verify this issue exists
            if gh issue view "$potential_issue" &>/dev/null; then
                echo -e "${BLUE}🔍 Found issue #${potential_issue} from branch name '${branch_name}'${NC}" >&2
                echo "$potential_issue"
                return 0
            fi
        fi
    fi

    # If search term provided, search for it
    if [[ -n "$search_term" ]]; then
        echo -e "${BLUE}🔍 Searching for issues matching '${search_term}'...${NC}" >&2
        local results=$(gh issue list --search "$search_term" --limit 5 --json number,title --jq '.[] | "\(.number)\t\(.title)"' 2>/dev/null)
        if [[ -n "$results" ]]; then
            echo -e "${YELLOW}Found issues:${NC}" >&2
            echo "$results" | while IFS=$'\t' read -r num title; do
                echo "  #$num: $title" >&2
            done
            local first_match=$(echo "$results" | head -1 | cut -f1)
            echo -e "${GREEN}Using #${first_match}${NC}" >&2
            echo "$first_match"
            return 0
        fi
    fi

    # List recent open issues for user to choose
    echo -e "${YELLOW}📋 Recent open issues:${NC}" >&2
    gh issue list --limit 10 --json number,title,labels --jq '.[] | "#\(.number): \(.title) [\(.labels | map(.name) | join(", "))]"' 2>/dev/null | head -10 >&2

    return 1
}

# Get issue interactively if not provided
get_issue_interactive() {
    local issue_number="$1"
    local search_term="$2"

    if [[ -n "$issue_number" ]]; then
        echo "$issue_number"
        return 0
    fi

    # Try to auto-detect
    local detected=$(detect_issue "$search_term")
    if [[ -n "$detected" ]]; then
        echo "$detected"
        return 0
    fi

    # Prompt user
    echo -e "${YELLOW}Enter issue number (or 'q' to quit): ${NC}" >&2
    read -r user_input
    if [[ "$user_input" == "q" ]]; then
        exit 0
    fi
    echo $(extract_issue_number "$user_input")
}

add_to_roadmap() {
    local issue_url=$1
    local story_points=$2
    local priority=$3
    local status=$4

    echo -e "${BLUE}📋 Adding to IJACK Roadmap (Project #${PROJECT_NUMBER})...${NC}"

    # Validate story points
    if [[ -z "${STORY_POINTS_MAP[$story_points]:-}" ]]; then
        echo -e "${YELLOW}⚠️  Invalid story points '$story_points', using default 3${NC}"
        story_points="3"
    fi

    # Validate priority
    if [[ -z "${PRIORITY_MAP[$priority]:-}" ]]; then
        echo -e "${YELLOW}⚠️  Invalid priority '$priority', using default 'low'${NC}"
        priority="low"
    fi

    # Validate status
    if [[ -z "${STATUS_MAP[$status]:-}" ]]; then
        echo -e "${YELLOW}⚠️  Invalid status '$status', using default 'ready'${NC}"
        status="ready"
    fi

    # Step 1: Add issue to project and get item ID
    ITEM_ID=$(gh project item-add $PROJECT_NUMBER --owner "$OWNER" --url "$issue_url" --format json 2>/dev/null | jq -r '.id')

    if [[ -z "$ITEM_ID" || "$ITEM_ID" == "null" ]]; then
        echo -e "${YELLOW}⚠️  Could not add to project (may already exist or permission issue)${NC}"
        return 1
    fi

    # Step 2: Set Started date (today)
    TODAY=$(date +%Y-%m-%d)
    gh project item-edit --id "$ITEM_ID" --project-id "$PROJECT_ID" \
        --field-id "$FIELD_STARTED" --date "$TODAY" 2>/dev/null || true

    # Step 3: Set Story Points
    gh project item-edit --id "$ITEM_ID" --project-id "$PROJECT_ID" \
        --field-id "$FIELD_STORY_POINTS" --single-select-option-id "${STORY_POINTS_MAP[$story_points]}" 2>/dev/null || true

    # Step 4: Set Priority
    gh project item-edit --id "$ITEM_ID" --project-id "$PROJECT_ID" \
        --field-id "$FIELD_PRIORITY" --single-select-option-id "${PRIORITY_MAP[$priority]}" 2>/dev/null || true

    # Step 5: Set Status
    gh project item-edit --id "$ITEM_ID" --project-id "$PROJECT_ID" \
        --field-id "$FIELD_STATUS" --single-select-option-id "${STATUS_MAP[$status]}" 2>/dev/null || true

    # Step 6: Set Sprint to latest
    LATEST_SPRINT=$(gh api graphql -f query='
    {
      organization(login: "ijack-technologies") {
        projectV2(number: 12) {
          field(name: "Sprint 2025 -") {
            ... on ProjectV2IterationField {
              configuration {
                completedIterations {
                  id
                  title
                }
              }
            }
          }
        }
      }
    }' 2>/dev/null | jq -r '.data.organization.projectV2.field.configuration.completedIterations[0].id')

    if [[ -n "$LATEST_SPRINT" && "$LATEST_SPRINT" != "null" ]]; then
        gh project item-edit --id "$ITEM_ID" --project-id "$PROJECT_ID" \
            --field-id "$FIELD_SPRINT" --iteration-id "$LATEST_SPRINT" 2>/dev/null || true
    fi

    echo -e "${GREEN}✅ Added to IJACK Roadmap${NC}"
    echo "   Started: $TODAY | Points: $story_points | Priority: $priority | Status: $status"
}

# === Command Functions ===

cmd_create() {
    local title=""
    local body=""
    local body_file=""
    local labels=""
    local assignee="$DEFAULT_ASSIGNEE"
    local story_points="$DEFAULT_STORY_POINTS"
    local priority="$DEFAULT_PRIORITY"
    local status="$DEFAULT_STATUS"
    local no_roadmap=false

    while [[ $# -gt 0 ]]; do
        case $1 in
            -t|--title) title="$2"; shift 2 ;;
            -b|--body) body="$2"; shift 2 ;;
            -f|--body-file) body_file="$2"; shift 2 ;;
            -l|--labels) labels="$2"; shift 2 ;;
            -a|--assignee) assignee="$2"; shift 2 ;;
            -p|--points) story_points="$2"; shift 2 ;;
            -P|--priority) priority="$2"; shift 2 ;;
            -s|--status) status="$2"; shift 2 ;;
            --no-roadmap) no_roadmap=true; shift ;;
            -h|--help) usage_create; exit 0 ;;
            *) echo -e "${RED}Unknown option: $1${NC}"; usage_create; exit 1 ;;
        esac
    done

    if [[ -z "$title" ]]; then
        echo -e "${RED}❌ Title required. Use -t flag${NC}"
        exit 1
    fi

    # Read body from file if specified
    if [[ -n "$body_file" ]]; then
        if [[ -f "$body_file" ]]; then
            body=$(cat "$body_file")
        else
            echo -e "${RED}❌ Body file not found: $body_file${NC}"
            exit 1
        fi
    fi

    local repo=$(get_current_repo)
    if [[ -z "$repo" ]]; then
        echo -e "${RED}❌ Not in a git repository${NC}"
        exit 1
    fi

    # Validate labels before creating issue
    if [[ -n "$labels" ]]; then
        if ! validate_labels "$labels"; then
            echo -e "${RED}❌ Please fix invalid labels and try again${NC}"
            exit 1
        fi
    fi

    # Add Claude footer to body
    local full_body=$(add_claude_footer "$body")

    # Build and execute gh issue create command
    local cmd=(gh issue create --title "$title" --body "$full_body" --assignee "$assignee")

    if [[ -n "$labels" ]]; then
        cmd+=(--label "$labels")
    fi

    echo -e "${BLUE}🚀 Creating issue in ${repo}...${NC}"
    local issue_url
    issue_url=$("${cmd[@]}")

    if [[ $? -eq 0 ]]; then
        echo -e "${GREEN}✅ Issue created successfully!${NC}"
        echo -e "${BLUE}📍 URL: ${issue_url}${NC}"

        # Add to IJACK Roadmap
        if [[ "$no_roadmap" == false ]]; then
            add_to_roadmap "$issue_url" "$story_points" "$priority" "$status"
        fi
    else
        echo -e "${RED}❌ Failed to create issue${NC}"
        exit 1
    fi
}

cmd_edit() {
    local issue_number=""
    local title=""
    local body=""
    local add_labels=()
    local remove_labels=()
    local assignee=""
    local search_term=""

    # First argument should be issue number (if not a flag)
    if [[ $# -gt 0 && ! "$1" =~ ^- ]]; then
        issue_number=$(extract_issue_number "$1")
        shift
    fi

    while [[ $# -gt 0 ]]; do
        case $1 in
            -t|--title) title="$2"; shift 2 ;;
            -b|--body) body="$2"; shift 2 ;;
            --add-label) add_labels+=("$2"); shift 2 ;;
            --remove-label) remove_labels+=("$2"); shift 2 ;;
            -a|--assignee) assignee="$2"; shift 2 ;;
            -S|--search) search_term="$2"; shift 2 ;;
            -h|--help) usage_edit; exit 0 ;;
            *) echo -e "${RED}Unknown option: $1${NC}"; usage_edit; exit 1 ;;
        esac
    done

    # Smart issue detection if not provided
    if [[ -z "$issue_number" ]]; then
        issue_number=$(get_issue_interactive "" "$search_term")
        if [[ -z "$issue_number" ]]; then
            echo -e "${RED}❌ Could not determine issue number${NC}"
            exit 1
        fi
    fi

    local cmd=(gh issue edit "$issue_number")
    local has_changes=false

    if [[ -n "$title" ]]; then
        cmd+=(--title "$title")
        has_changes=true
    fi

    if [[ -n "$body" ]]; then
        cmd+=(--body "$body")
        has_changes=true
    fi

    for label in "${add_labels[@]}"; do
        cmd+=(--add-label "$label")
        has_changes=true
    done

    for label in "${remove_labels[@]}"; do
        cmd+=(--remove-label "$label")
        has_changes=true
    done

    if [[ -n "$assignee" ]]; then
        cmd+=(--add-assignee "$assignee")
        has_changes=true
    fi

    if [[ "$has_changes" == false ]]; then
        echo -e "${YELLOW}⚠️  No changes specified${NC}"
        usage_edit
        exit 1
    fi

    echo -e "${BLUE}✏️  Editing issue #${issue_number}...${NC}"
    if "${cmd[@]}"; then
        echo -e "${GREEN}✅ Issue updated successfully!${NC}"
    else
        echo -e "${RED}❌ Failed to update issue${NC}"
        exit 1
    fi
}

cmd_comment() {
    local issue_number=""
    local body=""
    local body_file=""
    local search_term=""

    # First argument should be issue number (if not a flag)
    if [[ $# -gt 0 && ! "$1" =~ ^- ]]; then
        issue_number=$(extract_issue_number "$1")
        shift
    fi

    while [[ $# -gt 0 ]]; do
        case $1 in
            -b|--body) body="$2"; shift 2 ;;
            -f|--body-file) body_file="$2"; shift 2 ;;
            -S|--search) search_term="$2"; shift 2 ;;
            -h|--help) usage_comment; exit 0 ;;
            *) echo -e "${RED}Unknown option: $1${NC}"; usage_comment; exit 1 ;;
        esac
    done

    # Smart issue detection if not provided
    if [[ -z "$issue_number" ]]; then
        issue_number=$(get_issue_interactive "" "$search_term")
        if [[ -z "$issue_number" ]]; then
            echo -e "${RED}❌ Could not determine issue number${NC}"
            exit 1
        fi
    fi

    # Read body from file if specified
    if [[ -n "$body_file" ]]; then
        if [[ -f "$body_file" ]]; then
            body=$(cat "$body_file")
        else
            echo -e "${RED}❌ Body file not found: $body_file${NC}"
            exit 1
        fi
    fi

    if [[ -z "$body" ]]; then
        echo -e "${RED}❌ Comment body required. Use -b or -f flag${NC}"
        exit 1
    fi

    # Add Claude footer
    local full_body=$(add_claude_footer "$body")

    echo -e "${BLUE}💬 Adding comment to issue #${issue_number}...${NC}"
    if gh issue comment "$issue_number" --body "$full_body"; then
        echo -e "${GREEN}✅ Comment added successfully!${NC}"
    else
        echo -e "${RED}❌ Failed to add comment${NC}"
        exit 1
    fi
}

cmd_view() {
    local issue_number=""
    local search_term=""

    # First argument should be issue number (if not a flag)
    if [[ $# -gt 0 && ! "$1" =~ ^- ]]; then
        issue_number=$(extract_issue_number "$1")
        shift
    fi

    while [[ $# -gt 0 ]]; do
        case $1 in
            -S|--search) search_term="$2"; shift 2 ;;
            -h|--help) usage_view; exit 0 ;;
            *) echo -e "${RED}Unknown option: $1${NC}"; usage_view; exit 1 ;;
        esac
    done

    # Smart issue detection if not provided
    if [[ -z "$issue_number" ]]; then
        issue_number=$(get_issue_interactive "" "$search_term")
        if [[ -z "$issue_number" ]]; then
            echo -e "${RED}❌ Could not determine issue number${NC}"
            exit 1
        fi
    fi

    gh issue view "$issue_number"
}

cmd_close() {
    local issue_number=""
    local body=""
    local reason="completed"
    local search_term=""

    # First argument should be issue number (if not a flag)
    if [[ $# -gt 0 && ! "$1" =~ ^- ]]; then
        issue_number=$(extract_issue_number "$1")
        shift
    fi

    while [[ $# -gt 0 ]]; do
        case $1 in
            -b|--body) body="$2"; shift 2 ;;
            -r|--reason) reason="$2"; shift 2 ;;
            -S|--search) search_term="$2"; shift 2 ;;
            -h|--help) usage_close; exit 0 ;;
            *) echo -e "${RED}Unknown option: $1${NC}"; usage_close; exit 1 ;;
        esac
    done

    # Smart issue detection if not provided
    if [[ -z "$issue_number" ]]; then
        issue_number=$(get_issue_interactive "" "$search_term")
        if [[ -z "$issue_number" ]]; then
            echo -e "${RED}❌ Could not determine issue number${NC}"
            exit 1
        fi
    fi

    # Add comment if provided
    if [[ -n "$body" ]]; then
        local full_body=$(add_claude_footer "$body")
        gh issue comment "$issue_number" --body "$full_body" 2>/dev/null || true
    fi

    echo -e "${BLUE}🔒 Closing issue #${issue_number}...${NC}"
    if gh issue close "$issue_number" --reason "$reason"; then
        echo -e "${GREEN}✅ Issue closed successfully!${NC}"
    else
        echo -e "${RED}❌ Failed to close issue${NC}"
        exit 1
    fi
}

cmd_reopen() {
    local issue_number=""
    local body=""
    local search_term=""

    # First argument should be issue number (if not a flag)
    if [[ $# -gt 0 && ! "$1" =~ ^- ]]; then
        issue_number=$(extract_issue_number "$1")
        shift
    fi

    while [[ $# -gt 0 ]]; do
        case $1 in
            -b|--body) body="$2"; shift 2 ;;
            -S|--search) search_term="$2"; shift 2 ;;
            -h|--help) usage_reopen; exit 0 ;;
            *) echo -e "${RED}Unknown option: $1${NC}"; usage_reopen; exit 1 ;;
        esac
    done

    # Smart issue detection if not provided
    if [[ -z "$issue_number" ]]; then
        issue_number=$(get_issue_interactive "" "$search_term")
        if [[ -z "$issue_number" ]]; then
            echo -e "${RED}❌ Could not determine issue number${NC}"
            exit 1
        fi
    fi

    echo -e "${BLUE}🔓 Reopening issue #${issue_number}...${NC}"
    if gh issue reopen "$issue_number"; then
        echo -e "${GREEN}✅ Issue reopened successfully!${NC}"

        # Add comment if provided
        if [[ -n "$body" ]]; then
            local full_body=$(add_claude_footer "$body")
            gh issue comment "$issue_number" --body "$full_body" 2>/dev/null || true
        fi
    else
        echo -e "${RED}❌ Failed to reopen issue${NC}"
        exit 1
    fi
}

cmd_list() {
    local state="open"
    local labels=""
    local search_term=""
    local limit="10"

    while [[ $# -gt 0 ]]; do
        case $1 in
            -s|--state) state="$2"; shift 2 ;;
            -l|--labels) labels="$2"; shift 2 ;;
            -S|--search) search_term="$2"; shift 2 ;;
            -n|--limit) limit="$2"; shift 2 ;;
            -h|--help) usage_list; exit 0 ;;
            *) echo -e "${RED}Unknown option: $1${NC}"; usage_list; exit 1 ;;
        esac
    done

    local cmd=(gh issue list --state "$state" --limit "$limit")

    if [[ -n "$labels" ]]; then
        cmd+=(--label "$labels")
    fi

    if [[ -n "$search_term" ]]; then
        cmd+=(--search "$search_term")
    fi

    echo -e "${BLUE}📋 Listing issues (state: $state, limit: $limit)...${NC}"
    "${cmd[@]}"
}

# === Main Entry Point ===

check_gh_auth

if [[ $# -eq 0 ]]; then
    usage
    exit 1
fi

COMMAND=$1
shift

case "$COMMAND" in
    create)  cmd_create "$@" ;;
    edit)    cmd_edit "$@" ;;
    comment) cmd_comment "$@" ;;
    view)    cmd_view "$@" ;;
    close)   cmd_close "$@" ;;
    reopen)  cmd_reopen "$@" ;;
    list)    cmd_list "$@" ;;
    -h|--help) usage; exit 0 ;;
    *)
        echo -e "${RED}Unknown command: $COMMAND${NC}"
        usage
        exit 1
        ;;
esac
