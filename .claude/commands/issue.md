# GitHub Issue Management

Manage GitHub issues with automatic IJACK Roadmap (Project #12) integration.

## Quick Reference

### Create Issue
```bash
./scripts/github-issue.sh create -t "Issue title" -b "Description" -l "bug" -p 3 -P high
```

### Edit Issue (Smart Detection)
```bash
./scripts/github-issue.sh edit -t "New title"                    # Auto-detect from branch
./scripts/github-issue.sh edit 123 --add-label "urgent"          # By number
./scripts/github-issue.sh edit -S "auth bug" -t "Updated title"  # Search and edit
```

### Comment on Issue (Smart Detection)
```bash
./scripts/github-issue.sh comment -b "Working on this"           # Auto-detect from branch
./scripts/github-issue.sh comment 123 -b "Fixed in PR #456"      # By number
./scripts/github-issue.sh comment -S "auth" -b "Update"          # Search and comment
```

### View Issue
```bash
./scripts/github-issue.sh view                  # Auto-detect from branch
./scripts/github-issue.sh view 123              # By number
./scripts/github-issue.sh view -S "auth bug"    # Search and view
```

### Close Issue
```bash
./scripts/github-issue.sh close -b "Fixed"              # Auto-detect from branch
./scripts/github-issue.sh close 123 -b "Done"           # By number
./scripts/github-issue.sh close -S "auth" -r completed  # Search and close
```

### Reopen Issue
```bash
./scripts/github-issue.sh reopen 123 -b "Regression found"
./scripts/github-issue.sh reopen -S "auth" -b "Need to revisit"
```

### List Issues
```bash
./scripts/github-issue.sh list                      # Recent open issues
./scripts/github-issue.sh list -s closed -n 20      # Closed issues
./scripts/github-issue.sh list -l "bug" -S "auth"   # Filter by label and search
```

## Smart Issue Detection

The script automatically detects issues:
1. **From branch name**: Extracts issue number from branch (e.g., `feature/123-fix-bug`)
2. **From search**: Use `-S "keyword"` to find issues by keyword
3. **Interactive**: Shows recent issues if no match found

## Create Options

| Flag | Description | Default |
|------|-------------|---------|
| `-t, --title` | Issue title | Required |
| `-b, --body` | Issue body (markdown) | "" |
| `-f, --body-file` | Read body from file | - |
| `-l, --labels` | Labels (comma-separated) | None |
| `-a, --assignee` | Assignee (@me or username) | @me |
| `-p, --points` | Story points (1,2,3,5,8,13) | 3 |
| `-P, --priority` | Priority (very-low,low,high,very-high) | low |
| `-s, --status` | Status (backlog,ready,in-progress,done) | ready |
| `--no-roadmap` | Skip adding to Project #12 | false |

## Story Points Guide

| Points | Complexity | Examples |
|--------|------------|----------|
| 1 | Trivial | Typo fix, config change |
| 2 | Simple | Small bug fix, minor feature |
| 3 | Medium | Standard bug fix or feature |
| 5 | Complex | Multi-file changes, testing needed |
| 8 | Large | Significant feature, architectural |
| 13 | Epic | Major initiative, multiple PRs |

## Priority Guide

| Priority | When to Use |
|----------|-------------|
| very-high | Production down, security issues |
| high | Blocks other work, customer-facing |
| low | Nice to have, improvement (default) |
| very-low | Future enhancement, non-urgent |

## Automatic Behaviors

- **Create**: Always adds to IJACK Roadmap (Project #12) with all fields:
  - Started date (today)
  - Story Points
  - Priority
  - Status
  - Sprint (latest active)
- **Comment/Close**: Adds Claude Code attribution footer
- **Labels**: Check available labels first with `gh label list`

## Examples

### Bug Report
```bash
./scripts/github-issue.sh create \
  -t "Fix authentication timeout" \
  -l "bug" \
  -p 3 \
  -P high \
  -b "Users experiencing timeouts after 5 minutes of inactivity"
```

### Feature Request
```bash
./scripts/github-issue.sh create \
  -t "Add dark mode support" \
  -l "enhancement,ui" \
  -p 5 \
  -P low \
  -b "$(cat <<'EOF'
## Description
Add dark mode toggle to application settings.

## Acceptance Criteria
- [ ] Toggle in settings page
- [ ] Persists across sessions
- [ ] Respects system preference
EOF
)"
```

### Quick Comment from Branch
```bash
# If on branch feature/123-auth-fix
./scripts/github-issue.sh comment -b "Implemented the fix, ready for review"
```
