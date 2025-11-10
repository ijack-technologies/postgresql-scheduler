---
name: github-issue-creator
description: Create GitHub issues across any repository with proper formatting, labels, and project board assignment. Use when mentions "create issue", "GitHub issue", "open issue", "file issue", "report bug", "feature request".
allowed-tools: [Bash, Read, Write]
---

# GitHub Issue Creator Skill

Create well-formatted GitHub issues across any repository with automatic project board assignment and Claude Code attribution.

## Capabilities

- **Multi-repository support**: Create issues in any accessible GitHub repository
- **Flexible input**: Command-line flags, interactive mode, or file-based body
- **Template support**: Use predefined templates (bug, feature, idea, etc.)
- **Project integration**: Automatically add to project boards
- **Label management**: Apply multiple labels for categorization
- **Assignment**: Assign to yourself or team members
- **Claude attribution**: Automatic footer attribution

## Script Location

**Main Script**: `scripts/create-github-issue.sh`

This is a DRY, SOLID, KISS implementation that can be reused across:
- Different repositories
- Different GitHub organizations
- Different users
- Different issue types

## Usage Modes

### 1. Quick Issue Creation

```bash
# Create issue in current repo
./scripts/create-github-issue.sh \
    -t "Fix authentication timeout" \
    -l "bug,urgent" \
    -b "Users experiencing timeouts after 5 minutes"

# Create feature request
./scripts/create-github-issue.sh \
    -t "Add dark mode support" \
    -l "enhancement,ui" \
    -a "@me"
```

### 2. Cross-Repository Issues

```bash
# Create issue in different repo
./scripts/create-github-issue.sh \
    -r your-org/your-repo \
    -t "Update documentation" \
    -l "docs" \
    -a @me

# Create issue with project board assignment
./scripts/create-github-issue.sh \
    -r ijack-technologies/planning \
    -t "Q1 2026 Planning" \
    -p 12 \
    -l "epic,planning"
```

### 3. Interactive Mode

```bash
# Prompt for all fields
./scripts/create-github-issue.sh -i

# Interactive prompts:
# - Repository (defaults to current)
# - Title (required)
# - Labels (optional)
# - Assignee (defaults to @me)
# - Body (supports markdown, Ctrl+D when done)
# - Project ID (optional)
```

### 4. File-Based Body

```bash
# Use markdown file for issue body
./scripts/create-github-issue.sh \
    -t "Database migration plan" \
    -f docs/migration-plan.md \
    -l "database,migration" \
    -p 12
```

## Command-Line Options

| Flag | Long Form | Description | Default |
|------|-----------|-------------|---------|
| `-r` | `--repo` | Repository (owner/name) | Current repo |
| `-t` | `--title` | Issue title | Required |
| `-b` | `--body` | Issue body (markdown) | Empty |
| `-f` | `--body-file` | Read body from file | - |
| `-l` | `--labels` | Comma-separated labels | None |
| `-a` | `--assignee` | Username or @me | @me |
| `-p` | `--project` | Project board ID | None |
| `-T` | `--template` | Template name | None |
| `-i` | `--interactive` | Interactive mode | False |
| `-h` | `--help` | Show help | - |

## Issue Templates

Available templates (future enhancement):
- `bug`: Bug report format
- `feature`: Feature request format
- `idea`: Idea/enhancement format
- `service`: Service request format
- `user-story`: User story format
- `epic`: Epic/large initiative format

## Common Workflows

### Bug Report

```bash
./scripts/create-github-issue.sh \
    -t "🐛 Gateway crashes on startup" \
    -l "bug,critical" \
    -b "$(cat <<'EOF'
## Description
Gateway crashes immediately after startup on Axiomtek devices.

## Steps to Reproduce
1. Deploy version 3.334225
2. Restart gateway
3. Observe crash in logs

## Expected Behavior
Gateway starts successfully and connects to MQTT broker.

## Actual Behavior
Process exits with code 1 after 3 seconds.

## Environment
- Gateway: Axiomtek ICO300-83C
- Version: 3.334225
- OS: Ubuntu 22.04
EOF
)"
```

### Feature Request

```bash
./scripts/create-github-issue.sh \
    -t "📍 Add GPS coordinates to device shadow" \
    -l "enhancement,feature" \
    -p 12 \
    -b "$(cat <<'EOF'
## Feature Description
Add GPS coordinates (latitude, longitude) to device shadow for location tracking.

## Use Case
Operations team needs to verify gateway physical location matches database records.

## Proposed Solution
1. Add GPS module reading to `canpy/sensors/gps.py`
2. Update device shadow schema
3. Display location on web dashboard map

## Acceptance Criteria
- [ ] GPS coordinates read from hardware
- [ ] Coordinates published to device shadow
- [ ] Web dashboard shows location on map
- [ ] Location updates every 5 minutes
EOF
)"
```

### Idea/Enhancement

```bash
./scripts/create-github-issue.sh \
    -t "💡 Implement alert deduplication with PostgreSQL" \
    -l "idea,enhancement" \
    -b "$(cat <<'EOF'
## Problem
File-based alert deduplication fails when SD card corrupts, causing duplicate alerts.

## Proposed Solution
Use PostgreSQL database for robust alert deduplication with ACID guarantees.

## Benefits
- Survives SD card failures
- Centralized tracking
- Easier monitoring
- Better reliability
EOF
)"
```

## Integration with IJACK Roadmap

For IJACK Technologies projects, **always add issues to Project #12** (IJACK Roadmap):

```bash
# Create issue and add to roadmap automatically
./scripts/create-github-issue.sh \
    -r ijack-technologies/postgresql-scheduler \
    -t "Implement cellular failover" \
    -l "enhancement,networking" \
    -p 12 \
    -a "@me"
```

**Manual Project Addition** (if automatic fails):
```bash
# Get issue URL from output, then:
gh project item-add 12 --owner ijack-technologies --url <ISSUE_URL>
```

## User Trigger Examples

Claude will activate this skill when you say:

- "Create a GitHub issue"
- "File a bug report"
- "Open an issue for this"
- "Create feature request issue"
- "Report this bug on GitHub"
- "Make an issue in repo X"
- "File an issue about the duplicate alerts"

## Skill Activation Process

When triggered, Claude will:

1. **Analyze context**: Determine if current work should be documented as issue
2. **Extract information**: Pull relevant details from conversation
3. **Choose repository**: Use current repo or ask which one
4. **Format issue**: Create well-structured title and body
5. **Apply labels**: Add appropriate labels based on issue type
6. **Execute script**: Run create-github-issue.sh with correct parameters
7. **Confirm creation**: Return issue URL and next steps

## Best Practices

### Issue Titles

✅ **Good Titles**:
- "Fix: Database connection timeout after 5 minutes"
- "Feature: Add real-time alert dashboard"
- "Bug: Gateway fails to reconnect after network outage"
- "Idea: Implement predictive maintenance ML model"

❌ **Bad Titles**:
- "Fix bug" (too vague)
- "Add feature" (not descriptive)
- "Problem" (no context)
- "Help needed" (unclear)

### Issue Bodies

**Include**:
- Clear description
- Steps to reproduce (for bugs)
- Expected vs actual behavior
- Environment details
- Acceptance criteria (for features)
- References to related issues/PRs

**Avoid**:
- Vague descriptions
- Missing context
- No action items
- Duplicate information

### Labels

Use consistent labels across repositories:
- **Type**: `bug`, `enhancement`, `feature`, `idea`, `docs`
- **Priority**: `urgent`, `high`, `medium`, `low`
- **Component**: `database`, `networking`, `alerts`, `ui`
- **Status**: `blocked`, `in-progress`, `needs-review`

## Examples from Real Usage

### Example 1: Current Session

From the duplicate alerts work, this would create:

```bash
./scripts/create-github-issue.sh \
    -r ijack-technologies/postgresql-scheduler \
    -t "Implement database-backed alert deduplication" \
    -l "enhancement,database,alerts" \
    -p 12 \
    -b "$(cat <<'EOF'
## Problem
File-based alert deduplication fails when SD card corrupts, causing duplicate alerts every 3-6 minutes.

## Solution
Implement PostgreSQL-backed deduplication with automatic migration from file-based timestamps.

## Implementation
- Database table: public.alert_deduplication
- Migration script: 001_alert_deduplication_table.sql
- Automatic one-time migration on first check
- Fail-safe: Don't send if DB unavailable

## Files Changed
- canpy/alerts/alert_deduplication.py (new)
- canpy/alerts/egas.py (updated)
- canpy/alerts/warn.py (updated)
- test/test_fast/test_alert_deduplication.py (23 tests)

## Status
✅ Implementation complete
✅ All tests passing
✅ Ready for deployment
EOF
)"
```

### Example 2: Cross-User Usage

Example: Creating issue in different repository:

```bash
./scripts/create-github-issue.sh \
    -r your-org/your-repo \
    -t "Add pytest fixtures for database tests" \
    -l "testing,enhancement" \
    -a @me \
    -b "Standardize database fixtures across test suite"
```

## Script Design Principles

### DRY (Don't Repeat Yourself)
- Single script handles all issue types
- Reusable across repositories
- Common functions (auth check, validation, formatting)

### SOLID
- **Single Responsibility**: Only creates issues (doesn't modify, close, etc.)
- **Open/Closed**: Extensible via templates and flags
- **Liskov Substitution**: Works consistently across repos
- **Interface Segregation**: Clear, focused command-line interface
- **Dependency Inversion**: Depends on `gh` CLI abstraction

### KISS (Keep It Simple)
- Clear command-line flags
- Sensible defaults
- One task per execution
- Helpful error messages

## Troubleshooting

### Authentication Issues

```bash
# Check authentication
gh auth status

# Re-authenticate if needed
gh auth login --web
```

### Repository Not Found

```bash
# Verify repo exists and you have access
gh repo view owner/repo

# Check repository name format
# Correct: ijack-technologies/postgresql-scheduler
# Wrong: gateway-can-to-mqtt (missing owner)
```

### Project Board Issues

```bash
# List available projects
gh project list --owner ijack-technologies

# Manually add to project
gh project item-add 12 --owner ijack-technologies --url <ISSUE_URL>
```

## Advanced Usage

### Batch Issue Creation

```bash
# Create multiple issues from a list
while IFS='|' read -r title labels body; do
    ./scripts/create-github-issue.sh -t "$title" -l "$labels" -b "$body"
done < issues.txt
```

### CI/CD Integration

```bash
# Create issue from test failures in CI
if [[ $TEST_EXIT_CODE -ne 0 ]]; then
    ./scripts/create-github-issue.sh \
        -t "🚨 CI Test Failure: $GITHUB_SHA" \
        -l "bug,ci,automated" \
        -b "Test suite failed on commit $GITHUB_SHA"
fi
```

## Related Skills

- **smart-committer**: Create commits before filing issues
- **pr-creator**: Convert issues into pull requests
- **github-actions-monitor**: Track CI/CD for issue-related changes
- **technical-report-generator**: Generate detailed issue bodies

## Success Metrics

- ✅ Works across multiple repositories
- ✅ Supports different GitHub users
- ✅ Handles interactive and scripted modes
- ✅ Integrates with project boards
- ✅ Consistent formatting with Claude attribution
- ✅ Clear error messages and help text

---

*This skill uses the generic `scripts/create-github-issue.sh` script following DRY, SOLID, and KISS principles.*
