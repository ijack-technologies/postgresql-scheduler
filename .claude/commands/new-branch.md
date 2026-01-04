# Create New Feature Branch

Create a new feature branch: `<github-username>/YYYY-MM-DD-HHMM`

## Command

```bash
bash ./scripts/new-feature-branch.sh
```

This script will:
1. Stash any uncommitted changes
2. Switch to main and pull latest
3. Create new branch with timestamp
4. Push to origin
5. Restore stashed changes
