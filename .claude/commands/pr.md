# Create Pull Request

Push commits and create a draft pull request.

## Pre-PR Validation

### Step 1: Python Linting

```bash
ruff check --fix .
ruff format .
```

### Step 2: Commit & Push Fixes

If any fixes were made:

```bash
git add -A
git commit -m "chore: Fix lint and format issues"
git push
```

### Step 3: Create PR

```bash
gh pr create --draft --head $(git branch --show-current) --base main
```

Return the PR URL when done.
