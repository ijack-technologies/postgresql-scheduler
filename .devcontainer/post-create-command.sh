#!/bin/bash

echo "=========================================="
echo "Updating Claude Code to latest version..."
echo "=========================================="

# Update Claude Code (fast method - uses claude update instead of reinstalling)
echo "📦 Checking for Claude Code updates..."
if command -v claude &> /dev/null; then
    if claude update 2>/dev/null; then
        CLAUDE_VERSION=$(claude --version 2>/dev/null | head -n1)
        echo "✅ Claude Code updated to ${CLAUDE_VERSION}"
    else
        echo "   Update command failed, reinstalling..."
        curl -fsSL https://claude.ai/install.sh | bash
        CLAUDE_VERSION=$(claude --version 2>/dev/null | head -n1)
        echo "✅ Claude Code reinstalled: ${CLAUDE_VERSION}"
    fi
else
    echo "   Claude Code not found, installing..."
    curl -fsSL https://claude.ai/install.sh | bash
    CLAUDE_VERSION=$(claude --version 2>/dev/null | head -n1)
    echo "✅ Claude Code installed: ${CLAUDE_VERSION}"
fi
export PATH="/root/.local/bin:$PATH"

echo ""
echo "=========================================="
echo "Initializing authentication persistence..."
echo "=========================================="

# ===== Claude Code Configuration =====
echo ""
echo "📁 Setting up Claude Code configuration..."
mkdir -p /root/.claude
chmod 700 /root/.claude

if [ -f /root/.claude/.credentials.json ]; then
    chmod 600 /root/.claude/.credentials.json
    echo "✅ Found existing Claude Code credentials"
else
    echo "ℹ️  No Claude Code credentials found (run 'claude' to authenticate)"
fi

if [ ! -f /root/.claude/settings.json ]; then
    cat > /root/.claude/settings.json <<'EOF'
{
  "bypassPermissionsModeAccepted": true,
  "mcpServers": {
    "mcp-postgres": {
      "trusted": true
    },
    "aws-documentation": {
      "trusted": true
    }
  }
}
EOF
    echo "✅ Created Claude Code settings with MCP trust"
else
    echo "✅ Using existing Claude Code settings"
fi

# ===== GitHub CLI Configuration =====
echo ""
echo "📁 Setting up GitHub CLI configuration..."
mkdir -p /root/.config/gh
chmod 700 /root/.config/gh

if [ -f /root/.config/gh/hosts.yml ]; then
    chmod 600 /root/.config/gh/hosts.yml
    echo "✅ Found existing GitHub CLI credentials"
else
    echo "ℹ️  No GitHub CLI credentials found (run 'gh auth login --insecure-storage' to authenticate)"
fi

if [ ! -f /root/.config/gh/config.yml ]; then
    cat > /root/.config/gh/config.yml <<'EOF'
# GitHub CLI configuration
git_protocol: https
editor: vim
prompt: enabled
pager:
browser:
EOF
    echo "✅ Created GitHub CLI config"
else
    echo "✅ Using existing GitHub CLI config"
fi

# ===== Summary =====
echo ""
echo "=========================================="
echo "✅ Claude Code configuration initialized"
echo "✅ GitHub CLI configuration initialized"
echo "=========================================="
echo ""
echo "Next steps if this is your first time:"
echo "  1. Claude Code: Already authenticated in this session"
echo "  2. GitHub CLI: Run 'gh auth login --insecure-storage'"
echo ""
echo "Credentials will persist across container rebuilds!"
echo ""

