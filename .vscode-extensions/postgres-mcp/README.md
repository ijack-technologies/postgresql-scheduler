# RCOM PostgreSQL MCP Extension

This VS Code extension registers the PostgreSQL MCP server for use with GitHub Copilot Chat.

## What it does

- Registers the `http://mcp-postgres:5005/mcp` endpoint as an MCP server
- Makes PostgreSQL tools available in GitHub Copilot Chat's agent mode
- Provides access to both RDS and TimescaleDB databases

## Installation

1. Install dependencies:

   ```bash
   cd /project/.vscode-extensions/postgres-mcp
   npm install
   ```

2. Compile the extension:

   ```bash
   npm run compile
   ```

3. Install the extension in VS Code:
   - Press `F5` to open a new Extension Development Host window
   - OR: Package and install manually:
     ```bash
     npx @vscode/vsce package
     code --install-extension rcom-postgres-mcp-0.0.1.vsix
     ```

## Usage

Once installed, the PostgreSQL MCP tools will be automatically available in GitHub Copilot Chat when using agent mode.

Try queries like:

- "Query the users table in the RDS database"
- "Show me the schema for the time_series table in TimescaleDB"
- "List all tables in the RDS database"

## Troubleshooting

Check the Output panel (View → Output → select "PostgreSQL MCP") for activation logs.

## Configuration

The MCP server URL is hardcoded to `http://mcp-postgres:5005/mcp`. To change it, modify `src/extension.ts`.
