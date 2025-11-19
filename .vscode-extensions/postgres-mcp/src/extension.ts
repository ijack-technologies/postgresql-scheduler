import * as vscode from "vscode";

export function activate(context: vscode.ExtensionContext) {
  console.log("PostgreSQL MCP extension activated");

  // Register the MCP server definition provider
  const provider = vscode.lm.registerMcpServerDefinitionProvider(
    "rcom-postgres-mcp",
    {
      provideMcpServerDefinitions: async (token: vscode.CancellationToken) => {
        return [
          new vscode.McpHttpServerDefinition(
            "PostgreSQL Database",
            vscode.Uri.parse("http://mcp-postgres:5005/mcp"),
            {}, // No additional headers needed
            "1.0.0" // Version
          ),
        ];
      },
    }
  );

  context.subscriptions.push(provider);

  console.log("PostgreSQL MCP server registered successfully");
}

export function deactivate() {
  console.log("PostgreSQL MCP extension deactivated");
}
