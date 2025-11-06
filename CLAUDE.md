# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## ALWAYS do these things
- Think ultra hard
- Make a detailed plan and follow it
- Keep all code DRY and SOLID
- ALWAYS run linters before committing, and always fix any errors found
- After receiving tool results, carefully reflect on their quality and determine optimal next steps before proceeding. Use your thinking to plan and iterate based on this new information, and then take the best next action.
- For maximum efficiency, whenever you need to perform multiple independent operations, invoke all relevant tools simultaneously rather than sequentially.
- If you create any temporary new files, scripts, or helper files for iteration, clean up these files by removing them at the end of the task.

## Principles
- Write high quality, general purpose solutions. Implement solutions that work correctly for all valid inputs, not just the test cases. Do not hard-code values or create solutions that only work for specific test inputs. Instead, implement the actual logic that solves the problems generally.
- Focus on understanding the problem requirements and implementing the correct algorithm. Tests are there to verify correctness, not to define the solution. Provide a principled implementation that follows best practices and software design principles.
- If the task is unreasonable or infeasible, or if any of the tests are incorrect, please tell me. The solution should be robust, maintainable, and extendable.

## Standard workflow
1. Think through the problem, read the codebase for relevant files, and write a plan to projectplan.md
2. The plan should have a list of to do items that you can check off as you complete them.
3. Before you begin working, check in with me and I will verify the plan.
4. Begin working on the to do items, marking them complete as you go.
5. Every step of the way, give me a high level explanation of what changes you made.
6. Make every task and code change you do as simple and DRY as possible. Avoid making any massive or complex changes. Every change should impact as little code as possible. Everything is about simplicity.
7. Finally, add a review section to the projectplan.md file with a summary of the changes you made, and any other relevant information.

## Project Overview
This is a PostgreSQL Scheduler application that runs scheduled jobs for database operations. The project is designed for AWS RDS PostgreSQL databases that lack built-in scheduling capabilities.

### Core Architecture
- **Entry Point**: `project/scheduler_jobs.py` - Main scheduler that orchestrates all jobs using the `schedule` library
- **Job Modules**: Individual modules in `project/` for specific tasks (time series, AWS IoT shadow sync, FX rates, etc.)
- **Configuration**: `project/utils.py` contains the main `Config` class with database connections, alerting, and environment settings
- **Real Scripts**: `real/` directory contains production-ready versions of scripts (prefixed with `real_`)
- **Testing**: `test/` directory with fixtures and comprehensive test coverage

### Key Components
- **Time Series Processing**: Handles aggregation calculations, materialized view refreshes, and data cleanup
- **AWS IoT Integration**: Syncs IoT shadow data with RDS PostgreSQL configuration
- **FX Exchange Rates**: Daily updates of foreign exchange rates
- **Alerting System**: Email/SMS notifications using Twilio and AWS SES
- **Database Operations**: PostgreSQL connections with proper error handling and transaction management

## Development Commands

### Environment Setup
```bash
# Install Python dependencies
uv pip install -e .[dev]

# Install Node.js dependencies (for Claude Code)
bun install
```

### Code Quality & Testing
```bash
# Run linting and formatting (primary command)
./scripts/lint_apply.sh

# Check linting without applying fixes
./scripts/lint_check.sh

# Run all tests with linting
./scripts/pytest_run_all.sh

# Run tests only
pytest /workspace/test/ -v --durations=0
```

### Docker Development
```bash
# Start development environment
docker-compose -f docker-compose.dev.yml up

# Build development container
docker-compose -f docker-compose.dev.yml build
```

### Code Style Configuration
- **Linting**: Uses Ruff (configured in pyproject.toml) for linting and formatting
- **Line Length**: 88 characters (Black-compatible)
- **Python Version**: 3.11
- **Import Sorting**: Handled by Ruff
- **Type Checking**: Pyright configuration in pyproject.toml (strict mode with specific overrides)

### Project Structure Patterns
- **Production Scripts**: Scripts in `real/` directory are production-ready versions
- **Test Organization**: Tests mirror the project structure with fixtures in `test/fixtures/`
- **Configuration Management**: Centralized config in `utils.py` with environment-specific settings
- **Job Registration**: All scheduled jobs are registered in `scheduler_jobs.py:make_schedule()`

### Database Integration
- Uses `psycopg2` for PostgreSQL connections
- Connection pooling and error handling in `utils.py`
- Database operations follow transaction patterns with proper rollback handling
- TimescaleDB specific operations for time series data

### PostgreSQL MCP Server Usage

This repository includes a PostgreSQL MCP server for database inspection and queries.

**✅ Available Tools:**

- `mcp__mcp-postgres__list_tables(database="rds")` - List all tables
- `mcp__mcp-postgres__describe_table(table_name="users", database="rds")` - Get table schema
- `mcp__mcp-postgres__query_data(sql="SELECT * FROM users LIMIT 5", database="rds")` - Execute queries

**✅ Two Databases Available:**

- `database="rds"` (default) - Main app database with 300+ tables (users, work_orders, inventory)
- `database="timescale"` - Time-series database with IoT sensor data (use LIMIT on queries!)

**✅ Connection Details:**

- Uses `mcp_user` account with read access to both databases
- RDS: AWS hosted, SSL required
- TimescaleDB: EC2 hosted on port 7815, no SSL
- Container: `mcp-postgres` running on port 5005

**Usage Examples:**

```python
# Query AWS RDS database (default)
mcp__mcp-postgres__query_data(sql="SELECT COUNT(*) FROM users WHERE is_active = true")

# Query TimescaleDB database (always use LIMIT!)
mcp__mcp-postgres__query_data(sql="SELECT timestamp_utc, gateway, spm FROM time_series ORDER BY timestamp_utc DESC LIMIT 5", database="timescale")

# List tables
mcp__mcp-postgres__list_tables(database="rds")  # 300+ app tables
mcp__mcp-postgres__list_tables(database="timescale")  # Time-series data

# Describe table structure
mcp__mcp-postgres__describe_table(table_name="users", database="rds")
mcp__mcp-postgres__describe_table(table_name="time_series", database="timescale")
```

## Development Container Considerations
- We're inside a @Dockerfile.dev docker development container in wsl2, so don't optimize anything for windows 11 since we're working in debian inside a docker container
- Virtual environment is automatically created at `/workspace/.venv`
- All Python dependencies managed through UV (fast Python package manager)
- Node.js dependencies managed through bun

## Principles

- Write high quality, general purpose solutions. Implement solutions that work correctly for all valid inputs, not just the test cases. Do not hard-code values or create solutions that only work for specific test inputs. Instead, implement the actual logic that solves the problems generally
- Focus on understanding the problem requirements and implementing the correct algorithm. Tests are there to verify correctness, not to define the solution. Provide a principled implementation that follows best practices and software design principles
- If the task is unreasonable or infeasible, or if any of the tests are incorrect, please tell me. The solution should be robust, maintainable, and extendable

## Documentation Organization

All project documentation can be found in the `/project/docs` folder, organized by category:

- **`/docs/architecture/`** - System architecture, setup guides, READMEs, and core documentation
- **`/docs/implementation/`** - Migration plans, feature implementation guides, and deployment procedures
- **`/docs/analysis/`** - Analysis reports, system studies, and research documentation
- **`/docs/procedures/`** - Testing guides, operational procedures, and user guides

Files are named with `YYYY-MM-DD-HHMM` prefix based on creation date and time for chronological organization.

**Getting today's timestamp for file naming:**

```bash
date +"%Y-%m-%d-%H%M"
```

## GitHub Issue Management

**CRITICAL:** All issues for this project should be managed through the **IJACK Roadmap project board** (Project #12) at https://github.com/orgs/ijack-technologies/projects/12/views/, regardless of which repository they originate from.

**DEFAULT PROJECT:** IJACK Roadmap (Project #12) is the default project board for ALL issues - features, bugs, epics, user stories, service requests, and ideas.

### Issue Location Guidelines

- **Create issues** in the relevant repository (`rcom`, `ijack-technologies/planning`, etc.)
- **ALWAYS add ALL issues to the IJACK Roadmap project board (Project #12)** for centralized tracking
- The IJACK Roadmap project board is the single source of truth for all project management across repositories

### Creating Issues with Templates

**ALWAYS use the appropriate issue template** when creating GitHub issues:

- **🐛 Bug Reports**: Use the bug report template for defects and issues
- **📍 Features**: Use the feature template for new functionality
- **💡 Ideas**: Use the idea template for suggestions and improvements
- **🙋 Service Requests**: Use the service request template for support needs
- **🏌️ User Stories**: Use the user story template for user-focused requirements
- **⛳ Epics**: Use the epic template for large initiatives

Templates ensure consistent issue formatting and capture all necessary information. Templates are located in `.github/ISSUE_TEMPLATE/`.

### Adding Issues to Project Board

**MANDATORY:** ALL issues must be added to the IJACK Roadmap project board (Project #12):

```bash
# ALWAYS add every issue to the IJACK Roadmap project board (Project #12)
gh project item-add 12 --owner ijack-technologies --url <ISSUE_URL>

# Standard workflow for ALL issues:
# 1. Create issue with appropriate template
gh issue create --title "Issue Title" --assignee "username" --body "..."
# 2. IMMEDIATELY add to IJACK Roadmap project (REQUIRED)
gh project item-add 12 --owner ijack-technologies --url https://github.com/ijack-technologies/rcom/issues/XXX
# 3. Move to appropriate status via project board UI

# Examples:
gh project item-add 12 --owner ijack-technologies --url https://github.com/ijack-technologies/rcom/issues/976
gh project item-add 12 --owner ijack-technologies --url https://github.com/ijack-technologies/planning/issues/45
```

**Why this approach?**

- Technical issues stay close to the codebase where developers work
- Centralized project management across all repositories
- Better sprint planning and feature tracking
- Improved visibility for stakeholders and project managers

## Quick Reference

### Always Do

- Think ultra hard and make detailed plans
- Keep code DRY and SOLID while following the KISS principle
- Run linters before committing
- Use bun instead of npm
- Activate virtual environments before running code
- Always use IDE diagnostics to validate code after implementation
- Use Context7 for all package documentation lookups
- Add all issues to the ijack-technologies project board for tracking
