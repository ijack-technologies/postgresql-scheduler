import os
import asyncpg
from mcp.server.fastmcp import FastMCP
from dotenv import load_dotenv
from pathlib import Path

# Load .env file from the mcp-postgres-server directory with override
env_path = Path(__file__).parent.parent / ".env"
load_dotenv(env_path, override=True)

mcp = FastMCP("PostgreSQL Explorer")

# Multi-database configuration
DATABASES = {
    "rds": {
        "host": os.getenv("DB_HOST", ""),
        "port": int(os.getenv("DB_PORT", "5432")),
        "user": os.getenv("DB_USER", "mcp_user"),
        "password": os.getenv("DB_PASSWORD", ""),
        "database": os.getenv("DB_NAME", "ijack"),
        "ssl": os.getenv("DB_SSL", "disable"),
    },
    "timescale": {
        "host": os.getenv("DB_HOST_TS", ""),
        "port": int(os.getenv("DB_PORT_TS", "5432")),
        "user": os.getenv("DB_USER_TS", "mcp_user"),
        "password": os.getenv("DB_PASSWORD_TS", ""),
        "database": os.getenv("DB_NAME_TS", "ijack"),
        "ssl": os.getenv("DB_SSL_TS", "disable"),
    },
}

# Print configuration for debugging
print("Database configurations:")
for db_name, config in DATABASES.items():
    print(f"\n{db_name}:")
    print(f"  host: {config['host']}")
    print(f"  port: {config['port']}")
    print(f"  user: {config['user']}")
    print(f"  database: {config['database']}")
    print(f"  ssl: {config['ssl']}")
    print(f"  has_password: {bool(config['password'])}")

# Connection pools for each database
pools = {}


async def get_connection(db_name: str = "rds"):
    """Get connection pool for specified database"""
    global pools

    if db_name not in DATABASES:
        raise ValueError(
            f"Unknown database: {db_name}. Available databases: {list(DATABASES.keys())}"
        )

    if db_name not in pools or pools[db_name] is None:
        print(f"\nCreating new pool for database: {db_name}")
        print(
            f"Connection config: {DATABASES[db_name]['host']}:{DATABASES[db_name]['port']}"
        )
        try:
            pools[db_name] = await asyncpg.create_pool(
                **DATABASES[db_name], min_size=1, max_size=10, command_timeout=60
            )
            print(f"✅ Pool created successfully for {db_name}")
        except Exception as e:
            print(f"❌ Failed to create pool for {db_name}: {e}")
            raise

    return pools[db_name]


async def get_schema_for_db(pool, db_name: str) -> str:
    """Get schema for a specific database"""
    async with pool.acquire() as conn:
        # Get all tables with their schemas
        tables = await conn.fetch("""
            SELECT 
                table_schema,
                table_name,
                table_type
            FROM information_schema.tables
            WHERE table_schema NOT IN ('pg_catalog', 'information_schema')
            ORDER BY table_schema, table_name
        """)

        schema_info = []
        for table in tables:
            schema = table["table_schema"]
            table_name = table["table_name"]
            table_type = table["table_type"]

            # Get columns for this table
            columns = await conn.fetch(
                """
                SELECT 
                    column_name,
                    data_type,
                    is_nullable,
                    column_default
                FROM information_schema.columns
                WHERE table_schema = $1 AND table_name = $2
                ORDER BY ordinal_position
            """,
                schema,
                table_name,
            )

            # Build CREATE TABLE statement
            col_defs = []
            for col in columns:
                col_def = f"{col['column_name']} {col['data_type']}"
                if col["is_nullable"] == "NO":
                    col_def += " NOT NULL"
                if col["column_default"]:
                    col_def += f" DEFAULT {col['column_default']}"
                col_defs.append(col_def)

            create_stmt = (
                f"CREATE {table_type} {schema}.{table_name} (\n  "
                + ",\n  ".join(col_defs)
                + "\n);"
            )
            schema_info.append(create_stmt)

        return "\n\n".join(schema_info)


@mcp.resource("schema://databases")
async def get_all_schemas() -> str:
    """Provide schemas from all configured databases"""
    result = []

    for db_name in DATABASES:
        try:
            pool = await get_connection(db_name)
            schema = await get_schema_for_db(pool, db_name)
            result.append(f"=== DATABASE: {db_name.upper()} ===\n{schema}")
        except Exception as e:
            result.append(f"=== DATABASE: {db_name.upper()} ===\nError: {str(e)}")

    return "\n\n" + "\n\n".join(result)


@mcp.resource("schema://main")
async def get_schema() -> str:
    """Provide the database schema as a resource (backward compatibility - uses RDS)"""
    pool = await get_connection("rds")
    return await get_schema_for_db(pool, "rds")


@mcp.tool()
async def query_data(sql: str, database: str = "rds") -> str:
    """
    Execute SQL queries safely

    Args:
        sql: SQL query to execute
        database: Database to query ("rds" or "timescale", default: "rds")
    """
    print(f"\n[query_data] Database: {database}, SQL: {sql[:50]}...")
    try:
        pool = await get_connection(database)
        async with pool.acquire() as conn:
            # Check which database we're actually connected to
            current_db = await conn.fetchval("SELECT current_database()")
            print(f"[query_data] Connected to database: {current_db}")

            # Check if it's a SELECT query
            if sql.strip().upper().startswith(("SELECT", "WITH")):
                rows = await conn.fetch(sql)
                if not rows:
                    return f"No results found in {database} database (connected to: {current_db})"

                # Format results as a table
                if rows:
                    headers = list(rows[0].keys())
                    result = [" | ".join(headers)]
                    result.append("-" * len(result[0]))

                    for row in rows:
                        result.append(" | ".join(str(row[col]) for col in headers))

                    return (
                        f"Database: {database} (connected to: {current_db})\n"
                        + "\n".join(result)
                    )
            else:
                # For non-SELECT queries
                result = await conn.execute(sql)
                return f"Query executed successfully in {database}: {result}"
    except Exception as e:
        print(f"[query_data] Error: {e}")
        return f"Error in {database} database: {str(e)}"


@mcp.tool()
async def list_tables(database: str = "rds") -> str:
    """
    List all tables in the database

    Args:
        database: Database to query ("rds" or "timescale", default: "rds")
    """
    print(f"\n[list_tables] Database: {database}")
    try:
        pool = await get_connection(database)
        async with pool.acquire() as conn:
            # Check which database we're actually connected to
            current_db = await conn.fetchval("SELECT current_database()")
            print(f"[list_tables] Connected to database: {current_db}")

            rows = await conn.fetch("""
                SELECT 
                    table_schema,
                    table_name
                FROM information_schema.tables
                WHERE table_schema NOT IN ('pg_catalog', 'information_schema')
                ORDER BY table_schema, table_name
            """)

            if not rows:
                return f"No tables found in {database} database"

            result = [f"Database: {database} (connected to: {current_db})"]
            result.append("Schema | Table")
            result.append("-" * 30)
            for row in rows:
                result.append(f"{row['table_schema']} | {row['table_name']}")

            return "\n".join(result)
    except Exception as e:
        print(f"[list_tables] Error: {e}")
        return f"Error in {database} database: {str(e)}"


@mcp.tool()
async def describe_table(
    table_name: str, schema: str = "public", database: str = "rds"
) -> str:
    """
    Get detailed information about a table

    Args:
        table_name: Name of the table
        schema: Schema name (default: "public")
        database: Database to query ("rds" or "timescale", default: "rds")
    """
    print(f"\n[describe_table] Database: {database}, Table: {schema}.{table_name}")
    try:
        pool = await get_connection(database)
        async with pool.acquire() as conn:
            # Check which database we're actually connected to
            current_db = await conn.fetchval("SELECT current_database()")
            print(f"[describe_table] Connected to database: {current_db}")

            columns = await conn.fetch(
                """
                SELECT 
                    column_name,
                    data_type,
                    character_maximum_length,
                    is_nullable,
                    column_default
                FROM information_schema.columns
                WHERE table_schema = $1 AND table_name = $2
                ORDER BY ordinal_position
            """,
                schema,
                table_name,
            )

            if not columns:
                return f"Table {schema}.{table_name} not found in {database} database (connected to: {current_db})"

            result = [f"Database: {database} (connected to: {current_db})"]
            result.append(f"Table: {schema}.{table_name}")
            result.append("=" * 50)
            result.append("Column | Type | Nullable | Default")
            result.append("-" * 50)

            for col in columns:
                data_type = col["data_type"]
                if col["character_maximum_length"]:
                    data_type += f"({col['character_maximum_length']})"

                nullable = "YES" if col["is_nullable"] == "YES" else "NO"
                default = col["column_default"] or "NULL"

                result.append(
                    f"{col['column_name']} | {data_type} | {nullable} | {default}"
                )

            return "\n".join(result)
    except Exception as e:
        print(f"[describe_table] Error: {e}")
        return f"Error in {database} database: {str(e)}"


# Cleanup function
async def cleanup():
    global pools
    for db_name, pool in pools.items():
        if pool:
            await pool.close()
    pools.clear()


if __name__ == "__main__":
    import asyncio

    # Configure the server settings
    mcp.settings.host = os.getenv("MCP_SERVER_HOST", "0.0.0.0")
    mcp.settings.port = int(os.getenv("MCP_SERVER_PORT", "5005"))
    mcp.settings.streamable_http_path = "/mcp"

    # Run the FastMCP server
    asyncio.run(mcp.run(transport="streamable-http"))
