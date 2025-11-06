-- Create read-only user for MCP server
-- Run this script as a master/admin user
-- Create the user with a secure password
-- IMPORTANT: Change this password before running in production!
CREATE USER mcp_readonly WITH PASSWORD '__replace__';

-- Grant connect privilege on the database
-- Replace 'ijack' with your actual database name if different
GRANT CONNECT ON DATABASE ijack TO mcp_readonly;

-- Grant usage on all schemas
GRANT USAGE ON SCHEMA public TO mcp_readonly;

GRANT USAGE ON SCHEMA information_schema TO mcp_readonly;

GRANT USAGE ON SCHEMA pg_catalog TO mcp_readonly;

-- Grant SELECT on all tables in public schema
GRANT
SELECT
    ON ALL TABLES IN SCHEMA public TO mcp_readonly;

-- Grant SELECT on all sequences in public schema (for reading sequence values)
GRANT
SELECT
    ON ALL SEQUENCES IN SCHEMA public TO mcp_readonly;

-- Set default privileges for future tables
ALTER DEFAULT PRIVILEGES IN SCHEMA public GRANT
SELECT
    ON TABLES TO mcp_readonly;

ALTER DEFAULT PRIVILEGES IN SCHEMA public GRANT
SELECT
    ON SEQUENCES TO mcp_readonly;

-- If you have other schemas, add them here
-- Example for additional schemas:
-- GRANT USAGE ON SCHEMA your_schema TO mcp_readonly;
-- GRANT SELECT ON ALL TABLES IN SCHEMA your_schema TO mcp_readonly;
-- GRANT SELECT ON ALL SEQUENCES IN SCHEMA your_schema TO mcp_readonly;
-- ALTER DEFAULT PRIVILEGES IN SCHEMA your_schema GRANT SELECT ON TABLES TO mcp_readonly;
-- ALTER DEFAULT PRIVILEGES IN SCHEMA your_schema GRANT SELECT ON SEQUENCES TO mcp_readonly;
-- Verify the user was created
SELECT
    usename,
    usecreatedb,
    usesuper,
    userepl,
    usebypassrls
FROM
    pg_user
WHERE
    usename = 'mcp_readonly';

-- Show granted permissions
SELECT
    grantee,
    table_schema,
    table_name,
    privilege_type
FROM
    information_schema.table_privileges
WHERE
    grantee = 'mcp_readonly'
LIMIT
    10;