-- Script to remove the MCP read-only user
-- Run this if you need to clean up or recreate the user

-- Revoke all privileges
REVOKE ALL PRIVILEGES ON DATABASE ijack FROM mcp_readonly;
REVOKE ALL PRIVILEGES ON SCHEMA public FROM mcp_readonly;
REVOKE ALL PRIVILEGES ON ALL TABLES IN SCHEMA public FROM mcp_readonly;
REVOKE ALL PRIVILEGES ON ALL SEQUENCES IN SCHEMA public FROM mcp_readonly;

-- Drop the user
DROP USER IF EXISTS mcp_readonly;

-- Verify the user was dropped
SELECT COUNT(*) as user_exists
FROM pg_user 
WHERE usename = 'mcp_readonly';