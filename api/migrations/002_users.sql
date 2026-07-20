-- Migration: Add users table for admin webapp

CREATE TABLE IF NOT EXISTS core.user (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    email VARCHAR(255) UNIQUE NOT NULL,
    hashed_password TEXT NOT NULL,
    name VARCHAR(255) NOT NULL,
    role VARCHAR(50) DEFAULT 'viewer',
    is_active BOOLEAN DEFAULT TRUE,
    created_at TIMESTAMPTZ DEFAULT NOW(),
    last_login TIMESTAMPTZ
);

CREATE INDEX IF NOT EXISTS ix_user_email ON core.user(email);
CREATE INDEX IF NOT EXISTS ix_user_role ON core.user(role);

COMMENT ON TABLE core.user IS 'Users of the administration webapp';
COMMENT ON COLUMN core.user.role IS 'Roles: admin, user, viewer, experimenter';
