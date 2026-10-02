-- Emails must be unique only among live users. The original table-level UNIQUE
-- also counted soft-deleted rows, so a deleted user's email could never be reused.
ALTER TABLE users DROP CONSTRAINT IF EXISTS users_email_key;

CREATE UNIQUE INDEX IF NOT EXISTS uq_users_email_active
    ON users (email) WHERE deleted_at IS NULL;
