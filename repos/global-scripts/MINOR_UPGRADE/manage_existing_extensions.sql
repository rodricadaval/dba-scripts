SELECT current_database(), 'CREATE EXTENSION IF NOT EXISTS "' || extname || '";', 'DROP EXTENSION IF EXISTS "' || extname || '";' FROM pg_extension WHERE extname NOT IN ('plpgsql');
