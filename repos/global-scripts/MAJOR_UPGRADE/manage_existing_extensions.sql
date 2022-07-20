SELECT current_database(), 'CREATE EXTENSION IF NOT EXISTS "' || extname || '" CASCADE;', 'DROP EXTENSION IF EXISTS "' || extname || '" CASCADE;' FROM pg_extension WHERE extname NOT IN ('plpgsql');
