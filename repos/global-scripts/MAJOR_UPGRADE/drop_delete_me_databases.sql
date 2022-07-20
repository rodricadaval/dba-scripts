SELECT 'DROP DATABASE "' ||  datname || '";' FROM pg_database WHERE datname ILIKE '%delete_me%';
