SELECT datname FROM pg_database WHERE datname NOT IN ('postgres','template0','template1','rdsadmin') AND datname NOT ILIKE '%delete_me%';
