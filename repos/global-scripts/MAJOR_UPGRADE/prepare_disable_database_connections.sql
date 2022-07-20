SELECT 'ALTER DATABASE "' || datname || '" WITH ALLOW_CONNECTIONS FALSE;' FROM pg_database WHERE datname NOT IN ('postgres','template0','template1','rdsadmin');
