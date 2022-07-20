SELECT 'ALTER DATABASE "' || datname || '" WITH ALLOW_CONNECTIONS TRUE;' FROM pg_database WHERE datname NOT IN ('postgres','template0','template1','rdsadmin');
