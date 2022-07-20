SELECT 'ALTER ROLE "' || rolname || '" WITH CONNECTION LIMIT 0;', 'ALTER ROLE "' || rolname || '" WITH CONNECTION LIMIT ' || rolconnlimit || ';' FROM pg_database D LEFT JOIN pg_roles U ON D.datdba = U.oid WHERE datname NOT IN ('postgres','template0','template1','rdsadmin') AND U.rolname NOT IN ('postgres');
