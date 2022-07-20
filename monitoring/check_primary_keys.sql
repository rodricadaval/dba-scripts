SELECT n.nspname || '.' || c.relname as table 
FROM pg_catalog.pg_class c 
JOIN pg_namespace n ON 
( c.relnamespace = n.oid 
AND n.nspname NOT IN ('information_schema', 'pg_catalog', 'pglogical', 'tiger', 'topology') AND c.relkind='r' ) 
WHERE c.relhaspkey = false;
