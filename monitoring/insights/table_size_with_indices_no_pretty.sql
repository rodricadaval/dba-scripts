SELECT c.relnamespace::regnamespace::text, c.relname AS table,
  pg_total_relation_size(c.oid) AS size
FROM pg_class c
LEFT JOIN pg_namespace n ON (n.oid = c.relnamespace)
WHERE n.nspname NOT IN ('pg_catalog', 'information_schema')
  AND n.nspname !~ '^pg_toast'
  AND c.relkind='r'
ORDER BY pg_total_relation_size(c.oid) DESC;
