SELECT c.relname AS name,
  pg_size_pretty(pg_total_relation_size(reltoastrelid)) as toast_size
FROM pg_class c
LEFT JOIN pg_namespace n ON n.oid = c.relnamespace
WHERE n.nspname NOT IN ('pg_catalog', 'information_schema')
  AND n.nspname !~ '^pg_toast'
  AND c.relkind = 'r'
ORDER BY pg_total_relation_size(reltoastrelid) DESC NULLS LAST;
