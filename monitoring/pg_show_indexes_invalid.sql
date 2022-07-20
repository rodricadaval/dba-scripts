SELECT n.nspname,
       i.indrelid::regclass AS tabname,
       c.relname AS idxname
FROM   pg_index i
JOIN   pg_class c ON c.oid = i.indexrelid
JOIN   pg_namespace n ON n.oid = c.relnamespace
WHERE  i.indisvalid = false
ORDER BY 1, 2, 3;
