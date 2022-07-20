select nspname,relname, round(100 * pg_relation_size(indexrelid) / pg_relation_size(indrelid)) / 100 as index_ratio,
           pg_size_pretty(pg_relation_size(indexrelid)) as index_size,
           pg_size_pretty(pg_relation_size(indrelid)) as table_size from pg_index I
           left join pg_class C on (c.oid = i.indexrelid)
           left join pg_namespace N on (n.oid = c.relnamespace) where nspname not in ('pg_catalog','information_schema','pg_toast') and c.relkind='i' and pg_relation_size(indrelid) >0
	   ORDER BY pg_relation_size(indexrelid) DESC
	   LIMIT 60;
