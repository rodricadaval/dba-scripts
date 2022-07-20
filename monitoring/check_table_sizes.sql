select t1.schemaname, t1.relname AS table_name,
       pg_size_pretty(pg_table_size((t1.schemaname || '.' || t1.relname)::text)) as table_size,
       pg_size_pretty(pg_total_relation_size((t1.schemaname || '.' || t1.relname)::text)) as total_table_size,
       pg_size_pretty(pg_total_relation_size((t1.schemaname || '.' || t1.relname)::text)-pg_table_size((t1.schemaname || '.' || t1.relname)::text)) as index_table_size 
from pg_stat_user_tables t1
order by pg_table_size(t1.schemaname || '.' || t1.relname) desc
LIMIT 30;
