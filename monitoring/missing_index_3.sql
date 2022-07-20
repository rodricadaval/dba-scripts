SELECT relname                                 AS tablename
     , to_char(seq_scan  , '999,999,999,999')  AS total_seqscan
     , to_char(idx_scan  , '999,999,999,999')  AS total_indexscan
     , to_char(n_live_tup, '999,999,999,999')  AS tablerows
     , pg_size_pretty(pg_relation_size(relid)) AS tablesize
FROM   pg_stat_all_tables
WHERE  schemaname = 'public'
AND    50 * seq_scan > COALESCE(idx_scan, 0) -- more then 2%
AND    n_live_tup > 10000
AND    pg_relation_size(relid) > 1000000;
