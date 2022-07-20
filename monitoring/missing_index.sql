SELECT
  relname,
  seq_scan - idx_scan AS too_much_seq,
  CASE
    WHEN seq_scan - coalesce(idx_scan, 0) > 0 THEN 'Missing Index ?'
    ELSE 'OK'
  END,
  pg_relation_size(relid) AS rel_size, 
  seq_scan, idx_scan
FROM pg_stat_all_tables
WHERE schemaname = 'public' AND pg_relation_size(relname::regclass) > 80000 AND idx_scan > 0
ORDER BY too_much_seq DESC,3 ASC
LIMIT 15;
