SELECT
relname AS TableName
,seq_scan AS TotalSeqScan
,CASE WHEN round(((seq_scan+1)*100)/(idx_scan+1),2) > 0.10
THEN 'Missing Index Found'
ELSE 'Missing Index Not Found'
END AS MissingIndex
,pg_size_pretty(pg_relation_size(relname::regclass)) AS TableSize
,idx_scan AS TotalIndexScan
FROM pg_stat_all_tables
WHERE schemaname='public'
AND pg_relation_size(relname::regclass)>100000000
AND seq_scan > 500
ORDER BY 3 ASC, 2 DESC;
