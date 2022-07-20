SELECT (SELECT rolname FROM pg_roles WHERE oid = userid) as rolname, (SELECT datname FROM pg_stat_database WHERE datid = dbid) as datname, round(min_time::numeric,2) as min_time, round(mean_time::numeric,2) as mean_time, left(query,82) as query, calls, round(total_time::numeric,2) as total_time, rows,round(100.0 * shared_blks_hit /
nullif(shared_blks_hit + shared_blks_read, 0),2) AS hit_percent
FROM pg_stat_statements ORDER BY total_time DESC LIMIT 10;
