SELECT left(query,50) as query, calls, round((total_time/calls)::numeric,2) as avg_time_ms, round((rows/calls)::numeric,2) as avg_rows,
temp_blks_read/calls as avg_tmp_read, temp_blks_written/calls as avg_temp_written
FROM pg_stat_statements
WHERE calls != 0
ORDER BY total_time DESC LIMIT 10;
