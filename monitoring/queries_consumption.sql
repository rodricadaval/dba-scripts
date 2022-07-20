select pgu.usename,
    left(query,100), 
    round(total_time::numeric, 2) AS total_time, 
    calls, 
    rows,
    round(total_time::numeric / calls, 2) AS avg_time, 
    round((100 * total_time / sum(total_time::numeric) OVER ())::numeric, 2) AS percentage_cpu
FROM pg_stat_statements
inner join pg_catalog.pg_user pgu on (userid=pgu.usesysid)
ORDER BY total_time DESC LIMIT 20;
