SELECT usename, pg_terminate_backend(pid) FROM pg_stat_activity stat  WHERE stat.usename NOT SIMILAR TO 'postgres|dba_%|rds%|dms%';
