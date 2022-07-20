SELECT now()-query_start as running, pid, datname, usename, client_addr, wait_event, wait_event_type, state, left(query,82) as query FROM pg_stat_activity WHERE state <> 'idle' ORDER BY 1 DESC;
