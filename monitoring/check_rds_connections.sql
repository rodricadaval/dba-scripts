SELECT count(*) as total, count(1) FILTER (WHERE state = 'active') as active, count(1) FILTER (WHERE state = 'idle_in_transaction') as idle_in_tran, count(1) FILTER (WHERE state = 'idle in transaction (aborted)') as idle_in_tran_abort, count(1) FILTER (WHERE state ='idle') as idle, (SELECT setting FROM pg_settings WHERE name = 'max_connections') FROM pg_stat_activity;