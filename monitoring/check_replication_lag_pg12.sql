SELECT pid, usename, application_name, client_addr, state, sync_state,
       pg_wal_lsn_diff(sent_lsn, write_lsn) as write_lag,
       pg_wal_lsn_diff(sent_lsn, flush_lsn) as flush_lag, 
       pg_wal_lsn_diff(sent_lsn, replay_lsn) as replay_lag, 
       pg_wal_lsn_diff(sent_lsn, replay_lsn) as total_lag 
  FROM pg_stat_replication;
