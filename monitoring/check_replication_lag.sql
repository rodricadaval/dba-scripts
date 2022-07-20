SELECT pid, usename, application_name, client_addr, state, sync_state,
       pg_xlog_location_diff(sent_location, write_location) as write_lag,
       pg_xlog_location_diff(sent_location, flush_location) as flush_lag,
       pg_xlog_location_diff(sent_location, replay_location) as replay_lag,
       pg_xlog_location_diff(sent_location, replay_location) as total_lag
  FROM pg_stat_replication;
