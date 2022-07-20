SELECT
    slot_name,
    slot_type,
    database,
    pg_size_pretty(pg_wal_lsn_diff(pg_current_wal_lsn(), restart_lsn)) AS replicationSlotLag,
    active,
    pg_size_pretty(pg_database_size(database)) as db_size
FROM
    pg_replication_slots
ORDER BY
    pg_wal_lsn_diff(pg_current_wal_lsn(), restart_lsn) DESC;
