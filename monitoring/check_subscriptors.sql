SELECT
    slot_name,
    slot_type,
    database,
    pg_size_pretty(pg_wal_lsn_diff(pg_current_wal_lsn(), restart_lsn)) AS replicationSlotLag,
    active
FROM
    pg_replication_slots
WHERE
    plugin = 'pgoutput'
ORDER BY
    pg_wal_lsn_diff(pg_current_wal_lsn(), restart_lsn) DESC;
