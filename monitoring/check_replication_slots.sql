SELECT
    slot_name,
    slot_type,
    database,
    plugin,
    pg_size_pretty(pg_xlog_location_diff(pg_current_xlog_location(), restart_lsn)) AS replicationSlotLag,
    active,
    pg_size_pretty(pg_database_size(database)) as db_size               
-----------------------------------

FROM
    pg_replication_slots;
