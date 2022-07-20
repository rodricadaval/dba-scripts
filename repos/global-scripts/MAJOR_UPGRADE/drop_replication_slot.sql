\set var_slot_name replace(split_part('''' :values '''', '''','''', 1),''''-'''',''''_'''')

SELECT pg_drop_replication_slot(:var_slot_name);
