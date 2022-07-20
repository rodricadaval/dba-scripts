select pg_class.relname,
       pg_locks.mode,
       pg_locks.pid
from pg_class,
     pg_locks
where pg_class.oid = pg_locks.relation
and pg_class.relnamespace >= 2200
;
