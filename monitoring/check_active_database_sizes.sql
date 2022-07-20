select t1.datname AS db_name,
       pg_catalog.pg_get_userbyid(t1.datdba),  
       pg_size_pretty(pg_database_size(t1.datname)) as db_size
from pg_database t1
where datallowconn
and datname NOT SIMILAR TO 'rdsadmin|template%|postgres|%delete_me'
order by pg_database_size(t1.datname) desc;
