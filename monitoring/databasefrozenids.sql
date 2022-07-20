SELECT datname, age(datfrozenxid) FROM pg_database ORDER BY 2 DESC LIMIT 20;
