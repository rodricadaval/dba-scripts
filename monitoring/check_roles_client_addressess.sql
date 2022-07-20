 SELECT datname, usename, client_addr, SUM(CASE WHEN state='idle' THEN 1 ELSE 0 END) as idle, SUM(CASE WHEN state='idle_in_transaction' THEN 1 ELSE 0 END) as idle_in_tran, SUM(CASE WHEN state='active' THEN 1 ELSE 0 END) as active, count(*) as total, (SELECT count(distinct(client_addr)) FROM pg_stat_activity ac  WHERE ac.usename = st.usename) as instances, (SELECT rolconnlimit FROM pg_roles WHERE rolname = st.usename) as max_conn FROM pg_stat_activity st group by 1,2,3 order by 7 desc;
