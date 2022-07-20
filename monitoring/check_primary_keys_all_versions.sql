select tab.table_schema as schema,
       tab.table_name as table
from information_schema.tables tab
left join information_schema.table_constraints tco 
          on tab.table_schema = tco.table_schema
          and tab.table_name = tco.table_name 
          and tco.constraint_type = 'PRIMARY KEY'
where tab.table_type = 'BASE TABLE'
      and tab.table_schema not in ('pg_catalog', 'information_schema', 'pglogical', 'topology', 'tiger', 'logger')
      and tco.constraint_name is null
order by tab.table_schema,
         tab.table_name;
