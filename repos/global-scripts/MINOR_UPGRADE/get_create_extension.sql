\set var_country replace(split_part('''' :values '''', '''','''', 1),''''-'''',''''_'''')
\set var_instance_name replace(split_part('''' :values '''', '''','''', 2),''''-'''',''''_'''')
\set var_dbname replace(split_part('''' :values '''', '''','''', 3),''''-'''',''''_'''')

SELECT 'CREATE EXTENSION IF NOT EXISTS "' || extname || '";'FROM pg_extension WHERE extname NOT IN ('plpgsql');
