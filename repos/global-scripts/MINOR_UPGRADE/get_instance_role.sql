\set var_instance_name replace(split_part('''' :values '''', '''','''', 1),''''-'''',''''_'''')
\set var_country replace(split_part('''' :values '''', '''','''', 2),''''-'''',''''_'''')

SELECT 
	endp.instance_id, endp.instance_role
FROM 
	rds_postgresql_endpoints endp 
INNER JOIN 
	rds_instances_inventory inv ON inv.instance_name = endp.instance_id 
WHERE 
	lower(endp.instance_name) =  lower(:var_instance_name)
AND
	upper(endp.country) = upper(:var_country)
ORDER BY 
	inv.updated_at DESC 
LIMIT 1;
