\set var_instance split_part(quote_ident(:'values'),''',''',1)
\set var_country split_part(quote_ident(:'values'), ''',''', 2)

SELECT
	endp.instance_id, inv.instance_data->>'EngineVersion' as  pg_version 
FROM 
	rds_postgresql_endpoints endp 
INNER JOIN 
	rds_instances_inventory inv ON inv.instance_name = endp.instance_id 
WHERE 
	lower(endp.instance_name) =  replace(lower(:var_instance),'"','')
AND
	upper(endp.country) = replace(upper(:var_country),'"','')
AND
	inv.updated_at > now() - interval '7 days'
ORDER BY 
	inv.updated_at DESC 
LIMIT 1;
