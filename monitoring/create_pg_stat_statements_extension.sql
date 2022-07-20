DO $$DECLARE exists boolean;
BEGIN
	exists := false;
	SELECT exists(SELECT 1 FROM pg_extension WHERE extname = 'pg_stat_statements') INTO exists;
	IF exists THEN
		RAISE NOTICE 'EXISTE EN DB: %', current_database();
	ELSE
		RAISE NOTICE 'NO existe en db: %', current_database();
		RAISE NOTICE 'Creando extension...';
		CREATE EXTENSION pg_stat_statements;
	END IF;
END$$;
