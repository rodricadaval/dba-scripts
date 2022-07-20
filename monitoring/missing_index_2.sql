SELECT
  x1.table_in_trouble,
  pg_relation_size(x1.table_in_trouble) AS sz_n_byts,
  x1.seq_scan,
  x1.idx_scan,
  CASE
  WHEN pg_relation_size(x1.table_in_trouble) > 500000000
    THEN 'Exceeds 500 megs, too large to count in a view. For a count, count individually'::text
  ELSE count(x1.table_in_trouble)::text
  END                                   AS tbl_rec_count,
  x1.priority
FROM
  (
    SELECT
      (schemaname::text || '.'::text) || relname::text AS table_in_trouble,
      seq_scan,
      idx_scan,
      CASE
      WHEN (seq_scan - idx_scan) < 500
        THEN 'Minor Problem'::text
      WHEN (seq_scan - idx_scan) >= 500 AND (seq_scan - idx_scan) < 2500
        THEN 'Major Problem'::text
      WHEN (seq_scan - idx_scan) >= 2500
        THEN 'Extreme Problem'::text
      ELSE NULL::text
      END AS priority
    FROM
      pg_stat_all_tables
    WHERE
      seq_scan > idx_scan
      AND schemaname != 'pg_catalog'::name
    AND seq_scan > 100) x1
GROUP BY
  x1.table_in_trouble,
  x1.seq_scan,
  x1.idx_scan,
  x1.priority
ORDER BY
  x1.priority DESC,
  x1.seq_scan;
