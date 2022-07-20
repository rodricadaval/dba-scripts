SELECT
       gap_start, gap_end, diff FROM (
              SELECT :idval + 1 AS gap_start,
              next_nr - 1 AS gap_end,
	      (next_nr - 1 - :idval + 1) as diff
              FROM (
                     SELECT :idval, lead(:idval) OVER (ORDER BY :idval) AS next_nr
                     FROM :tablname
              ) as nr
              WHERE :idval + 1 <> nr.next_nr
       ) AS g
UNION ALL (
       SELECT
              0 AS gap_start,
              :idval AS gap_end,
	      :idval - 0 as diff
       FROM
              :tablname
       ORDER BY
              :idval
       ASC LIMIT 1
)
ORDER BY
       --gap_start
      diff DESC
LIMIT 
	10;
