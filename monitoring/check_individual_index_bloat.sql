WITH RECURSIVE index_details AS (
  SELECT
    'name_of_index_idx'::text idx
),
size_in_pages_index AS (
  SELECT
    (pg_relation_size(idx::regclass) / (2^13))::int4 size_pages
  FROM
    index_details
),
page_stats AS (
  SELECT
    index_details.*,
    stats.*
  FROM
    index_details,
    size_in_pages_index,
    lateral (SELECT i FROM generate_series(1, size_pages - 1) i) series,
    lateral (SELECT * FROM bt_page_stats(idx, i)) stats
),
meta_stats AS (
  SELECT
    *
  FROM
    index_details s,
    lateral (SELECT * FROM bt_metap(s.idx)) meta
),
pages_raw AS (
  SELECT
    *
  FROM
    page_stats
  ORDER BY
    btpo DESC
),
pages_walk(item, blk, level) AS (
  SELECT
    1,
    blkno,
    btpo
  FROM
    pages_raw
  WHERE
    btpo_prev = 0
    AND btpo = (SELECT level FROM meta_stats)
  UNION
  SELECT
    CASE WHEN level = btpo THEN w.item + 1 ELSE 1 END,
    blkno,
    btpo
  FROM
    pages_raw i,
    pages_walk w
  WHERE
    i.btpo_prev = w.blk OR (btpo_prev = 0 AND btpo = w.level - 1)
)
SELECT
  idx,
  btpo_prev,
  btpo_next,
  btpo AS level,
  item AS l_item,
  blkno,
  btpo_flags,
  TYPE,
  live_items,
  dead_items,
  avg_item_size,
  page_size,
  free_size,

  distinct_real_item_keys,

  CASE WHEN btpo_next != 0 THEN first_item END AS highkey,

  distinct_block_pointers

FROM
  pages_walk w,
  pages_raw i,
  lateral (
    SELECT
    count(distinct (case when btpo_next = 0 or itemoffset > 1 then (data collate "C") end)) as distinct_real_item_keys,
    count(distinct (case when btpo_next = 0 or itemoffset > 1 then (ctid::text::point)[0]::bigint end)) as distinct_block_pointers,
    (array_agg(data))[1] as first_item
    FROM bt_page_items(idx, blkno)
  ) items
  where w.blk = i.blkno
ORDER BY btpo DESC, item;
