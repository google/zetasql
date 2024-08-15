FROM
  partsupp,
  part
|> WHERE
     p_partkey = ps_partkey
     AND p_brand <> 'Brand#13'
     AND p_type NOT LIKE 'LARGE BURNISHED%'
     AND p_size IN (39, 47, 37, 5, 20, 11, 25, 27)
     AND ps_suppkey NOT IN (
       SELECT s_suppkey
       FROM supplier
       WHERE s_comment LIKE '%Customer%Complaints%'
     )
|> AGGREGATE
     COUNT(DISTINCT ps_suppkey) AS supplier_cnt
   GROUP BY
     p_brand,
     p_type,
     p_size
|> ORDER BY
     supplier_cnt DESC,
     p_brand,
     p_type,
     p_size;
