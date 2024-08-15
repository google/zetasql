# This is much shorter because it can apply AGGREGATE twice for
# two-level aggregation without needing a subquery.
# This uses pipe JOIN. Using JOIN inside FROM would also work.
FROM customer
|> LEFT OUTER JOIN orders ON c_custkey = o_custkey AND o_comment NOT LIKE '%unusual%packages%'
|> AGGREGATE COUNT(o_orderkey) c_count
   GROUP BY c_custkey
|> AGGREGATE COUNT(*) AS custdist
   GROUP BY c_count
|> ORDER BY
     custdist DESC,
     c_count DESC;
