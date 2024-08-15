SELECT
  c_count,
  COUNT(*) AS custdist
FROM
  (
    SELECT
      c_custkey,
      COUNT(o_orderkey) c_count
    FROM
      customer
    LEFT OUTER JOIN orders
      ON
        c_custkey = o_custkey
        AND o_comment NOT LIKE '%unusual%packages%'
    GROUP BY
      c_custkey
  ) AS c_orders
GROUP BY
  c_count
ORDER BY
  custdist DESC,
  c_count DESC;
