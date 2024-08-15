# This uses a WITH clause, with the subquery in pipe syntax.
# It aliases the grouping column like the original query, although
# this is unnecessary.
WITH
  revenue AS (
    FROM lineitem
    |> WHERE
         l_shipdate >= date '1997-05-01'
         AND l_shipdate < date_add(date '1997-05-01', INTERVAL 3 month)
    |> AGGREGATE
         sum(l_extendedprice * (1 - l_discount)) AS total_revenue
       GROUP BY l_suppkey AS supplier_no
  )
FROM
  supplier,
  revenue
|> WHERE
     s_suppkey = supplier_no
     AND total_revenue = (
       SELECT max(total_revenue)
       FROM revenue
     )
|> SELECT
     s_suppkey,
     s_name,
     s_address,
     s_phone,
     total_revenue
|> ORDER BY s_suppkey;
