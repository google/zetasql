FROM
  customer,
  orders,
  lineitem
|> WHERE
     c_mktsegment = 'FURNITURE'
     AND c_custkey = o_custkey
     AND l_orderkey = o_orderkey
     AND o_orderdate < date '1995-03-29'
     AND l_shipdate > date '1995-03-29'
|> AGGREGATE
     sum(l_extendedprice * (1 - l_discount)) AS revenue,
   GROUP BY
     l_orderkey,
     o_orderdate,
     o_shippriority
|> SELECT
     l_orderkey,
     revenue,
     o_orderdate,
     o_shippriority
|> ORDER BY
     revenue DESC,
     o_orderdate
|> LIMIT 10;
