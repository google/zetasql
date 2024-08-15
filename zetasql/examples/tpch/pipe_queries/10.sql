# This is written with SELECT after ORDER BY and LIMIT.
# The other order would work too since SELECT preserves order.
FROM
  customer,
  orders,
  lineitem,
  nation
|> WHERE
     c_custkey = o_custkey
     AND l_orderkey = o_orderkey
     AND o_orderdate >= date '1994-02-01'
     AND o_orderdate < date_add(date '1994-02-01', INTERVAL 3 month)
     AND l_returnflag = 'R'
     AND c_nationkey = n_nationkey
|> AGGREGATE
     sum(l_extendedprice * (1 - l_discount)) AS revenue,
   GROUP BY
     c_custkey,
     c_name,
     c_acctbal,
     c_phone,
     n_name,
     c_address,
     c_comment
|> ORDER BY revenue DESC
|> LIMIT 20
|> SELECT
     c_custkey,
     c_name,
     revenue,
     c_acctbal,
     n_name,
     c_address,
     c_phone,
     c_comment;
