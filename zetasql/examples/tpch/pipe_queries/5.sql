FROM
  customer,
  orders,
  lineitem,
  supplier,
  nation,
  region
|> WHERE
     c_custkey = o_custkey
     AND l_orderkey = o_orderkey
     AND l_suppkey = s_suppkey
     AND c_nationkey = s_nationkey
     AND s_nationkey = n_nationkey
     AND n_regionkey = r_regionkey
     AND r_name = 'AFRICA'
     AND o_orderdate >= date '1994-01-01'
     AND o_orderdate < date_add(date '1994-01-01', INTERVAL 1 year)
|> AGGREGATE sum(l_extendedprice * (1 - l_discount)) AS revenue DESC
   GROUP BY n_name;
